/**
 * BBS Server (JavaScript/Node.js) - Parte 4
 * Novidades:
 *  - Eleição pelo algoritmo de Bully (menor rank ganha)
 *  - Sincronização do relógio via algoritmo de Berkeley (coordenador → servidor)
 *  - Heartbeat não retorna mais hora (apenas keepalive)
 *  - Porta REP dedicada para comunicação servidor↔servidor (S2S)
 *  - Coordenador eleito anunciado no tópico PUB/SUB 'servers'
 */

const { Reply, Publisher, Request, Subscriber } = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");
const Database = require("better-sqlite3");
const fs = require("fs");
const path = require("path");

// ---------------------------------------------------------------------------
// Configuração
// ---------------------------------------------------------------------------
const BROKER_HOST    = process.env.BROKER_HOST    || "broker";
const BROKER_PORT    = process.env.BROKER_PORT    || "5556";
const PROXY_HOST     = process.env.PROXY_HOST     || "proxy-pubsub";
const PROXY_PORT     = process.env.PROXY_PORT     || "5557";
const PROXY_SUB_PORT = process.env.PROXY_SUB_PORT || "5558";
const REF_HOST       = process.env.REF_HOST       || "reference";
const REF_PORT       = process.env.REF_PORT       || "5560";
const SERVER_ID      = process.env.SERVER_ID      || "js-server-1";
const DB_PATH        = process.env.DB_PATH        || `/data/bbs.db`;
const S2S_PORT       = process.env.S2S_PORT       || "6000";
const S2S_PEERS_ENV  = process.env.S2S_PEERS      || "";

const HEARTBEAT_EVERY      = 10;
const SYNC_EVERY           = 15;
const ELECTION_TIMEOUT_MS  = 2000;
const CLOCK_TIMEOUT_MS     = 2000;

// ---------------------------------------------------------------------------
// Parse dos peers servidor↔servidor
// formato: "nome1=host1:porta1,nome2=host2:porta2"
// ---------------------------------------------------------------------------
function parsePeers(env) {
  const peers = {};
  for (const part of env.split(",")) {
    const [name, addr] = part.trim().split("=");
    if (name && addr) peers[name.trim()] = addr.trim();
  }
  return peers;
}

const S2S_PEERS = parsePeers(S2S_PEERS_ENV);

// ---------------------------------------------------------------------------
// Relógio lógico de Lamport
// ---------------------------------------------------------------------------
let logicalClock = 0;

function clockTick()           { logicalClock += 1; return logicalClock; }
function clockUpdate(received) { logicalClock = Math.max(logicalClock, Number(received || 0)) + 1; }
function clockGet()            { return logicalClock; }

// ---------------------------------------------------------------------------
// Relógio físico
// ---------------------------------------------------------------------------
let physicalOffsetMs = 0;
function physicalNowMs() { return Date.now() + physicalOffsetMs; }

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------
function log(msg) {
  const ts = new Date().toTimeString().slice(0, 8);
  console.log(`[${SERVER_ID} ${ts}] ${msg}`);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ---------------------------------------------------------------------------
// Banco de dados
// ---------------------------------------------------------------------------
function initDb() {
  const dir = path.dirname(DB_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.exec(`
    CREATE TABLE IF NOT EXISTS logins (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL, timestamp INTEGER NOT NULL, logical_clock INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS channels (
      name TEXT PRIMARY KEY,
      created_by TEXT NOT NULL, timestamp INTEGER NOT NULL, logical_clock INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS publications (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL,
      timestamp INTEGER NOT NULL, logical_clock INTEGER NOT NULL DEFAULT 0);
  `);
  // migração caso o volume venha da parte 3
  for (const tbl of ["logins","channels","publications"]) {
    const cols = db.prepare(`PRAGMA table_info(${tbl})`).all().map(r => r.name);
    if (!cols.includes("logical_clock"))
      db.exec(`ALTER TABLE ${tbl} ADD COLUMN logical_clock INTEGER NOT NULL DEFAULT 0`);
  }
  log(`Banco de dados inicializado em '${DB_PATH}'`);
  return db;
}

// ---------------------------------------------------------------------------
// Protocolo
// ---------------------------------------------------------------------------
function makeMsg(type, payload) {
  return { type, timestamp: physicalNowMs(), logical_clock: clockTick(), payload };
}
function makeResponse(type, payload) { return encode(makeMsg(type, payload)); }

// ---------------------------------------------------------------------------
// Estado de eleição / coordenador
// ---------------------------------------------------------------------------
let coordinator         = null;
let myRank              = 0;
let electionInProgress  = false;

// ---------------------------------------------------------------------------
// Comunicação com serviço de referência
// ---------------------------------------------------------------------------
let refSocket = null;

async function refRequest(type, payload) {
  const msg = makeMsg(type, payload);
  await refSocket.send(encode(msg));
  const [raw] = await refSocket.receive();
  const resp  = decode(raw);
  clockUpdate(resp.logical_clock || 0);
  return resp;
}

async function registerWithReference() {
  refSocket = new Request();
  refSocket.connect(`tcp://${REF_HOST}:${REF_PORT}`);
  const resp = await refRequest("GET_RANK", { name: SERVER_ID });
  myRank = resp.payload?.rank || 0;
  log(`Registrado na referência | rank=${myRank} | lc=${clockGet()}`);
}

async function doHeartbeat() {
  try {
    await refRequest("HEARTBEAT", { name: SERVER_ID });
    log(`HEARTBEAT OK | lc=${clockGet()}`);
  } catch (e) { log(`HEARTBEAT ERRO: ${e.message}`); }
}

async function getServerList() {
  try {
    const resp = await refRequest("LIST_SERVERS", {});
    return resp.payload?.servers || [];
  } catch (e) { log(`LIST_SERVERS ERRO: ${e.message}`); return []; }
}

// ---------------------------------------------------------------------------
// Comunicação servidor↔servidor (REQ com timeout)
// ---------------------------------------------------------------------------
async function s2sRequest(peerAddr, type, payload, timeoutMs = ELECTION_TIMEOUT_MS) {
  const sock = new Request();
  sock.sendTimeout  = timeoutMs;
  sock.receiveTimeout = timeoutMs;
  try {
    sock.connect(`tcp://${peerAddr}`);
    const msg = makeMsg(type, payload);
    await sock.send(encode(msg));
    const [raw]  = await sock.receive();
    const resp   = decode(raw);
    clockUpdate(resp.logical_clock || 0);
    return resp;
  } catch (e) {
    return null;
  } finally {
    sock.close();
  }
}

// ---------------------------------------------------------------------------
// Eleição — algoritmo de Bully (menor rank = maior prioridade)
// ---------------------------------------------------------------------------
async function startElection(pubSocket) {
  if (electionInProgress) return;
  electionInProgress = true;
  log(`ELEIÇÃO iniciada | meu rank=${myRank}`);

  const servers = await getServerList();
  let higherResponded = false;

  for (const srv of servers) {
    if (srv.name === SERVER_ID) continue;
    if (srv.rank >= myRank) continue;   // só contata quem tem prioridade maior

    const peerAddr = S2S_PEERS[srv.name];
    if (!peerAddr) continue;

    log(`ELEIÇÃO → contactando '${srv.name}' (rank=${srv.rank})`);
    const resp = await s2sRequest(peerAddr, "ELECTION", { candidate: SERVER_ID });
    if (resp?.type === "ELECTION_OK") {
      log(`ELEIÇÃO → '${srv.name}' respondeu OK, ele assumirá`);
      higherResponded = true;
      break;
    }
  }

  if (!higherResponded) {
    log(`ELEIÇÃO → ninguém respondeu, sou o coordenador!`);
    coordinator = SERVER_ID;
    log(`★ Coordenador definido: '${SERVER_ID}'`);
    await announceCoordinator(pubSocket);
  }

  electionInProgress = false;
}

async function announceCoordinator(pubSocket) {
  const lc = clockTick();
  const payload = encode({ coordinator: SERVER_ID, timestamp: physicalNowMs(), logical_clock: lc });
  await pubSocket.send([Buffer.from("servers"), payload]);
  log(`ANUNCIO coordenador='${SERVER_ID}' publicado no tópico 'servers' | lc=${lc}`);
}

// ---------------------------------------------------------------------------
// Sincronização de relógio com o coordenador (Berkeley simplificado)
// ---------------------------------------------------------------------------
async function syncClockWithCoordinator(pubSocket) {
  if (!coordinator) {
    log("SYNC: sem coordenador, iniciando eleição...");
    await startElection(pubSocket);
    return;
  }
  if (coordinator === SERVER_ID) {
    log("SYNC: sou o coordenador, relógio já é a referência");
    return;
  }
  const peerAddr = S2S_PEERS[coordinator];
  if (!peerAddr) {
    log(`SYNC: endereço do coordenador '${coordinator}' desconhecido, iniciando eleição...`);
    await startElection(pubSocket);
    return;
  }

  const tSend = Date.now();
  const resp  = await s2sRequest(peerAddr, "GET_CLOCK", { requester: SERVER_ID }, CLOCK_TIMEOUT_MS);
  const tRecv = Date.now();

  if (!resp) {
    log(`SYNC: coordenador '${coordinator}' não respondeu → iniciando eleição`);
    coordinator = null;
    await startElection(pubSocket);
    return;
  }

  const coordClock = resp.payload?.clock || 0;
  if (coordClock) {
    const rttHalf    = Math.floor((tRecv - tSend) / 2);
    physicalOffsetMs = coordClock + rttHalf - tRecv;
    const sign       = physicalOffsetMs >= 0 ? "+" : "";
    log(`SYNC OK | coord='${coordinator}' | coord_clock=${coordClock} | offset=${sign}${physicalOffsetMs}ms | lc=${clockGet()}`);
  }
}

// ---------------------------------------------------------------------------
// Thread SUB — escuta tópico 'servers' para atualizar coordenador
// ---------------------------------------------------------------------------
async function coordinatorSubscriber() {
  const sub = new Subscriber();
  sub.connect(`tcp://${PROXY_HOST}:${PROXY_SUB_PORT}`);
  sub.subscribe("servers");
  log("SUB inscrito no tópico 'servers'");

  for await (const [topicBuf, dataBuf] of sub) {
    try {
      const data   = decode(dataBuf);
      const coord  = data.coordinator || "";
      clockUpdate(data.logical_clock || 0);
      if (coord) {
        coordinator = coord;
        log(`SUB [servers] novo coordenador='${coord}' | lc=${clockGet()}`);
      }
    } catch (e) { log(`SUB [servers] ERRO: ${e.message}`); }
  }
}

// ---------------------------------------------------------------------------
// REP servidor↔servidor (S2S)
// ---------------------------------------------------------------------------
async function s2sServer(pubSocket) {
  const rep = new Reply();
  rep.bind(`tcp://*:${S2S_PORT}`);
  log(`S2S REP escutando na porta ${S2S_PORT}`);

  for await (const [rawMsg] of rep) {
    try {
      const msg     = decode(rawMsg);
      const mtype   = msg.type || "";
      const payload = msg.payload || {};
      clockUpdate(msg.logical_clock || 0);

      if (mtype === "ELECTION") {
        const candidate = payload.candidate || "?";
        log(`S2S ELECTION ← '${candidate}' | respondendo OK e iniciando minha eleição`);
        await rep.send(makeResponse("ELECTION_OK", {}));
        startElection(pubSocket);   // não await para não bloquear o loop

      } else if (mtype === "GET_CLOCK") {
        const requester = payload.requester || "?";
        const now = physicalNowMs();
        log(`S2S GET_CLOCK ← '${requester}' | respondendo clock=${now}`);
        await rep.send(makeResponse("CLOCK", { clock: now }));

      } else {
        log(`S2S tipo desconhecido: '${mtype}'`);
        await rep.send(makeResponse("ERROR", { reason: `unknown: ${mtype}` }));
      }
    } catch (e) {
      log(`S2S ERRO: ${e.message}`);
      try { await rep.send(makeResponse("ERROR", { reason: e.message })); } catch {}
    }
  }
}

// ---------------------------------------------------------------------------
// Handlers de mensagens de clientes
// ---------------------------------------------------------------------------
const loggedInUsers = new Set();

function handleLogin(db, payload, ts, recvLc) {
  clockUpdate(recvLc);
  const username = String(payload.username || "").trim();
  if (!username) return makeResponse("LOGIN_ERROR", { username: "", reason: "invalid_username" });
  if (loggedInUsers.has(username)) return makeResponse("LOGIN_ERROR", { username, reason: "already_logged_in" });
  loggedInUsers.add(username);
  const lc = clockGet();
  db.prepare("INSERT INTO logins (username, timestamp, logical_clock) VALUES (?,?,?)").run(username, ts, lc);
  log(`LOGIN_OK: '${username}' | lc=${lc}`);
  return makeResponse("LOGIN_OK", { username });
}

function handleListChannels(db, payload, recvLc) {
  clockUpdate(recvLc);
  const channels = db.prepare("SELECT name FROM channels ORDER BY name").all().map(r => r.name);
  log(`LIST_CHANNELS: ${channels.length} canal(is) | lc=${clockGet()}`);
  return makeResponse("CHANNEL_LIST", { channels });
}

function handleCreateChannel(db, payload, ts, recvLc) {
  clockUpdate(recvLc);
  const channel  = String(payload.channel  || "").trim();
  const username = String(payload.username || "unknown");
  if (!channel || channel.includes(" ")) return makeResponse("CHANNEL_ERROR", { reason: "invalid_channel_name" });
  const exists = db.prepare("SELECT 1 FROM channels WHERE name=?").get(channel);
  if (exists) return makeResponse("CHANNEL_EXISTS", { channel });
  const lc = clockGet();
  db.prepare("INSERT INTO channels (name, created_by, timestamp, logical_clock) VALUES (?,?,?,?)").run(channel, username, ts, lc);
  log(`CHANNEL_CREATED: '${channel}' | lc=${lc}`);
  return makeResponse("CHANNEL_CREATED", { channel });
}

async function handlePublish(db, pubSocket, payload, ts, recvLc) {
  clockUpdate(recvLc);
  const channel  = String(payload.channel  || "").trim();
  const username = String(payload.username || "unknown");
  const message  = String(payload.message  || "").trim();
  const exists = db.prepare("SELECT 1 FROM channels WHERE name=?").get(channel);
  if (!channel || !exists) return makeResponse("PUBLISH_ERROR", { reason: "channel_not_found" });
  if (!message) return makeResponse("PUBLISH_ERROR", { reason: "empty_message" });
  const lc  = clockTick();
  const now = physicalNowMs();
  await pubSocket.send([Buffer.from(channel), encode({ channel, username, message, timestamp: now, logical_clock: lc })]);
  db.prepare("INSERT INTO publications (channel, username, message, timestamp, logical_clock) VALUES (?,?,?,?,?)").run(channel, username, message, now, lc);
  log(`PUBLISH_OK: canal='${channel}' | user='${username}' | lc=${lc}`);
  return makeResponse("PUBLISH_OK", { channel, timestamp: ts });
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const db = initDb();

  await registerWithReference();

  const pubSocket = new Publisher();
  pubSocket.connect(`tcp://${PROXY_HOST}:${PROXY_PORT}`);

  // Inicia threads assíncronas
  coordinatorSubscriber();
  s2sServer(pubSocket);

  await sleep(1000);

  // Eleição inicial
  log("Iniciando eleição inicial...");
  await startElection(pubSocket);
  await sleep(500);

  const repSocket = new Reply();
  await repSocket.connect(`tcp://${BROKER_HOST}:${BROKER_PORT}`);
  log(`Servidor pronto | rank=${myRank} | coordenador=${coordinator} | lc=${clockGet()}`);

  let messageCount = 0;

  for await (const [rawMsg] of repSocket) {
    try {
      const msg     = decode(rawMsg);
      const type    = msg.type    || "";
      const payload = msg.payload || {};
      const ts      = msg.timestamp || physicalNowMs();
      const recvLc  = msg.logical_clock || 0;

      log(`RECEBIDO | type=${type} | lc_msg=${recvLc} | lc_local=${clockGet()}`);

      let response;
      switch (type) {
        case "LOGIN":          response = handleLogin(db, payload, ts, recvLc); break;
        case "LIST_CHANNELS":  response = handleListChannels(db, payload, recvLc); break;
        case "CREATE_CHANNEL": response = handleCreateChannel(db, payload, ts, recvLc); break;
        case "PUBLISH":        response = await handlePublish(db, pubSocket, payload, ts, recvLc); break;
        default:
          clockUpdate(recvLc);
          response = makeResponse("ERROR", { reason: `unknown_type: ${type}` });
      }

      await repSocket.send(response);

      messageCount += 1;
      if (messageCount % HEARTBEAT_EVERY === 0) doHeartbeat();
      if (messageCount % SYNC_EVERY === 0) syncClockWithCoordinator(pubSocket);

    } catch (e) {
      log(`ERRO: ${e.message}`);
      try { await repSocket.send(makeResponse("ERROR", { reason: e.message })); } catch {}
    }
  }
}

main().catch(e => { log(`ERRO FATAL: ${e.message}`); process.exit(1); });
