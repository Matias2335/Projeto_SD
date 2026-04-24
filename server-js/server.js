/**
 * BBS Server (JavaScript/Node.js) - Parte 3
 * Adicionado:
 *  - Relógio lógico de Lamport em todas as mensagens enviadas/recebidas
 *  - Registro no serviço de referência (GET_RANK)
 *  - Heartbeat a cada 10 mensagens de clientes
 *  - Sincronização do relógio físico pelo horário retornado no heartbeat
 */

const { Reply, Publisher, Request } = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");
const Database = require("better-sqlite3");
const fs = require("fs");
const path = require("path");

const BROKER_HOST = process.env.BROKER_HOST || "broker";
const BROKER_PORT = process.env.BROKER_PORT || "5556";
const PROXY_HOST = process.env.PROXY_HOST || "proxy-pubsub";
const PROXY_PORT = process.env.PROXY_PORT || "5557";
const REF_HOST = process.env.REF_HOST || "reference";
const REF_PORT = process.env.REF_PORT || "5560";
const SERVER_ID = process.env.SERVER_ID || "js-server-1";
const DB_PATH = process.env.DB_PATH || `/data/${SERVER_ID}.db`;

const HEARTBEAT_EVERY = 10;
const REF_RETRY_DELAY_MS = 1000;

// ---------------------------------------------------------------------------
// Relógio lógico de Lamport
// ---------------------------------------------------------------------------
let logicalClock = 0;

function clockTick() {
  logicalClock += 1;
  return logicalClock;
}

function clockUpdate(received) {
  const recv = Number(received || 0);
  logicalClock = Math.max(logicalClock, Number.isFinite(recv) ? recv : 0) + 1;
  return logicalClock;
}

function clockGet() {
  return logicalClock;
}

// ---------------------------------------------------------------------------
// Relógio físico ajustado pelo serviço de referência
// ---------------------------------------------------------------------------
let physicalOffsetMs = 0;

function systemNowMs() {
  return Date.now();
}

function physicalNowMs() {
  return Date.now() + physicalOffsetMs;
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------
function log(msg) {
  const ts = new Date().toTimeString().slice(0, 8);
  console.log(`[${SERVER_ID} ${ts}] ${msg}`);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Banco de dados
// ---------------------------------------------------------------------------
function addColumnIfMissing(db, table, column, definition) {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all().map(row => row.name);
  if (!cols.includes(column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }
}

function initDb() {
  const dir = path.dirname(DB_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");

  db.exec(`
    CREATE TABLE IF NOT EXISTS logins (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      logical_clock INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS channels (
      name TEXT PRIMARY KEY,
      created_by TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      logical_clock INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS publications (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      channel TEXT NOT NULL,
      username TEXT NOT NULL,
      message TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      logical_clock INTEGER NOT NULL DEFAULT 0
    );
  `);

  // Caso o volume antigo tenha sido criado pela Parte 2, adiciona a coluna nova.
  addColumnIfMissing(db, "logins", "logical_clock", "INTEGER NOT NULL DEFAULT 0");
  addColumnIfMissing(db, "channels", "logical_clock", "INTEGER NOT NULL DEFAULT 0");
  addColumnIfMissing(db, "publications", "logical_clock", "INTEGER NOT NULL DEFAULT 0");

  log(`Banco de dados inicializado em '${DB_PATH}'`);
  return db;
}

// ---------------------------------------------------------------------------
// Protocolo
// ---------------------------------------------------------------------------
function makeMessage(type, payload) {
  return {
    type,
    timestamp: physicalNowMs(),
    logical_clock: clockTick(),
    payload,
  };
}

function makeResponse(type, payload) {
  return encode(makeMessage(type, payload));
}

function requestToReference(socket, type, payload) {
  const msg = makeMessage(type, payload);
  socket.send(encode(msg));
  return socket.receive().then(([responseRaw]) => {
    const response = decode(responseRaw);
    clockUpdate(response.logical_clock || 0);
    return response;
  });
}

// ---------------------------------------------------------------------------
// Comunicação com serviço de referência
// ---------------------------------------------------------------------------
let serverRank = 0;
let messageCount = 0;
let refSocket = null;
let heartbeatRunning = false;

async function registerWithReference() {
  refSocket = new Request();
  const refAddr = `tcp://${REF_HOST}:${REF_PORT}`;
  refSocket.connect(refAddr);

  while (true) {
    try {
      const response = await requestToReference(refSocket, "GET_RANK", { name: SERVER_ID });
      serverRank = response.payload?.rank || 0;
      log(`Registrado no serviço de referência em ${refAddr} | rank=${serverRank} | lc=${clockGet()}`);
      return;
    } catch (err) {
      log(`Falha ao registrar no reference: ${err.message}. Tentando novamente...`);
      await sleep(REF_RETRY_DELAY_MS);
    }
  }
}

async function doHeartbeat() {
  if (!refSocket || heartbeatRunning) return;
  heartbeatRunning = true;

  try {
    const tSend = systemNowMs();
    const response = await requestToReference(refSocket, "HEARTBEAT", { name: SERVER_ID });
    const tRecv = systemNowMs();

    const refClock = response.payload?.clock || 0;
    if (refClock) {
      const rttHalf = Math.floor((tRecv - tSend) / 2);
      physicalOffsetMs = refClock + rttHalf - tRecv;
      const sign = physicalOffsetMs >= 0 ? "+" : "";
      log(`HEARTBEAT OK | clock_ref=${refClock} | offset=${sign}${physicalOffsetMs}ms | lc=${clockGet()}`);
    }
  } catch (err) {
    log(`HEARTBEAT ERRO: ${err.message}`);
  } finally {
    heartbeatRunning = false;
  }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------
const loggedInUsers = new Set();

function handleLogin(db, payload, ts, recvLc) {
  clockUpdate(recvLc);
  const username = String(payload.username || "").trim();

  if (!username) {
    log("LOGIN_ERROR: username vazio");
    return makeResponse("LOGIN_ERROR", { username: "", reason: "invalid_username" });
  }

  if (loggedInUsers.has(username)) {
    log(`LOGIN_ERROR: '${username}' já está logado`);
    return makeResponse("LOGIN_ERROR", { username, reason: "already_logged_in" });
  }

  loggedInUsers.add(username);
  const lc = clockGet();
  db.prepare("INSERT INTO logins (username, timestamp, logical_clock) VALUES (?, ?, ?)")
    .run(username, ts, lc);

  log(`LOGIN_OK: '${username}' | timestamp=${ts} | lc=${lc}`);
  return makeResponse("LOGIN_OK", { username });
}

function handleListChannels(db, payload, recvLc) {
  clockUpdate(recvLc);
  const channels = db.prepare("SELECT name FROM channels ORDER BY name").all().map(row => row.name);
  log(`LIST_CHANNELS: ${channels.length} canal(is) | lc=${clockGet()}`);
  return makeResponse("CHANNEL_LIST", { channels });
}

function handleCreateChannel(db, payload, ts, recvLc) {
  clockUpdate(recvLc);
  const channel = String(payload.channel || "").trim();
  const username = String(payload.username || "unknown");

  if (!channel || channel.includes(" ")) {
    log(`CHANNEL_ERROR: nome inválido '${channel}'`);
    return makeResponse("CHANNEL_ERROR", { reason: "invalid_channel_name" });
  }

  const exists = db.prepare("SELECT 1 FROM channels WHERE name = ?").get(channel);
  if (exists) {
    log(`CHANNEL_EXISTS: '${channel}' já existe`);
    return makeResponse("CHANNEL_EXISTS", { channel });
  }

  const lc = clockGet();
  db.prepare("INSERT INTO channels (name, created_by, timestamp, logical_clock) VALUES (?, ?, ?, ?)")
    .run(channel, username, ts, lc);

  log(`CHANNEL_CREATED: '${channel}' por '${username}' | timestamp=${ts} | lc=${lc}`);
  return makeResponse("CHANNEL_CREATED", { channel });
}

async function handlePublish(db, pubSocket, payload, ts, recvLc) {
  clockUpdate(recvLc);
  const channel = String(payload.channel || "").trim();
  const username = String(payload.username || "unknown");
  const message = String(payload.message || "").trim();

  const exists = db.prepare("SELECT 1 FROM channels WHERE name = ?").get(channel);
  if (!channel || !exists) {
    log(`PUBLISH_ERROR: canal '${channel}' não existe`);
    return makeResponse("PUBLISH_ERROR", { reason: "channel_not_found" });
  }

  if (!message) {
    log("PUBLISH_ERROR: mensagem vazia");
    return makeResponse("PUBLISH_ERROR", { reason: "empty_message" });
  }

  const lc = clockTick();
  const pubTs = physicalNowMs();
  const pubPayload = encode({
    channel,
    username,
    message,
    timestamp: pubTs,
    logical_clock: lc,
  });

  await pubSocket.send([Buffer.from(channel), pubPayload]);

  db.prepare("INSERT INTO publications (channel, username, message, timestamp, logical_clock) VALUES (?, ?, ?, ?, ?)")
    .run(channel, username, message, pubTs, lc);

  log(`PUBLISH_OK: canal='${channel}' | user='${username}' | msg='${message}' | lc=${lc}`);
  return makeResponse("PUBLISH_OK", { channel, timestamp: pubTs });
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const db = initDb();

  await registerWithReference();

  const repSocket = new Reply();
  const repAddr = `tcp://${BROKER_HOST}:${BROKER_PORT}`;
  await repSocket.connect(repAddr);
  log(`Conectado ao broker em ${repAddr}`);

  const pubSocket = new Publisher();
  const pubAddr = `tcp://${PROXY_HOST}:${PROXY_PORT}`;
  await pubSocket.connect(pubAddr);
  log(`Conectado ao proxy PUB/SUB em ${pubAddr}`);

  await sleep(500);
  log(`Servidor pronto | rank=${serverRank} | lc=${clockGet()}`);

  for await (const [rawMsg] of repSocket) {
    try {
      const msg = decode(rawMsg);
      const type = msg.type || "";
      const payload = msg.payload || {};
      const ts = msg.timestamp || physicalNowMs();
      const recvLc = msg.logical_clock || 0;

      log(`RECEBIDO | type=${type} | lc_msg=${recvLc} | lc_local=${clockGet()}`);

      let response;
      switch (type) {
        case "LOGIN":
          response = handleLogin(db, payload, ts, recvLc);
          break;
        case "LIST_CHANNELS":
          response = handleListChannels(db, payload, recvLc);
          break;
        case "CREATE_CHANNEL":
          response = handleCreateChannel(db, payload, ts, recvLc);
          break;
        case "PUBLISH":
          response = await handlePublish(db, pubSocket, payload, ts, recvLc);
          break;
        default:
          clockUpdate(recvLc);
          log(`TIPO DESCONHECIDO: '${type}'`);
          response = makeResponse("ERROR", { reason: `unknown_type: ${type}` });
      }

      const respDecoded = decode(response);
      log(`ENVIANDO | type=${respDecoded.type} | lc=${respDecoded.logical_clock} | payload=${JSON.stringify(respDecoded.payload)}`);
      await repSocket.send(response);

      messageCount += 1;
      if (messageCount % HEARTBEAT_EVERY === 0) {
        doHeartbeat();
      }
    } catch (err) {
      log(`ERRO: ${err.message}`);
      await repSocket.send(makeResponse("ERROR", { reason: err.message }));
    }
  }
}

main().catch(err => {
  log(`ERRO FATAL: ${err.message}`);
  process.exit(1);
});
