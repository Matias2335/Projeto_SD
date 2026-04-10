/**
 * BBS Server (JavaScript/Node.js) - Parte 2
 * Adicionado: handler PUBLISH, conexão ao proxy PUB/SUB, persistência de publicações.
 */

const { Reply } = require("zeromq");
const { Publisher } = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");
const Database = require("better-sqlite3");
const fs = require("fs");
const path = require("path");

const BROKER_HOST = process.env.BROKER_HOST || "broker";
const BROKER_PORT = process.env.BROKER_PORT || "5556";
const PROXY_HOST  = process.env.PROXY_HOST  || "proxy-pubsub";
const PROXY_PORT  = process.env.PROXY_PORT  || "5557";
const SERVER_ID   = process.env.SERVER_ID   || "js-server-1";
const DB_PATH     = process.env.DB_PATH     || `/data/${SERVER_ID}.db`;

function log(msg) {
  const ts = new Date().toTimeString().slice(0, 8);
  console.log(`[${SERVER_ID} ${ts}] ${msg}`);
}

// ---------------------------------------------------------------------------
// Banco de dados
// ---------------------------------------------------------------------------
function initDb() {
  const dir = path.dirname(DB_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const db = new Database(DB_PATH);
  db.exec(`
    CREATE TABLE IF NOT EXISTS logins (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      username  TEXT NOT NULL,
      timestamp INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS channels (
      name       TEXT PRIMARY KEY,
      created_by TEXT NOT NULL,
      timestamp  INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS publications (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      channel   TEXT NOT NULL,
      username  TEXT NOT NULL,
      message   TEXT NOT NULL,
      timestamp INTEGER NOT NULL
    );
  `);
  log(`Banco de dados inicializado em '${DB_PATH}'`);
  return db;
}

// ---------------------------------------------------------------------------
// Protocolo
// ---------------------------------------------------------------------------
function makeResponse(type, payload) {
  return encode({ type, timestamp: Date.now(), payload });
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------
const loggedInUsers = new Set();

function handleLogin(db, payload, ts) {
  const username = (payload.username || "").trim();
  if (!username) {
    log("LOGIN_ERROR: username vazio");
    return makeResponse("LOGIN_ERROR", { username: "", reason: "invalid_username" });
  }
  if (loggedInUsers.has(username)) {
    log(`LOGIN_ERROR: '${username}' já está logado`);
    return makeResponse("LOGIN_ERROR", { username, reason: "already_logged_in" });
  }
  loggedInUsers.add(username);
  db.prepare("INSERT INTO logins (username, timestamp) VALUES (?, ?)").run(username, ts);
  log(`LOGIN_OK: '${username}' | timestamp=${ts}`);
  return makeResponse("LOGIN_OK", { username });
}

function handleListChannels(db, payload) {
  const channels = db.prepare("SELECT name FROM channels ORDER BY name").all().map(r => r.name);
  log(`LIST_CHANNELS: ${channels.length} canal(is) → ${JSON.stringify(channels)}`);
  return makeResponse("CHANNEL_LIST", { channels });
}

function handleCreateChannel(db, payload, ts) {
  const channel  = (payload.channel  || "").trim();
  const username = payload.username || "unknown";
  if (!channel || channel.includes(" ")) {
    log(`CHANNEL_ERROR: nome inválido '${channel}'`);
    return makeResponse("CHANNEL_ERROR", { reason: "invalid_channel_name" });
  }
  const exists = db.prepare("SELECT 1 FROM channels WHERE name = ?").get(channel);
  if (exists) {
    log(`CHANNEL_EXISTS: '${channel}' já existe`);
    return makeResponse("CHANNEL_EXISTS", { channel });
  }
  db.prepare("INSERT INTO channels (name, created_by, timestamp) VALUES (?, ?, ?)").run(channel, username, ts);
  log(`CHANNEL_CREATED: '${channel}' por '${username}' | timestamp=${ts}`);
  return makeResponse("CHANNEL_CREATED", { channel });
}

async function handlePublish(db, pubSocket, payload, ts) {
  const channel  = (payload.channel  || "").trim();
  const username = payload.username || "unknown";
  const message  = (payload.message  || "").trim();

  const exists = db.prepare("SELECT 1 FROM channels WHERE name = ?").get(channel);
  if (!channel || !exists) {
    log(`PUBLISH_ERROR: canal '${channel}' não existe`);
    return makeResponse("PUBLISH_ERROR", { reason: "channel_not_found" });
  }
  if (!message) {
    log("PUBLISH_ERROR: mensagem vazia");
    return makeResponse("PUBLISH_ERROR", { reason: "empty_message" });
  }

  // Publica no proxy: primeiro frame = tópico, segundo = corpo msgpack
  const pubPayload = encode({ channel, username, message, timestamp: ts });
  await pubSocket.send([Buffer.from(channel), pubPayload]);

  db.prepare("INSERT INTO publications (channel, username, message, timestamp) VALUES (?, ?, ?, ?)")
    .run(channel, username, message, ts);

  log(`PUBLISH_OK: canal='${channel}' | user='${username}' | msg='${message}' | timestamp=${ts}`);
  return makeResponse("PUBLISH_OK", { channel, timestamp: ts });
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const db = initDb();

  const repSocket = new Reply();
  const repAddr   = `tcp://${BROKER_HOST}:${BROKER_PORT}`;
  await repSocket.connect(repAddr);
  log(`Conectado ao broker em ${repAddr}`);

  const pubSocket = new Publisher();
  const pubAddr   = `tcp://${PROXY_HOST}:${PROXY_PORT}`;
  await pubSocket.connect(pubAddr);
  log(`Conectado ao proxy PUB/SUB em ${pubAddr}`);

  // Pausa para o socket PUB estabilizar
  await new Promise(r => setTimeout(r, 500));
  log("Aguardando mensagens...");

  for await (const [rawMsg] of repSocket) {
    try {
      const msg     = decode(rawMsg);
      const { type, payload = {}, timestamp: ts = Date.now() } = msg;

      log(`RECEBIDO | type=${type} | payload=${JSON.stringify(payload)} | timestamp=${ts}`);

      let response;
      switch (type) {
        case "LOGIN":          response = handleLogin(db, payload, ts);                       break;
        case "LIST_CHANNELS":  response = handleListChannels(db, payload);                    break;
        case "CREATE_CHANNEL": response = handleCreateChannel(db, payload, ts);               break;
        case "PUBLISH":        response = await handlePublish(db, pubSocket, payload, ts);    break;
        default:
          log(`TIPO DESCONHECIDO: '${type}'`);
          response = makeResponse("ERROR", { reason: `unknown_type: ${type}` });
      }

      const respDecoded = decode(response);
      log(`ENVIANDO | type=${respDecoded.type} | payload=${JSON.stringify(respDecoded.payload)}`);
      await repSocket.send(response);

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
