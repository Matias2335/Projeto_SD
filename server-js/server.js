/**
 * BBS Server (JavaScript/Node.js)
 * Conecta ao broker via ZeroMQ REP, processa login/canais e persiste em SQLite.
 */

const { Reply } = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");
const Database = require("better-sqlite3");
const fs = require("fs");
const path = require("path");

const BROKER_HOST = process.env.BROKER_HOST || "broker";
const BROKER_PORT = process.env.BROKER_PORT || "5556";
const SERVER_ID = process.env.SERVER_ID || "js-server-1";
const DB_PATH = process.env.DB_PATH || `/data/${SERVER_ID}.db`;

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------
function log(msg) {
  const ts = new Date().toTimeString().slice(0, 8);
  console.log(`[${SERVER_ID} ${ts}] ${msg}`);
}

// ---------------------------------------------------------------------------
// Banco de dados (SQLite)
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
  log(`LIST_CHANNELS: retornando ${channels.length} canal(is) → ${JSON.stringify(channels)}`);
  return makeResponse("CHANNEL_LIST", { channels });
}

function handleCreateChannel(db, payload, ts) {
  const channel = (payload.channel || "").trim();
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

// ---------------------------------------------------------------------------
// Loop principal
// ---------------------------------------------------------------------------
async function main() {
  const db = initDb();
  const socket = new Reply();
  const addr = `tcp://${BROKER_HOST}:${BROKER_PORT}`;
  await socket.connect(addr);
  log(`Conectado ao broker em ${addr}. Aguardando mensagens...`);

  for await (const [rawMsg] of socket) {
    try {
      const msg = decode(rawMsg);
      const { type, payload = {}, timestamp: ts = Date.now() } = msg;

      log(`RECEBIDO | type=${type} | payload=${JSON.stringify(payload)} | timestamp=${ts}`);

      let response;
      switch (type) {
        case "LOGIN":
          response = handleLogin(db, payload, ts);
          break;
        case "LIST_CHANNELS":
          response = handleListChannels(db, payload);
          break;
        case "CREATE_CHANNEL":
          response = handleCreateChannel(db, payload, ts);
          break;
        default:
          log(`TIPO DESCONHECIDO: '${type}'`);
          response = makeResponse("ERROR", { reason: `unknown_type: ${type}` });
      }

      const respDecoded = decode(response);
      log(`ENVIANDO | type=${respDecoded.type} | payload=${JSON.stringify(respDecoded.payload)}`);
      await socket.send(response);

    } catch (err) {
      log(`ERRO: ${err.message}`);
      await socket.send(makeResponse("ERROR", { reason: err.message }));
    }
  }
}

main().catch(err => {
  log(`ERRO FATAL: ${err.message}`);
  process.exit(1);
});
