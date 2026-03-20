/**
 * BBS Client Bot (JavaScript/Node.js)
 * Realiza login, lista canais e cria um canal automaticamente.
 * Não requer interação humana.
 */

const { Request } = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const BROKER_HOST = process.env.BROKER_HOST || "broker";
const BROKER_PORT = process.env.BROKER_PORT || "5555";
const BOT_NAME = process.env.BOT_NAME || "bot-js-1";
const CHANNEL_TO_CREATE = process.env.CHANNEL_TO_CREATE || "geral-js";
const RETRY_DELAY_MS = 2000;

function log(msg) {
  const ts = new Date().toTimeString().slice(0, 8);
  console.log(`[${BOT_NAME} ${ts}] ${msg}`);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function makeRequest(type, payload) {
  return encode({ type, timestamp: Date.now(), payload });
}

async function sendRecv(socket, type, payload) {
  const raw = makeRequest(type, payload);
  const outMsg = decode(raw);
  log(`ENVIANDO | type=${type} | payload=${JSON.stringify(payload)} | timestamp=${outMsg.timestamp}`);
  await socket.send(raw);
  const [responseRaw] = await socket.receive();
  const response = decode(responseRaw);
  log(`RECEBIDO | type=${response.type} | payload=${JSON.stringify(response.payload)} | timestamp=${response.timestamp}`);
  return response;
}

async function main() {
  // Aguarda broker e servidores subirem
  await sleep(4000);

  const addr = `tcp://${BROKER_HOST}:${BROKER_PORT}`;
  let socket = new Request();
  socket.connect(addr);
  log(`Conectado ao broker em ${addr}`);

  // 1. Login com retry
  let loggedIn = false;
  while (!loggedIn) {
    try {
      const resp = await sendRecv(socket, "LOGIN", { username: BOT_NAME });
      if (resp.type === "LOGIN_OK") {
        log(`✓ Login realizado com sucesso como '${BOT_NAME}'`);
        loggedIn = true;
      } else {
        const reason = resp.payload?.reason || "desconhecido";
        log(`✗ Falha no login: ${reason}. Tentando novamente em ${RETRY_DELAY_MS / 1000}s...`);
        socket.close();
        await sleep(RETRY_DELAY_MS);
        socket = new Request();
        socket.connect(addr);
      }
    } catch (err) {
      log(`Erro de comunicação: ${err.message}. Recriando socket...`);
      socket.close();
      await sleep(RETRY_DELAY_MS);
      socket = new Request();
      socket.connect(addr);
    }
  }

  // 2. Listar canais
  await sleep(1000);
  const listResp = await sendRecv(socket, "LIST_CHANNELS", { username: BOT_NAME });
  let channels = listResp.payload?.channels || [];
  if (channels.length === 0) {
    log("✓ Nenhum canal disponível ainda.");
  } else {
    log(`✓ Canais disponíveis: ${JSON.stringify(channels)}`);
  }

  // 3. Criar canal
  await sleep(1000);
  if (!channels.includes(CHANNEL_TO_CREATE)) {
    const createResp = await sendRecv(socket, "CREATE_CHANNEL", {
      username: BOT_NAME,
      channel: CHANNEL_TO_CREATE,
    });
    if (createResp.type === "CHANNEL_CREATED") {
      log(`✓ Canal '${CHANNEL_TO_CREATE}' criado com sucesso!`);
    } else if (createResp.type === "CHANNEL_EXISTS") {
      log(`→ Canal '${CHANNEL_TO_CREATE}' já existe, usando o existente.`);
    } else {
      log(`✗ Erro ao criar canal: ${JSON.stringify(createResp.payload)}`);
    }
  } else {
    log(`→ Canal '${CHANNEL_TO_CREATE}' já estava na lista.`);
  }

  // 4. Listar novamente para confirmar
  await sleep(1000);
  log("Listando canais após criação:");
  await sendRecv(socket, "LIST_CHANNELS", { username: BOT_NAME });

  log("Bot finalizou todas as tarefas da Parte 1.");
  socket.close();
}

main().catch(err => {
  log(`ERRO FATAL: ${err.message}`);
  process.exit(1);
});
