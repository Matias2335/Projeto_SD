/**
 * BBS Client Bot (JavaScript/Node.js) - Parte 2
 * Loop infinito: inscreve em canais aleatórios e publica mensagens a cada 1 segundo.
 */

const { Request, Subscriber } = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const BROKER_HOST    = process.env.BROKER_HOST    || "broker";
const BROKER_PORT    = process.env.BROKER_PORT    || "5555";
const PROXY_HOST     = process.env.PROXY_HOST     || "proxy-pubsub";
const PROXY_SUB_PORT = process.env.PROXY_SUB_PORT || "5558";
const BOT_NAME       = process.env.BOT_NAME       || "bot-js-1";
const RETRY_DELAY_MS = 2000;

const RANDOM_MESSAGES = [
  "Olá a todos!",
  "Alguém online?",
  "Testando o sistema BBS...",
  "Mensagem automática do bot.",
  "O sistema está funcionando!",
  "Distribuído e funcionando.",
  "ZeroMQ é incrível.",
  "MessagePack é eficiente.",
  "Mais uma mensagem de teste.",
  "BBS no ar!",
  "Ping!",
  "Sistemas distribuídos rocks.",
  "Canal ativo.",
  "Bot operacional.",
  "Transmissão em andamento.",
];

function log(msg) {
  const ts = new Date().toTimeString().slice(0, 8);
  console.log(`[${BOT_NAME} ${ts}] ${msg}`);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function makeRequest(type, payload) {
  return encode({ type, timestamp: Date.now(), payload });
}

async function sendRecv(socket, type, payload) {
  const raw    = makeRequest(type, payload);
  const outMsg = decode(raw);
  log(`ENVIANDO | type=${type} | payload=${JSON.stringify(payload)} | timestamp=${outMsg.timestamp}`);
  await socket.send(raw);
  const [responseRaw] = await socket.receive();
  const response = decode(responseRaw);
  log(`RECEBIDO | type=${response.type} | payload=${JSON.stringify(response.payload)} | timestamp=${response.timestamp}`);
  return response;
}

// ---------------------------------------------------------------------------
// Loop de recebimento SUB (roda em paralelo via Promise)
// ---------------------------------------------------------------------------
async function subscriberLoop(channels) {
  const subSocket = new Subscriber();
  const addr = `tcp://${PROXY_HOST}:${PROXY_SUB_PORT}`;
  subSocket.connect(addr);

  for (const ch of channels) {
    subSocket.subscribe(ch);
    log(`[SUB] Inscrito no canal '${ch}'`);
  }
  log(`[SUB] Aguardando mensagens nos canais: ${JSON.stringify(channels)}`);

  for await (const parts of subSocket) {
    try {
      const recvTs  = Date.now();
      const channel = Buffer.from(parts[0]).toString();
      const data    = decode(parts[1]);

      log(
        `[SUB] MENSAGEM RECEBIDA | canal='${channel}' | ` +
        `de='${data.username}' | msg='${data.message}' | ` +
        `timestamp_envio=${data.timestamp} | timestamp_recebimento=${recvTs}`
      );
    } catch (e) {
      log(`[SUB] ERRO: ${e.message}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
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
        log(`✓ Login OK como '${BOT_NAME}'`);
        loggedIn = true;
      } else {
        const reason = resp.payload?.reason || "?";
        log(`✗ Login falhou: ${reason}. Tentando em ${RETRY_DELAY_MS / 1000}s...`);
        socket.close();
        await sleep(RETRY_DELAY_MS);
        socket = new Request();
        socket.connect(addr);
      }
    } catch (e) {
      log(`Erro: ${e.message}. Recriando socket...`);
      socket.close();
      await sleep(RETRY_DELAY_MS);
      socket = new Request();
      socket.connect(addr);
    }
  }

  // 2. Listar canais
  await sleep(1000);
  let listResp = await sendRecv(socket, "LIST_CHANNELS", { username: BOT_NAME });
  let channels = listResp.payload?.channels || [];
  log(`Canais disponíveis: ${JSON.stringify(channels)}`);

  // 3. Se menos de 5 canais, criar um
  if (channels.length < 5) {
    const newChannel = `canal-${BOT_NAME}-${Math.floor(Math.random() * 900) + 100}`;
    const resp = await sendRecv(socket, "CREATE_CHANNEL", { username: BOT_NAME, channel: newChannel });
    if (resp.type === "CHANNEL_CREATED" || resp.type === "CHANNEL_EXISTS") {
      if (!channels.includes(newChannel)) channels.push(newChannel);
      log(`✓ Canal '${newChannel}' pronto.`);
    }
  }

  // Atualiza lista
  listResp = await sendRecv(socket, "LIST_CHANNELS", { username: BOT_NAME });
  channels  = listResp.payload?.channels || [];

  // 4. Inscrever em no mínimo 3 canais aleatórios
  const shuffled    = shuffle(channels);
  const subscribed  = shuffled.slice(0, Math.min(3, shuffled.length));

  if (subscribed.length === 0) {
    log("Nenhum canal disponível para inscrição.");
    return;
  }

  // Inicia loop SUB em paralelo (não aguarda)
  subscriberLoop(subscribed).catch(e => log(`[SUB] ERRO FATAL: ${e.message}`));

  await sleep(1000);

  // 5. Loop infinito de publicação
  log("Iniciando loop de publicação...");
  while (true) {
    const channel = randomChoice(channels);
    log(`Publicando 10 mensagens no canal '${channel}'...`);
    for (let i = 0; i < 10; i++) {
      const message = randomChoice(RANDOM_MESSAGES);
      const resp    = await sendRecv(socket, "PUBLISH", {
        username: BOT_NAME,
        channel,
        message,
      });
      if (resp.type === "PUBLISH_OK") {
        log(`✓ Publicado: '${message}' em '${channel}'`);
      } else {
        const reason = resp.payload?.reason || "?";
        log(`✗ Erro ao publicar: ${reason}`);
      }
      await sleep(1000);
    }
  }
}

main().catch(err => {
  log(`ERRO FATAL: ${err.message}`);
  process.exit(1);
});
