"""
BBS Client Bot (Python) - Parte 3
Adicionado:
  - Relógio lógico de Lamport em todas as mensagens enviadas e recebidas
"""

import os
import time
import random
import threading
import zmq
import msgpack


BROKER_HOST    = os.environ.get("BROKER_HOST",    "broker")
BROKER_PORT    = int(os.environ.get("BROKER_PORT",    "5555"))
PROXY_HOST     = os.environ.get("PROXY_HOST",     "proxy-pubsub")
PROXY_SUB_PORT = int(os.environ.get("PROXY_SUB_PORT", "5558"))
BOT_NAME       = os.environ.get("BOT_NAME",       "bot-python-1")
RETRY_DELAY    = 2

RANDOM_MESSAGES = [
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
]


# ---------------------------------------------------------------------------
# Relógio lógico de Lamport
# ---------------------------------------------------------------------------
_clock_lock    = threading.Lock()
_logical_clock = 0


def clock_tick() -> int:
    global _logical_clock
    with _clock_lock:
        _logical_clock += 1
        return _logical_clock


def clock_update(received: int):
    global _logical_clock
    with _clock_lock:
        _logical_clock = max(_logical_clock, received) + 1


def clock_get() -> int:
    with _clock_lock:
        return _logical_clock


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{BOT_NAME} {ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Protocolo
# ---------------------------------------------------------------------------
def make_request(msg_type: str, payload: dict) -> bytes:
    lc = clock_tick()
    return msgpack.packb({
        "type":          msg_type,
        "timestamp":     int(time.time() * 1000),
        "logical_clock": lc,
        "payload":       payload,
    }, use_bin_type=True)


def send_recv(socket, msg_type: str, payload: dict) -> dict:
    raw  = make_request(msg_type, payload)
    msg  = msgpack.unpackb(raw, raw=False)
    log(f"ENVIANDO | type={msg_type} | lc={msg['logical_clock']} | payload={payload}")
    socket.send(raw)
    resp    = msgpack.unpackb(socket.recv(), raw=False)
    recv_lc = resp.get("logical_clock", 0)
    clock_update(recv_lc)
    log(f"RECEBIDO | type={resp['type']} | lc_msg={recv_lc} | lc_local={clock_get()}")
    return resp


# ---------------------------------------------------------------------------
# Thread SUB — exibe mensagens dos canais inscritos
# ---------------------------------------------------------------------------
def subscriber_thread(subscribed_channels: list):
    context    = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{PROXY_HOST}:{PROXY_SUB_PORT}")

    for channel in subscribed_channels:
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, channel)
        log(f"[SUB] Inscrito no canal '{channel}'")

    while True:
        try:
            parts   = sub_socket.recv_multipart()
            recv_ts = int(time.time() * 1000)
            channel = parts[0].decode()
            data    = msgpack.unpackb(parts[1], raw=False)

            send_ts  = data.get("timestamp",     0)
            lc       = data.get("logical_clock", 0)
            username = data.get("username",      "?")
            message  = data.get("message",       "")

            clock_update(lc)

            log(
                f"[SUB] canal='{channel}' | de='{username}' | msg='{message}' | "
                f"lc={lc} | ts_envio={send_ts} | ts_recebimento={recv_ts}"
            )
        except Exception as e:
            log(f"[SUB] ERRO: {e}")


# ---------------------------------------------------------------------------
# Fluxo principal
# ---------------------------------------------------------------------------
def main():
    time.sleep(3)

    context = zmq.Context()
    socket  = context.socket(zmq.REQ)
    addr    = f"tcp://{BROKER_HOST}:{BROKER_PORT}"
    socket.connect(addr)
    log(f"Conectado ao broker em {addr}")

    # 1. Login com retry
    while True:
        try:
            resp = send_recv(socket, "LOGIN", {"username": BOT_NAME})
            if resp["type"] == "LOGIN_OK":
                log(f"✓ Login OK como '{BOT_NAME}' | lc={clock_get()}")
                break
            reason = resp.get("payload", {}).get("reason", "?")
            log(f"✗ Login falhou: {reason}. Tentando em {RETRY_DELAY}s...")
            socket.close()
            time.sleep(RETRY_DELAY)
            socket = context.socket(zmq.REQ)
            socket.connect(addr)
        except Exception as e:
            log(f"Erro: {e}. Recriando socket...")
            socket.close()
            time.sleep(RETRY_DELAY)
            socket = context.socket(zmq.REQ)
            socket.connect(addr)

    # 2. Listar canais
    time.sleep(1)
    resp     = send_recv(socket, "LIST_CHANNELS", {"username": BOT_NAME})
    channels = resp.get("payload", {}).get("channels", [])
    log(f"Canais disponíveis: {channels}")

    # 3. Se menos de 5 canais, criar um novo
    if len(channels) < 5:
        new_channel = f"canal-{BOT_NAME}-{random.randint(100, 999)}"
        resp = send_recv(socket, "CREATE_CHANNEL", {"username": BOT_NAME, "channel": new_channel})
        if resp["type"] in ("CHANNEL_CREATED", "CHANNEL_EXISTS"):
            if new_channel not in channels:
                channels.append(new_channel)

    # Atualiza lista
    resp     = send_recv(socket, "LIST_CHANNELS", {"username": BOT_NAME})
    channels = resp.get("payload", {}).get("channels", [])

    # 4. Inscrever em até 3 canais aleatórios
    subscribed = []
    available  = list(channels)
    random.shuffle(available)
    while len(subscribed) < 3 and available:
        subscribed.append(available.pop())

    if not subscribed:
        log("Nenhum canal disponível. Encerrando.")
        return

    threading.Thread(target=subscriber_thread, args=(subscribed,), daemon=True).start()
    time.sleep(1)

    # 5. Loop infinito de publicação
    log(f"Iniciando loop de publicação | lc={clock_get()}")
    while True:
        channel = random.choice(channels)
        log(f"Publicando 10 mensagens no canal '{channel}'...")
        for _ in range(10):
            message = random.choice(RANDOM_MESSAGES)
            resp    = send_recv(socket, "PUBLISH", {
                "username": BOT_NAME,
                "channel":  channel,
                "message":  message,
            })
            if resp["type"] == "PUBLISH_OK":
                log(f"✓ Publicado: '{message}' em '{channel}' | lc={clock_get()}")
            else:
                reason = resp.get("payload", {}).get("reason", "?")
                log(f"✗ Erro ao publicar: {reason}")
            time.sleep(1)


if __name__ == "__main__":
    main()
