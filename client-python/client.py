"""
BBS Client Bot (Python) - Parte 2
Loop infinito: inscreve em canais aleatórios e publica mensagens a cada 1 segundo.
"""

import os
import time
import random
import threading
import zmq
import msgpack


BROKER_HOST      = os.environ.get("BROKER_HOST",      "broker")
BROKER_PORT      = int(os.environ.get("BROKER_PORT",      "5555"))
PROXY_HOST       = os.environ.get("PROXY_HOST",       "proxy-pubsub")
PROXY_SUB_PORT   = int(os.environ.get("PROXY_SUB_PORT",   "5558"))
BOT_NAME         = os.environ.get("BOT_NAME",         "bot-python-1")
RETRY_DELAY      = 2

# Mensagens aleatórias para simular conversa
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


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{BOT_NAME} {ts}] {msg}", flush=True)


def make_request(msg_type: str, payload: dict) -> bytes:
    return msgpack.packb({
        "type":      msg_type,
        "timestamp": int(time.time() * 1000),
        "payload":   payload,
    }, use_bin_type=True)


def send_recv(socket, msg_type: str, payload: dict) -> dict:
    raw  = make_request(msg_type, payload)
    out  = msgpack.unpackb(raw, raw=False)
    log(f"ENVIANDO | type={msg_type} | payload={payload} | timestamp={out['timestamp']}")
    socket.send(raw)
    resp = msgpack.unpackb(socket.recv(), raw=False)
    log(f"RECEBIDO | type={resp['type']} | payload={resp['payload']} | timestamp={resp['timestamp']}")
    return resp


# ---------------------------------------------------------------------------
# Thread de recebimento SUB (roda em paralelo ao loop de publicação)
# ---------------------------------------------------------------------------
def subscriber_thread(subscribed_channels: list):
    context = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    addr = f"tcp://{PROXY_HOST}:{PROXY_SUB_PORT}"
    sub_socket.connect(addr)

    for channel in subscribed_channels:
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, channel)
        log(f"[SUB] Inscrito no canal '{channel}'")

    log(f"[SUB] Aguardando mensagens nos canais: {subscribed_channels}")

    while True:
        try:
            parts = sub_socket.recv_multipart()
            recv_ts = int(time.time() * 1000)
            channel = parts[0].decode()
            data    = msgpack.unpackb(parts[1], raw=False)

            send_ts  = data.get("timestamp", 0)
            username = data.get("username",  "?")
            message  = data.get("message",   "")

            log(
                f"[SUB] MENSAGEM RECEBIDA | canal='{channel}' | "
                f"de='{username}' | msg='{message}' | "
                f"timestamp_envio={send_ts} | timestamp_recebimento={recv_ts}"
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
                log(f"✓ Login OK como '{BOT_NAME}'")
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
            log(f"✓ Canal '{new_channel}' pronto.")

    # Atualiza lista de canais
    resp     = send_recv(socket, "LIST_CHANNELS", {"username": BOT_NAME})
    channels = resp.get("payload", {}).get("channels", [])

    # 4. Inscrever em canais aleatórios (mínimo 3)
    subscribed = []
    available  = list(channels)
    random.shuffle(available)
    while len(subscribed) < 3 and available:
        subscribed.append(available.pop())

    if not subscribed:
        log("Nenhum canal disponível para inscrição. Encerrando.")
        return

    # Inicia thread SUB em paralelo
    t = threading.Thread(target=subscriber_thread, args=(subscribed,), daemon=True)
    t.start()

    time.sleep(1)

    # 5. Loop infinito de publicação
    log("Iniciando loop de publicação...")
    while True:
        channel = random.choice(channels)
        log(f"Publicando 10 mensagens no canal '{channel}'...")
        for i in range(10):
            message = random.choice(RANDOM_MESSAGES)
            resp    = send_recv(socket, "PUBLISH", {
                "username": BOT_NAME,
                "channel":  channel,
                "message":  message,
            })
            if resp["type"] == "PUBLISH_OK":
                log(f"✓ Publicado: '{message}' em '{channel}'")
            else:
                reason = resp.get("payload", {}).get("reason", "?")
                log(f"✗ Erro ao publicar: {reason}")
            time.sleep(1)


if __name__ == "__main__":
    main()
