"""
BBS Client Bot (Python) - Realiza login, lista canais e cria um canal automaticamente.
Não requer interação humana.
"""

import os
import time
import zmq
import msgpack


BROKER_HOST = os.environ.get("BROKER_HOST", "broker")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "5555"))
BOT_NAME = os.environ.get("BOT_NAME", "bot-python-1")
CHANNEL_TO_CREATE = os.environ.get("CHANNEL_TO_CREATE", "geral-python")
RETRY_DELAY = 2  # segundos entre tentativas de login


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{BOT_NAME} {ts}] {msg}", flush=True)


def make_request(msg_type: str, payload: dict) -> bytes:
    return msgpack.packb({
        "type": msg_type,
        "timestamp": int(time.time() * 1000),
        "payload": payload,
    }, use_bin_type=True)


def send_recv(socket, msg_type: str, payload: dict) -> dict:
    raw = make_request(msg_type, payload)
    parsed_out = msgpack.unpackb(raw, raw=False)
    log(f"ENVIANDO | type={msg_type} | payload={payload} | timestamp={parsed_out['timestamp']}")
    socket.send(raw)
    response_raw = socket.recv()
    response = msgpack.unpackb(response_raw, raw=False)
    log(f"RECEBIDO | type={response['type']} | payload={response['payload']} | timestamp={response['timestamp']}")
    return response


def do_login(socket) -> bool:
    """Tenta fazer login. Retorna True se bem-sucedido."""
    response = send_recv(socket, "LOGIN", {"username": BOT_NAME})
    if response["type"] == "LOGIN_OK":
        log(f"✓ Login realizado com sucesso como '{BOT_NAME}'")
        return True
    else:
        reason = response.get("payload", {}).get("reason", "desconhecido")
        log(f"✗ Falha no login: {reason}. Tentando novamente em {RETRY_DELAY}s...")
        return False


def do_list_channels(socket) -> list[str]:
    response = send_recv(socket, "LIST_CHANNELS", {"username": BOT_NAME})
    channels = response.get("payload", {}).get("channels", [])
    if channels:
        log(f"✓ Canais disponíveis: {channels}")
    else:
        log("✓ Nenhum canal disponível ainda.")
    return channels


def do_create_channel(socket, channel_name: str):
    response = send_recv(socket, "CREATE_CHANNEL", {
        "username": BOT_NAME,
        "channel": channel_name,
    })
    rtype = response["type"]
    if rtype == "CHANNEL_CREATED":
        log(f"✓ Canal '{channel_name}' criado com sucesso!")
    elif rtype == "CHANNEL_EXISTS":
        log(f"→ Canal '{channel_name}' já existe, usando o existente.")
    else:
        reason = response.get("payload", {}).get("reason", "desconhecido")
        log(f"✗ Erro ao criar canal '{channel_name}': {reason}")


def main():
    # Aguarda broker e servidores subirem
    time.sleep(3)

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    addr = f"tcp://{BROKER_HOST}:{BROKER_PORT}"
    socket.connect(addr)
    log(f"Conectado ao broker em {addr}")

    # 1. Login (com retry em caso de falha)
    while True:
        try:
            if do_login(socket):
                break
        except Exception as e:
            log(f"Erro de comunicação: {e}. Recriando socket...")
            socket.close()
            time.sleep(RETRY_DELAY)
            socket = context.socket(zmq.REQ)
            socket.connect(addr)
        time.sleep(RETRY_DELAY)

    # 2. Listar canais disponíveis
    time.sleep(1)
    channels = do_list_channels(socket)

    # 3. Criar canal (se ainda não existir)
    time.sleep(1)
    if CHANNEL_TO_CREATE not in channels:
        do_create_channel(socket, CHANNEL_TO_CREATE)
    else:
        log(f"→ Canal '{CHANNEL_TO_CREATE}' já estava na lista.")

    # 4. Listar novamente para confirmar
    time.sleep(1)
    log("Listando canais após criação:")
    do_list_channels(socket)

    log("Bot finalizou todas as tarefas da Parte 1.")
    socket.close()
    context.term()


if __name__ == "__main__":
    main()
