"""
BBS Server (Python) - Parte 2
Adicionado: handler PUBLISH, conexão ao proxy PUB/SUB, persistência de publicações.
"""

import os
import time
import sqlite3
import zmq
import msgpack


BROKER_HOST  = os.environ.get("BROKER_HOST",  "broker")
BROKER_PORT  = int(os.environ.get("BROKER_PORT",  "5556"))
PROXY_HOST   = os.environ.get("PROXY_HOST",   "proxy-pubsub")
PROXY_PORT   = int(os.environ.get("PROXY_PORT",   "5557"))
SERVER_ID    = os.environ.get("SERVER_ID",    "python-server-1")
DB_PATH      = os.environ.get("DB_PATH",      f"/data/{SERVER_ID}.db")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{SERVER_ID} {ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------
def init_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logins (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            username  TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            name       TEXT PRIMARY KEY,
            created_by TEXT NOT NULL,
            timestamp  INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS publications (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            channel   TEXT NOT NULL,
            username  TEXT NOT NULL,
            message   TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        )
    """)
    conn.commit()
    return conn


def save_login(conn, username, ts):
    conn.execute("INSERT INTO logins (username, timestamp) VALUES (?, ?)", (username, ts))
    conn.commit()


def channel_exists(conn, name) -> bool:
    return conn.execute("SELECT 1 FROM channels WHERE name = ?", (name,)).fetchone() is not None


def save_channel(conn, name, created_by, ts):
    conn.execute("INSERT INTO channels (name, created_by, timestamp) VALUES (?, ?, ?)", (name, created_by, ts))
    conn.commit()


def list_channels(conn) -> list:
    return [r[0] for r in conn.execute("SELECT name FROM channels ORDER BY name").fetchall()]


def save_publication(conn, channel, username, message, ts):
    conn.execute(
        "INSERT INTO publications (channel, username, message, timestamp) VALUES (?, ?, ?, ?)",
        (channel, username, message, ts)
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Protocolo
# ---------------------------------------------------------------------------
def make_response(msg_type: str, payload: dict) -> bytes:
    return msgpack.packb({
        "type": msg_type,
        "timestamp": int(time.time() * 1000),
        "payload": payload,
    }, use_bin_type=True)


def parse_message(raw: bytes) -> dict:
    return msgpack.unpackb(raw, raw=False)


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------
logged_in_users: set = set()


def handle_login(conn, payload, ts):
    username = payload.get("username", "").strip()
    if not username:
        log("LOGIN_ERROR: username vazio")
        return make_response("LOGIN_ERROR", {"username": "", "reason": "invalid_username"})
    if username in logged_in_users:
        log(f"LOGIN_ERROR: '{username}' já está logado")
        return make_response("LOGIN_ERROR", {"username": username, "reason": "already_logged_in"})
    logged_in_users.add(username)
    save_login(conn, username, ts)
    log(f"LOGIN_OK: '{username}' | timestamp={ts}")
    return make_response("LOGIN_OK", {"username": username})


def handle_list_channels(conn, payload):
    channels = list_channels(conn)
    log(f"LIST_CHANNELS: {len(channels)} canal(is) → {channels}")
    return make_response("CHANNEL_LIST", {"channels": channels})


def handle_create_channel(conn, payload, ts):
    channel  = payload.get("channel", "").strip()
    username = payload.get("username", "unknown")
    if not channel or " " in channel:
        log(f"CHANNEL_ERROR: nome inválido '{channel}'")
        return make_response("CHANNEL_ERROR", {"reason": "invalid_channel_name"})
    if channel_exists(conn, channel):
        log(f"CHANNEL_EXISTS: '{channel}' já existe")
        return make_response("CHANNEL_EXISTS", {"channel": channel})
    save_channel(conn, channel, username, ts)
    log(f"CHANNEL_CREATED: '{channel}' por '{username}' | timestamp={ts}")
    return make_response("CHANNEL_CREATED", {"channel": channel})


def handle_publish(conn, pub_socket, payload, ts):
    channel  = payload.get("channel", "").strip()
    username = payload.get("username", "unknown")
    message  = payload.get("message", "").strip()

    if not channel or not channel_exists(conn, channel):
        log(f"PUBLISH_ERROR: canal '{channel}' não existe")
        return make_response("PUBLISH_ERROR", {"reason": "channel_not_found"})
    if not message:
        log("PUBLISH_ERROR: mensagem vazia")
        return make_response("PUBLISH_ERROR", {"reason": "empty_message"})

    # Monta o frame PUB: tópico = nome do canal, corpo = msgpack
    pub_payload = msgpack.packb({
        "channel":   channel,
        "username":  username,
        "message":   message,
        "timestamp": ts,
    }, use_bin_type=True)

    # ZeroMQ PUB/SUB: primeiro frame é o tópico (string), segundo é o corpo
    pub_socket.send_multipart([channel.encode(), pub_payload])
    save_publication(conn, channel, username, message, ts)
    log(f"PUBLISH_OK: canal='{channel}' | user='{username}' | msg='{message}' | timestamp={ts}")
    return make_response("PUBLISH_OK", {"channel": channel, "timestamp": ts})


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------
def main():
    conn = init_db(DB_PATH)
    log(f"Banco de dados inicializado em '{DB_PATH}'")

    context = zmq.Context()

    # Socket REP — recebe requisições via broker
    rep_socket = context.socket(zmq.REP)
    rep_addr = f"tcp://{BROKER_HOST}:{BROKER_PORT}"
    rep_socket.connect(rep_addr)
    log(f"Conectado ao broker em {rep_addr}")

    # Socket PUB — publica no proxy PUB/SUB
    pub_socket = context.socket(zmq.PUB)
    pub_addr = f"tcp://{PROXY_HOST}:{PROXY_PORT}"
    pub_socket.connect(pub_addr)
    log(f"Conectado ao proxy PUB/SUB em {pub_addr}")

    # Pequena pausa para o socket PUB estabilizar
    time.sleep(0.5)
    log("Aguardando mensagens...")

    while True:
        try:
            raw = rep_socket.recv()
            msg      = parse_message(raw)
            msg_type = msg.get("type", "")
            payload  = msg.get("payload", {})
            ts       = msg.get("timestamp", int(time.time() * 1000))

            log(f"RECEBIDO | type={msg_type} | payload={payload} | timestamp={ts}")

            if msg_type == "LOGIN":
                response = handle_login(conn, payload, ts)
            elif msg_type == "LIST_CHANNELS":
                response = handle_list_channels(conn, payload)
            elif msg_type == "CREATE_CHANNEL":
                response = handle_create_channel(conn, payload, ts)
            elif msg_type == "PUBLISH":
                response = handle_publish(conn, pub_socket, payload, ts)
            else:
                log(f"TIPO DESCONHECIDO: '{msg_type}'")
                response = make_response("ERROR", {"reason": f"unknown_type: {msg_type}"})

            resp_parsed = parse_message(response)
            log(f"ENVIANDO | type={resp_parsed['type']} | payload={resp_parsed['payload']}")
            rep_socket.send(response)

        except Exception as e:
            log(f"ERRO: {e}")
            rep_socket.send(make_response("ERROR", {"reason": str(e)}))


if __name__ == "__main__":
    main()
