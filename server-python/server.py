"""
BBS Server (Python) - Parte 3
Adicionado:
  - Relógio lógico de Lamport (incrementa antes de enviar, max ao receber)
  - Registro no serviço de referência (GET_RANK) na inicialização
  - Heartbeat periódico a cada 10 mensagens → sincroniza relógio físico
"""

import os
import time
import sqlite3
import threading
import zmq
import msgpack


BROKER_HOST   = os.environ.get("BROKER_HOST",   "broker")
BROKER_PORT   = int(os.environ.get("BROKER_PORT",   "5556"))
PROXY_HOST    = os.environ.get("PROXY_HOST",    "proxy-pubsub")
PROXY_PORT    = int(os.environ.get("PROXY_PORT",    "5557"))
REF_HOST      = os.environ.get("REF_HOST",      "reference")
REF_PORT      = int(os.environ.get("REF_PORT",      "5560"))
SERVER_ID     = os.environ.get("SERVER_ID",     "python-server-1")
DB_PATH       = os.environ.get("DB_PATH",       f"/data/{SERVER_ID}.db")

HEARTBEAT_EVERY = 10   # mensagens de clientes


# ---------------------------------------------------------------------------
# Relógio lógico de Lamport
# ---------------------------------------------------------------------------
_clock_lock    = threading.Lock()
_logical_clock = 0


def clock_tick() -> int:
    """Incrementa o relógio antes de enviar. Retorna novo valor."""
    global _logical_clock
    with _clock_lock:
        _logical_clock += 1
        return _logical_clock


def clock_update(received: int):
    """Atualiza o relógio ao receber (max + 1)."""
    global _logical_clock
    with _clock_lock:
        _logical_clock = max(_logical_clock, received) + 1


def clock_get() -> int:
    with _clock_lock:
        return _logical_clock


# ---------------------------------------------------------------------------
# Relógio físico — offset ajustado pelo serviço de referência
# ---------------------------------------------------------------------------
_physical_offset_ms = 0


def physical_now_ms() -> int:
    return int(time.time() * 1000) + _physical_offset_ms


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
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT    NOT NULL,
            timestamp     INTEGER NOT NULL,
            logical_clock INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            name          TEXT    PRIMARY KEY,
            created_by    TEXT    NOT NULL,
            timestamp     INTEGER NOT NULL,
            logical_clock INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS publications (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            channel       TEXT    NOT NULL,
            username      TEXT    NOT NULL,
            message       TEXT    NOT NULL,
            timestamp     INTEGER NOT NULL,
            logical_clock INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.commit()
    return conn


def save_login(conn, username, ts, lc):
    conn.execute(
        "INSERT INTO logins (username, timestamp, logical_clock) VALUES (?, ?, ?)",
        (username, ts, lc))
    conn.commit()


def channel_exists(conn, name) -> bool:
    return conn.execute(
        "SELECT 1 FROM channels WHERE name = ?", (name,)).fetchone() is not None


def save_channel(conn, name, created_by, ts, lc):
    conn.execute(
        "INSERT INTO channels (name, created_by, timestamp, logical_clock) VALUES (?, ?, ?, ?)",
        (name, created_by, ts, lc))
    conn.commit()


def list_channels(conn) -> list:
    return [r[0] for r in
            conn.execute("SELECT name FROM channels ORDER BY name").fetchall()]


def save_publication(conn, channel, username, message, ts, lc):
    conn.execute(
        "INSERT INTO publications (channel, username, message, timestamp, logical_clock)"
        " VALUES (?, ?, ?, ?, ?)",
        (channel, username, message, ts, lc))
    conn.commit()


# ---------------------------------------------------------------------------
# Protocolo helpers
# ---------------------------------------------------------------------------
def make_response(msg_type: str, payload: dict) -> bytes:
    lc = clock_tick()
    return msgpack.packb({
        "type":          msg_type,
        "timestamp":     physical_now_ms(),
        "logical_clock": lc,
        "payload":       payload,
    }, use_bin_type=True)


def parse_message(raw: bytes) -> dict:
    return msgpack.unpackb(raw, raw=False)


# ---------------------------------------------------------------------------
# Comunicação com o serviço de referência
# ---------------------------------------------------------------------------
_rank       = 0
_msg_count  = 0
_ref_socket = None
_ref_lock   = threading.Lock()


def ref_request(sock, msg_type: str, payload: dict) -> dict:
    lc  = clock_tick()
    raw = msgpack.packb({
        "type":          msg_type,
        "timestamp":     physical_now_ms(),
        "logical_clock": lc,
        "payload":       payload,
    }, use_bin_type=True)
    sock.send(raw)
    resp     = msgpack.unpackb(sock.recv(), raw=False)
    recv_lc  = resp.get("logical_clock", 0)
    clock_update(recv_lc)
    return resp


def register_with_reference(context):
    global _rank, _ref_socket
    _ref_socket = context.socket(zmq.REQ)
    _ref_socket.connect(f"tcp://{REF_HOST}:{REF_PORT}")
    resp  = ref_request(_ref_socket, "GET_RANK", {"name": SERVER_ID})
    _rank = resp.get("payload", {}).get("rank", 0)
    log(f"Registrado no serviço de referência | rank={_rank}")


def do_heartbeat():
    """Heartbeat → sincroniza relógio físico usando o algoritmo de Cristian."""
    global _physical_offset_ms
    try:
        with _ref_lock:
            t_send   = int(time.time() * 1000)
            resp     = ref_request(_ref_socket, "HEARTBEAT", {"name": SERVER_ID})
            t_recv   = int(time.time() * 1000)

        ref_clock = resp.get("payload", {}).get("clock", 0)
        if ref_clock:
            rtt_half            = (t_recv - t_send) // 2
            _physical_offset_ms = ref_clock + rtt_half - t_recv
            log(f"HEARTBEAT OK | clock_ref={ref_clock} | offset={_physical_offset_ms:+d}ms")
    except Exception as e:
        log(f"HEARTBEAT ERRO: {e}")


# ---------------------------------------------------------------------------
# Handlers de mensagem
# ---------------------------------------------------------------------------
logged_in_users: set = set()


def handle_login(conn, payload, ts, recv_lc):
    clock_update(recv_lc)
    username = payload.get("username", "").strip()
    if not username:
        return make_response("LOGIN_ERROR", {"username": "", "reason": "invalid_username"})
    if username in logged_in_users:
        return make_response("LOGIN_ERROR", {"username": username, "reason": "already_logged_in"})
    logged_in_users.add(username)
    lc = clock_get()
    save_login(conn, username, ts, lc)
    log(f"LOGIN_OK: '{username}' | ts={ts} | lc={lc}")
    return make_response("LOGIN_OK", {"username": username})


def handle_list_channels(conn, payload, recv_lc):
    clock_update(recv_lc)
    channels = list_channels(conn)
    log(f"LIST_CHANNELS: {len(channels)} canal(is) | lc={clock_get()}")
    return make_response("CHANNEL_LIST", {"channels": channels})


def handle_create_channel(conn, payload, ts, recv_lc):
    clock_update(recv_lc)
    channel  = payload.get("channel", "").strip()
    username = payload.get("username", "unknown")
    if not channel or " " in channel:
        return make_response("CHANNEL_ERROR", {"reason": "invalid_channel_name"})
    if channel_exists(conn, channel):
        return make_response("CHANNEL_EXISTS", {"channel": channel})
    lc = clock_get()
    save_channel(conn, channel, username, ts, lc)
    log(f"CHANNEL_CREATED: '{channel}' | ts={ts} | lc={lc}")
    return make_response("CHANNEL_CREATED", {"channel": channel})


def handle_publish(conn, pub_socket, payload, ts, recv_lc):
    clock_update(recv_lc)
    channel  = payload.get("channel", "").strip()
    username = payload.get("username", "unknown")
    message  = payload.get("message", "").strip()

    if not channel or not channel_exists(conn, channel):
        return make_response("PUBLISH_ERROR", {"reason": "channel_not_found"})
    if not message:
        return make_response("PUBLISH_ERROR", {"reason": "empty_message"})

    lc = clock_tick()
    pub_payload = msgpack.packb({
        "channel":       channel,
        "username":      username,
        "message":       message,
        "timestamp":     physical_now_ms(),
        "logical_clock": lc,
    }, use_bin_type=True)

    pub_socket.send_multipart([channel.encode(), pub_payload])
    save_publication(conn, channel, username, message, physical_now_ms(), lc)
    log(f"PUBLISH_OK: canal='{channel}' | user='{username}' | lc={lc}")
    return make_response("PUBLISH_OK", {"channel": channel, "timestamp": ts})


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------
def main():
    global _msg_count

    conn    = init_db(DB_PATH)
    context = zmq.Context()

    register_with_reference(context)

    rep_socket = context.socket(zmq.REP)
    rep_socket.connect(f"tcp://{BROKER_HOST}:{BROKER_PORT}")
    log(f"Conectado ao broker em {BROKER_HOST}:{BROKER_PORT}")

    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(f"tcp://{PROXY_HOST}:{PROXY_PORT}")
    log(f"Conectado ao proxy PUB/SUB em {PROXY_HOST}:{PROXY_PORT}")

    time.sleep(0.5)
    log(f"Servidor pronto | rank={_rank} | lc={clock_get()}")

    while True:
        try:
            raw      = rep_socket.recv()
            msg      = parse_message(raw)
            msg_type = msg.get("type", "")
            payload  = msg.get("payload", {})
            ts       = msg.get("timestamp", physical_now_ms())
            recv_lc  = msg.get("logical_clock", 0)

            log(f"RECEBIDO | type={msg_type} | lc_msg={recv_lc} | lc_local={clock_get()}")

            if msg_type == "LOGIN":
                response = handle_login(conn, payload, ts, recv_lc)
            elif msg_type == "LIST_CHANNELS":
                response = handle_list_channels(conn, payload, recv_lc)
            elif msg_type == "CREATE_CHANNEL":
                response = handle_create_channel(conn, payload, ts, recv_lc)
            elif msg_type == "PUBLISH":
                response = handle_publish(conn, pub_socket, payload, ts, recv_lc)
            else:
                clock_update(recv_lc)
                response = make_response("ERROR", {"reason": f"unknown_type: {msg_type}"})

            rep_socket.send(response)

            # Heartbeat a cada 10 mensagens recebidas de clientes
            _msg_count += 1
            if _msg_count % HEARTBEAT_EVERY == 0:
                threading.Thread(target=do_heartbeat, daemon=True).start()

        except Exception as e:
            log(f"ERRO: {e}")
            rep_socket.send(make_response("ERROR", {"reason": str(e)}))


if __name__ == "__main__":
    main()
