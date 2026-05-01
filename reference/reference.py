"""
Serviço de Referência — Parte 4
Mudança em relação à parte 3:
  - HEARTBEAT_OK não retorna mais o campo 'clock'
    (sincronização de relógio agora é feita entre servidores via eleição/Berkeley)
"""

import os
import time
import threading
import zmq
import msgpack

REQ_PORT          = int(os.environ.get("REF_REQ_PORT",      "5560"))
HEARTBEAT_TIMEOUT = int(os.environ.get("HEARTBEAT_TIMEOUT", "60"))


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[REFERENCE {ts}] {msg}", flush=True)


lock      = threading.Lock()
servers: dict = {}
next_rank = 1


def register_server(name: str) -> int:
    global next_rank
    with lock:
        if name in servers:
            return servers[name]["rank"]
        rank = next_rank
        next_rank += 1
        servers[name] = {"rank": rank, "last_seen": time.time()}
        log(f"Servidor registrado: '{name}' → rank {rank}")
        return rank


def heartbeat(name: str):
    with lock:
        if name not in servers:
            register_server(name)
        servers[name]["last_seen"] = time.time()


def get_server_list() -> list:
    with lock:
        return [{"name": n, "rank": d["rank"]} for n, d in servers.items()]


def watchdog():
    while True:
        time.sleep(10)
        now = time.time()
        with lock:
            dead = [n for n, d in servers.items()
                    if now - d["last_seen"] > HEARTBEAT_TIMEOUT]
            for name in dead:
                log(f"Servidor removido por inatividade: '{name}'")
                del servers[name]


def handle(msg: dict) -> dict:
    msg_type = msg.get("type", "")
    payload  = msg.get("payload", {})

    if msg_type == "GET_RANK":
        name = payload.get("name", "").strip()
        if not name:
            return {"type": "ERROR", "payload": {"reason": "missing_name"}}
        rank = register_server(name)
        log(f"GET_RANK ← '{name}' → rank={rank}")
        return {"type": "RANK", "payload": {"rank": rank}}

    elif msg_type == "LIST_SERVERS":
        srv_list = get_server_list()
        log(f"LIST_SERVERS → {len(srv_list)} servidor(es)")
        return {"type": "SERVER_LIST", "payload": {"servers": srv_list}}

    elif msg_type == "HEARTBEAT":
        name = payload.get("name", "").strip()
        if not name:
            return {"type": "ERROR", "payload": {"reason": "missing_name"}}
        heartbeat(name)
        log(f"HEARTBEAT ← '{name}'")
        # Parte 4: não retorna mais 'clock' — sincronização feita entre servidores
        return {"type": "HEARTBEAT_OK", "payload": {}}

    else:
        log(f"Tipo desconhecido: '{msg_type}'")
        return {"type": "ERROR", "payload": {"reason": f"unknown_type: {msg_type}"}}


def main():
    threading.Thread(target=watchdog, daemon=True).start()

    context = zmq.Context()
    rep = context.socket(zmq.REP)
    rep.bind(f"tcp://*:{REQ_PORT}")
    log(f"Serviço de referência iniciado na porta {REQ_PORT}")
    log(f"Timeout de heartbeat: {HEARTBEAT_TIMEOUT}s")

    while True:
        try:
            raw  = rep.recv()
            msg  = msgpack.unpackb(raw, raw=False)
            resp = handle(msg)
            resp["timestamp"] = int(time.time() * 1000)
            rep.send(msgpack.packb(resp, use_bin_type=True))
        except Exception as e:
            log(f"ERRO: {e}")
            rep.send(msgpack.packb(
                {"type": "ERROR", "payload": {"reason": str(e)},
                 "timestamp": int(time.time() * 1000)},
                use_bin_type=True
            ))


if __name__ == "__main__":
    main()
