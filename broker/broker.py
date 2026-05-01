"""
BBS Broker - Roteia mensagens entre clientes e servidores usando ZeroMQ ROUTER/DEALER.
Clientes conectam no FRONTEND (porta 5555) usando REQ.
Servidores conectam no BACKEND (porta 5556) usando REP.
"""

import zmq
import time

FRONTEND_PORT = 5555
BACKEND_PORT  = 5556

def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[BROKER {ts}] {msg}", flush=True)

def main():
    context  = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind(f"tcp://*:{FRONTEND_PORT}")
    backend  = context.socket(zmq.DEALER)
    backend.bind(f"tcp://*:{BACKEND_PORT}")
    log(f"Broker iniciado. Frontend={FRONTEND_PORT}, Backend={BACKEND_PORT}")
    try:
        zmq.proxy(frontend, backend)
    except KeyboardInterrupt:
        log("Broker encerrado.")
    finally:
        frontend.close()
        backend.close()
        context.term()

if __name__ == "__main__":
    main()
