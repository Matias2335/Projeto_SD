"""
BBS Proxy PUB/SUB
XSUB na porta 5557 — servidores publicam aqui
XPUB na porta 5558 — clientes se inscrevem aqui
O proxy repassa automaticamente todas as mensagens dos tópicos.
"""

import zmq
import time


XSUB_PORT = 5557  # servidores conectam aqui como PUB
XPUB_PORT = 5558  # clientes conectam aqui como SUB


def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[PROXY-PUBSUB {ts}] {msg}", flush=True)


def main():
    context = zmq.Context()

    # XSUB recebe publicações dos servidores
    xsub = context.socket(zmq.XSUB)
    xsub.bind(f"tcp://*:{XSUB_PORT}")

    # XPUB distribui para os clientes inscritos
    xpub = context.socket(zmq.XPUB)
    xpub.bind(f"tcp://*:{XPUB_PORT}")

    log(f"Proxy PUB/SUB iniciado. XSUB={XSUB_PORT}, XPUB={XPUB_PORT}")

    try:
        # proxy repassa mensagens entre XSUB e XPUB automaticamente
        zmq.proxy(xsub, xpub)
    except KeyboardInterrupt:
        log("Proxy encerrado.")
    finally:
        xsub.close()
        xpub.close()
        context.term()


if __name__ == "__main__":
    main()
