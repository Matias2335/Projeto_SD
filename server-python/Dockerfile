FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pyzmq msgpack

COPY server.py .

CMD ["python", "server.py"]
