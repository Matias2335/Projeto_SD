FROM python:3.13-alpine

WORKDIR /app

RUN pip install --no-cache-dir pyzmq msgpack

COPY reference.py .

CMD ["python", "reference.py"]
