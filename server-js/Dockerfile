FROM node:20-slim

WORKDIR /app

# Dependências nativas (better-sqlite3 precisa de build tools)
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*

COPY package.json .
RUN npm install --omit=dev

COPY server.js .

CMD ["node", "server.js"]
