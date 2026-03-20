# BBS - Bulletin Board System (Parte 1)

Sistema de troca de mensagens inspirado nos antigos BBS/IRC, desenvolvido para a disciplina de Sistemas Distribuídos.

---

## O que esse projeto faz?

Simula um sistema de mensagens onde **bots** (clientes automáticos) se conectam a servidores e realizam ações sem nenhuma interação humana. Ao iniciar o projeto, os bots sobem automaticamente e executam:

1. **Login** — cada bot se identifica no servidor pelo nome
2. **Listagem de canais** — o bot consulta quais canais existem
3. **Criação de canal** — o bot cria um canal novo (se ainda não existir)
4. **Nova listagem** — confirma que o canal foi criado com sucesso

---

## Como executar

Você só precisa ter o **Docker** instalado. Depois, rode:

```bash
docker compose up --build
```

Isso vai:
- Construir todas as imagens automaticamente
- Subir o broker, os 4 servidores e os 4 bots
- Exibir no terminal todas as mensagens trocadas em tempo real

Para parar, pressione `Ctrl + C`.

> Não é necessário nenhuma configuração manual. Tudo sobe e funciona sozinho.

---

## O que aparece no terminal?

Cada mensagem enviada ou recebida é exibida no terminal com o formato:

```
[bot-js-2 23:15:50] ENVIANDO | type=LOGIN | payload={"username":"bot-js-2"} | timestamp=1773962150836
[bot-js-2 23:15:50] RECEBIDO | type=LOGIN_OK | payload={"username":"bot-js-2"} | timestamp=1773962150873
[bot-js-2 23:15:50] ✓ Login realizado com sucesso como 'bot-js-2'
```

---

## Arquitetura

O projeto é composto por três tipos de componentes:

```
[bot-python-1] ──┐
[bot-python-2] ──┤                    ┌── [server-python-1]
[bot-js-1]     ──┼──► BROKER (ZMQ) ──┤── [server-python-2]
[bot-js-2]     ──┘    ROUTER/DEALER  ├── [server-js-1]
                                      └── [server-js-2]
```

| Componente | Quantidade | Função |
|---|---|---|
| **Broker** | 1 | Recebe mensagens dos bots e distribui entre os servidores |
| **Servidores** | 4 (2 Python + 2 JS) | Processam as requisições e salvam os dados |
| **Bots (clientes)** | 4 (2 Python + 2 JS) | Enviam login, listam e criam canais automaticamente |

As mensagens chegam ao broker e são distribuídas entre os servidores em **round-robin** (um por vez, na ordem). Cada servidor guarda seus próprios dados de forma independente.

---

## Estrutura de arquivos

```
bbs-project/
├── docker-compose.yaml   ← orquestra todos os containers
├── README.md             ← este arquivo
├── PROTOCOL.md           ← documentação técnica do protocolo de mensagens
├── broker/               ← broker que roteia as mensagens
│   ├── Dockerfile
│   └── broker.py
├── server-python/        ← servidor em Python
│   ├── Dockerfile
│   └── server.py
├── client-python/        ← bot em Python
│   ├── Dockerfile
│   └── client.py
├── server-js/            ← servidor em JavaScript (Node.js)
│   ├── Dockerfile
│   ├── package.json
│   └── server.js
└── client-js/            ← bot em JavaScript (Node.js)
    ├── Dockerfile
    ├── package.json
    └── client.js
```

---

## Escolhas técnicas

### Linguagens
- **Python 3.12** — usado no servidor e no cliente
- **JavaScript com Node.js 20** — usado no servidor e no cliente

### Serialização: MessagePack
As mensagens trocadas entre bots e servidores são serializadas em **binário** usando MessagePack — semelhante ao JSON, porém mais compacto e eficiente. Toda mensagem segue esta estrutura:

```
{
  "type":      string,   // o que a mensagem representa (ex: LOGIN, CREATE_CHANNEL)
  "timestamp": int,      // momento do envio em milissegundos
  "payload":   map       // dados da mensagem (ex: nome do usuário, nome do canal)
}
```

### Persistência: SQLite
Cada servidor salva seus dados em um arquivo `.db` (SQLite) próprio, isolado dos outros servidores. Os dados são mantidos mesmo após reiniciar os containers.

O que é salvo:
- **Logins:** nome do usuário + horário do login
- **Canais:** nome do canal + quem criou + horário de criação

### ZeroMQ: padrão ROUTER/DEALER
O broker usa o padrão `ROUTER/DEALER` do ZeroMQ para receber mensagens dos bots (`REQ`) e distribuir para os servidores (`REP`) de forma balanceada, sem configuração manual de endereços.

---

## Tratamento de erros

| Situação | O que acontece |
|---|---|
| Nome de usuário vazio | Servidor recusa com `invalid_username` |
| Bot tenta logar com nome já em uso | Servidor recusa com `already_logged_in` |
| Nome de canal com espaço ou vazio | Servidor recusa com `invalid_channel_name` |
| Canal já existente | Servidor avisa com `CHANNEL_EXISTS` — bot usa o canal existente |
| Falha de comunicação no login | Bot fecha o socket, aguarda 2 segundos e tenta novamente |
