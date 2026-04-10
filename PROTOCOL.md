# Protocolo de Mensagens BBS

## Serialização
Todas as mensagens são serializadas com **MessagePack**.

## Estrutura base de toda mensagem

```
{
  "type":      string,   // tipo da mensagem
  "timestamp": int,      // unix timestamp em milissegundos (obrigatório)
  "payload":   map       // dados específicos do tipo
}
```

## Tipos de mensagem — REQ/REP (via broker)

### Cliente → Servidor

| type             | payload                                              |
|------------------|------------------------------------------------------|
| LOGIN            | { "username": string }                               |
| LIST_CHANNELS    | { "username": string }                               |
| CREATE_CHANNEL   | { "username": string, "channel": string }            |
| PUBLISH          | { "username": string, "channel": string, "message": string } |

### Servidor → Cliente

| type             | payload                                              |
|------------------|------------------------------------------------------|
| LOGIN_OK         | { "username": string }                               |
| LOGIN_ERROR      | { "username": string, "reason": string }             |
| CHANNEL_LIST     | { "channels": [string, ...] }                        |
| CHANNEL_CREATED  | { "channel": string }                                |
| CHANNEL_EXISTS   | { "channel": string }                                |
| CHANNEL_ERROR    | { "reason": string }                                 |
| PUBLISH_OK       | { "channel": string, "timestamp": int }              |
| PUBLISH_ERROR    | { "reason": string }                                 |

## Mensagens PUB/SUB (via proxy)

### Servidor → Proxy → Clientes inscritos

Frame 1 (tópico): nome do canal em bytes  
Frame 2 (corpo): MessagePack com:

```
{
  "channel":   string,
  "username":  string,
  "message":   string,
  "timestamp": int
}
```

## Erros definidos

| reason               | Situação                                      |
|----------------------|-----------------------------------------------|
| invalid_username     | Username vazio                                |
| already_logged_in    | Username já está logado                       |
| invalid_channel_name | Nome de canal vazio ou com espaços            |
| channel_not_found    | Canal não existe ao tentar publicar           |
| empty_message        | Mensagem vazia ao tentar publicar             |
