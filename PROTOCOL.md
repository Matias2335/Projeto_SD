# Protocolo de Mensagens BBS

## Serialização
Todas as mensagens são serializadas com **MessagePack**.

## Estrutura base de toda mensagem

```
{
  "type":      string,   // tipo da mensagem (ver abaixo)
  "timestamp": int,      // unix timestamp em milissegundos (obrigatório)
  "payload":   map       // dados específicos do tipo
}
```

## Tipos de mensagem (type)

### Cliente → Broker → Servidor

| type             | payload campos                        |
|------------------|---------------------------------------|
| LOGIN            | { "username": string }                |
| LIST_CHANNELS    | { "username": string }                |
| CREATE_CHANNEL   | { "username": string, "channel": string } |

### Servidor → Broker → Cliente

| type             | payload campos                                      |
|------------------|-----------------------------------------------------|
| LOGIN_OK         | { "username": string }                              |
| LOGIN_ERROR      | { "username": string, "reason": string }            |
| CHANNEL_LIST     | { "channels": [string, ...] }                       |
| CHANNEL_CREATED  | { "channel": string }                               |
| CHANNEL_EXISTS   | { "channel": string }                               |
| CHANNEL_ERROR    | { "reason": string }                                |

## Regras de erro

- Login duplicado (mesmo username já logado): LOGIN_ERROR com reason "already_logged_in"
- Nome de usuário vazio: LOGIN_ERROR com reason "invalid_username"
- Nome de canal vazio ou com espaços: CHANNEL_ERROR com reason "invalid_channel_name"
- Canal já existente: CHANNEL_EXISTS (não é erro fatal, cliente apenas usa o canal)
