# Atividade 5 — Table Storage e Processamento Orientado a Mensagens

Aplicação Node.js que integra **Azure Queue Storage** e **Azure Table Storage**.

## Fluxo implementado

1. Uma requisição de usuário é transformada em uma mensagem JSON e enviada para o Queue Storage.
2. O endpoint `/api/processar` consome mensagens pendentes da fila.
3. Para cada mensagem, os dados são estruturados em uma entidade do Table Storage.
4. A entidade recebe `PartitionKey` e `RowKey`.
5. **Somente depois de `createEntity()` concluir com sucesso**, a mensagem correspondente é removida da fila com `deleteMessage()`.
6. Se a gravação na tabela falhar, a mensagem não é excluída, evitando a perda da mensagem e permitindo novo processamento.

## Endpoints

- `POST /api/requisicoes` — adiciona uma requisição de teste à fila.
- `GET /api/fila` — consulta mensagens da fila sem removê-las.
- `POST /api/processar` — executa o fluxo principal da atividade.
- `GET /api/tabela` — lista as entidades processadas.
- `GET /api/status` — mostra o nome da fila, tabela e quantidade aproximada de mensagens.

## Variáveis de ambiente

```env
AZURE_STORAGE_CONNECTION_STRING=SUA_CONNECTION_STRING
AZURE_QUEUE_NAME=requisicoes-pendentes
AZURE_TABLE_NAME=requisicoes-processadas
```

Não publique o `.env` no GitHub.

## Execução local

```bash
npm install
npm start
```

Acesse `http://localhost:3000`.

## Vercel

Configure as três variáveis de ambiente na Vercel e faça um novo deploy.
