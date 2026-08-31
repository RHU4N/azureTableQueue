require("dotenv").config();

const express = require("express");
const path = require("path");
const { QueueClient } = require("@azure/storage-queue");
const { TableClient } = require("@azure/data-tables");

const app = express();
const PORT = process.env.PORT || 3000;

const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
const queueName = process.env.AZURE_QUEUE_NAME || "requisicoes-pendentes";
const tableName = process.env.AZURE_TABLE_NAME || "requisicoes-processadas";

if (!connectionString) {
  console.error("ERRO: configure AZURE_STORAGE_CONNECTION_STRING no .env ou nas variáveis da Vercel.");
  process.exit(1);
}

const queueClient = new QueueClient(connectionString, queueName);
const tableClient = TableClient.fromConnectionString(connectionString, tableName);

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

async function garantirRecursos() {
  await queueClient.createIfNotExists();

  try {
    await tableClient.createTable();
  } catch (erro) {
    if (erro.statusCode !== 409) throw erro;
  }
}

// Coloca uma requisição de teste na fila.
// Essa rota existe para simular a entrada de mensagens pendentes.
app.post("/api/requisicoes", async (req, res) => {
  try {
    await garantirRecursos();

    const { usuario, email, tipo, descricao } = req.body;

    if (!usuario || !email || !tipo || !descricao) {
      return res.status(400).json({ erro: "Preencha todos os dados da requisição." });
    }

    const requisicao = {
      usuario,
      email,
      tipo,
      descricao,
      criadaEm: new Date().toISOString()
    };

    const resposta = await queueClient.sendMessage(JSON.stringify(requisicao));

    res.status(201).json({
      sucesso: true,
      mensagem: "Requisição enviada para a fila.",
      messageId: resposta.messageId,
      requisicao
    });
  } catch (erro) {
    console.error(erro);
    res.status(500).json({ erro: "Não foi possível enviar a requisição para a fila." });
  }
});

// Lista mensagens sem removê-las da fila (apenas demonstração).
app.get("/api/fila", async (req, res) => {
  try {
    await garantirRecursos();

    const resposta = await queueClient.peekMessages({ numberOfMessages: 32 });
    const mensagens = resposta.peekedMessageItems.map((mensagem) => ({
      id: mensagem.messageId,
      conteudo: mensagem.messageText,
      inseridaEm: mensagem.insertedOn
    }));

    res.json({
      fila: queueName,
      quantidade: mensagens.length,
      mensagens
    });
  } catch (erro) {
    console.error(erro);
    res.status(500).json({ erro: "Não foi possível consultar a fila." });
  }
});

// NÚCLEO DA ATIVIDADE 5:
// recebe mensagens pendentes -> grava no Table Storage -> exclui da fila.
app.post("/api/processar", async (req, res) => {
  try {
    await garantirRecursos();

    const resposta = await queueClient.receiveMessages({
      numberOfMessages: 10,
      visibilityTimeout: 60
    });

    const mensagens = resposta.receivedMessageItems;
    const processadas = [];

    for (const mensagem of mensagens) {
      try {
        let dados;

        try {
          dados = JSON.parse(mensagem.messageText);
        } catch {
          dados = { mensagem: mensagem.messageText };
        }

        const partitionKey = String(dados.tipo || "requisicao")
          .replace(/[\\/#?]/g, "-");

        const rowKey = `${Date.now()}-${mensagem.messageId}`
          .replace(/[\\/#?]/g, "-");

        const entidade = {
          partitionKey,
          rowKey,
          usuario: String(dados.usuario || "Não informado"),
          email: String(dados.email || "Não informado"),
          tipo: String(dados.tipo || "requisicao"),
          descricao: String(dados.descricao || dados.mensagem || ""),
          criadaEm: String(dados.criadaEm || new Date().toISOString()),
          processadaEm: new Date().toISOString(),
          mensagemOriginal: mensagem.messageText
        };

        // 1) Registra no Table Storage.
        await tableClient.createEntity(entidade);

        // 2) Só exclui da fila depois do registro bem-sucedido.
        await queueClient.deleteMessage(
          mensagem.messageId,
          mensagem.popReceipt
        );

        processadas.push({
          sucesso: true,
          messageId: mensagem.messageId,
          partitionKey,
          rowKey
        });
      } catch (erroMensagem) {
        console.error(`Erro ao processar ${mensagem.messageId}:`, erroMensagem);

        // Não exclui a mensagem quando o Table Storage falha.
        processadas.push({
          sucesso: false,
          messageId: mensagem.messageId,
          erro: erroMensagem.message
        });
      }
    }

    res.json({
      fila: queueName,
      tabela: tableName,
      quantidadeRecebida: mensagens.length,
      processadas
    });
  } catch (erro) {
    console.error(erro);
    res.status(500).json({ erro: "Erro ao processar as mensagens." });
  }
});

// Lista as entidades já registradas no Table Storage.
app.get("/api/tabela", async (req, res) => {
  try {
    await garantirRecursos();

    const entidades = [];
    for await (const entidade of tableClient.listEntities()) {
      entidades.push(entidade);
    }

    res.json({
      tabela: tableName,
      quantidade: entidades.length,
      entidades
    });
  } catch (erro) {
    console.error(erro);
    res.status(500).json({ erro: "Não foi possível consultar o Table Storage." });
  }
});

app.get("/api/status", async (req, res) => {
  try {
    await garantirRecursos();
    const propriedades = await queueClient.getProperties();

    res.json({
      fila: queueName,
      tabela: tableName,
      mensagensPendentes: propriedades.approximateMessagesCount
    });
  } catch (erro) {
    console.error(erro);
    res.status(500).json({ erro: "Não foi possível consultar o Azure Storage." });
  }
});

module.exports = app;

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Aplicação rodando em http://localhost:${PORT}`);
    console.log(`Queue Storage: ${queueName}`);
    console.log(`Table Storage: ${tableName}`);
  });
}
