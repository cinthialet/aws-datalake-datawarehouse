# Estudo de Caso: Solução de Pipeline de Dados Automatizada para Análise de Vendas Multicanais com Data Lake e Data Warehouse Modelado

### Stack:
AWS, Spark/Pyspark, Python, SQL, AWS Lambda, AWS Glue (Jobs, Workflow), AWS CloudWatch, AWS IAM, AWS Redshift, AWS S3

## Estudo de Caso:
Uma marca de varejo enfrentava desafios na integração dos dados de vendas de diversas plataformas. Era essencial consolidar esses dados em um sistema unificado para análise e inteligência de negócios.

### Requisitos:
- Automatização da ingestão e processamento de arquivos CSV.
- Limpeza, transformação e aplicação de regras de negócios nos dados.
- Estruturação do Data Lake em camadas para facilitar o monitoramento.
- Preocupação com performance e custo de armazenamento.
- Estruturação de um Data Warehouse atualizado e rastreável.

### Sobre os Dados de Origem (RAW):
Os dados são fictícios, criados através de prompt para o ChatGPT com ajustes manuais.

#### Schema RAW/Bronze (csv):
- Timestamp do Registro
- Nome
- Sobrenome
- Email
- Telefone
- Produto Adquirido
- Valor da Compra
- Canal
- Plataforma de Interação
- Campanha de Marketing
- Vendedor
- Desconto Oferecido
- Código de Desconto Usado

### Arquitetura Desenvolvida:
![Imagem da Arquitetura](link-da-imagem-aqui)

### Passo a Passo do Processo pela Arquitetura:
1. **Lambda detecta o evento de carregar o csv no bucket inicial:**
   1.1. Renomear o arquivo, colocando a timestamp do evento.
   1.2. Mover o arquivo CSV renomeado do bucket de entrada para o da camada Bronze.
   1.3. Deletar o arquivo CSV do bucket de entrada.
   1.4. Iniciar o Workflow da Pipeline de dados.

2. **Glue job 1 usa Spark para a primeira camada de tratamento de dados (transformações simples):**
   2.1. Extrair os dados do arquivo CSV no bucket da camada Bronze.
   2.2. Correção da tipagem dos dados das colunas.
   2.3. Tratamento de dados duplicados.
   2.4. Tratamento de valores NULL.
   2.5. Extração do ano das datas para uma nova coluna.
   2.6. Criar partições dos dados por ano.
   2.7. Criar coluna para identificar de qual arquivo de entrada vieram os dados.
   2.8. Carregar os dados transformados para o bucket da camada Silver como parquet.
   2.9. O Workflow garante que, com o sucesso do Glue job 1, inicia-se o Glue job 2.

3. **Glue job 2 usa Spark para a segunda camada de tratamento de dados (transformações de negócio):**
   3.1. Extrair os dados do arquivo parquet no bucket da camada Silver.
   3.2. Padronização dos nomes das colunas.
   3.3. Padronização do nome dos clientes - nome completo.
   3.4. Extração do domínio do email.
   3.5. Classificação do momento da compra.
   3.6. Cálculo do valor da compra com descontos aplicados.
   3.7. Carregar os dados transformados para o bucket da camada Gold como parquet particionado por ano.
   3.8. O Workflow garante que, com o sucesso do Glue job 2, inicia-se o Glue job 3.

4. **Glue job 3 usa Spark para conectar ao DW e carregar os dados da camada Gold nele:**
   4.1. Extrair os dados do bucket da camada Gold.
   4.2. Conectar com o Redshift via conexão JDBC.
   4.3. Inserir os dados na tabela previamente criada manualmente no Redshift.
   4.4. Encerrar a conexão.

5. **Criação da Modelagem de Dados no Redshift:**
   5.1. Criação da estrutura das tabelas (FATO e DIMs) e inserção dos respectivos dados.
   5.2. Agendamento da query de criação das tabelas para que elas sejam refeitas periodicamente, mantendo os dados atualizados.


### Schema Silver (parquet particionado por ano):
- Nome
- Sobrenome
- Email
- Telefone
- Produto Adquirido
- Valor da Compra
- Canal
- Plataforma de Interação
- Campanha de Marketing
- Vendedor
- Desconto Oferecido
- Código de Desconto Usado
- Arquivo de Origem

### Schema Gold (parquet particionado por ano):
- timestamp_do_registro
- turno_compra
- nome_completo
- email
- dominio_email
- telefone
- produto_adquirido
- valor_da_compra
- desconto_oferecido
- valor_com_desconto
- canal
- plataforma_de_interacao
- campanha_de_marketing
- vendedor
- codigo_de_desconto_usado
- arquivo_de_origem

### Sobre a Modelagem de Dados:
Abordagem clássica de tabelas DIMENSÃO e FATO.

![Diagrama de Modelagem](link-do-diagrama-aqui)
