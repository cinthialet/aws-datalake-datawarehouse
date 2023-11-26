# Pipeline de Dados Automatizada para Análise de Vendas Multicanais com Data Lake e Data Warehouse Modelado

## Stack
AWS, Spark/Pyspark, Python, SQL, AWS Lambda, AWS Glue (Jobs, Workflow), AWS CloudWatch, AWS IAM, AWS Redshift, AWS S3

## Vídeos
[Arquitetura]()

[Serviços AWS]()

[Redshift e Modelagem]()

[Demo]()

## Estudo de Caso
Uma marca de varejo requisitou uma solução para a integração dos dados de vendas de diversas plataformas. 
Era essencial consolidar esses dados em um sistema unificado para análise e inteligência de negócios.

### Requisitos do Caso
- Identificar o arquivo correto para iniciar o processamento : arquivos .csv que iniciam com a palavra 'registros'
- Identificar, para cada registro, o arquivo de origem ingerido com a data/hora em que foi ingerido na solução - Lambda
- Automatização da ingestão e processamento de arquivos CSV oriundos de diversas plataformas - AWS Glue Workflow
- Estruturação de solução para integração e centralização dos dados - Data Lake
- Limpeza, transformação e aplicação de regras de negócios nos dados - Pipeline ETL
- Preocupação com performance e custo de armazenamento - Spark, particionamento dos dados, arquivo parquet
- Estruturação de um Data Warehouse atualizado e rastreável - Modelagem de dados Estrela

## Sobre os Dados de Origem (RAW)
Os dados são fictícios, criados através de prompt no ChatGPT e com revisões e ajustes manuais para a consistência de dados.
[Dados corretos para ingestão](https://github.com/cinthialet/aws-datalake-datawarehouse/tree/main/dados/arquivos_consumidos_correto)

## Arquitetura da Solução
![Imagem da Arquitetura](https://github.com/cinthialet/aws-datalake-datawarehouse/blob/main/img/datalake-aws-projeto-arquitetura.png)

### Passo a Passo do Processo pela Arquitetura
1. **Lambda detecta o evento de carregar o csv no bucket inicial:**
 - 1.1. Renomear o arquivo, colocando a timestamp do evento.
 - 1.2. Mover o arquivo CSV renomeado do bucket de entrada para o da camada Bronze.
 - 1.3. Deletar o arquivo CSV do bucket de entrada.
 - 1.4. Iniciar o Workflow da Pipeline de dados.

[Codigo da Lambda](link)

2. **Glue job 1 usa Spark para a primeira camada de tratamento de dados (transformações simples)**
 - 2.1. Extrair os dados do arquivo CSV no bucket da camada Bronze.
 - 2.2. Correção da tipagem dos dados das colunas.
 - 2.3. Tratamento de dados duplicados (Registros 100% iguais)
 - 2.4. Tratamento de valores NULL.
 - 2.5. Extração do ano das datas para uma nova coluna.
 - 2.6. Criar partições dos dados por ano.
 - 2.7. Criar coluna para identificar de qual arquivo de entrada vieram os dados.
 - 2.8. Carregar os dados transformados para o bucket da camada Silver como parquet.
 - 2.9. O Workflow garante que, com o sucesso do Glue job 1, inicia-se o Glue job 2.
   
[Transformações Glue Job 1](link-da-imagem-aqui)

[Codigo do Glue1](link)

3. **Glue job 2 usa Spark para a segunda camada de tratamento de dados (transformações de negócio)**
 - 3.1. Extrair os dados do arquivo parquet no bucket da camada Silver.
 - 3.2. Padronização dos nomes das colunas.
 - 3.3. Padronização do nome dos clientes - nome completo.
 - 3.4. Extração do domínio do email.
 - 3.5. Classificação do momento da compra.
 - 3.6. Cálculo do valor da compra com descontos aplicados.
 - 3.7. Carregar os dados transformados para o bucket da camada Gold como parquet particionado por ano.
 - 3.8. O Workflow garante que, com o sucesso do Glue job 2, inicia-se o Glue job 3.
   
[Transformações Glue Job 2](link-da-imagem-aqui)

[Codigo do Glue2](link)

4. **Glue job 3 usa Spark para conectar ao DW e carregar os dados da camada Gold nele**
   - 4.1. Extrair os dados do bucket da camada Gold.
   - 4.2. Conectar com o Redshift via conexão JDBC.
   - 4.3. Inserir os dados na tabela previamente criada manualmente no Redshift.
   - 4.4. Encerrar a conexão.

[Codigo do Glue3](link)

5. **Criação da Modelagem de Dados no Redshift**
   - 5.1. Criação da estrutura das tabelas (FATO e DIMs) e inserção dos respectivos dados.
   - 5.2. Agendamento da query de criação das tabelas para que elas sejam refeitas periodicamente, mantendo os dados atualizados.
     
[Queries do Redshift](link)

Criação Manual da tabela : 

![Criação Manual da Tabela](https://github.com/cinthialet/aws-datalake-datawarehouse/blob/main/img/redshift-criacao-tabela.png)

## Camadas do Datalake
### Schema RAW/Bronze (csv):
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
### Camada Bronze na AWS
![Imagem da bronze](https://github.com/cinthialet/aws-datalake-datawarehouse/blob/main/img/datalake_bronze.png)

### Schema Silver (parquet particionado por ano)
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
### Camada Silver na AWS
![Imagem da bronze](https://github.com/cinthialet/aws-datalake-datawarehouse/blob/main/img/datalake_silver1%20e%202.png)

### Schema Gold (parquet particionado por ano)
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
### Camada Gold na AWS
![Imagem da bronze](https://github.com/cinthialet/aws-datalake-datawarehouse/blob/main/img/datalake_gold1%202%202.png)

## Modelagem de Dados
Abordagem clássica de tabelas DIMENSÃO e FATO.
Os identificadores únicos de cada registro (chave primária) foram criados no Redshift com SQL.

![Diagrama de Modelagem](https://github.com/cinthialet/aws-datalake-datawarehouse/blob/main/img/diagrama-relacional-de-entidades-.png)
