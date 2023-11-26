import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, regexp_replace, year , lit
from pyspark.sql.types import FloatType
from awsglue.utils import getResolvedOptions
import boto3
import json


##########################################################
# Código do Glue job 1 - Primeiras transformações no Lake#
##########################################################

# Argumentos passados para o Glue Job
args = getResolvedOptions(sys.argv, [
                                    'JOB_NAME', 
                                    'SOURCE_LAYER', 
                                    'DESTINY_LAYER',
                                    'S3_JOB_PARAMS',
                                    'PARAMS_KEY_JOB'
                                    ])



# Atribuir os argumentos estaticos do job para variáveis
bronze_layer = args['SOURCE_LAYER'] # camada origem, bronze
silver_layer = args['DESTINY_LAYER'] # camada destino, silver
s3_job_params = args['S3_JOB_PARAMS'] # bucket para acessar os parametros dinamicos para ler e escrever
params_key = args['PARAMS_KEY_JOB'] # chave do objeto s3 onde esse job lerá os parametros dinamicos escritos pela lambda

# Inicializar cliente S3 para interação
s3_client = boto3.client('s3')

# Parametros dinâmicos recebidos do arquivo json criado pela lambda
try:
    obj = s3_client.get_object(Bucket=s3_job_params, Key=params_key) # pegando o json no bucket de parâmetros
    params_content = json.loads(obj['Body'].read()) # ler o json
    file_name = params_content['FILENAME'] # Parâmetro 1 - nome do arquivo, pego do json
    datalake_bucket = params_content['DATALAKE_BUCKET']  # Parâmetro 2 - nome do bucket do datalake, pego do json
    print(f'Parâmetros recebidos: FILENAME={file_name}, DATALAKE_BUCKET={datalake_bucket}')
except Exception as e:
    print(f'Erro ao ler o arquivo de parâmetros: {str(e)}')

# Definir os caminhos completos dos buckets
bronze_path = f's3://{datalake_bucket}/{bronze_layer}'
silver_path = f's3://{datalake_bucket}/{silver_layer}'
print(f'bronze_path: {bronze_path}')
print(f'silver_path: {silver_path}')

# Inicializar a sessão Spark
spark = SparkSession.builder.appName(args['JOB_NAME']).getOrCreate()

############################## Extracao
# Ler o arquivo CSV da camada Bronze
df_bronze = spark.read.csv(f'{bronze_path}/{file_name}.csv', header=True, inferSchema=False)  # inferSchema=False mantém tudo como string, sem alterar dado ao tentar inferir o schema
print('Arquivo csv lido com sucesso')

############################## Transformacao
# 1. Corrigir os tipos de dados e formatação do separador decimal de ',' para '.'
df = df_bronze.withColumn("Timestamp do Registro", to_timestamp("Timestamp do Registro", "yyyy-MM-dd HH:mm:ss")) \
              .withColumn("Valor da Compra", regexp_replace("Valor da Compra", ",", ".").cast(FloatType())) \
              .withColumn("Desconto Oferecido", regexp_replace("Desconto Oferecido", "%", "").cast(FloatType()) / 100)
print('Tipos de dados corrigidos')

# 2. Excluir registros duplicados
df = df.dropDuplicates()
print('Registros duplicados excluidos')

# 3. Tratar valores nulos com valores padrão
df = df.fillna({'Desconto Oferecido': 0, 'Código de Desconto Usado': 'NÃO DISPONÍVEL', 'Vendedor': 'NÃO INFORMADO'})
print('Valores nulos tratados')

# 4. Extrair a informação de ano da timestamp e guardar na nova coluna ANO
df = df.withColumn("Ano", year("Timestamp do Registro"))
print('Criada nova coluna de "Ano"')

# 5. Criar coluna para armazenar o nome do arquivo de origem do dado
df = df.withColumn("Arquivo de Origem", lit(file_name))
print('Criada nova coluna de "Arquivo de Origem"')

# Printando o resultado
print(f'Schema resultante: {df.printSchema}')

############################## Carregamento (Load)
# 6. Escrever os dados como arquivo Parquet particionado por ano, em pasta com nome do arquivo, na camada Silver
output_path = f'{silver_path}/{file_name}'
df.write.mode("overwrite").partitionBy("Ano").parquet(output_path)
print(f'Arquivo parquet com dados transformados particionados por Ano e enviados para pasta {file_name} em {silver_path}')

# Fechar a sessão Spark
spark.stop()