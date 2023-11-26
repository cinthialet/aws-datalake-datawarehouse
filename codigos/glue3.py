import sys
import boto3
import json
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

#####################################################################
# Código do Glue job 3 - Carregar dados da Gold para tabela Redshift#
#####################################################################

# Argumentos passados para o Glue Job
args = getResolvedOptions(sys.argv, [
                                    'JOB_NAME', 
                                    'SOURCE_LAYER', 
                                    'S3_JOB_PARAMS',
                                    'PARAMS_KEY_JOB',
                                    'REDSHIFT_DB',
                                    'REDSHIFT_TABLE', 
                                    'REDSHIFT_SCHEMA',
                                    'TempDir'
                                    ])

# Atribuir os argumentos estaticos do job para variáveis
gold_layer = args['SOURCE_LAYER'] # camada origem, gold
s3_job_params = args['S3_JOB_PARAMS'] # bucket para guardar parâmetros dinâmicos do próximo glue
params_key = args['PARAMS_KEY_JOB'] # chave do objeto s3 onde esse job buscará os parametros dinamicos
redshift_db = args['REDSHIFT_DB'] # banco do redshift
redshift_schema = args['REDSHIFT_SCHEMA'] # schema do redshift
redshift_table = args['REDSHIFT_TABLE'] # nome da tabela no redshift
glue_tempdir = args['TempDir']


# Inicializar cliente S3
s3_client = boto3.client('s3')

# Parametros dinâmicos recebidos do arquivo json criado pelo glue job 2
try:
    obj = s3_client.get_object(Bucket=s3_job_params, Key=params_key) # pegando o json no bucket de parâmetros
    params_content = json.loads(obj['Body'].read()) # ler o json
    file_name = params_content['FILENAME'] # Parâmetro 1 - nome do arquivo, pego do json
    datalake_bucket = params_content['DATALAKE_BUCKET']  # Parâmetro 2 - nome do bucket do datalake, pego do json
    print(f'Parâmetros recebidos: FILENAME={file_name}, DATALAKE_BUCKET={datalake_bucket}')
except Exception as e:
    print(f'Erro ao ler o arquivo de parâmetros: {str(e)}')

print('Todos os parametros GLUE carregados com sucesso')

# Definir os caminhos completos dos buckets
gold_path = f's3://{datalake_bucket}/{gold_layer}'
print(f'gold_path: {gold_path}')

# Inicializar o GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

############################## Extracao
# Extrair os dados da Gold Layer
gold_data_path = f'{gold_path}/{file_name}/' # Caminho até a pasta que contém os dados particionados por ano
## 'option("basePath", silver_data_path)' é usada para garantir que o Spark entenda a estrutura de diretórios de particionamento
df_gold = spark.read.parquet(gold_data_path)
print('Dados extraídos da gold com sucesso')

# Nome completo da tabela, incluindo o schema
full_table_name = f"{redshift_schema}.{redshift_table}"
print(f'Dados serão inseridos em {full_table_name}')

print('Iniciando conexão com Redshift...')
# Criar DynamicFrame a partir do  DataFrame Spark
dynamic_frame_write = DynamicFrame.fromDF(df_gold, glueContext, "dynamic_frame_write")
print('Amostra de dados que serão inseridos:')
print(dynamic_frame_write.show(2))

############################## Carregamento (Load)
# Escrever dados no Redshift usando a conexão configurada no Data Catalog
# https://docs.aws.amazon.com/pt_br/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame-writer.html
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = dynamic_frame_write,                 # O DynamicFrame que contém os dados a serem escritos
    catalog_connection = "dyf-conn",             # Nome da conexão com o banco de dados no catálogo do Glue
    connection_options = {
        "dbtable": full_table_name,              
        "database": redshift_db                  
    },
    redshift_tmp_dir = glue_tempdir,          # Diretório temporário para operações intermediárias
    transformation_ctx = "write_dynamic_frame"   # Contexto de transformação
)

print(f"Dados inseridos na tabela {full_table_name}")

# Encerrar a sessão Spark
spark.stop()
