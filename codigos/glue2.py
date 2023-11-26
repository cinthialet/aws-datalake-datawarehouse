import sys
import boto3
import json
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F 
from pyspark.sql.functions import initcap, col, split, when, hour,year  


################################################################
# Código do Glue job 2 - Segunda etapa de transformacao no Lake#
################################################################

# Argumentos passados para o Glue Job
args = getResolvedOptions(sys.argv, [
                                    'JOB_NAME', 
                                    'SOURCE_LAYER', 
                                    'DESTINY_LAYER',
                                    'S3_JOB_PARAMS',
                                    'PARAMS_KEY_JOB'
                                    ])

# Atribuir os argumentos estaticos do job para variáveis
silver_layer = args['SOURCE_LAYER'] # camada origem, silver
gold_layer = args['DESTINY_LAYER'] # camada destino, gold
s3_job_params = args['S3_JOB_PARAMS'] # bucket para ler parâmetros dinâmicos escritos pela lambda
params_key = args['PARAMS_KEY_JOB'] # chave do objeto s3 onde esse job buscará os parametros dinamicos

# Inicializar cliente S3 para interação
s3_client = boto3.client('s3')

# Parametros dinâmicos recebidos do arquivo json criado pelo glue job 1
try:
    obj = s3_client.get_object(Bucket=s3_job_params, Key=params_key) # pegando o json no bucket de parâmetros
    params_content = json.loads(obj['Body'].read()) # ler o json
    file_name = params_content['FILENAME'] # Parâmetro 1 - nome do arquivo, pego do json
    datalake_bucket = params_content['DATALAKE_BUCKET']  # Parâmetro 2 - nome do bucket do datalake, pego do json
    print(f'Parâmetros recebidos: FILENAME={file_name}, DATALAKE_BUCKET={datalake_bucket}')
except Exception as e:
    print(f'Erro ao ler o arquivo de parâmetros: {str(e)}')

# Definir os caminhos completos dos buckets
silver_path = f's3://{datalake_bucket}/{silver_layer}'
gold_path = f's3://{datalake_bucket}/{gold_layer}'
print(f'silver_path: {silver_path}')
print(f'gold_path: {gold_path}')

# Inicializar a sessão Spark
spark = SparkSession.builder.appName(args['JOB_NAME']).getOrCreate()

############################## Extracao
# 1 - Extrair os dados do arquivo Parquet da camada Silver, considerando o particionamento por ANO
silver_data_path = f'{silver_path}/{file_name}/'  # Caminho até a pasta que contém os dados particionados por ANO

## 'option("basePath", silver_data_path)' é usada para garantir que o Spark entenda a estrutura de diretórios de particionamento
#df_silver = spark.read.option("basePath", silver_data_path).parquet(silver_data_path)
df_silver = spark.read.parquet(silver_data_path)

print('Dados lidos da camada silver com sucesso')

############################## Transformacao
# 2 - Padronização dos nomes das colunas para lower case e substituição de espaços por '_'
df = df_silver.toDF(*[c.lower().replace(" ", "_") for c in df_silver.columns])

# 2.1 - Retirar cedilha  e acentos dos nomes das colunas
# Mapeamento de caracteres acentuados para os seus equivalentes não acentuados
mapeamento_acentos_cedilha = {
    'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a', 'å': 'a', 'ç': 'c',
    'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
    'ñ': 'n', 'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o',
    'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ý': 'y', 'ÿ': 'y'
}

# Criando uma função para substituir acentos e cedilha em uma string
def tira_acento_cedilha(texto):
    """
    Remove acentos e cedilha de todos os caracteres de uma string.

    Esta função recebe uma string e substitui todos os caracteres acentuados 
    e a cedilha ('ç') por seus equivalentes sem acentuação, utilizando um 
    mapeamento pré-definido. O mapeamento deve ser fornecido em um dicionário 
    global chamado `mapeamento_acentos_cedilha`, onde cada chave é um caractere 
    acentuado ou 'ç', e cada valor é o caractere correspondente sem acento.

    Parameters:
    texto (str): A string de entrada que contém os caracteres a serem substituídos.

    Returns:
    str: Uma nova string com os caracteres acentuados e a cedilha substituídos por 
    seus equivalentes não acentuados.
    """
    for letra_acentuada, letra_corrigida in mapeamento_acentos_cedilha.items(): # O '.items()' retorna par chave-valor do dicionário, iterando sobre ambos ao mesmo tempo.
        texto = texto.replace(letra_acentuada, letra_corrigida)
    return texto

# Substituindo os acentos em cada nome de coluna e a cedilha
for nome_coluna in df.columns:
    novo_nome_coluna = tira_acento_cedilha(nome_coluna)
    df = df.withColumnRenamed(nome_coluna, novo_nome_coluna)

print(f'Colunas padronizadas com sucesso: {df.columns}')

# 3 - Concatenação dos campos 'nome' e 'sobrenome' para criar o nome completo do cliente
df = df.withColumn("nome_completo", F.initcap(F.concat_ws(" ", F.col("nome"), F.col("sobrenome"))))

print('Coluna de nome completo criada')


# 4 - Extração do domínio do email
df = df.withColumn("dominio_email", F.split(F.col("email"), "@")[1]) # [1] para pegar o segundo elemento da divisão
print('Coluna  com dominio de email criada')


# 5 - Classificação do momento da compra a cada 6 horas
df = df.withColumn("turno_compra",
                                           F.when(F.hour("timestamp_do_registro") < 6, "Madrugada")
                                           .when(F.hour("timestamp_do_registro") < 12, "Manhã")
                                           .when(F.hour("timestamp_do_registro") < 18, "Tarde")
                                           .otherwise("Noite"))

print('Coluna com classificacao dos tunos da venda criada')

# 6 - Cálculo do valor da compra com descontos aplicados (assumindo que o valor da compra está na coluna 'valor_da_compra' e o desconto em 'desconto_oferecido')
df = df.withColumn("valor_com_desconto", F.col("valor_da_compra") * (1 - F.col("desconto_oferecido")))

print('Coluna com valor com desconto aplicado criada')

############################## Carregamento (Load)
# 7 - Carregar os dados transformados para o bucket da camada Gold como parquet
output_path = f'{gold_path}/{file_name}' # camada/nome_do_arquivo

## extraindo o Ano para uma coluna nova, para poder criar a particao. 
## Aqui, a col de timestamp já passou pela padronizacao do passo 2
df = df.withColumn("ano", year("timestamp_do_registro"))

## reordenando as colunas para fazer mais sentido para o usuario final
colunas_ordenadas = [
    'timestamp_do_registro', 'turno_compra', 'nome', 'sobrenome', 'nome_completo', 'email', 
    'dominio_email', 'telefone', 'produto_adquirido', 'valor_da_compra', 'desconto_oferecido', 
    'valor_com_desconto', 'canal', 'plataforma_de_interacao', 'campanha_de_marketing', 
    'vendedor', 'codigo_de_desconto_usado', 'arquivo_de_origem', 'ano'
    ]
### '*' para o spark pegar cada elemento seperadamente da lista das colunas.
df = df.select(*colunas_ordenadas) 
print(f'Colunas reordenadas:{df.printSchema}')

## escrevendo como parquet na gold, particionado por ano e segmentado por nome do arquivo
df.write.mode("overwrite").partitionBy("ano").parquet(output_path)

print(f'Arquivo parquet com dados transformados particionados por Ano e enviados para pasta {file_name} em {gold_path}')

# Fechar a sessão Spark
spark.stop()