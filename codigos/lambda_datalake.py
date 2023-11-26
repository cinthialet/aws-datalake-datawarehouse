import json
import boto3
from datetime import datetime
import os

##########################################################
# Código da Lambda - Execução de ações após ser disparada#
##########################################################

# Criar clientes para a lambda interagir com o S3 e Glue
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    # Imprimir a estrutura do evento recebido
    print("Evento recebido:",json.dumps(event)) # json.dumps para estruturar o evento no print do cloudwatch, para ficar mais legível

    # Obter os valores das variáveis de ambiente
    datalake_bucket = os.environ['DATALAKE_BUCKET']
    datalake_layer = os.environ['DATALAKE_LAYER']
    workflow_name = os.environ['GLUE_WORKFLOW_NAME']
    s3_job_params = os.environ['S3_JOB_PARAMS'] # bucket para guardar parametros dinamicos dos glue jobs
    params_key = os.environ['PARAMS_KEY_JOB'] # chave do objeto s3 onde os parametros serao escritos

    # Obter informações do evento S3
    ## Obter nome do bucket de origem (bucket_name)
    ## Obter nome do arquivo a ser processado (original_key)
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    original_key = event['Records'][0]['s3']['object']['key']
    
    print("Bucket:", bucket_name)
    print("Key original:", original_key)

    try:
        # Configurar o novo nome do arquivo com o timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S') # UTC time
        new_key = f"{timestamp}_{original_key}"  # Nome para renomear o arquivo no bucket de origem
        caminho_datalake = f"{datalake_layer}/{new_key}"  # Caminho final do arquivo no bucket de destino. '/' identifica que 'datalake_layer' será uma pasta no bucket.
        
        # Renomear o arquivo no bucket de origem
        print("Renomeando o arquivo no bucket de origem...")
        s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': original_key}, Key=new_key)
        s3_client.delete_object(Bucket=bucket_name, Key=original_key)   #deletando o arquivo original
        print("Arquivo renomeado.")

        # Copiar o arquivo para a camada especificada no bucket do Data Lake
        print(f"Iniciando a cópia do arquivo para a camada {datalake_layer} do Data Lake...")
        s3_client.copy_object(Bucket=datalake_bucket, CopySource={'Bucket': bucket_name, 'Key': new_key}, Key=caminho_datalake)
        print("Cópia concluída.")

        # Deletar o arquivo renomeado no bucket de origem
        print("Deletando o arquivo renomeado no bucket de origem...")
        s3_client.delete_object(Bucket=bucket_name, Key=new_key)
        print("Arquivo deletado.")

        # Salvando apenas o nome do arquivo para a passar para o glue job, sem extensao
        # rsplit começa a divisao pelo fim da string
        # rsplit dividirá pelo ponto, 1 vez, e pega o [0], primeiro elemento, que é o nome do arquivo sem a extensão
        file_name = new_key.rsplit('.', 1)[0] 

        # Preparar o conteúdo do arquivo de parâmetros dinâmicos
        params = {
            'FILENAME': file_name,
            'DATALAKE_BUCKET': datalake_bucket
        }

        # Escrever o arquivo de parâmetros no S3
        print(f"Escrevendo o arquivo de parâmetros em s3://{s3_job_params}/{params_key}...")
        s3_client.put_object(Bucket=s3_job_params, Key=params_key, Body=json.dumps(params))
        print('Arquivo de parâmetros foi escrito com sucesso')

        # Iniciar o Workflow do Glue
        print(f"Iniciando o Workflow {workflow_name}...")
        response = glue_client.start_workflow_run(Name=workflow_name)
        print("Workflow iniciado com ID:", response['RunId'])

        print("Processamento concluído com sucesso!")

    except Exception as e:
        print("Erro:", e)