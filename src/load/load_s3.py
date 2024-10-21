import os
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from io import BytesIO
from dotenv import load_dotenv

# Carregar as variáveis do arquivo .env
load_dotenv()

# Obter as variáveis de ambiente
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name = os.getenv('AWS_REGION')

# Função para ler múltiplos arquivos CSV e retornar um dicionário de DataFrames
def load_data(file_list):
    data_frames = {}
    for file_path in file_list:
        try:
            year = file_path.split("_")[-1].split(".")[0]
            data_frames[year] = pd.read_csv(file_path)
            print(f"Dados de {year} carregados com sucesso!")
        except Exception as e:
            print(f"Erro ao carregar o arquivo {file_path}: {e}")
    return data_frames

# Função para fazer upload de um DataFrame para o S3 como arquivo Parquet
def upload_dataframe_to_s3(dfs, bucket_name):
    s3 = boto3.client("s3", 
                      aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key, 
                      region_name=region_name)
    for ano, df in dfs.items():
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow')
        parquet_buffer.seek(0)
        
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=f"bronze/dados_{ano}.parquet",
                Body=parquet_buffer.getvalue()
            )
            print(f"Dados de {ano} enviados com sucesso para o S3.")
        except ClientError as e:
            print(f"Erro ao enviar os dados de {ano}: {e}")
        finally:
            parquet_buffer.close()

# Função para fazer upload de um arquivo específico para o S3
def upload_to_s3(file_name, bucket_name, object_name):
    s3 = boto3.client("s3", 
                      aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key, 
                      region_name=region_name)
    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print(f"Arquivo {file_name} enviado com sucesso para {object_name} no bucket {bucket_name}!")
    except ClientError as e:
        print(f"Erro durante o upload para o S3: {e}")

def main():
    # Caminhos dos arquivos
    file_paths = [
        "data/raw/dados_2015.csv",
        "data/raw/dados_2016.csv",
        "data/raw/dados_2017.csv",
        "data/raw/dados_2018.csv",
        "data/raw/dados_2019.csv",
        "data/raw/dados_2020.csv"
    ]
    
    # Carregar os dados
    dfs = load_data(file_paths)

    # Fazer upload dos DataFrames como arquivos Parquet para o S3
    upload_dataframe_to_s3(dfs, 'datalakeaws-pipeline')

    # Fazer upload de um arquivo CSV específico para o S3
    upload_to_s3("data/raw/dados_2015.csv", 'datalakeaws-pipeline', 'bronze/dados_2015.csv')

if __name__ == "__main__":
    main()
