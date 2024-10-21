import os
import urllib.request
from dotenv import load_dotenv

# Carregar as variáveis do arquivo .env
load_dotenv()

# Função para baixar os dados de uma URL e salvar em um arquivo local
def extract_data(url, filename):
    try:
        urllib.request.urlretrieve(url, filename)
        print(f"Arquivo {filename} baixado com sucesso!")
    except Exception as e:
        print(f"Erro ao baixar o arquivo {filename}: {e}")

# URLs e nomes de arquivos para os dados de Boston dos anos de 2015 a 2020
urls = [
    "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/c9509ab4-6f6d-4b97-979a-0cf2a10c922b/download/311_service_requests_2015.csv",
    "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/b7ea6b1b-3ca4-4c5b-9713-6dc1db52379a/download/311_service_requests_2016.csv",
    "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/30022137-709d-465e-baae-ca155b51927d/download/311_service_requests_2017.csv",
    "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/2be28d90-3a90-4af1-a3f6-f28c1e25880a/download/311_service_requests_2018.csv",
    "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/ea2e4696-4a2d-429c-9807-d02eb92e0222/download/311_service_requests_2019.csv",
    "https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/6ff6a6fd-3141-4440-a880-6f60a37fe789/download/script_105774672_20210108153400_combine.csv"
]

# Caminhos onde os arquivos serão salvos localmente
file_paths = [
    "data/dados_2015.csv",
    "data/dados_2016.csv",
    "data/dados_2017.csv",
    "data/dados_2018.csv",
    "data/dados_2019.csv",
    "data/dados_2020.csv"
]

def main():
    # Extração dos dados
    for url, file_path in zip(urls, file_paths):
        extract_data(url, file_path)

if __name__ == "__main__":
    main()
