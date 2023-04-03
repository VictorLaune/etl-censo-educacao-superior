from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from zipfile import ZipFile
import os
import requests
import os
import io

# Criando o SparkSession
spark = SparkSession\
   .builder\
   .master("local")\
   .appName("convert_to_parquet")\
   .getOrCreate()

# Automatiza o download dos arquivos
def baixando_e_extraindo_arquivos(anos_para_download):
    for ano in anos_para_download:
        print("\nRealizando download dos dados do ano: " + ano + "\n")
        url = (
            "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_" + ano + ".zip"
        )
        try:
            solicitacao = requests.get(url, verify=False)
            dado_zipado = ZipFile(io.BytesIO(solicitacao.content))
            dado_zipado.extractall("./download/")
        except:
            return

# Renomeia as pastas que estavam com caracteres fora do utf-8
def renomeando_pastas():
    lista_pastas = ([name for name in os.listdir("./download")])
    for pasta in lista_pastas:
        os.rename(f"./download/{pasta}", f"./data/{pasta[-4:]}")

# Verificar se a pasta possui o nome DADOS ou dados
def verifica_pasta(nome_da_pasta):
    try:
        path = f"./data/{nome_da_pasta}/dados"
    except:
        print("Pasta com o nome 'DADOS'")
        try:
            path = f"./data/{nome_da_pasta}/DADOS"
        except:
            print("Error!")
    return path

def conversao_parquet_csv_sep_ponto_e_virgula(path, arquivo, pasta):
    params = {'header':True, 'inferSchema':True, 'sep':';'}
    path_csv = path + '/' + arquivo
    df = (
        spark
        .read
        .csv(path_csv, **params)
        )

    df = df.select([f.col(coluna).alias(coluna.lower()) for coluna in df.columns])

    df.write.parquet(f"to_parquet/{pasta}/{arquivo}", mode='overwrite')

def conversao_parquet_csv_sep_barra(path, arquivo, pasta):
    params = {'header':True, 'inferSchema':True, 'sep':'|'}
    path_csv = path + '/' + arquivo
    df = (
        spark
        .read
        .csv(path_csv, **params)
        )

    df = df.select([f.col(coluna).alias(coluna.lower()) for coluna in df.columns])

    df.write.parquet(f"to_parquet/{pasta}/{arquivo}", mode='overwrite')