import os
from zipfile import ZipFile
import requests
import os
import io

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