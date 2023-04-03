from helper import *
import shutil

# Definindo os anos que serao selecionados para download
anos_para_download = []
ano = 2021
ano_final = 1995


while ano >= ano_final:
    anos_para_download.append(str(ano))
    ano -= 1

print("Anos que iremos utilizar para a extração:" + str(anos_para_download) + "\n")

baixando_e_extraindo_arquivos(anos_para_download)

# Criando pasta data, renomeando arquivos não utf-8 e removendo pasta download
os.mkdir("./data")
renomeando_pastas()
os.rmdir('./download')

# Listando pastas fazermos a verificacao 
pastas_para_conversao = ([pasta for pasta in os.listdir("./data")])


for pasta in pastas_para_conversao:
    path = verifica_pasta(pasta)
    lista_arquivos = ([arquivo for arquivo in os.listdir(f"{path}")])
    for arquivo in lista_arquivos:
        if(arquivo[-3:] == 'CSV' or arquivo[-3:] == 'csv'):
            try: 
                conversao_parquet_csv_sep_ponto_e_virgula(path, arquivo, pasta)
            except:
                print("É preciso utilizar '|' como separador")
                try:
                    conversao_parquet_csv_sep_barra(path, arquivo, pasta)                    
                except:
                    print('Error!')

shutil.rmtree('./data')

enviar_s3('nome_do_bucket')