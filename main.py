from helper import *

# Definindo os anos que serao selecionados para download
anos_para_download = []
ano = 2021
ano_final = 2021

while ano >= ano_final:
    anos_para_download.append(str(ano))
    ano -= 1

print("Anos que iremos utilizar para a extração:" + str(anos_para_download) + "\n")

baixando_e_extraindo_arquivos(anos_para_download)


os.mkdir("./data")
renomeando_pastas()
os.rmdir('./download')