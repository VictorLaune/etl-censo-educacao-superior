import os
from helper import *
###
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession\
   .builder\
   .master("local")\
   .appName("app name")\
   .config("spark.some.config.option", True)\
   .getOrCreate()
# ###

lista_pastas = ([pasta for pasta in os.listdir("./data")])


for pasta in lista_pastas:
    path = verifica_pasta(pasta)
    lista_arquivos = ([arquivo for arquivo in os.listdir(f"{path}")])
    for arquivo in lista_arquivos:
        if(arquivo[-3:] == 'CSV' or arquivo[-3:] == 'csv'):
            try: 
                params = {'header':True, 'inferSchema':True, 'sep':';'}
                path_csv = path + '/' + arquivo

                df = (
                spark
                .read
                .csv(path_csv, **params)
                )

                df = df.select([f.col(coluna).alias(coluna.lower()) for coluna in df.columns])

                df.write.parquet(f"to_parquet/{pasta}/{arquivo}", mode='overwrite')
            except:
                print('É preciso utilizar | como separador')
                try:
                    params = {'header':True, 'inferSchema':True, 'sep':'|'}
                    path_csv = path + '/' + arquivo

                    df = (
                    spark
                    .read
                    .csv(path_csv, **params)
                    )

                    df = df.select([f.col(coluna).alias(coluna.lower()) for coluna in df.columns])

                    df.write.parquet(f"to_parquet/{pasta}/{arquivo}", mode='overwrite')                    
                except:
                    print('Error!')
            