from helper import *
import shutil

# Defining the years that will be selected for download
years_list = list(range(1995,2022))
print("Years we will use for extraction:" + str(years_list) + "\n")

# Downloading and extracting files
url = "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_"

downloading_and_extracting_files(years_list, url)

# Creating the spark session
spark = SparkSession\
   .builder\
   .master("local")\
   .appName("convert_to_parquet")\
   .getOrCreate()

for folder in years_list:
    path = f"./data/{folder}/dados" # Windows is case insensitive

    files_list = ([file for file in os.listdir(f"{path}")])
    for file in files_list:
        if(file[-3:] == 'CSV'):
            if (folder < 2009):
                convert_to_parquet(spark, path, file, folder, sep="|")
            else: 
                convert_to_parquet(spark, path, file, folder, sep=";")

upload_s3('a3-case-avaliacao', years_list)

shutil.rmtree('./data')
shutil.rmtree('./parquet_files')

spark.stop()