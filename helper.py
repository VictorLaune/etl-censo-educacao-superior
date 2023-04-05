from pyspark.sql import functions as f
from main import spark
from zipfile import ZipFile
import requests
import os
import io
import boto3

def downloading_and_extracting_files(years_for_download, url):
    """
    Text
    """
    for year in years_for_download:
        print("\nDownloading the data of the year: " + str(year) + "\n")
        url_for_download = (url + str(year) + ".zip")
        try:
            request = requests.get(url_for_download, verify=False)
        except:
            print('Some error! - arrumar isso aqui.')
        else:
            data_zip = ZipFile(io.BytesIO(request.content))
            data_zip.extractall("./data")

            # Renomeia as pastas que estavam com caracteres fora do utf-8
            lista_pastas = ([folder for folder in os.listdir("./data")])
            for folder in lista_pastas:
                os.rename(f"./data/{folder}", f"./data/{folder[-4:]}")

def verify_folder(folder):
    """
    Text
    """
    try:
        path = f"./data/{folder}/dados"
    except:
        print("Trying with 'DADOS'\n")
        try:
            path = f"./data/{folder}/DADOS"
        except:
            print("Folder without 'dados' or 'DADOS'\n")
    return path

def convert_to_parquet(path, file, folder, sep):
    """
    Text
    """
    params = {'header':True, 'inferSchema':True, 'sep': sep}
    path_csv = path + '/' + file
    df = (
        spark
        .read
        .csv(path_csv, **params)
        )

    df = df.select([f.col(column).alias(column.lower()) for column in df.columns])
    df.write.parquet(f"parquet_files/{folder}/{file.replace('.CSV','')}", mode='overwrite')

def upload_s3(bucket_name, years_list):
    """
    Text
    """
    s3 = boto3.client('s3')
    for year in years_list:
        folders_to_upload_s3 = ([folder for folder in os.listdir(f"./to_parquet/{year}")])
        for folder in folders_to_upload_s3:
            file_list = ([file for file in os.listdir(f"./to_parquet/{year}/{folder}")])
            for file in file_list:
                file_path = f'./to_parquet/{year}/{folder}/{file}'
                key_name = f'{year}/{folder}/{file}' 
                try:
                    s3.upload_file(
                                    Filename = file_path,
                                    Bucket = bucket_name, 
                                    Key = key_name
                    )
                except:
                    return print("Error") ## LOOK AT HERE
                else:
                    print(f'Successfully uploaded: {file}')