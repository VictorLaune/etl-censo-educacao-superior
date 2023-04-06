from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from zipfile import ZipFile
import requests
import os
import io
import boto3

def downloading_and_extracting_files(years_for_download, url):
    """
    Download, extract the zip files that will be received and rename the folders.\n
    Receive a list of the years that will be used for download and the URL.\n
    Creates a folder called "data" with the files divided by year.
    """
    for year in years_for_download:
        print("\nDownloading the data of the year: " + str(year) + "\n")
        url_for_download = (url + str(year) + ".zip")
        try:
            request = requests.get(url_for_download, verify=False)
        except:
            print('Some error!')
        else:
            data_zip = ZipFile(io.BytesIO(request.content))
            data_zip.extractall("./data")

            # Rename folders
            folders_list = ([folder for folder in os.listdir("./data")])
            for folder in folders_list:
                os.rename(f"./data/{folder}", f"./data/{folder[-4:]}")

def convert_to_parquet(spark, path, file, folder, sep):
    """
    Converts CSV files to parquet files.\n
    Need to receive the variable created by SparkSession, a path that indicates 
    the folder where the CSV files are, the file name, folder name, and csv file separator.\n
    Path, file, folder and separator, must be passed as string type.\n
    It will create a folder called "parquet_files" that will contain the folders with the same 
    names that were passed, with the CSV files converted to Parquet files with the name of all 
    the columns in lowercase.
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
    Sends the files to an aws s3 bucket.\n
    It must be given a bucket name and the list of years used 
    for download for it to use in the path and find the parquet files for upload.\n
    Creates folders with the names of the years used for download with the parquet files inside them. 
    Print in the terminal if the upload was successful.
    """
    s3 = boto3.client('s3')
    for year in years_list:
        folders_to_upload_s3 = ([folder for folder in os.listdir(f"./parquet_files/{year}")])
        for folder in folders_to_upload_s3:
            file_list = ([file for file in os.listdir(f"./parquet_files/{year}/{folder}")])
            for file in file_list:
                file_path = f'./parquet_files/{year}/{folder}/{file}'
                key_name = f'{year}/{folder}/{file}' 
                try:
                    s3.upload_file(
                                    Filename = file_path,
                                    Bucket = bucket_name, 
                                    Key = key_name
                    )
                except:
                    print("Error") 
                else:
                    print(f'Successfully uploaded: {file}')