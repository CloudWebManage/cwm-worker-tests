import datetime
from minio import Minio
from minio.error import S3Error


def get_now_string():
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')


def get_test_filename():
    return 'cwm-test-' + get_now_string() + '.txt'


def main(endpoint:str, access_key=str, secret_key=str, bucket=str, num_files=int, file_size=int,
         only_upload=bool, only_download=bool, download_iterations=int, download_threads=int, output_dir=str):
    print(get_test_filename())
