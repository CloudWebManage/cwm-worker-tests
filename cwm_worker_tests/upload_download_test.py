import os
import subprocess
import datetime
from minio import Minio
from minio.error import S3Error


def get_minio_client(endpoint, access_key, secret_key):
    try:
        return Minio(endpoint, access_key=access_key, secret_key=secret_key)
    except S3Error as e:
        print(f'❌ MinIO Exception: {e}')

def get_now_string():
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')


def get_test_filename():
    return 'cwm-test-' + get_now_string() + '.test'


def create_test_file(file_size):
    filepath = '/tmp/' + get_test_filename()
    print(f'Creating test file... [{filepath}] ({file_size} bytes)')
    ret, _ = subprocess.getstatusoutput(f'fallocate -l {file_size} {filepath}')
    assert ret == 0
    print(f'Test file created successfully! [{filepath}]')
    return filepath


def get_upload_filename():
    return 'cwm-test-' + get_now_string() + '.blob'


def upload(client, bucket, num_files, file_size, output_dir):
    print(f'📤 Upload test [bucket: {bucket}, num_files: {num_files}, file_size: {file_size}]')

    filepath = None
    try:
        print(f'Checking bucket... [{bucket}]')
        bucket_exists = client.bucket_exists(bucket)
        if not bucket_exists:
            print(f'Bucket does not exist! [{bucket}]')
            client.make_bucket(bucket)
            print(f'Bucket created! [{bucket}]')
        else:
            print(f'Bucket exists! [{bucket}]')

        # Create test file, remove at the end
        filepath = create_test_file(file_size)

        print(f'Uploading files...')
        for i in range(num_files):
            filename = get_upload_filename()
            client.fput_object(bucket, filename, filepath)
            print(f'\rUploaded: {i+1} files', end='', flush=True)

        print(f'\nFiles uploaded successfully!')

    except S3Error as e:
        print(f'❌ MinIO exception: {e}')
    except Exception as e:
        print(f'❌ Exception: {e}')
    finally:
        if filepath:
            os.remove(filepath)


def download(client, bucket, download_iterations, download_threads, output_dir):
    print(f'📥 Download test [bucket: {bucket}, download_iterations: {download_iterations}, download_threads: {download_threads}]')

    try:
        print(f'Checking bucket... [{bucket}]')
        bucket_exists = client.bucket_exists(bucket)
        if not bucket_exists:
            print(f'Bucket does not exist! [{bucket}]')
            return

        print(f'Downloading files...')
        objects = client.list_objects(bucket, recursive=True)
        num_downloaded = 0
        for obj in objects:
            object_name = obj.object_name
            object_filepath = '/tmp/' + object_name
            client.fget_object(bucket, object_name, object_filepath)
            num_downloaded += 1
            print(f'\rDownloaded: {num_downloaded} files', end='', flush=True)
            os.remove(object_filepath)

        print(f'\nFiles downloaded successfully!')

    except S3Error as e:
        print(f'❌ MinIO exception: {e}')
    except Exception as e:
        print(f'❌ Exception: {e}')


def main(endpoint:str, access_key=str, secret_key=str, bucket=str, num_files=int, file_size=int,
         only_upload=bool, only_download=bool, download_iterations=int, download_threads=int, output_dir=str):
    print('Arguments:')
    for k, v in locals().items():
        print(f'  {k: >{20}}  =  {v}')
    print()

    client = get_minio_client(endpoint=endpoint, access_key=access_key, secret_key=secret_key)

    if not only_download:
        upload(client=client, bucket=bucket, num_files=num_files, file_size=file_size, output_dir=output_dir)

    if not only_upload:
        download(client=client, bucket=bucket, download_iterations=download_iterations, download_threads=download_threads, output_dir=output_dir)

    print(f'--- [DONE] ---')
