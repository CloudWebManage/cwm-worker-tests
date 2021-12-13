import os
import subprocess
import datetime
from threading import Thread
from minio import Minio
from minio.error import S3Error


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


def upload(endpoint, access_key, secret_key, bucket, num_files, file_size, output_dir):
    print(f'📤 Upload test')

    filepath = None
    try:
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key)

        bucket_exists = client.bucket_exists(bucket)
        if not bucket_exists:
            print(f'Bucket does not exist! [{bucket}]')
            client.make_bucket(bucket)
            print(f'Bucket created! [{bucket}]')
        else:
            print(f'Bucket exists! [{bucket}]')

        start_time = datetime.datetime.now()

        # Create test file, remove at the end
        filepath = create_test_file(file_size)

        print(f'Uploading files...')
        for i in range(num_files):
            filename = get_upload_filename()
            client.fput_object(bucket, filename, filepath)
            print(f'\rUploaded: {i+1} files', end='', flush=True)

        end_time = datetime.datetime.now()
        elapsed_seconds = (end_time - start_time).total_seconds()
        print(f'\nFiles uploaded! [{elapsed_seconds} seconds]')

    except S3Error as e:
        print(f'\nMinIO exception: {e}')
    except Exception as e:
        print(f'\nException: {e}')
    finally:
        if filepath:
            os.remove(filepath)


class DownloadIteration(Thread):

    def __init__(self, threadid, endpoint, access_key, secret_key, bucket, download_iterations):
        super().__init__()
        self.threadid = threadid
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.download_iterations = download_iterations
        self.stats = {}

    def get_stats(self):
        return self.stats

    def run(self):
        try:
            thread_start_time = datetime.datetime.now()
 
            context = f'Thread # {self.threadid}'
            print(f'{context} | Started')

            client = Minio(endpoint=self.endpoint, access_key=self.access_key, secret_key=self.secret_key)

            bucket_exists = client.bucket_exists(self.bucket)
            if not bucket_exists:
                print(f'Bucket does not exist! [{self.bucket}]')
                return

            iterations = []
            for iteration in range(self.download_iterations):
                iteration_no = iteration + 1
                iteration_start_time = datetime.datetime.now()
                print(f'{context} | Iteration # {iteration_no} | Started')
                objects = client.list_objects(self.bucket, recursive=True)
                num_downloaded_files = 0
                for obj in objects:
                    object_name = obj.object_name
                    object_filepath = f'/tmp/{object_name}.{self.threadid}' 
                    client.fget_object(self.bucket, object_name, object_filepath)
                    num_downloaded_files += 1
                    os.remove(object_filepath)
                # print(f'{context} | Downloaded: {num_downloaded_files} files', end='', flush=True)
                iteration_end_time = datetime.datetime.now()
                iteration_elapsed_seconds = (iteration_end_time - iteration_start_time).total_seconds()
                print(f'{context} | Iteration # {iteration_no} | Finished [{iteration_elapsed_seconds} seconds] [{num_downloaded_files} files]')

                iterations.append({
                    'iteration': iteration_no,
                    'elapsed_seconds': iteration_elapsed_seconds,
                    'num_downloaded_files': num_downloaded_files
                })

            thread_end_time = datetime.datetime.now()
            thread_elapsed_seconds = (thread_end_time - thread_start_time).total_seconds()
            print(f'{context} | Finished [{thread_elapsed_seconds} seconds]')

            self.stats = {
                'threadid': self.threadid,
                'thread_elapsed_seconds': thread_elapsed_seconds,
                'iterations': iterations
            }

        except S3Error as e:
            print(f'MinIO Exception: {e}')
        except Exception as e:
            print(f'Exception: {e}')


def download(endpoint, access_key, secret_key, bucket, download_iterations, download_threads, output_dir):
    print(f'📥 Download test')

    try:
        print(f'Spawning threads... [download_threads: {download_threads}]')
        threads = []
        for i in range(download_threads):
            thread = DownloadIteration(i+1, endpoint=endpoint, access_key=access_key, secret_key=secret_key, bucket=bucket, download_iterations=download_iterations)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for thread in threads:
            print(thread.get_stats())

    except Exception as e:
        print(f'Exception: {e}')


def main(endpoint:str, access_key=str, secret_key=str, bucket=str, num_files=int, file_size=int,
         only_upload=bool, only_download=bool, download_iterations=int, download_threads=int, output_dir=str):
    print('Arguments:')
    for k, v in locals().items():
        print(f'  {k: >{20}}  =  {v}')
    print()

    if not only_download:
        upload(endpoint=endpoint, access_key=access_key, secret_key=secret_key, bucket=bucket, num_files=num_files, file_size=file_size, output_dir=output_dir)

    if not only_upload:
        download(endpoint=endpoint, access_key=access_key, secret_key=secret_key, bucket=bucket, download_iterations=download_iterations, download_threads=download_threads, output_dir=output_dir)

    print(f'--- [DONE] ---')
