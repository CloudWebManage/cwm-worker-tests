import os
import subprocess
import datetime
from threading import Thread, Lock
from minio import Minio
from minio.error import S3Error

def get_now_string():
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')


def create_test_file(file_size):
    now = get_now_string()
    filepath = f'/tmp/cwm-test-{now}.test'
    ret, _ = subprocess.getstatusoutput(f'fallocate -l {file_size} {filepath}')
    assert ret == 0
    print(f'Test file created! [{filepath}]')
    return filepath


def get_filename(file_size, index):
    return f'file-{file_size}-{index}.blob'


def upload(endpoint, access_key, secret_key, bucket, num_files, file_size, output_dir):
    print(f'📤 Upload test')

    upload_start_time = datetime.datetime.now()

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key)

    bucket_exists = client.bucket_exists(bucket)
    if not bucket_exists:
        print(f'Bucket does not exist! [{bucket}]')
        client.make_bucket(bucket)
        print(f'Bucket created! [{bucket}]')
    else:
        print(f'Bucket exists! [{bucket}]')

    test_filepath = create_test_file(file_size)

    timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000%fZ')
    output_filename = f'upload-report-{timestamp}.csv'
    output_filepath = output_dir + '/' + output_filename

    with open(output_filepath, 'w', buffering=1) as csvfile:
        csvfile.write(f'file_index,filename,file_upload_elapsed_time_seconds\n')
        print(f'Uploading files... [CSV report: {output_filepath}]')
        for i in range(num_files):
            index = i+1
            file_upload_start_time = datetime.datetime.now()
            filename = get_filename(file_size=file_size, index=index)
            client.fput_object(bucket, filename, test_filepath)
            file_upload_end_time = datetime.datetime.now()
            file_upload_elapsed_time_seconds = (file_upload_end_time - file_upload_start_time).total_seconds()
            print(f'\rUploaded: {index} files', end='', flush=True)
            csvfile.write(f'{index},{filename},{file_upload_elapsed_time_seconds}\n')

    output_filesize = os.path.getsize(output_filepath)
    print(f'\nCSV report generated! [{output_filepath}] ({output_filesize} bytes)')

    upload_end_time = datetime.datetime.now()
    upload_elapsed_time_seconds = (upload_end_time - upload_start_time).total_seconds()
    print(f'Upload finished! [{upload_elapsed_time_seconds} seconds]')
    if test_filepath:
        os.remove(test_filepath)


class DownloadIteration(Thread):

    def __init__(self, threadid, endpoint, access_key, secret_key, bucket, num_files, file_size, download_iterations, csvfile, csvfile_lock):
        super().__init__()
        self.threadid = threadid
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.num_files = num_files
        self.file_size = file_size
        self.download_iterations = download_iterations
        self.csvfile = csvfile
        self.csvfile_lock = csvfile_lock

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

            for iteration in range(self.download_iterations):
                iteration_no = iteration + 1
                iteration_start_time = datetime.datetime.now()
                print(f'{context} | Iteration # {iteration_no} | Started')
                for i in range(self.num_files):
                    download_start_time = datetime.datetime.now()
                    error = None
                    try:
                        index = i+1
                        object_name = get_filename(self.file_size, index)
                        object_filepath = f'/tmp/{object_name}.{self.threadid}'
                        client.fget_object(self.bucket, object_name, object_filepath)
                        object_size_bytes = os.path.getsize(object_filepath)
                        os.remove(object_filepath)
                    except Exception as e:
                        error = str(e)
                    download_end_time = datetime.datetime.now()
                    download_elapsed_time_seconds = (download_end_time - download_start_time).total_seconds()
                    with self.csvfile_lock:
                        self.csvfile.write(f'{self.threadid},{iteration_no},{object_name},{object_size_bytes},{download_elapsed_time_seconds},{error}\n')
                iteration_end_time = datetime.datetime.now()
                iteration_elapsed_seconds = (iteration_end_time - iteration_start_time).total_seconds()
                print(f'{context} | Iteration # {iteration_no} | Finished [{iteration_elapsed_seconds} seconds]')

            thread_end_time = datetime.datetime.now()
            thread_elapsed_seconds = (thread_end_time - thread_start_time).total_seconds()
            print(f'{context} | Finished [{thread_elapsed_seconds} seconds]')
        except S3Error as e:
            print(f'MinIO Exception: {e}')
        except Exception as e:
            print(f'Exception: {e}')


def download(endpoint, access_key, secret_key, bucket, num_files, file_size, download_iterations, download_threads, output_dir):
    print(f'📥 Download test')

    try:
        download_start_time = datetime.datetime.now()

        timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000%fZ')
        output_filename = f'download-report-{timestamp}.csv'
        output_filepath = output_dir + '/' + output_filename

        with open(output_filepath, 'w', buffering=1) as csvfile:
            csvfile.write(f'threadid,iteration_no,object_name,object_size_bytes,object_elapsed_time_seconds,error\n')
            csvfile_lock = Lock()

            print(f'Downloading files... [CSV report: {output_filepath}]')
            print(f'Spawning threads... [download_threads: {download_threads}]')
            threads = []
            for i in range(download_threads):
                threadid = i+1
                thread = DownloadIteration(threadid=threadid, endpoint=endpoint, access_key=access_key, secret_key=secret_key, bucket=bucket,
                                           num_files=num_files, file_size=file_size, download_iterations=download_iterations,
                                           csvfile=csvfile, csvfile_lock=csvfile_lock)
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

        output_filesize = os.path.getsize(output_filepath)
        print(f'CSV report generated! [{output_filepath}] ({output_filesize} bytes)')

        download_end_time = datetime.datetime.now()
        download_elapsed_time_seconds = (download_end_time - download_start_time).total_seconds()
        print(f'Download finished! [{download_elapsed_time_seconds} seconds]')
    except Exception as e:
        print(f'Exception: {e}')


def main(endpoint:str, access_key:str, secret_key:str, bucket:str, num_files:int, file_size:int,
         download_iterations:int, download_threads:int, output_dir:str, only_upload:bool, only_download:bool):
    print('Arguments:')
    for k, v in locals().items():
        print(f'  {k: >{20}}  =  {v}')
    print()

    if not os.path.isdir(output_dir):
        raise Exception(f'ERROR: Invalid output directory! [{output_dir}]')

    if not only_download:
        upload(endpoint=endpoint, access_key=access_key, secret_key=secret_key, bucket=bucket,
               num_files=num_files, file_size=file_size, output_dir=output_dir)

    if not only_upload:
        download(endpoint=endpoint, access_key=access_key, secret_key=secret_key, bucket=bucket,
                 num_files=num_files, file_size=file_size, download_iterations=download_iterations,
                 download_threads=download_threads, output_dir=output_dir)

    print(f'--- [DONE] ---')
