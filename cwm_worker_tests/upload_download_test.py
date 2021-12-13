import os
import subprocess
import datetime
from threading import Thread
from minio import Minio
from minio.error import S3Error
import csv

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

        uploads = []

        print(f'Uploading files...')
        for i in range(num_files):
            file_upload_start_time = datetime.datetime.now()
            filename = get_upload_filename()
            error = None
            try:
                client.fput_object(bucket, filename, filepath)
            except S3Error as e:
                print(f'\rException: {e}')
                error = str(e)
            except Exception as e:
                print(f'\rException: {e}')
                error = str(e)
            file_upload_end_time = datetime.datetime.now()
            file_upload_elapsed_time_seconds = (file_upload_end_time - file_upload_start_time).total_seconds()
            print(f'\rUploaded: {i+1} files [{file_upload_elapsed_time_seconds} seconds]', end='', flush=True)
            uploads.append({
                'file_index': i+1,
                'filename': filename,
                'file_upload_elapsed_time_seconds': file_upload_elapsed_time_seconds,
                'error': error
            })

        end_time = datetime.datetime.now()
        elapsed_seconds = (end_time - start_time).total_seconds()
        print(f'\nFiles uploaded! [{elapsed_seconds} seconds]')

        # Generate CSV report
        timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000%fZ')
        output_filename = f'upload-report-{timestamp}.csv'
        output_filepath = output_dir + '/' + output_filename

        print(f'Generating CSV report... [{output_filepath}]')
        with open(output_filepath, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['file_index', 'filename', 'file_upload_elapsed_time_seconds', 'error'])
            for upload in uploads:
                file_index = upload['file_index']
                filename = upload['filename']
                file_upload_elapsed_time_seconds = upload['file_upload_elapsed_time_seconds']
                error = upload['error']
                csv_writer.writerow([file_index, filename, file_upload_elapsed_time_seconds, error])
        output_filesize = os.path.getsize(output_filepath)
        print(f'CSV report generated! [{output_filepath}] ({output_filesize} bytes)')
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
                downloads = []
                for obj in objects:
                    download_start_time = datetime.datetime.now()
                    object_name = obj.object_name
                    object_filepath = f'/tmp/{object_name}.{self.threadid}'
                    error = None
                    try:
                        client.fget_object(self.bucket, object_name, object_filepath)
                        num_downloaded_files += 1
                        object_size_bytes = os.path.getsize(object_filepath)
                        os.remove(object_filepath)
                    except S3Error as e:
                        print(f'{context} | Exception: {e}')
                        error = str(e)
                    except Exception as e:
                        print(f'{context} | Exception: {e}')
                        error = str(e)
                    download_end_time = datetime.datetime.now()
                    download_elapsed_time = (download_end_time - download_start_time).total_seconds()
                    downloads.append({
                        'object_name': object_name,
                        'object_size_bytes': object_size_bytes,
                        'object_elapsed_time_seconds': download_elapsed_time,
                        'error': error
                    })
                # print(f'{context} | Downloaded: {num_downloaded_files} files', end='', flush=True)
                iteration_end_time = datetime.datetime.now()
                iteration_elapsed_seconds = (iteration_end_time - iteration_start_time).total_seconds()
                print(f'{context} | Iteration # {iteration_no} | Finished [{iteration_elapsed_seconds} seconds] [{num_downloaded_files} files]')

                iterations.append({
                    'iteration_no': iteration_no,
                    'iteration_elapsed_seconds': iteration_elapsed_seconds,
                    'num_downloaded_files': num_downloaded_files,
                    'downloads': downloads
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

        # Generate CSV report
        timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000%fZ')
        output_filename = f'download-report-{timestamp}.csv'
        output_filepath = output_dir + '/' + output_filename

        print(f'Generating CSV report... [{output_filepath}]')
        with open(output_filepath, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['threadid', 'iteration_no', 'object_name', 'object_size_bytes', 'object_elapsed_time_seconds', 'error'])
            for thread in threads:
                stats = thread.get_stats()
                threadid = stats['threadid']
                iterations = stats['iterations']
                for iteration in iterations:
                    iteration_no = iteration['iteration_no']
                    downloads = iteration['downloads']
                    for download in downloads:
                        object_name = download['object_name']
                        object_size_bytes = download['object_size_bytes']
                        object_elapsed_time_seconds = download['object_elapsed_time_seconds']
                        error = download['error']
                        csv_writer.writerow([threadid, iteration_no, object_name, object_size_bytes, object_elapsed_time_seconds, error])
        output_filesize = os.path.getsize(output_filepath)
        print(f'CSV report generated! [{output_filepath}] ({output_filesize} bytes)')
    except Exception as e:
        print(f'Exception: {e}')


def main(endpoint:str, access_key=str, secret_key=str, bucket=str, num_files=int, file_size=int,
         only_upload=bool, only_download=bool, download_iterations=int, download_threads=int, output_dir=str):
    print('Arguments:')
    for k, v in locals().items():
        print(f'  {k: >{20}}  =  {v}')
    print()

    if not os.path.isdir(output_dir):
        print(f'ERROR: Invalid output directory! [{output_dir}]')
        exit(1)

    if not only_download:
        upload(endpoint=endpoint, access_key=access_key, secret_key=secret_key, bucket=bucket, num_files=num_files, file_size=file_size, output_dir=output_dir)

    if not only_upload:
        download(endpoint=endpoint, access_key=access_key, secret_key=secret_key, bucket=bucket, download_iterations=download_iterations, download_threads=download_threads, output_dir=output_dir)

    print(f'--- [DONE] ---')
