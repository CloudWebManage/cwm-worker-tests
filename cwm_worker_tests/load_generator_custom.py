import uuid
import json
import time
import random
import urllib3
import datetime
import contextlib
import subprocess
import concurrent.futures
from threading import Thread, Lock
from collections import defaultdict

import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError

from cwm_worker_cluster import common
from cwm_worker_cluster.worker import api as worker_api
from cwm_worker_cluster.test_instance import api as test_instance_api
from cwm_worker_tests.retry import retry_exception_dynamic


urllib3.disable_warnings()


DEFAULT_BUCKET_NAME = 'cwmwtlgcustomdefault'


@contextlib.contextmanager
def get_start_end_duration_ns():
    res = {}
    start_datetime = datetime.datetime.now()
    start_monotonic = time.monotonic()
    yield res
    duration_seconds = time.monotonic() - start_monotonic
    res['duration_ns'] = round(duration_seconds * 1000000000)
    assert res['duration_ns'] >= 0
    res['start_time'] = start_datetime
    res['end_time'] = start_datetime + datetime.timedelta(seconds=duration_seconds)


class RunCustomThread(Thread):

    def __init__(self, thread_num, method, objects, duration_seconds, obj_size_kb, custom_load_options, benchdatafile, benchdatafilelock, domain_name_buckets):
        super().__init__()
        self.method = method
        self.duration_seconds = duration_seconds
        self.thread_num = thread_num
        self.objects = objects
        self.obj_size_kb = obj_size_kb
        self.stats = defaultdict(int)
        self.last_put_object_key = None
        self.last_put_object_domain_name = None
        self.custom_load_options = custom_load_options
        self.benchdatafile = benchdatafile
        self.benchdatafilelock = benchdatafilelock
        self.domain_name_buckets = domain_name_buckets
        self.domain_name_buckets_cache = {}

    def get_bucket(self, domain_name):
        if domain_name in self.domain_name_buckets_cache:
            return self.domain_name_buckets_cache[domain_name]
        else:
            start_time = datetime.datetime.now()
            s3 = None
            while not s3:
                try:
                    s3 = get_s3_resource(self.method, domain_name, with_retries=False)
                except:
                    if (datetime.datetime.now() - start_time).total_seconds() > 60:
                        print("Timeout trying to get s3 resource")
                        raise
            bucket = s3.Bucket(self.domain_name_buckets[domain_name])
            if self.custom_load_options.get('enable_s3_buckets_cache'):
                self.domain_name_buckets_cache[domain_name] = bucket
            return bucket

    def write_benchdata(self, op, bytes, file, error, start_end_duration, domain_name):
        file = file.replace(',', '_') if file else ''
        error = error.replace(',', '_') if error else ''
        endpoint = '{}://{}'.format(self.method, domain_name)
        duration_ns = start_end_duration['duration_ns']
        start = first_byte = start_end_duration['start_time'].strftime('%Y-%m-%dT%H:%M:%S.000%fZ')
        end = start_end_duration['end_time'].strftime('%Y-%m-%dT%H:%M:%S.000%fZ')
        writearg = ",".join(map(str, [op, bytes if bytes else 0, endpoint, file, error if error else '', start, first_byte, end, duration_ns])) + "\n"
        if self.benchdatafile:
            with self.benchdatafilelock:
                self.benchdatafile.write(writearg)
                self.benchdatafile.flush()

    def make_head_request(self, object, domain_name):
        self.stats['num_head_requests'] += 1
        head_request_error = None
        with get_start_end_duration_ns() as start_end_duration:
            try:
                object.load()
            except Exception as e:
                head_request_error = str(e)
        if not head_request_error:
            if object.content_length != self.obj_size_kb * 1024:
                head_request_error = "invalid content length"
        self.write_benchdata('STAT', 0, object.key, head_request_error, start_end_duration, domain_name)
        return not head_request_error

    def make_cached_get_request(self, filename, domain_name):
        self.stats['num_cached_get_requests'] += 1
        cached_get_request_error = None
        with get_start_end_duration_ns() as start_end_duration:
            urlpath = '{}/{}'.format(self.domain_name_buckets[domain_name], filename)
            url = '{}://{}/{}'.format(self.method, domain_name, urlpath)
            try:
                res = requests.get(url, stream=True)
                obj_size = sum(len(data) for data in res.iter_content(chunk_size=1))
            except Exception as e:
                cached_get_request_error = str(e)
        if not cached_get_request_error:
            if obj_size != self.obj_size_kb * 1024:
                cached_get_request_error = "invalid object size"
        self.write_benchdata('CACHED_GET', obj_size, urlpath, cached_get_request_error, start_end_duration, domain_name)
        return not cached_get_request_error

    def make_get_request(self, object, domain_name):
        self.stats['num_get_requests'] += 1
        get_request_error = None
        obj_size = 0
        with get_start_end_duration_ns() as start_end_duration:
            try:
                obj_size = len(object.get()['Body'].read())
            except Exception as e:
                get_request_error = str(e)
        if not get_request_error:
            if obj_size != self.obj_size_kb * 1024:
                get_request_error = "invalid object size"
        self.write_benchdata('GET', obj_size, object.key, get_request_error, start_end_duration, domain_name)
        return not get_request_error

    def make_del_request(self, key, domain_name):
        self.stats['num_del_requests'] += 1
        del_request_error = None
        with get_start_end_duration_ns() as start_end_duration:
            try:
                self.get_bucket(domain_name).Object(key).delete()
            except Exception as e:
                del_request_error = str(e)
        self.write_benchdata('DELETE', 0, key, del_request_error, start_end_duration, domain_name)
        return not del_request_error

    def make_put_request(self, key, domain_name):
        self.stats['num_put_requests'] += 1
        put_request_error = None
        with get_start_end_duration_ns() as start_end_duration:
            try:
                self.get_bucket(domain_name).put_object(Key=key, Body=random.getrandbits(int(self.obj_size_kb) * 1024 * 8).to_bytes(int(self.obj_size_kb) * 1024, 'little'))
            except Exception as e:
                put_request_error = str(e)
        self.write_benchdata('PUT', self.obj_size_kb * 1024, key, put_request_error, start_end_duration, domain_name)
        return not put_request_error

    def make_put_or_del_request(self):
        if self.last_put_object_key and self.last_put_object_domain_name:
            if not self.make_del_request(self.last_put_object_key, self.last_put_object_domain_name):
                self.stats['num_del_errors'] += 1
            self.last_put_object_key = None
            self.last_put_object_domain_name = None
        else:
            put_object_key = self.name + '/' + str(uuid.uuid4()) + '.rnd'
            put_object_domain_name = random.choice(list(self.domain_name_buckets.keys()))
            if self.make_put_request(put_object_key, put_object_domain_name):
                self.last_put_object_key = put_object_key
                self.last_put_object_domain_name = put_object_domain_name
            else:
                self.stats['num_put_errors'] += 1

    def get_domain_name_bucket(self):
        domain_name = random.choice(list(self.domain_name_buckets.keys()))
        return domain_name, self.get_bucket(domain_name)

    def get_domain_name(self):
        return random.choice(list(self.domain_name_buckets.keys()))

    def run(self):
        make_put_or_del_every_iterations = self.custom_load_options.get('make_put_or_del_every_iterations', 100)
        start_time = datetime.datetime.now()
        while (datetime.datetime.now() - start_time).total_seconds() <= self.duration_seconds:
            for i in range(self.objects):
                if (datetime.datetime.now() - start_time).total_seconds() > self.duration_seconds:
                    break
                self.stats['num_object_iterations'] += 1
                filename = 'file{}.rnd'.format(i+1)
                if self.custom_load_options.get('do_cached_get'):
                    domain_name = self.get_domain_name()
                    if not self.make_cached_get_request(filename, domain_name):
                        self.stats['num_cached_get_errors'] += 1
                else:
                    domain_name, bucket = self.get_domain_name_bucket()
                    object = bucket.Object(filename)
                    if self.make_head_request(object, domain_name):
                        if not self.make_get_request(object, domain_name):
                            self.stats['num_get_errors'] += 1
                    else:
                        self.stats['num_head_errors'] += 1
                if make_put_or_del_every_iterations and self.stats['num_object_iterations'] % (int(self.objects / make_put_or_del_every_iterations) + 1) == 0:
                    self.make_put_or_del_request()
        self.stats['elapsed_seconds'] = (datetime.datetime.now() - start_time).total_seconds()


def get_s3_resource(method, domain_name, with_retries=False, retry_max_attempts=None, connect_timeout=15, read_timeout=60):
    if retry_max_attempts is None:
        retry_max_attempts = 10 if with_retries else 0
    else:
        assert with_retries, 'must set with_retries=True if you set retry_max_attempts'
    return boto3.resource(
        's3', **test_instance_api.get_deployment_s3_resource_kwargs_by_hostname(method, domain_name),
        config=Config(
            signature_version='s3v4',
            retries={'max_attempts': retry_max_attempts, 'mode': 'standard'},
            connect_timeout=connect_timeout, read_timeout=read_timeout
        )
    )


def prepare_custom_bucket(method, worker_id, hostname, objects=10, duration_seconds=10, concurrency=6,
                          obj_size_kb=1, bucket_name=None, skip_delete_worker=False,
                          skip_clear_cache=False, skip_clear_volume=False, skip_all=True,
                          upload_concurrency=5, skip_create_bucket=False, only_upload_filenums=None, delete_keys=None):
    if not skip_all:
        worker_api.volume_api_recreate(worker_id=worker_id, skip_delete_worker=skip_delete_worker, skip_clear_cache=skip_clear_cache, skip_clear_volume=skip_clear_volume)
    if not bucket_name:
        bucket_name = str(uuid.uuid4())
    print("Creating bucket {} in domain_name {} method {}".format(bucket_name, hostname, method))
    s3 = get_s3_resource(method, hostname, with_retries=True)
    if not skip_create_bucket:
        try:
            s3.create_bucket(Bucket=bucket_name)
            time.sleep(10)
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print('got botocore ClientError "BucketAlreadyOwnedByYou", this is probably fine')
            else:
                raise
        s3.BucketPolicy(bucket_name).put(
            Policy=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": ["*"]},
                        "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                        "Resource": ["arn:aws:s3:::{}".format(bucket_name)]
                    },
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": ["*"]},
                        "Action": ["s3:GetObject"],
                        "Resource": ["arn:aws:s3:::{}/*".format(bucket_name)]
                    }
                ]
            })
        )
    print("Uploading {} files, {} kb each".format(objects if only_upload_filenums is None else len(only_upload_filenums), obj_size_kb))
    filenums_iterator = range(int(objects)) if only_upload_filenums is None else (int(i)-1 for i in only_upload_filenums)
    if delete_keys:
        print("Deleting {} files".format(len(delete_keys)))

    def put_or_del_object(op, key):
        s3 = get_s3_resource(method, hostname, with_retries=True)
        if op == 'put':
            bucket = s3.Bucket(bucket_name)
            i = int(key)
            bucket.put_object(Key='file{}.rnd'.format(i + 1), Body=random.getrandbits(int(obj_size_kb) * 1024 * 8).to_bytes(int(obj_size_kb) * 1024, 'little'))
        elif op == 'del':
            s3.delete_object(Bucket=bucket_name, Key=key)

    if upload_concurrency:
        print("Starting {} threads".format(upload_concurrency))
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=upload_concurrency)
        for i in filenums_iterator:
            executor.submit(put_or_del_object, 'put', i)
        if delete_keys:
            for key in delete_keys:
                executor.submit(put_or_del_object, 'del', key)
        executor.shutdown(wait=True)
    else:
        for i in filenums_iterator:
            put_or_del_object('put', i)
        if delete_keys:
            for key in delete_keys:
                put_or_del_object('del', key)
    return bucket_name


@contextlib.contextmanager
def open_benchdata_file(benchdatafilename, method):
    if benchdatafilename:
        with open('{}.{}.csv'.format(benchdatafilename, method), 'w') as benchdatafile:
            yield benchdatafile
    else:
        yield None


def get_default_bucket_name(obj_size_kb, objects):
    return '{}-{}kb-{}'.format(DEFAULT_BUCKET_NAME, obj_size_kb, objects)


def _prepare_default_bucket(
    method, worker_id, hostname, objects, obj_size_kb, with_delete=False, upload_concurrency=5,
    with_recreate=False
):
    if with_recreate:
        time.sleep(10)
        common.assert_site(
            worker_id, hostname,
            skip_clear_cache=True, skip_clear_volume=True, skip_all=False,
            protocols=['https']
        )
    bucket_name = get_default_bucket_name(obj_size_kb, objects)
    bucket = get_s3_resource(method, hostname, with_retries=True).Bucket(bucket_name)
    bucket.load()
    if not bucket.creation_date:
        bucket.create()
        time.sleep(10)
    bucket.Policy().put(
        Policy=json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                    "Resource": ["arn:aws:s3:::{}".format(bucket_name)]
                },
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": ["arn:aws:s3:::{}/*".format(bucket_name)]
                }
            ]
        })
    )
    delete_threadkeys = set()
    missing_filenums = set()
    for i in range(int(objects)):
        missing_filenums.add(i+1)
    for object in bucket.objects.all():
        if object.key.startswith('file') and object.key.endswith('.rnd') and object.size == obj_size_kb * 1024:
            filenum = int(object.key.replace('file', '').replace('.rnd', ''))
            if filenum in missing_filenums:
                missing_filenums.remove(filenum)
        elif object.key.startswith('Thread') and with_delete:
            delete_threadkeys.add(object.key)
    if len(missing_filenums) > 0 or len(delete_threadkeys) > 0:
        prepare_custom_bucket(method, worker_id, hostname, obj_size_kb=obj_size_kb, bucket_name=bucket_name,
                              skip_create_bucket=True, only_upload_filenums=missing_filenums, delete_keys=delete_threadkeys,
                              upload_concurrency=upload_concurrency)


def prepare_default_bucket(method, worker_id, hostname, objects, obj_size_kb, with_delete=False, upload_concurrency=5, with_retries=False):
    if with_retries:
        retry_exception_dynamic(
            _prepare_default_bucket,
            [
                {},
                {'sleep_seconds': 5},
                {'sleep_seconds': 5},
                {'sleep_seconds': 5},
                {'sleep_seconds': 1, 'extra_kwargs': {'with_recreate': True}},
                {'sleep_seconds': 5},
                {'sleep_seconds': 10},
                {'sleep_seconds': 20},
                {'sleep_seconds': 60},
            ],
            callback_args=[method, worker_id, hostname, objects, obj_size_kb],
            callback_kwargs=dict(with_delete=with_delete, upload_concurrency=upload_concurrency)
        )
    else:
        _prepare_default_bucket(method, worker_id, hostname, objects, obj_size_kb, with_delete=with_delete, upload_concurrency=upload_concurrency)


def run(method, worker_id, hostname, objects, duration_seconds, concurrency, obj_size_kb,
        benchdatafilename, custom_load_options=None, use_default_bucket=False,
        benchdata_format=None):
    if not benchdata_format:
        benchdata_format = 'csv.gz'
    assert method in ['http', 'https'], "invalid method: {}".format(method)
    if not custom_load_options.get('random_domain_names'):
        assert worker_id and hostname, "missing worker_id or hostname arguments"
    assert objects > 0
    assert duration_seconds > 0, "duration_seconds must be bigger than 0"
    assert obj_size_kb > 0, "obj_size argument should be empty for custom load generator"
    if custom_load_options.get('use_default_bucket'):
        use_default_bucket = True
    skip_prepare_bucket = custom_load_options.get('skip_prepare_bucket')
    bucket_name = custom_load_options.get("bucket_name")
    if bucket_name:
        assert not use_default_bucket, 'cannot use default bucket if you specified a bucket name'
        print("Using pre-prepared bucket {}".format(bucket_name))
    elif use_default_bucket:
        bucket_name = get_default_bucket_name(obj_size_kb, objects)
        print("Using default bucket: {}".format(bucket_name))
    else:
        assert not custom_load_options.get('random_domain_names'), 'bucket must be pre-prepared when using random domain names'
        assert not skip_prepare_bucket, 'cannot skip prepare bucket if no bucket_name'
        bucket_name = prepare_custom_bucket(method, worker_id, hostname, objects, duration_seconds, concurrency, obj_size_kb)
    if custom_load_options.get('random_domain_names'):
        domain_name_buckets = {
            hostname: bucket_name
            for worker_id, hostname in custom_load_options['random_domain_names'].items()
        }
        if use_default_bucket and not skip_prepare_bucket:
            for worker_id, hostname in custom_load_options['random_domain_names'].items():
                prepare_default_bucket(method, worker_id, hostname, objects, obj_size_kb)
    else:
        domain_name_buckets = {hostname: bucket_name}
        if use_default_bucket and not skip_prepare_bucket:
            prepare_default_bucket(method, worker_id, hostname, objects, obj_size_kb)
    print("Starting {} threads of custom load ({})".format(concurrency, method))
    with open_benchdata_file(benchdatafilename, method) as benchdatafile:
        if benchdatafile:
            benchdatafile.write("op,bytes,endpoint,file,error,start,first_byte,end,duration_ns\n")
            benchdatafilelock = Lock()
        else:
            benchdatafilelock = None
        threads = {}
        for i in range(concurrency):
            thread = RunCustomThread(i+1, method, objects, duration_seconds, obj_size_kb, custom_load_options, benchdatafile, benchdatafilelock, domain_name_buckets)
            threads[i] = thread
            thread.start()
        start_time = datetime.datetime.now()
        max_wait_seconds = duration_seconds * 10
        print("Waiting for all threads to complete (up to {} seconds)".format(max_wait_seconds))
        is_alive = True
        while is_alive:
            total_get_errors = sum([thread.stats['num_get_errors'] for thread in threads.values()])
            total_cached_get_errors = sum([thread.stats['num_cached_get_errors'] for thread in threads.values()])
            total_del_errors = sum([thread.stats['num_del_errors'] for thread in threads.values()])
            total_put_errors = sum([thread.stats['num_put_errors'] for thread in threads.values()])
            total_head_errors = sum([thread.stats['num_head_errors'] for thread in threads.values()])
            total_errors = sum([total_get_errors, total_cached_get_errors, total_del_errors, total_put_errors, total_head_errors])
            if total_errors > 0 and total_errors % 100 == 0:
                print("get_errors={}".format(total_get_errors))
                print("total_cached_get_errors={}".format(total_cached_get_errors))
                print("del_errors={}".format(total_del_errors))
                print("put_errors={}".format(total_put_errors))
                print("head_errors={}".format(total_head_errors))
            assert (datetime.datetime.now() - start_time).total_seconds() <= max_wait_seconds, "Waited too long for all threads to complete"
            time.sleep(1)
            is_alive = False
            for i in reversed(range(concurrency)):
                if threads[i].is_alive():
                    is_alive = True
                    break
    if benchdatafilename and benchdata_format == 'csv.gz':
        print("Compressing benchdata file")
        ret, out = subprocess.getstatusoutput("gzip -fq {}.{}.csv".format(benchdatafilename, method))
        assert ret == 0, out
    total_elapsed_seconds = sum([thread.stats['elapsed_seconds'] for thread in threads.values()])
    total_get_requests = sum([thread.stats['num_get_requests'] for thread in threads.values()])
    total_cached_get_requests = sum([thread.stats['num_cached_get_requests'] for thread in threads.values()])
    total_del_requests = sum([thread.stats['num_del_requests'] for thread in threads.values()])
    total_put_requests = sum([thread.stats['num_put_requests'] for thread in threads.values()])
    total_head_requests = sum([thread.stats['num_head_requests'] for thread in threads.values()])
    total_get_errors = sum([thread.stats['num_get_errors'] for thread in threads.values()])
    total_cached_get_errors = sum([thread.stats['num_cached_get_errors'] for thread in threads.values()])
    total_del_errors = sum([thread.stats['num_del_errors'] for thread in threads.values()])
    total_put_errors = sum([thread.stats['num_put_errors'] for thread in threads.values()])
    total_head_errors = sum([thread.stats['num_head_errors'] for thread in threads.values()])
    total_object_iterations = sum([thread.stats['num_object_iterations'] for thread in threads.values()])
    print("total_elapsed_seconds={}".format(total_elapsed_seconds))
    print("total_get_requests={}".format(total_get_requests))
    print("total_cached_get_requests={}".format(total_cached_get_requests))
    print("total_del_requests={}".format(total_del_requests))
    print("total_put_requests={}".format(total_put_requests))
    print("total_head_requests={}".format(total_head_requests))
    print("total_get_errors={}".format(total_get_errors))
    print("total_cached_get_errors={}".format(total_cached_get_errors))
    print("total_del_errors={}".format(total_del_errors))
    print("total_put_errors={}".format(total_put_errors))
    print("total_head_errors={}".format(total_head_errors))
    print("total_object_iterations={}".format(total_object_iterations))
    elapsed_seconds = (datetime.datetime.now() - start_time).total_seconds()
    out = "Completed in {} seconds".format(elapsed_seconds)
    return out, bucket_name


def run_multi(method, num_test_instances, test_instances_zones, test_instances_roles, objects, duration_seconds, concurrency,
              obj_size_kb, benchdatafilename, make_put_or_del_every_iterations=100,
              benchdata_format=None, do_cached_get=False, test_instances_random=False,
              test_instances_worker_ids=None):
    test_instances = []
    for test_instance in test_instance_api.iterate_all():
        if test_instances_zones and test_instance['zone'] not in test_instances_zones:
            continue
        if test_instances_roles and test_instance['role'] not in test_instances_roles:
            continue
        if test_instances_worker_ids and test_instance['worker_id'] not in test_instances_worker_ids:
            continue
        test_instances.append(test_instance)
    if test_instances_random:
        random.shuffle(test_instances)
    test_instances = test_instances[:num_test_instances]
    print('selected test instances:')
    for t in test_instances:
        print('{} {:^15} {}'.format(t['worker_id'], t['zone'], t['role']))
    return run(
        method, None, None, objects, duration_seconds, concurrency, obj_size_kb, benchdatafilename,
        custom_load_options={
            'random_domain_names': {test_instance['worker_id']: test_instance['hostname'] for test_instance in test_instances},
            'use_default_bucket': True,
            'make_put_or_del_every_iterations': make_put_or_del_every_iterations,
            'do_cached_get': do_cached_get
        },
        benchdata_format=benchdata_format
    )


def prepare_default_bucket_multi(method, test_instances_zones, test_instances_roles, objects, duration_seconds, concurrency,
                                 obj_size_kb, upload_concurrency, test_instances_worker_ids):
    for test_instance in test_instance_api.iterate_all():
        if test_instances_zones and test_instance['zone'] not in test_instances_zones:
            continue
        if test_instances_roles and test_instance['role'] not in test_instances_roles:
            continue
        if test_instances_worker_ids and test_instance['worker_id'] not in test_instances_worker_ids:
            continue
        print("preparing default bucket for test instance {} ({} {})".format(test_instance['worker_id'], test_instance['zone'], test_instance['role']))
        prepare_default_bucket(method, test_instance['worker_id'], test_instance['hostname'], objects, obj_size_kb, upload_concurrency=upload_concurrency)
