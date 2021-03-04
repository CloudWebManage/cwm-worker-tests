import time
import uuid
import datetime
import subprocess

from cwm_worker_cluster import config


def assert_warp_version():
    ret, out = subprocess.getstatusoutput('warp --version')
    assert ret == 0, out
    assert out.strip() == 'warp version 0.3.40 - d3ee60c'


def is_warp_preparing_server_error(out):
    out = out.strip()
    return (
        'Error preparing server' in out
        and (
            out.endswith('504 Gateway Time-out.')
            or out.endswith('timeout awaiting response headers.')
            or out.endswith('The specified bucket does not exist.')
            or out.endswith('Access Denied.')
            or out.endswith('A timeout exceeded while waiting to proceed with the request, please reduce your request rate.')
            or out.endswith('Retry again.')
            or out.endswith('We encountered an internal error, please try again.')
            or out.endswith('503 Service Unavailable.')
            or out.endswith('The specified key does not exist.')
            or out.endswith('You did not provide the number of bytes specified by the Content-Length HTTP header.')
        )
    )


def get_warp_cmd(method, domain_name, objects, duration, concurrency, obj_size, benchdatafilename):
    bucket_name = str(uuid.uuid4())
    print("Load testing {} bucket={}".format(method, bucket_name))
    cmd = (
        'warp mixed --no-color --noclear '
        '{deployment_warp_args} '
        '--objects "{objects}" '
        '--duration "{duration}" '
        '--concurrent {concurrency} '
        '--obj.size {obj_size} '
        '--benchdata {benchdatafilename}.{method} '
        '--bucket {bucket_name} 2>&1'
    ).format(
        objects=objects,
        duration=duration,
        bucket_name=bucket_name,
        concurrency=concurrency,
        obj_size=obj_size,
        benchdatafilename=benchdatafilename,
        deployment_warp_args=config.get_deployment_warp_args(method, domain_name),
        method=method
    )
    return cmd, bucket_name


def run(method, domain_name, objects, duration_seconds, concurrency, obj_size_kb,
        benchdatafilename, custom_load_options=None, use_default_bucket=False):
    assert not use_default_bucket, 'use_default_bucket is not supported for warp load generator'
    duration = '{}s'.format(duration_seconds)
    obj_size = '{}KiB'.format(obj_size_kb)
    start_time = datetime.datetime.now()
    max_prepare_server_seconds = int(duration_seconds / 5)
    assert_warp_version()
    cmd, bucket_name = get_warp_cmd(method, domain_name, objects, duration, concurrency, obj_size, benchdatafilename)
    ret, out = subprocess.getstatusoutput(cmd)
    while ret != 0 and is_warp_preparing_server_error(out):
        elapsed_seconds = (datetime.datetime.now() - start_time).total_seconds()
        assert elapsed_seconds <= max_prepare_server_seconds, "Waited too long for prepare server, giving up ({}s)".format(elapsed_seconds)
        print(out)
        print("Failed to prepare server, waiting 5 seconds and retrying ({}s)".format(elapsed_seconds))
        time.sleep(5)
        cmd, bucket_name = get_warp_cmd(method, domain_name, objects, duration, concurrency, obj_size, benchdatafilename)
        ret, out = subprocess.getstatusoutput(cmd)
    elapsed_seconds = (datetime.datetime.now() - start_time).total_seconds()
    print("warp completed ({}s)".format(elapsed_seconds))
    assert ret == 0, out
    return out, bucket_name
