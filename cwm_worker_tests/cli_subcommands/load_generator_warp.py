import click

from cwm_worker_tests import common_cli
import cwm_worker_tests.load_test


@click.group()
def warp():
    """load generation based on Minio warp load testing tool, supports only a single hostname"""
    pass


@warp.command()
@click.option('--method', default='http', show_default=True, type=str, help='http / https')
@click.option('--worker-id', type=str, help='test instance worker id')
@click.option('--hostname', type=str, help='test instance hostname')
@click.option('--objects', default=10, show_default=True, type=int, help='number of objects to upload for testing')
@click.option('--duration-seconds', default=10, show_default=True, type=int, help='duration of the test in seconds')
@click.option('--concurrency', default=6, show_default=True, type=int, help='number of concurrent threads')
@click.option('--obj-size-kb', default=10, show_default=True, type=int, help='size in kb of each object')
@click.option('--benchdatafilename', default='warp-bench-data', show_default=True, type=str, help='name of file to save results to')
@click.option('--custom-load-options', type=str, hidden=True, help="not used in warp load generation")
def run(**kwargs):
    kwargs['custom_load_options'] = common_cli.parse_json_data(kwargs.get('custom_load_options'))
    out, bucket_name = cwm_worker_tests.load_test.get_load_generator_run_method('warp')(**kwargs)
    print('bucket_name={}'.format(bucket_name))
    print(out)
