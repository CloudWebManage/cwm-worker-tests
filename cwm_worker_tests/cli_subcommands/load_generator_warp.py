import click

from cwm_worker_tests import common_cli
import cwm_worker_tests.load_test


@click.group()
def warp():
    pass


@warp.command()
@click.option('--method', default='http', type=str)
@click.option('--worker-id', type=str)
@click.option('--hostname', type=str)
@click.option('--objects', default=10, type=int)
@click.option('--duration-seconds', default=10, type=int)
@click.option('--concurrency', default=6, type=int)
@click.option('--obj-size-kb', default=10, type=int)
@click.option('--benchdatafilename', default='warp-bench-data', type=str)
@click.option('--custom-load-options', type=str, help="json string or base64 encoded json string prefixed with 'b64:'")
def run(**kwargs):
    kwargs['custom_load_options'] = common_cli.parse_json_data(kwargs.get('custom_load_options'))
    out, bucket_name = cwm_worker_tests.load_test.get_load_generator_run_method('warp')(**kwargs)
    print('bucket_name={}'.format(bucket_name))
    print(out)
