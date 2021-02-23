import json
import base64

import click

from cwm_worker_cluster import config
import cwm_worker_tests.load_test
import cwm_worker_tests.distributed_load_test
import cwm_worker_tests.distributed_load_test_multi


@click.group(context_settings={'max_content_width': 200})
def main():
    """Run integration tests and related tools for cwm-worker components"""
    pass


def parse_custom_load_options(custom_load_options):
    if custom_load_options:
        if not isinstance(custom_load_options, dict):
            custom_load_options = custom_load_options.strip()
            if custom_load_options.startswith('b64:'):
                custom_load_options = base64.b64decode(custom_load_options[4:]).decode()
            elif not custom_load_options.startswith('{'):
                custom_load_options = base64.b64decode(custom_load_options).decode()
            custom_load_options = json.loads(custom_load_options)
    else:
        custom_load_options = {}
    return custom_load_options


@main.command()
@click.option('--objects', default=10, type=int)
@click.option('--duration-seconds', default=10, type=int)
@click.option('--domain-name', type=str)
@click.option('--skip-delete-worker', is_flag=True)
@click.option('--skip-clear-volume', is_flag=True)
@click.option('--eu-load-test-domain-num', type=int)
@click.option('--concurrency', default=6, type=int)
@click.option('--obj-size-kb', default=10, type=int)
@click.option('--benchdatafilename', default='warp-bench-data', type=str)
@click.option('--skip_add_worker', is_flag=True)
@click.option('--protocol', type=str)
@click.option('--load_generator', default='warp', type=str)
@click.option('--custom-load-options', type=str, help="json string or base64 encoded json string prefixed with 'b64:'")
def load_test(**kwargs):
    kwargs['custom_load_options'] = parse_custom_load_options(kwargs.get('custom_load_options'))
    cwm_worker_tests.load_test.main(**kwargs)


@main.command()
@click.argument('TYPE', default='warp')
@click.option('--method', default='http', type=str)
@click.option('--domain-name', default=config.LOAD_TESTING_DOMAIN, type=str)
@click.option('--objects', default=10, type=int)
@click.option('--duration-seconds', default=10, type=int)
@click.option('--concurrency', default=6, type=int)
@click.option('--obj-size-kb', default=10, type=int)
@click.option('--benchdatafilename', default='warp-bench-data', type=str)
@click.option('--custom-load-options', type=str, help="json string or base64 encoded json string prefixed with 'b64:'")
def load_generator(type, **kwargs):
    kwargs['custom_load_options'] = parse_custom_load_options(kwargs.get('custom_load_options'))
    out, bucket_name = cwm_worker_tests.load_test.get_load_generator_run_method(type)(**kwargs)
    print('bucket_name={}'.format(bucket_name))
    print(out)


@main.command()
@click.option('--objects', default=10, type=int)
@click.option('--duration-seconds', default=10, type=int)
@click.option('--concurrency', default=6, type=int)
@click.option('--obj-size-kb', default=10, type=int)
@click.option('--num-extra-eu-servers', default=0, type=int)
@click.option('--num-base-servers', default=4, type=int)
@click.option('--base-servers-all-eu', is_flag=True)
@click.option('--only-test-method', type=str)
@click.option('--load_generator', default='warp', type=str)
@click.option('--custom-load-options', type=str, help="json string or base64 encoded json string prefixed with 'b64:'")
@click.option('--with-deploy', is_flag=True)
def distributed_load_test(**kwargs):
    """Run a distributed load test using Kamatera servers

    Required environment variables:

    see cwm_worker_cluster.config.get_distributed_load_tests_global_env_vars
    """
    kwargs['custom_load_options'] = parse_custom_load_options(kwargs.get('custom_load_options'))
    cwm_worker_tests.distributed_load_test.main(**kwargs)


@main.command()
@click.argument('TESTS_CONFIG', help="json string or base64 encoded json string prefixed with 'b64:'")
def distributed_load_test_multi(tests_config):
    """Run multiple distributed load tests

    TESTS_CONFIG keys:

    defaults - default values for all tests (correspond to distributed load test arguments)
    tests - list of objects, each object is a test which will run with args to override in the defaults for this test
    dry_run - boolean
    stop_on_error - boolean (default=true)
    """
    tests_config = parse_custom_load_options(tests_config)
    cwm_worker_tests.distributed_load_test_multi.main(tests_config)
