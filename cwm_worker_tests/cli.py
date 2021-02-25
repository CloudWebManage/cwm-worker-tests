import click

import cwm_worker_tests.load_test
import cwm_worker_tests.distributed_load_test
import cwm_worker_tests.distributed_load_test_multi
import cwm_worker_tests.cli_subcommands.load_generator_custom
import cwm_worker_tests.cli_subcommands.load_generator_warp
from cwm_worker_tests import common_cli


@click.group(context_settings={'max_content_width': 200})
def main():
    """Run integration tests and related tools for cwm-worker components"""
    pass


@main.group()
def load_generator():
    pass


load_generator.add_command(cwm_worker_tests.cli_subcommands.load_generator_custom.custom)
load_generator.add_command(cwm_worker_tests.cli_subcommands.load_generator_warp.warp)


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
    kwargs['custom_load_options'] = common_cli.parse_json_data(kwargs.get('custom_load_options'))
    cwm_worker_tests.load_test.main(**kwargs)


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
    kwargs['custom_load_options'] = common_cli.parse_json_data(kwargs.get('custom_load_options'))
    cwm_worker_tests.distributed_load_test.main(**kwargs)


@main.command()
@click.argument('TESTS_CONFIG')
def distributed_load_test_multi(tests_config):
    """Run multiple distributed load tests

    TESTS_CONFIG keys:

    defaults - default values for all tests (correspond to distributed load test arguments)
    tests - list of objects, each object is a test which will run with args to override in the defaults for this test
    dry_run - boolean
    stop_on_error - boolean (default=true)

    example tests_config with all defaults:
    {
        "defaults": {
            "objects": 10, "duration_seconds": 10, "concurrency": 6, "obj_size_kb": 10, "num_extra_eu_servers": 0,
            "num_base_servers": 4, "base_servers_all_eu": true, "only_test_method": null, "load_generator": "warp",
            "custom_load_options": {}
        },
        "tests": [
            {"objects": 20},
            {"concurrency": 20}
        ],
        "dry_run": false,
        "stop_on_error": true
    }
    """
    tests_config = common_cli.parse_json_data(tests_config)
    cwm_worker_tests.distributed_load_test_multi.main(tests_config)
