import sys
import click
from ruamel import yaml

import cwm_worker_tests.load_test
import cwm_worker_tests.distributed_load_test
import cwm_worker_tests.distributed_load_test_multi
import cwm_worker_tests.cli_subcommands.load_generator_custom
import cwm_worker_tests.cli_subcommands.load_generator_warp
from cwm_worker_tests import common_cli
import cwm_worker_tests.distributed_tests.distributed_load_tests
import cwm_worker_tests.dns
import cwm_worker_tests.distributed_tests.create_servers


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
@click.option('--hostname', type=str)
@click.option('--worker-id', type=str)
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
@click.option('--base-servers-all-zone', type=str)
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
@click.option('--num-servers', required=True, type=int)
@click.option('--total-duration-seconds', default=10, type=int)
@click.option('--base-servers-all-zone', type=str)
@click.option('--only-test-method', type=str)
@click.option('--load-generator', default='warp', type=str)
def distributed_load_test_aggregate_test_results(**kwargs):
    kwargs['servers'] = {i: {} for i in range(1, kwargs.pop('num_servers')+1)}
    cwm_worker_tests.distributed_tests.distributed_load_tests.aggregate_test_results(**kwargs)


@main.command()
@click.argument('TESTS_CONFIG')
def distributed_load_test_multi(tests_config):
    """Run multiple distributed load tests

    TESTS_CONFIG keys:

    defaults - default values for all tests (correspond to distributed load test arguments)
    multi_values - keys to fill with multiple values from lists
    tests - list of objects, each object is a test which will run with args to override in the defaults for this test
    custom_load_options - custom load options to apply to all tests
    dry_run - boolean
    stop_on_error - boolean (default=true)
    add_clear_workers - none - will determine based on test values, skip - will skip for all tests, force = will force for all tests
    prepare_load_generator - none - will determine based on test values, skip - will skip for all tests, force = will force for all tests

    example tests_config with all defaults:
    {
        "defaults": {
            "add_clear_workers": null,
            "prepare_load_generator": null,
            "objects": 100,
            "duration_seconds": 600,
            "obj_size_kb": 100,
            "num_base_servers": 4,
            "base_servers_all_zone": "EU",
            "only_test_method": null,
            "load_generator": "custom",
            "concurrency": 1,
            "num_extra_eu_servers": 4,
            "number_of_random_domain_names": 7,
            "make_put_or_del_every_iterations": 1000
        },
        "multi_values": {
            "concurrency": [1,5],
            "num_extra_eu_servers": [1,4],
            "number_of_random_domain_names": [3, 5, 7]
        },
        "tests": [
            {"obj_size_kb": 100, "make_put_or_del_every_iterations": 1000},
            {"obj_size_kb": 1000, "make_put_or_del_every_iterations": 5000},
            {"obj_size_kb": 10000, "make_put_or_del_every_iterations": 20000}
        ],
        "custom_load_options": {
            "test_all_external_gateways": false,
            "test_all_cluster_zone": null
        },
        "dry_run": true,
        "stop_on_error": true
    }
    """
    tests_config = common_cli.parse_json_data(tests_config)
    cwm_worker_tests.distributed_load_test_multi.main(tests_config)


@main.command()
def check_domain_dns():
    res = cwm_worker_tests.dns.check_domain()
    yaml.safe_dump(res, sys.stdout)


@main.command()
def distributed_load_test_delete_kept_servers():
    cwm_worker_tests.distributed_tests.create_servers.delete_kept_servers()
