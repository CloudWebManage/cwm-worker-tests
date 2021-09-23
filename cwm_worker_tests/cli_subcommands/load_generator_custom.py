import click

from cwm_worker_tests import common_cli
from cwm_worker_tests import load_generator_custom


@click.group()
def custom():
    """load generation based on custom Python multi-threaded implementation"""
    pass


custom.add_command(common_cli.LoadGeneratorRunCommand('custom'))


@custom.command()
@click.option('--method', default='http', show_default=True, required=False)
@click.option('--worker-id', help='test instance worker id, if specified, hostname is not required')
@click.option('--hostname', help='test instance hostname, if specified, worker id is not required')
@click.option('--objects', type=int, default=10, show_default=True, help='number of objects to prepare the bucket for')
@click.option('--duration_seconds', type=int, default=10, show_default=True, help='duration in seconds of the test to prepare the bucket for')
@click.option('--concurrency', type=int, default=6, show_default=True, help='concurrency to prepare the bucket for')
@click.option('--obj_size_kb', type=int, default=1, show_default=True, help='object size in kb to preapre the bucket for')
@click.option('--bucket_name', type=str, help='name of the bucket to create and prepare')
@click.option('--skip-delete-worker', is_flag=True, help="don't delete and recreate the worker before preparing the bucket")
@click.option('--skip-clear-cache', is_flag=True, help="don't clear the worker cache before preparing the bucket")
@click.option('--skip-clear-volume', is_flag=True, help="don't clear the worker volume before preparing the bucket")
@click.option('--skip-all', is_flag=True, help="skip all operations and only prepare the bucket")
@click.option('--upload-concurrency', type=int, help='number of threads to start for uploading objects to the bucket')
def prepare_custom_bucket(**kwargs):
    """creates and uploads objects to a bucket in preparation for load generation"""
    bucket_name = load_generator_custom.prepare_custom_bucket(**kwargs)
    print('OK')
    print('bucket_name={}'.format(bucket_name))


@custom.command()
@click.option('--method', default='https', show_default=True, required=False)
@click.option('--num-test-instances', type=int, help='number of random instances to generate load for', default=5, show_default=True)
@click.option('--test-instances-zones', help='comma-separated list of test instance zones to test with, if not provided - tests all zones')
@click.option('--test-instances-roles', help='comma-separated list of test instance roles to test with', default='loadtest', show_default=True)
@click.option('--objects', type=int, default=10, show_default=True, help='number of objects to test with')
@click.option('--duration-seconds', type=int, default=10, show_default=True, help='duration in seconds of the test')
@click.option('--concurrency', type=int, default=6, show_default=True, help='test concurrency')
@click.option('--obj-size-kb', type=int, default=1, show_default=True, help='object size in kb')
@click.option('--benchdatafilename', default='custom-bench-data', help='write results to this file', show_default=True)
@click.option('--benchdata-format', type=click.Choice(['csv.gz', 'csv']), default='csv.gz', show_choices=True, show_default=True, help='format of the results file')
@click.option('--skip-prepare-bucket', is_flag=True, help="don't prepare the bucket, assuming it was prepared beforehand for all relevant test instances with prepare_default_bucket_multi")
@click.option('--do-cached-get', is_flag=True, help="Make unauthenticated HTTP GET requests which will go via the cdn cache layer")
def run_multi(**kwargs):
    kwargs['test_instances_zones'] = [s.strip() for s in kwargs['test_instances_zones'].split(',') if s.strip()] if kwargs.get('test_instances_zones') else []
    kwargs['test_instances_roles'] = [s.strip() for s in kwargs['test_instances_roles'].split(',') if s.strip()] if kwargs.get('test_instances_roles') else []
    out, bucket_name = load_generator_custom.run_multi(**kwargs)
    print('bucket_name={}'.format(bucket_name))
    print(out)


@custom.command()
@click.option('--method', default='http', show_default=True, required=False)
@click.option('--test-instances-zones', help='comma-separated list of test instance zones to prepare buckets for, if not provided - tests all zones')
@click.option('--test-instances-roles', help='comma-separated list of test instance roles to prepare buckets for', default='loadtest', show_default=True)
@click.option('--objects', type=int, default=10, show_default=True, help='number of objects to prepare the bucket for')
@click.option('--duration_seconds', type=int, default=10, show_default=True, help='duration in seconds of the test to prepare the bucket for')
@click.option('--concurrency', type=int, default=6, show_default=True, help='concurrency to prepare the bucket for')
@click.option('--obj_size_kb', type=int, default=1, show_default=True, help='object size in kb to preapre the bucket for')
@click.option('--upload-concurrency', type=int, help='number of threads to start for uploading objects to the bucket')
def prepare_default_bucket_multi(**kwargs):
    kwargs['test_instances_zones'] = [s.strip() for s in kwargs['test_instances_zones'].split(',') if s.strip()] if kwargs.get('test_instances_zones') else []
    kwargs['test_instances_roles'] = [s.strip() for s in kwargs['test_instances_roles'].split(',') if s.strip()] if kwargs.get('test_instances_roles') else []
    load_generator_custom.prepare_default_bucket_multi(**kwargs)
