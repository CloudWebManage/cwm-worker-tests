import click

from cwm_worker_tests import common_cli
from cwm_worker_tests import load_generator_custom


@click.group()
def custom():
    pass


custom.add_command(common_cli.LoadGeneratorRunCommand('custom'))


@custom.command(help="default METHOD: http")
@click.argument('METHOD', default='http', required=False)
@click.argument('WORKER_ID', required=True)
@click.argument('HOSTNAME', required=True)
@click.option('--objects', type=int, default=10, show_default=True)
@click.option('--duration_seconds', type=int, default=10, show_default=True)
@click.option('--concurrency', type=int, default=6, show_default=True)
@click.option('--obj_size_kb', type=int, default=1, show_default=True)
@click.option('--bucket_name', type=str)
@click.option('--skip-delete-worker', is_flag=True)
@click.option('--skip-clear-cache', is_flag=True)
@click.option('--skip-clear-volume', is_flag=True)
@click.option('--skip-all', is_flag=True)
@click.option('--upload-concurrency', type=int)
def prepare_custom_bucket(**kwargs):
    bucket_name = load_generator_custom.prepare_custom_bucket(**kwargs)
    print('OK')
    print('bucket_name={}'.format(bucket_name))
