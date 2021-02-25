import json
import click
import base64

from cwm_worker_cluster import config
import cwm_worker_tests.load_test


def parse_json_data(data):
    if data:
        if not isinstance(data, dict):
            data = data.strip()
            if data.startswith('b64:'):
                data = base64.b64decode(data[4:]).decode()
            elif not data.startswith('{'):
                data = base64.b64decode(data).decode()
            data = json.loads(data)
    else:
        data = {}
    return data


class LoadGeneratorRunCommand(click.Command):
    
    def __init__(self, load_generator_run_type):
        self.load_generator_run_type = load_generator_run_type
        super(LoadGeneratorRunCommand, self).__init__(
            name='run',
            callback=self.load_generator_run_callback,
            params=[
                click.Option(['--method'], default='http', type=str, show_default=True),
                click.Option(['--domain-name'], default=config.LOAD_TESTING_DOMAIN, type=str, show_default=True),
                click.Option(['--objects'], default=10, type=int, show_default=True),
                click.Option(['--duration-seconds'], default=10, type=int, show_default=True),
                click.Option(['--concurrency'], default=6, type=int, show_default=True),
                click.Option(['--obj-size-kb'], default=10, type=int, show_default=True),
                click.Option(['--benchdatafilename'], type=str, help="optional file to save with benchdata"),
                click.Option(['--custom-load-options'], type=str, help="json string or base64 encoded json string prefixed with 'b64:'")
            ]
        )

    def load_generator_run_callback(self, **kwargs):
        kwargs['custom_load_options'] = parse_json_data(kwargs.get('custom_load_options'))
        out, bucket_name = cwm_worker_tests.load_test.get_load_generator_run_method(self.load_generator_run_type)(**kwargs)
        print('bucket_name={}'.format(bucket_name))
        print(out)
