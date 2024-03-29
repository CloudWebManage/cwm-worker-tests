import os
import json
import click
import base64

import cwm_worker_tests.load_test


def parse_json_data(data):
    if data:
        if os.path.exists(data):
            with open(data) as f:
                data = f.read()
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
                click.Option(['--worker-id'], type=str, help='test instance worker id, if specified, hostname is not required'),
                click.Option(['--hostname'], type=str, help='test instance hostname, if specified, worker id is not required'),
                click.Option(['--objects'], default=10, type=int, show_default=True, help='number of objects to test with'),
                click.Option(['--duration-seconds'], default=10, type=int, show_default=True, help='duration in seconds'),
                click.Option(['--concurrency'], default=6, type=int, show_default=True, help='number of load generation threads to start'),
                click.Option(['--obj-size-kb'], default=10, type=int, show_default=True, help='object size in kb'),
                click.Option(['--benchdatafilename'], type=str, help="optional file to save with benchdata"),
                click.Option(['--custom-load-options'], type=str, help="json string or base64 encoded json string prefixed with 'b64:'"),
                click.Option(['--use-default-bucket'], is_flag=True, help="use a default bucket, verify it is valid and prepare only if needed")
            ]
        )

    def load_generator_run_callback(self, **kwargs):
        kwargs['custom_load_options'] = parse_json_data(kwargs.get('custom_load_options'))
        out, bucket_name = cwm_worker_tests.load_test.get_load_generator_run_method(self.load_generator_run_type)(**kwargs)
        print('bucket_name={}'.format(bucket_name))
        print(out)
