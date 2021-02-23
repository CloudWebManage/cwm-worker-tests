import os
import json
import datetime
import traceback
import contextlib

import pytz

from cwm_worker_tests.distributed_tests import distributed_load_tests


class SubProgress:

    def __init__(self, sub, root_progress):
        self.sub = sub
        self.root_progress = root_progress

    def set(self, key, value):
        return self.root_progress.set(self.sub, key, value)

    def set_now(self, key):
        return self.root_progress.set_now(self.sub, key)

    def get(self, key):
        return self.root_progress.get(self.sub, key)

    @contextlib.contextmanager
    def set_start_end(self, start_key, end_key):
        self.set_now(start_key)
        try:
            yield
        except:
            self.set('{}__exception'.format(end_key), traceback.format_exc())
            raise
        finally:
            self.set_now(end_key)


class RootProgress:

    def __init__(self):
        self.progress = {}

    def set(self, sub, key, value):
        self.progress.setdefault(sub, {})[key] = value
        with open(os.path.join(distributed_load_tests.DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, 'progress.json'), 'w') as f:
            json.dump(self.progress, f, indent=2)
        return value

    def set_now(self, sub, key):
        return self.set(sub, key, datetime.datetime.now().astimezone(pytz.timezone('Israel')).strftime('%Y-%m-%dT%H:%M:%S'))

    def get(self, sub, key):
        return self.progress.get(sub, {}).get(key)

    def get_sub(self, sub, *args):
        return SubProgress(':'.join([sub, *args]), self)

    @contextlib.contextmanager
    def start_sub(self, sub, *args):
        sub_progress = self.get_sub(sub, *args)
        sub_progress.set_now('start')
        try:
            yield sub_progress
        except:
            sub_progress.set('end__exception', traceback.format_exc())
            raise
        finally:
            sub_progress.set_now('end')
