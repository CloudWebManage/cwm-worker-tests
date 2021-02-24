import os
import json
import shutil
import traceback
from pprint import pprint

from cwm_worker_cluster import config
import cwm_worker_tests.distributed_load_test


def run_test(testnum, test, dry_run):
    kwargs = dict(
        objects=test.get('objects', 10), duration_seconds=test.get('duration_seconds', 10),
        concurrency=test.get('concurrency', 6), obj_size_kb=test.get('obj_size_kb', 10),
        num_extra_eu_servers=test.get('num_extra_eu_servers', 0),
        num_base_servers=test.get('num_base_servers', 4),
        base_servers_all_eu=test.get('base_servers_all_eu', True),
        only_test_method=test.get('only_test_method'),
        load_generator=test.get('load_generator', 'warp'),
        custom_load_options=test.get('custom_load_options', {}),
        with_deploy=True if testnum == 1 else False
    )
    if dry_run:
        pprint(kwargs)
    else:
        cwm_worker_tests.distributed_load_test.main(**kwargs)


def main(tests_config):
    tests = tests_config['tests']
    dry_run = tests_config.get('dry_run', False)
    defaults = tests_config.get('defaults', None)
    stop_on_error = tests_config.get('stop_on_error', True)
    print("Running multiple tests ({})".format(len(tests)))
    if not defaults:
        defaults = {}
    pprint({"dry_run": dry_run, "defaults": defaults})
    multi_path_root = ".distributed-load-test-multi"
    shutil.rmtree(multi_path_root, ignore_errors=True)
    os.mkdir(multi_path_root)
    results = {}
    for i, test in enumerate(tests, 1):
        test = {**defaults, **test}
        results[i] = {}
        test_path = os.path.join(".distributed-load-test-multi", str(i))
        os.mkdir(test_path)
        with open(os.path.join(test_path, "test.json"), "w") as f:
            json.dump({**test, **config.get_distributed_load_tests_global_env_vars()}, f)
        shutil.rmtree(".distributed-load-test-output", ignore_errors=True)
        print("Multi test #{}".format(i))
        pprint(test)
        ok = results[i]['ok'] = True
        try:
            run_test(i, test, dry_run)
        except:
            traceback.print_exc()
            ok = results[i]['ok'] = False
        with open(os.path.join(test_path, "result.json"), "w") as f:
            f.write("true" if ok else "false")
        print("ok={}".format(ok))
        if os.path.exists(".distributed-load-test-output"):
            shutil.move(".distributed-load-test-output", os.path.join(test_path, "output"))
        if not ok and stop_on_error:
            break
    pprint(results)
    exit(0 if all([r['ok'] for r in results.values()]) else 1)
