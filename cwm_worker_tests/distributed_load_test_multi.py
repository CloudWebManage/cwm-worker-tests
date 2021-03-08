import os
import json
import shutil
import traceback
from pprint import pprint

from cwm_worker_cluster import config
import cwm_worker_tests.distributed_load_test


def run_test(testnum, test, dry_run, skip_add_clear_prepare=False):
    kwargs = dict(
        objects=test.get('objects', 10), duration_seconds=test.get('duration_seconds', 10),
        concurrency=test.get('concurrency', 6), obj_size_kb=test.get('obj_size_kb', 10),
        num_extra_eu_servers=test.get('num_extra_eu_servers', 0),
        num_base_servers=test.get('num_base_servers', 4),
        base_servers_all_eu=test.get('base_servers_all_eu', True),
        only_test_method=test.get('only_test_method'),
        load_generator=test.get('load_generator', 'warp'),
        custom_load_options={
            **test.get('custom_load_options', {}),
            **(
                {
                   "skip_add_clear_workers": True, "skip_prepare_load_generator": True
                } if skip_add_clear_prepare else {}
            )
        },
        with_deploy=True if testnum == 1 else False
    )
    if dry_run:
        pprint(kwargs)
    else:
        cwm_worker_tests.distributed_load_test.main(**kwargs)


def check_skip_add_clear_prepare(tests, testnum):
    cur_test = tests[testnum-1]
    if testnum == 1:
        if cur_test.get('allow_skip_add_clear_prepare_first_test'):
            print("skipping add clear prepare even though it's the first test because allow_skip_add_clear_prepare_first_test is true")
            return True
        else:
            print("test {} is the first test, will not skip add clear prepare".format(testnum))
            return False
    prev_test = tests[testnum-2]
    if prev_test.get('objects', 10) != cur_test.get('objects', 10):
        print("test {} has different objects than last test, will not skip add clear prepare".format(testnum))
        return False
    if prev_test.get('obj_size_kb', 10) != cur_test.get('obj_size_kb', 10):
        print("test {} has different obj_size_kb than last test, will not skip add clear prepare".format(testnum))
        return False
    if prev_test.get('load_generator', 'warp') != cur_test.get('load_generator', 'warp'):
        print("test {} has different load_generator than last test, will not skip add clear prepare".format(testnum))
        return False
    if not cur_test.get('custom_load_options', {}).get('number_of_random_domain_names'):
        print("test {} does not have number_of_random_domain_names, will not skip add clear prepare".format(testnum))
        return False
    if not prev_test.get('custom_load_options', {}).get('number_of_random_domain_names'):
        print("previous test of {} does not have number_of_random_domain_names, will not skip add clear prepare".format(testnum))
        return False
    if cur_test['custom_load_options']['number_of_random_domain_names'] != prev_test['custom_load_options']['number_of_random_domain_names']:
        print("test {} has different number_of_random_domain_names than last test, will not skip add clear prepare".format(testnum))
        return False
    print("test {} qualifies for skipping add clear prepare".format(testnum))
    return True


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
            run_test(i, test, dry_run, check_skip_add_clear_prepare(tests, i))
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
