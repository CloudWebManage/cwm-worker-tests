import os
import json
import shutil
import traceback
from pprint import pprint

from cwm_worker_cluster import config
import cwm_worker_tests.distributed_load_test
from cwm_worker_tests.multi_dict_generator import multi_dict_generator


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
    if cur_test.get('force_skip_add_clear_prepare'):
        print("skipping add clear prepare because force_skip_add_clear_prepare is true")
        return True
    if testnum == 1:
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


def parse_multi_tests_custom_load_options(custom_load_options, tests):
    new_tests = []
    for test in tests:
        test['custom_load_options'] = {**custom_load_options}
        for key in ['number_of_random_domain_names', 'make_put_or_del_every_iterations']:
            value = test.pop(key, None)
            if value is not None:
                test['custom_load_options'] = {**test['custom_load_options'], key: value}
        new_tests.append(test)
    return new_tests


def get_multi_tests(tests, defaults, multi_values, custom_load_options):
    return parse_multi_tests_custom_load_options(custom_load_options, multi_dict_generator(defaults, multi_values, tests))


def main(tests_config):
    tests = tests_config['tests']
    dry_run = tests_config.get('dry_run', False)
    defaults = tests_config.get('defaults', {})
    stop_on_error = tests_config.get('stop_on_error', True)
    multi_values = tests_config.get('multi_values', {})
    custom_load_options = tests_config.get('custom_load_options', {})
    print("Running multiple tests")
    pprint({"dry_run": dry_run,
            "defaults": defaults,
            "multi_values": multi_values,
            "custom_load_options": custom_load_options,
            "stop_on_error": stop_on_error,
            "tests": tests})
    tests = get_multi_tests(tests, defaults, multi_values, custom_load_options)
    print("Number of multi-tests: {}".format(len(tests)))
    multi_path_root = ".distributed-load-test-multi"
    shutil.rmtree(multi_path_root, ignore_errors=True)
    os.mkdir(multi_path_root)
    results = {}
    for i, test in enumerate(tests, 1):
        results[i] = {}
        test_path = os.path.join(".distributed-load-test-multi", str(i))
        os.mkdir(test_path)
        with open(os.path.join(test_path, "test.json"), "w") as f:
            json.dump({**test, **config.get_distributed_load_tests_global_env_vars()}, f)
        shutil.rmtree(".distributed-load-test-output", ignore_errors=True)
        print("Multi test #{}".format(i))
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
