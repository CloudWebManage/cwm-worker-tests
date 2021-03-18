import os
import json
import pytz
import shutil
import datetime
import traceback
from glob import glob
from pprint import pprint
from collections import defaultdict

import dataflows as DF

from cwm_worker_cluster import config
import cwm_worker_tests.distributed_load_test
from cwm_worker_tests.multi_dict_generator import multi_dict_generator


def run_test(testnum, test, dry_run, skip_add_clear_workers=False, skip_prepare_load_generator=False):
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
            'skip_add_clear_workers': skip_add_clear_workers,
            'skip_prepare_load_generator': skip_prepare_load_generator
        },
        with_deploy=True if testnum == 1 else False
    )
    if dry_run:
        pprint(kwargs)
    else:
        cwm_worker_tests.distributed_load_test.main(**kwargs)


def check_skip_prepare_load_generator(tests, testnum, prepare_load_generator):
    if prepare_load_generator == 'skip':
        print("skipping prepare_load_generator because global prepare_load_generator = skip")
        return True
    if prepare_load_generator == 'force':
        print("forcing prepare_load_generator because global prepare_load_generator = force")
        return False
    cur_test = tests[testnum-1]
    if cur_test.get('prepare_load_generator') == 'skip':
        print("test {}: skipping prepare_load_generator because prepare_load_generator for test is skip".format(testnum))
        return True
    if cur_test.get('prepare_load_generator') == 'force':
        print("test {}: forcing prepare_load_generator because prepare_load_generator for test is force".format(testnum))
        return False
    if testnum == 1:
        print("test {} is the first test, will not skip prepare_load_generator".format(testnum))
        return False
    prev_test = tests[testnum-2]
    if prev_test.get('objects', 10) != cur_test.get('objects', 10):
        print("test {} has different objects than last test, will not skip prepare_load_generator".format(testnum))
        return False
    if prev_test.get('obj_size_kb', 10) != cur_test.get('obj_size_kb', 10):
        print("test {} has different obj_size_kb than last test, will not skip prepare_load_generator".format(testnum))
        return False
    if prev_test.get('load_generator', 'warp') != cur_test.get('load_generator', 'warp'):
        print("test {} has different load_generator than last test, will not skip prepare_load_generator".format(testnum))
        return False
    if not cur_test.get('custom_load_options', {}).get('number_of_random_domain_names'):
        print("test {} does not have number_of_random_domain_names, will not skip prepare_load_generator".format(testnum))
        return False
    if not prev_test.get('custom_load_options', {}).get('number_of_random_domain_names'):
        print("previous test of {} does not have number_of_random_domain_names, will not skip prepare_load_generator".format(testnum))
        return False
    if cur_test['custom_load_options']['number_of_random_domain_names'] != prev_test['custom_load_options']['number_of_random_domain_names']:
        print("test {} has different number_of_random_domain_names than last test, will not skip prepare_load_generator".format(testnum))
        return False
    print("test {} qualifies for skipping prepare_load_generator".format(testnum))
    return True


def check_skip_add_clear_workers(tests, testnum, add_clear_workers):
    if add_clear_workers == 'skip':
        print("skipping add clear workers because global add_clear_workers = skip")
        return True
    if add_clear_workers == 'force':
        print("forcing add clear workers because global add_clear_workers = force")
        return False
    cur_test = tests[testnum-1]
    if cur_test.get('add_clear_workers') == 'skip':
        print("test {}: skipping add clear workers because add_clear_workers for this test is skip")
        return True
    if cur_test.get('add_clear_workers') == 'force':
        print("test {}: forcing add clear workers because add_clear_workers for this test is force")
        return False
    if testnum == 1:
        print("test {} is the first test, will not skip add clear workers".format(testnum))
        return False
    prev_test = tests[testnum-2]
    if not cur_test.get('custom_load_options', {}).get('number_of_random_domain_names'):
        print("test {} does not have number_of_random_domain_names, will not skip add clear workers".format(testnum))
        return False
    if not prev_test.get('custom_load_options', {}).get('number_of_random_domain_names'):
        print("previous test of {} does not have number_of_random_domain_names, will not skip add clear workers".format(testnum))
        return False
    if cur_test['custom_load_options']['number_of_random_domain_names'] != prev_test['custom_load_options']['number_of_random_domain_names']:
        print("test {} has different number_of_random_domain_names than last test, will not skip add clear workers".format(testnum))
        return False
    print("test {} qualifies for skipping add clear workers".format(testnum))
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


def get_from_path_parts(*pathparts, parse_json=False):
    path = os.path.join(*pathparts)
    if os.path.exists(path):
        if parse_json:
            with open(path) as f:
                return json.load(f)
        else:
            return path
    else:
        return None


def aggregate_multi_test_stats():
    tests = {}
    error_percent_buckets = [1, 5, 50, 100]
    for testpath in glob(".distributed-load-test-multi/*"):
        try:
            testnum = int(testpath.replace(".distributed-load-test-multi/", ''))
        except:
            testnum = None
        if testnum is None:
            continue
        tests[testnum] = test = {
            'path': testpath,
            'test': get_from_path_parts(testpath, 'test.json', parse_json=True),
            'result': get_from_path_parts(testpath, 'result.json', parse_json=True),
            'progress': get_from_path_parts(testpath, 'output', 'progress.json', parse_json=True),
            'bench_data_stats_package': get_from_path_parts(testpath, 'output', 'warp_bench_data_stats', 'datapackage.json'),
            'bench_data_errors_package': get_from_path_parts(testpath, 'output', 'warp_bench_data_errors', 'datapackage.json'),
        }
        if test['bench_data_stats_package']:
            totals_row = None
            try:
                proto_sums = {'http': defaultdict(int), 'https': defaultdict(int)}
                proto_counts = {'http': 0, 'https': 0}
                endpoint_total_error_percent_buckets = {bucket: 0 for bucket in error_percent_buckets}
                for row in DF.Flow(DF.load(test['bench_data_stats_package'])).results()[0][0]:
                    if row['endpoint'] == '*' or row['datacenter'] == '*':
                        assert not totals_row
                        totals_row = row
                        proto = None
                    elif row['endpoint'].startswith('https://'):
                        proto = 'https'
                    elif row['endpoint'].startswith('http://'):
                        proto = 'http'
                    else:
                        raise Exception("Invalid endpoint: {}".format(row['endpoint']))
                    if proto is not None:
                        proto_counts[proto] += 1
                        for key in ['total-percent-errors', 'total-successful-requests-per-second', 'total-megabytes-per-second']:
                            proto_sums[proto][key] += row[key]
                        for bucket in sorted(endpoint_total_error_percent_buckets.keys()):
                            if row['total-percent-errors'] <= bucket:
                                endpoint_total_error_percent_buckets[bucket] += 1
                                break
                assert totals_row
                for proto in ['http', 'https']:
                    for key, value in proto_sums[proto].items():
                        totals_row['{}-avg-{}'.format(proto, key)] = value / proto_counts[proto]
                for bucket in endpoint_total_error_percent_buckets.keys():
                    totals_row['endpoints_error_percent_{}'.format(bucket)] = endpoint_total_error_percent_buckets[bucket]
                totals_row['result'] = test['result']
            except Exception as e:
                print("Exception processing testnum {}".format(testnum))
                traceback.print_exc()
                if not totals_row:
                    totals_row = {
                        '__has_error__': True,
                        'result': False,
                        'error': 'unexpected exception: {}'.format(e)
                    }
        else:
            totals_row = {
                '__has_error__': True,
                'result': False,
                'error': 'missing bench_data_stats package'
            }
        test['totals_row'] = totals_row
        totals_row['testnum'] = testnum
        for key in ["objects", "duration_seconds", "concurrency", "obj_size_kb", "only_test_method", "load_generator", "force_skip_add_clear_prepare"]:
            totals_row[key] = (test['test'].get(key) or '')
        totals_row['num_load_servers'] = (test['test'].get('num_extra_eu_servers') or 0) + (test['test'].get('num_base_servers') or 0)
        totals_row['num_domain_names'] = test['test'].get('custom_load_options', {}).get('number_of_random_domain_names', 0)
        totals_row['make_put_or_del_every_iterations'] = test['test'].get('custom_load_options', {}).get('make_put_or_del_every_iterations', 0)
        totals_row['start'] = (test.get('progress') or {}).get('cwm_worker_tests.distributed_tests.distributed_load_tests:run_distributed_load_tests', {}).get('start_load_tests_start')
        totals_row['end'] = (test.get('progress') or {}).get('cwm_worker_tests.distributed_tests.distributed_load_tests:run_distributed_load_tests', {}).get('finalize_load_tests_end')
        start_ts = str(int(datetime.datetime.strptime(totals_row['start'], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.timezone('Israel')).timestamp() * 1000)) if totals_row['start'] else ''
        end_ts = str(int(datetime.datetime.strptime(totals_row['end'], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.timezone('Israel')).timestamp() * 1000)) if totals_row['end'] else ''
        totals_row['grafana'] = config.LOAD_TESTING_GRAFANA_DASHBOARD_URL_TEMPLATE.format(start_ts=start_ts, end_ts=end_ts)

    def totals_rows():
        for testnum in list(sorted([k for k, v in tests.items() if not v.get('__has_error__')])) + list(sorted([k for k, v in tests.items() if v.get('__has_error__')])):
            r = tests[testnum]['totals_row']
            yield {
                **{k: r.get(k, '') for k in [
                    'testnum', 'result', 'error', 'start', 'end', 'grafana',
                    "objects", "duration_seconds", "concurrency", "obj_size_kb", "only_test_method", "load_generator", "force_skip_add_clear_prepare",
                    'num_load_servers', 'num_domain_names', 'make_put_or_del_every_iterations',
                    'total-percent-errors', 'total-successful-requests-per-second', 'total-megabytes-per-second',
                    *['endpoints_error_percent_{}'.format(bucket) for bucket in error_percent_buckets],
                    'http-avg-total-percent-errors', 'https-avg-total-percent-errors',
                    'http-avg-total-successful-requests-per-second', 'https-avg-total-successful-requests-per-second',
                    'http-avg-total-megabytes-per-second', 'https-avg-total-megabytes-per-second',
                    'GET-megabytes-per-second', 'PUT-megabytes-per-second',
                    'GET-percent-errors', 'PUT-percent-errors', 'STAT-percent-errors', 'DELETE-percent-errors',
                    'GET-successful-requests-per-second', 'PUT-successful-requests-per-second', 'STAT-successful-requests-per-second', 'DELETE-successful-requests-per-second',
                    'GET-request-duration-percent-1', 'GET-request-duration-percent-5', 'GET-request-duration-percent-10', 'GET-request-duration-percent-20', 'GET-request-duration-percent-inf',
                    'PUT-request-duration-percent-1', 'PUT-request-duration-percent-5', 'PUT-request-duration-percent-10', 'PUT-request-duration-percent-20', 'PUT-request-duration-percent-inf'
                ]},
            }

    DF.Flow(
        totals_rows(),
        DF.dump_to_path(os.path.join('.distributed-load-test-multi', 'totals'))
    ).process()


def main(tests_config):
    tests = tests_config['tests']
    dry_run = tests_config.get('dry_run', False)
    defaults = tests_config.get('defaults', {})
    stop_on_error = tests_config.get('stop_on_error', True)
    multi_values = tests_config.get('multi_values', {})
    custom_load_options = tests_config.get('custom_load_options', {})
    add_clear_workers = tests_config.get('force_add_clear_workers')
    prepare_load_generator = tests_config.get('force_prepare_load_generator')
    assert add_clear_workers in [None, 'skip', 'force']
    assert prepare_load_generator in [None, 'skip', 'force']
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
            run_test(
                i, test, dry_run,
                skip_add_clear_workers=check_skip_add_clear_workers(tests, i, add_clear_workers),
                skip_prepare_load_generator=check_skip_prepare_load_generator(tests, i, prepare_load_generator)
            )
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
    aggregate_multi_test_stats()
    exit(0 if all([r['ok'] for r in results.values()]) else 1)
