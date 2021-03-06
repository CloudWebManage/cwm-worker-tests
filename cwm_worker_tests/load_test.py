import time
import datetime
from pprint import pprint

from cwm_worker_cluster import config
from cwm_worker_cluster import common
from cwm_worker_cluster import worker
from cwm_worker_cluster import dummy_api


def get_now_string():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M')


def get_load_generator_run_method(load_generator):
    if load_generator == 'warp':
        from cwm_worker_tests.load_generator_warp import run
    elif load_generator == 'custom':
        from cwm_worker_tests.load_generator_custom import run
    else:
        raise Exception("Unknown load generator: {}".format(load_generator))
    return run


def assert_load_test(domain_name, objects, duration_seconds, concurrency, obj_size_kb, benchdatafilename, method, load_generator,
                     custom_load_options=None):
    run = get_load_generator_run_method(load_generator)
    out, bucket_name = run(method, domain_name, objects, duration_seconds, concurrency, obj_size_kb, benchdatafilename,
                           custom_load_options)
    # WARNING! Do not change this string, it is checked in stats aggregation code
    print('--- {} load test successful, bucket_name={} | {} ---'.format(method, bucket_name, get_now_string()))
    print(out)


def main(objects:int, duration_seconds:int, domain_name:str, skip_delete_worker:bool, skip_clear_volume:bool,
         eu_load_test_domain_num:int, concurrency:int, obj_size_kb:int, benchdatafilename:str,
         skip_add_worker:bool, protocol:str, load_generator:str, custom_load_options:dict):
    if custom_load_options.get('random_domain_names'):
        assert not domain_name and not eu_load_test_domain_num, "when using random domain names, specific domain name params are not allowed"
        assert load_generator != 'warp', 'warp load_generator does not support random domain names'
        assert isinstance(custom_load_options['random_domain_names'], list)
        assert skip_delete_worker, 'skip_delete_worker must be set to true when using random domain names'
        assert skip_clear_volume, 'skip_clear_volume must be set to true when using random domain names'
        assert skip_add_worker, 'skip_add_worker must be set to true when using random domain names'
    cwmc_name = common.get_cwmc_name()
    print('test_load CWMC_NAME={} load_generator={}'.format(cwmc_name, load_generator))
    if skip_add_worker:
        node, node_name, node_ip = None, None, None
        print('skipping adding worker api / volume')
        skip_delete_worker = True
        skip_clear_volume = True
    else:
        node = common.get_cluster_nodes('worker')[0]
        node_name = node['name']
        node_ip = node['ip']
        print('node_name={} node_ip={}'.format(node_name, node_ip))
    if not domain_name and not custom_load_options.get('random_domain_names'):
        if cwmc_name == config.LOAD_TESTING_CWMC_NAME:
            if eu_load_test_domain_num and int(eu_load_test_domain_num) > 1:
                domain_name = config.LOAD_TESTING_DOMAIN_NUM_TEMPLATE.format(eu_load_test_domain_num)
            else:
                domain_name = config.LOAD_TESTING_DOMAIN
        else:
            assert node_ip, 'missing node_ip'
            domain_name = node_ip.replace('.', '-') + config.AUTO_DOMAIN_IP_SUFFIX
    if not skip_delete_worker:
        worker.delete(domain_name)
    if not skip_add_worker:
        cluster_zone = common.get_cluster_zone()
        dummy_api.add_example_site(domain_name, domain_name, cluster_zone)
        volume_id = domain_name.replace('.', '--')
        worker.add_clear_volume(volume_id, skip_clear_volume=skip_clear_volume)
        print("Sleeping 5 seconds to ensure everything is ready...")
        time.sleep(5)
        print("Warming up the site")
        pprint(common.assert_site(domain_name, node_ip))
    if not protocol or protocol == 'http':
        assert_load_test(domain_name, objects, duration_seconds, concurrency, obj_size_kb, benchdatafilename,
                         'http', load_generator, custom_load_options)
    if not protocol or protocol == 'https':
        assert_load_test(domain_name, objects, duration_seconds, concurrency, obj_size_kb, benchdatafilename,
                         'https', load_generator, custom_load_options)
