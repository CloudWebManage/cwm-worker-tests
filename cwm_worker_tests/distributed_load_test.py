import datetime
import subprocess
from copy import deepcopy
from pprint import pprint

import pytz

from cwm_worker_cluster import config
from cwm_worker_cluster import common
from cwm_worker_tests.distributed_tests import distributed_load_tests
from cwm_worker_tests.distributed_tests.progress import RootProgress


def main(objects:int, duration_seconds:int, concurrency:int, obj_size_kb:int, num_extra_eu_servers:int,
         num_base_servers:int, base_servers_all_eu:bool, only_test_method:str, load_generator:str,
         custom_load_options:dict, with_deploy:bool):
    ret, out = subprocess.getstatusoutput("rm -rf {distdir}; mkdir {distdir}".format(distdir=distributed_load_tests.DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR))
    assert ret == 0, out
    root_progress = RootProgress()
    with root_progress.start_sub(__spec__.name, 'main') as progress:
        pprint(progress.set('global_env_vars', config.get_distributed_load_tests_global_env_vars()))
        if with_deploy:
            with progress.set_start_end('cwm_deploy_start', 'cwm_deploy_end'):
                common.cwm_deploy()
        if only_test_method == 'None':
            only_test_method = None
        assert not only_test_method or only_test_method in ['http', 'https']
        assert load_generator in ['warp', 'custom']
        if load_generator == 'custom' and 'random_domain_names' not in custom_load_options:
            if custom_load_options.get('test_all_external_gateways'):
                assert not custom_load_options.get('number_of_random_domain_names'), 'cannot specify number_of_random_domain_names with test_all_external_gateways'
                custom_load_options['random_domain_names'] = {
                    dnc['worker_id']: dnc['hostname']
                    for dnc in config.LOAD_TESTING_GATEWAYS.values() if dnc['type'] not in ['mock_geo', 'cwm']
                }
            else:
                custom_load_options['random_domain_names'] = {
                    config.get_load_testing_domain_num_worker_id(i+1): config.get_load_testing_domain_num_hostname(i+1)
                    for i in range(int(custom_load_options.get('number_of_random_domain_names', 7)))
                }
        if custom_load_options.get('random_domain_names'):
            assert load_generator == 'custom', 'random domain names is only supported for custom load generator'
            prepare_domain_names = custom_load_options['random_domain_names']
        else:
            prepare_domain_names = None
        start_time = datetime.datetime.now()
        print('{} start'.format(start_time))
        if num_base_servers > 4:
            raise Exception("Cannot have more then 4 base servers")
        elif num_base_servers < 4 and num_extra_eu_servers > 0:
            raise Exception("Cannot have extra eu servers if base servers are less then 4")
        load_test_kwargs = {
            'objects': objects,
            'duration_seconds': duration_seconds,
            'concurrency': concurrency,
            'obj_size_kb': obj_size_kb,
            'only_test_method': only_test_method,
            "custom_load_options": custom_load_options
        }
        pprint(progress.set('all_test_kwargs', {
            **load_test_kwargs,
            "num_extra_eu_servers": num_extra_eu_servers,
            "num_base_servers": num_base_servers,
            "base_servers_all_eu": base_servers_all_eu,
            "load_generator": load_generator
        }))
        servers = {}
        for i in range(num_base_servers):
            server_num = i+1
            if base_servers_all_eu:
                datacenter = 'EU'
            else:
                datacenter = {1: 'EU', 2: 'IL', 3: 'CA-TR', 4: 'EU-LO'}[server_num]
            servers[server_num] = {'datacenter': datacenter, **deepcopy(load_test_kwargs)}
        for i in range(5, 5+num_extra_eu_servers):
            servers[i] = {'datacenter': 'EU', **deepcopy(load_test_kwargs)}
        # run_failed = True
        run_failed = not distributed_load_tests.run_distributed_load_tests(servers, load_generator, prepare_domain_names, root_progress, custom_load_options)
        pprint({"run_failed": progress.set('run_failed_after_run_distributed_load_tests', run_failed)})
        end_time = datetime.datetime.now()
        print('{} end load test'.format(end_time))
        if not distributed_load_tests.aggregate_test_results(servers, duration_seconds, base_servers_all_eu, only_test_method, load_generator):
            run_failed = True
        pprint({"run_failed": progress.set('run_failed_after_aggregate_test_results', run_failed)})
        pprint({
            **load_test_kwargs,
            "num_extra_eu_servers": num_extra_eu_servers,
            "num_base_servers": num_base_servers,
            "base_servers_all_eu": base_servers_all_eu,
            'start_time': str(start_time.astimezone(pytz.timezone('Israel'))),
            'end_time': str(end_time.astimezone(pytz.timezone('Israel'))),
            "load_generator": load_generator
        })
        assert not run_failed
