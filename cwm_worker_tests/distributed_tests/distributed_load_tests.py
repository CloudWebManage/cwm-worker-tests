import os
import json
import time
import base64
import datetime
import traceback
import subprocess
from pprint import pprint
from collections import defaultdict

import dataflows as DF
import zstandard as zstd

from cwm_worker_cluster import common, config
from cwm_worker_cluster.worker import api as worker_api
from cwm_worker_cluster.cluster import api as cluster_api
from cwm_worker_tests.load_generator_custom import prepare_default_bucket
from cwm_worker_tests.distributed_tests import create_servers


DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR = '.distributed-load-test-output'
BUCKET_MAX_SECONDS = [1, 5, 10, 20]
BUCKET_INF = 'inf'


def get_domain_name_from_num(domain_num):
    # return config.get_load_testing_domain_num_hostname(domain_num if domain_num < 5 else 1)
    raise Exception("Using domain_num to determine hostname is not supported!")


def get_worker_id_from_num(domain_num):
    # return config.get_load_testing_domain_num_worker_id(domain_num if domain_num < 5 else 1)
    raise Exception("Using domain_num to determinet worker_id is not supported!")


def prepare_server_for_load_test(tempdir, server_ip):
    scp = 'scp -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(tempdir)
    ssh = 'ssh root@{} -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(server_ip, tempdir)
    ret, out = subprocess.getstatusoutput('''
        {ssh} "docker rm -f load_test_https; docker rm -f load_test_http; rm -rf /root/cwm-worker-cluster/.output" &&\
        {ssh} "mkdir /root/cwm-worker-cluster/.output" &&\
        {scp} -r /etc/kamatera root@{ip}:/etc/kamatera
    '''.format(ssh=ssh, scp=scp, ip=server_ip))
    assert ret == 0, out


def start_server_load_tests(tempdir, server_name, server_ip, load_test_domain_num, objects, duration_seconds, concurrency,
                            obj_size_kb, protocol, custom_load_options, load_generator):
    print("Running {} load tests from server {} load_test_domain_num={} load_generator={}".format(protocol, server_name, load_test_domain_num, load_generator))
    ssh = 'ssh root@{} -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(server_ip, tempdir)
    scp = 'scp -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(tempdir)
    test_load_args = '--objects {objects} --duration-seconds {duration_seconds} ' \
                     '--skip-delete-worker --skip-clear-volume --concurrency {concurrency} --obj-size-kb {obj_size_kb} ' \
                     '--benchdatafilename /output/warp-bench-data --skip_add_worker --protocol {protocol} ' \
                     '--load_generator {load_generator} --custom-load-options {custom_load_options}'.format(
        objects=objects,
        duration_seconds=duration_seconds,
        concurrency=concurrency,
        obj_size_kb=obj_size_kb,
        protocol=protocol,
        load_generator=load_generator,
        custom_load_options='b64:' + base64.b64encode(json.dumps(custom_load_options).encode()).decode()
    )
    print("test_load_args: {}".format(test_load_args))
    ret, out = subprocess.getstatusoutput('''
        {ssh} "docker run --name load_test_{protocol} -d -e CLUSTER_NAME={cluster_name} -v /etc/kamatera:/etc/kamatera -v /root/cwm-worker-cluster/.output:/output tests cwm-worker-tests load-test {test_load_args} 2>&1"
    '''.format(scp=scp, ssh=ssh, test_load_args=test_load_args, ip=server_ip, num=load_test_domain_num, protocol=protocol, cluster_name=config.assert_connected_cluster_name()))
    assert ret == 0, out


def wait_for_server_load_tests_to_complete(tempdir, server_ip, protocol):
    ssh = 'ssh root@{} -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(server_ip, tempdir)
    ret, out = subprocess.getstatusoutput('{ssh} "docker ps | grep load_test_{protocol}"'.format(ssh=ssh, protocol=protocol))
    if ret == 1:
        ret, out = subprocess.getstatusoutput('{ssh} "docker logs load_test_{protocol} >> /root/cwm-worker-cluster/.output/log.txt"'.format(ssh=ssh, protocol=protocol))
        assert ret == 0, out
        return True
    else:
        return False


def show_server_load_tests_log(tempdir, server_ip, protocol):
    ssh = 'ssh root@{} -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(server_ip, tempdir)
    ret, out = subprocess.getstatusoutput('{ssh} "docker logs load_test_{protocol}"'.format(ssh=ssh, protocol=protocol))
    print(out)


def copy_load_test_data_from_remote_server(tempdir, server_ip, load_test_domain_num):
    scp = 'scp -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(tempdir)
    ret, out = subprocess.getstatusoutput('''
        mkdir -p {distributed_load_test_output_dir}/{num} &&\
        {scp} -r root@{ip}:/root/cwm-worker-cluster/.output/ ./{distributed_load_test_output_dir}/{num}/
    '''.format(scp=scp, ip=server_ip, num=load_test_domain_num, distributed_load_test_output_dir=DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR))
    assert ret == 0, out


def add_clear_worker(worker_id, hostname, node_ip, cluster_zone, root_progress, skip_clear_volume=False):
    with root_progress.start_sub(__spec__.name, 'add_clear_worker', worker_id) as progress:
        if not node_ip:
            node = common.get_cluster_nodes('worker')[0]
            node_ip = node['ip']
        volume_id = common.get_namespace_name_from_worker_id(worker_id)
        with progress.set_start_end('delete_worker_start', 'delete_worker_end'):
            worker_api.delete(worker_id)
        if not skip_clear_volume:
            with progress.set_start_end('clear_worker_volume_start', 'clear_worker_volume_end'):
                worker_api.clear_volume(volume_id)
        print("Sleeping 5 seconds to ensure everything is ready...")
        time.sleep(5)
        print("Warming up the site")
        ok = False
        with progress.set_start_end('assert_site_start', 'assert_site_end'):
            for try_num in range(3):
                try:
                    pprint(common.assert_site(worker_id, hostname, node_ip))
                    ok = True
                    break
                except Exception:
                    traceback.print_exc()
                    print("Failed to assert_site, will retry up to 3 times")
                    time.sleep(5)
            assert ok, "Failed to assert_site 3 times, giving up"


def add_clear_workers(servers, prepare_domain_names, root_progress, skip_clear_volume=False,
                      skip_warm_site=False, skip_wait_inactive_namespaces=False):
    with root_progress.start_sub(__spec__.name, 'add_clear_workers') as progress:
        print("Adding and clearing workers")
        cluster_zone = common.get_cluster_zone()
        node = common.get_cluster_nodes('worker')[0]
        node_name = node['name']
        node_ip = node['ip']
        print('node_name={} node_ip={}'.format(node_name, node_ip))
        if not prepare_domain_names:
            raise Exception("Not using specific predefined domain_names in prepare_domain_names is not supported")
            # prepare_domain_names = {}
            # eu_load_test_domain_nums = set()
            # for server_num in servers.keys():
            #     eu_load_test_domain_nums.add(server_num if server_num < 5 else 1)
            # for eu_load_test_domain_num in eu_load_test_domain_nums:
            #     hostname = get_domain_name_from_num(eu_load_test_domain_num)
            #     worker_id = get_worker_id_from_num(eu_load_test_domain_num)
            #     prepare_domain_names[worker_id] = hostname
        delete_worker_ids = set(prepare_domain_names.keys())
        # for i in range(1, 50):
        #     delete_worker_ids.add(get_worker_id_from_num(i))
        for worker_id in delete_worker_ids:
            with progress.set_start_end('worker_delete_start_{}'.format(worker_id), 'worker_delete_end_{}'.format(worker_id)):
                worker_api.delete(worker_id)
        if not skip_clear_volume:
            for worker_id, hostname in prepare_domain_names.items():
                volume_id = common.get_namespace_name_from_worker_id(worker_id)
                with progress.set_start_end('clear_worker_volume_start_{}'.format(volume_id), 'clear_worker_volume_end_{}'.format(volume_id)):
                    worker_api.clear_volume(volume_id)
        if not skip_wait_inactive_namespaces:
            print("Waiting for inactive namespaces...")
            while True:
                num_inactive_namespaces = 0
                for namespace in cluster_api.iterate_inactive_namespaces():
                    num_inactive_namespaces += 1
                    print(namespace)
                if num_inactive_namespaces > 0:
                    print("Sleeping 5 seconds to wait for inactive namespaces...")
                    time.sleep(5)
                else:
                    break
        if not skip_warm_site:
            for worker_id, hostname in prepare_domain_names.items():
                with progress.set_start_end('assert_site_start_{}'.format(worker_id), 'assert_site_end_{}'.format(worker_id)):
                    ok = False
                    for try_num in range(3):
                        try:
                            pprint(common.assert_site(worker_id, hostname, node_ip))
                            ok = True
                            break
                        except Exception:
                            traceback.print_exc()
                            print("Failed to assert_site, will retry up to 3 times")
                            time.sleep(5)
                    assert ok, "Failed to assert_site 3 times, giving up"


def prepare_custom_load_generator(servers, prepare_domain_names, root_progress, use_default_bucket, skip_prepare_load_generator=False):
    with root_progress.start_sub(__spec__.name, 'prepare_custom_load_generator') as progress:
        # worker_id_servers = {}
        # worker_id_hostnames = {}
        objects, duration_seconds, concurrency, obj_size_kb = None, None, None, None
        for server_num, server in servers.items():
            if not server['custom_load_options'].get('bucket_name'):
                if objects is None:
                    objects = int(server['objects'])
                else:
                    assert int(server['objects']) == objects
                if duration_seconds is None:
                    duration_seconds = int(server['duration_seconds'])
                else:
                    assert int(server['duration_seconds']) == duration_seconds
                if concurrency is None:
                    concurrency = int(server['concurrency'])
                else:
                    assert int(server['concurrency']) == concurrency
                if obj_size_kb is None:
                    obj_size_kb = int(server['obj_size_kb'])
                else:
                    assert int(server['obj_size_kb']) == obj_size_kb
                if not prepare_domain_names:
                    raise Exception("must use specific predefined domain_names in prepare_domain_names")
                    # hostname = get_domain_name_from_num(server_num if server_num < 5 else 1)
                    # worker_id = get_worker_id_from_num(server_num if server_num < 5 else 1)
                    # worker_id_servers.setdefault(worker_id, []).append(server)
                    # worker_id_hostnames[worker_id] = hostname
        if prepare_domain_names:
            assert use_default_bucket
            print("preparing custom load generator using the default bucket for all domain names / servers")
            if skip_prepare_load_generator:
                print("Skipping actual prepare task because skip_prepare_load_generator=True")
            for worker_id, hostname in prepare_domain_names.items():
                print("worker_id={}".format(worker_id))
                with progress.set_start_end('prepare_custom_bucket_start_{}'.format(worker_id), 'prepare_custom_bucket_end_{}'.format(worker_id)):
                    ok, i = False, 1
                    while not ok:
                        try:
                            if not skip_prepare_load_generator:
                                prepare_default_bucket('https', worker_id, hostname, objects, obj_size_kb, with_delete=True)
                            ok = True
                        except:
                            traceback.print_exc()
                            if i <= 5:
                                print("Failed to prepare default bucket for worker {}, will retry ({}/5)".format(worker_id, i))
                                i += 1
                                time.sleep(5)
                            else:
                                raise
            for server in servers.values():
                server['custom_load_options']['use_default_bucket'] = True
                server['custom_load_options']['skip_prepare_bucket'] = True
        else:
            raise Exception('must have domain in prepare_domain_names')
            # assert not use_default_bucket
            # for worker_id in worker_id_servers.keys():
            #     hostname = worker_id_hostnames[worker_id]
            #     print("preparing custom load generator for worker {}".format(worker_id))
            #     assert not skip_prepare_load_generator, "Cannot skip_prepare_load_generator when not using default bucket"
            #     with progress.set_start_end('prepare_custom_bucket_start_{}'.format(worker_id), 'prepare_custom_bucket_end_{}'.format(worker_id)):
            #         bucket_name = prepare_custom_bucket('https', worker_id, hostname, objects, duration_seconds, concurrency, obj_size_kb)
            #     for server in worker_id_servers[worker_id]:
            #         server['custom_load_options']['bucket_name'] = bucket_name
            #         server['custom_load_options']['use_default_bucket'] = False


def post_delete_cleanup(servers, create_servers_stats):
    # no need to cleanup after every load test, we rely on cleanup before
    pass
#     eu_load_test_domain_nums = set()
#     for server_num in servers.keys():
#         eu_load_test_domain_nums.add(server_num if server_num < 5 else 1)
#     print("Cleaning up volumes")
#     for eu_load_test_domain_num in eu_load_test_domain_nums:
#         domain_name = get_domain_name_from_num(eu_load_test_domain_num)
#         volume_id = domain_name.replace('.', '--')
#         ok = False
#         for try_num in range(3):
#             try:
#                 worker.delete(domain_name)
#                 time.sleep(5)
#                 worker.add_clear_volume(volume_id)
#                 ok = True
#                 break
#             except Exception:
#                 traceback.print_exc()
#                 print("Failed to clear worker, will retry up to 3 times")
#         if not ok:
#             print("Failed to clear worker {} 3 times, giving up".format(domain_name))
#             create_servers_stats["post_delete_failed"] = True


def run_distributed_load_tests(servers, load_generator, prepare_domain_names, root_progress, custom_load_options):
    skip_add_clear_workers = custom_load_options.get('skip_add_clear_workers')
    skip_prepare_load_generator = custom_load_options.get('skip_prepare_load_generator')
    with root_progress.start_sub(__spec__.name, 'run_distributed_load_tests') as progress:
        if skip_add_clear_workers:
            print("Skipping add clear workers")
        else:
            add_clear_workers(servers, prepare_domain_names, root_progress,
                              skip_clear_volume=custom_load_options.get('skip_clear_volume', load_generator == 'custom'),
                              skip_warm_site=custom_load_options.get('skip_warm_site', False))
        if load_generator == 'custom':
            if prepare_domain_names:
                use_default_bucket = True
            else:
                raise Exception('must have prepare_domain_names')
                # assert not custom_load_options.get('use_default_bucket')
                # use_default_bucket = False
            prepare_custom_load_generator(servers, prepare_domain_names, root_progress, use_default_bucket, skip_prepare_load_generator)
        create_servers_stats = {}
        with create_servers.create_servers(servers, post_delete_cleanup, create_servers_stats, root_progress) as (tempdir, servers):
            with progress.set_start_end('prepare_remote_servers_start', 'prepare_remote_servers_end'):
                print("Preparing remote servers for load test")
                for server in servers.values():
                    print(server['name'])
                    prepare_server_for_load_test(tempdir, server['ip'])
            with progress.set_start_end('start_load_tests_start', 'start_load_tests_end'):
                print("Starting load tests")
                for server in servers.values():
                    print(server['name'])
                    if not server.get('only_test_method') or server.get('only_test_method') == 'http':
                        server['http_completed'] = False
                        print('http')
                        start_server_load_tests(
                            tempdir, server['name'], server['ip'],
                            server['load_test_domain_num'], server['objects'], server['duration_seconds'], server['concurrency'],
                            server['obj_size_kb'], 'http', server['custom_load_options'], load_generator
                        )
                    else:
                        server['http_completed'] = True
                    if not server.get('only_test_method') or server.get('only_test_method') == 'https':
                        print('https')
                        server['https_completed'] = False
                        start_server_load_tests(
                            tempdir, server['name'], server['ip'],
                            server['load_test_domain_num'], server['objects'], server['duration_seconds'], server['concurrency'],
                            server['obj_size_kb'], 'https', server['custom_load_options'], load_generator
                        )
                    else:
                        server['https_completed'] = True
            with progress.set_start_end('wait_load_tests_start', 'wait_load_tests_end'):
                print("Waiting for load tests to complete")
                num_completed = 0
                start_time = datetime.datetime.now()
                iteration_num = 0
                expected_num_completed = 0
                for server in servers.values():
                    expected_num_completed += (2 if not server.get('only_test_method') else 1)
                while num_completed < expected_num_completed:
                    iteration_num += 1
                    time.sleep(30)
                    for server_num, server in servers.items():
                        if not server.get('http_completed'):
                            if wait_for_server_load_tests_to_complete(tempdir, server['ip'], 'http'):
                                server['http_completed'] = True
                                num_completed += 1
                                progress.set_now('server_{}_http_completed'.format(server_num))
                        if not server.get('https_completed'):
                            if wait_for_server_load_tests_to_complete(tempdir, server['ip'], 'https'):
                                server['https_completed'] = True
                                num_completed += 1
                                progress.set_now('server_{}_https_completed'.format(server_num))
                    if iteration_num == 1 or iteration_num % 10 == 0:
                        print("Waiting... ({}s)".format((datetime.datetime.now() - start_time).total_seconds()))
                        for server_num, server in servers.items():
                            print('server {}: {}'.format(server_num, server['name']))
                            if not server.get('only_test_method') or server.get('only_test_method') == 'http':
                                print('--- http')
                                show_server_load_tests_log(tempdir, server['ip'], 'http')
                            if not server.get('only_test_method') or server.get('only_test_method') == 'https':
                                print('--- https')
                                show_server_load_tests_log(tempdir, server['ip'], 'https')
                print("All done, copying data from remote servers")
            with progress.set_start_end('finalize_load_tests_start', 'finalize_load_tests_end'):
                for server_num, server in servers.items():
                    print('server {}: {}'.format(server_num, server['name']))
                    if not server.get('only_test_method') or server.get('only_test_method') == 'http':
                        print('--- http')
                        show_server_load_tests_log(tempdir, server['ip'], 'http')
                    if not server.get('only_test_method') or server.get('only_test_method') == 'https':
                        print('--- https')
                        show_server_load_tests_log(tempdir, server['ip'], 'https')
                    copy_load_test_data_from_remote_server(tempdir, server['ip'], server['load_test_domain_num'])
        return not create_servers_stats.get("post_delete_failed")


def get_duration_ns(start, end):
    if start and end:
        start_dt, start_ns = start.split('.')
        start_dt = datetime.datetime.strptime(start_dt, '%Y-%m-%dT%H:%M:%S')
        start_ns = int(start_ns.replace('Z', '').ljust(9, '0'))
        end_dt, end_ns = end.split('.')
        end_dt = datetime.datetime.strptime(end_dt, '%Y-%m-%dT%H:%M:%S')
        end_ns = int(end_ns.replace('Z', '').ljust(9, '0'))
        duration_seconds = (end_dt - start_dt).total_seconds()
        if duration_seconds == 0:
            return end_ns - start_ns
        elif duration_seconds > 0:
            return (1000000000 - start_ns) + (1000000000 * duration_seconds) + end_ns
        else:
            raise Exception("Invalid start/end: {} - {}".format(start, end))
    else:
        return 0


def csv_decompress_zst(csv_zst_filename, csv_filename):
    with open(csv_filename + '.raw', 'wb') as write_f:
        with open(csv_zst_filename, 'rb') as read_f:
            with zstd.ZstdDecompressor().stream_reader(read_f) as reader:
                while True:
                    data = reader.read(100)
                    if data == b'':
                        break
                    write_f.write(data)
    with open(csv_filename + '.raw') as read_f:
        with open(csv_filename, 'w') as write_f:
            for line in read_f:
                if not line.startswith('#') and len(line.strip()) > 1:
                    write_f.write(line)
    os.unlink(csv_filename + '.raw')


def csv_decompress_gz(csv_compressed_filename, csv_filename):
    ret, out = subprocess.getstatusoutput('gzip -cfqd {} > {}'.format(csv_compressed_filename, csv_filename))
    assert ret == 0, out


def csv_decompress(csv_compression_method, csv_compressed_filename, csv_filename):
    if csv_compression_method == 'zst':
        csv_decompress_zst(csv_compressed_filename, csv_filename)
    else:
        csv_decompress_gz(csv_compressed_filename, csv_filename)


def get_datacenter(endpoint, base_servers_all_zone):
    if base_servers_all_zone:
        return base_servers_all_zone
    else:
        # we can't determine the zone based on endpoint
        return '??'
    # {
    #     'http://{}'.format(config.get_load_testing_domain_num_hostname(1)): 'EU',
    #     'https://{}'.format(config.get_load_testing_domain_num_hostname(1)): 'EU',
    #     'http://{}'.format(config.get_load_testing_domain_num_hostname(2)): 'IL',
    #     'https://{}'.format(config.get_load_testing_domain_num_hostname(2)): 'IL',
    #     'http://{}'.format(config.get_load_testing_domain_num_hostname(3)): 'CA-TR',
    #     'https://{}'.format(config.get_load_testing_domain_num_hostname(3)): 'CA-TR',
    #     'http://{}'.format(config.get_load_testing_domain_num_hostname(4)): 'EU-LO',
    #     'https://{}'.format(config.get_load_testing_domain_num_hostname(4)): 'EU-LO',
    # }.get(endpoint, '??') if not base_servers_all_zone else base_servers_all_zone


def aggregate_test_results(servers, total_duration_seconds, base_servers_all_zone, only_test_method, load_generator):
    overview_report = {}
    load_steps = []
    for num, _ in servers.items():
        log_txt_filename = os.path.join(DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, str(num), '.output', 'log.txt')
        http_csv_zst_filename = os.path.join(DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, str(num), '.output', 'warp-bench-data.http.csv.zst')
        http_csv_gz_filename = os.path.join(DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, str(num), '.output', 'warp-bench-data.http.csv.gz')
        http_csv_filename = os.path.join(DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, str(num), '.output', 'warp-bench-data.http.{}'.format('tsv' if load_generator == 'warp' else 'csv'))
        https_csv_zst_filename = os.path.join(DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, str(num), '.output', 'warp-bench-data.https.csv.zst')
        https_csv_gz_filename = os.path.join(DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, str(num), '.output', 'warp-bench-data.https.csv.gz')
        https_csv_filename = os.path.join(DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, str(num), '.output', 'warp-bench-data.https.{}'.format('tsv' if load_generator == 'warp' else 'csv'))
        if os.path.exists(log_txt_filename):
            try:
                with open(log_txt_filename) as f:
                    log_txt = f.read().strip()
                    for method in ('http', 'https'):
                        if not only_test_method or only_test_method == method:
                            tmp = log_txt.split("--- {} load test successful, bucket_name=".format(method))[1].split(" ---")[0]
                            bucket_name, dt = tmp.split(' | ')
                            assert len(bucket_name) > 5
                            overview_report.setdefault('server_{}_{}'.format(num, method), {}).update(
                                bucket_name=bucket_name,
                                is_successful=True,
                                dt=dt
                            )
            except Exception:
                traceback.print_exc()
        for csv_filename, csv_zst_filename, csv_gz_filename, method in ((http_csv_filename, http_csv_zst_filename, http_csv_gz_filename, 'http'), (https_csv_filename, https_csv_zst_filename, https_csv_gz_filename, 'https')):
            csv_compressed_filename = csv_zst_filename if load_generator == 'warp' else csv_gz_filename
            csv_compression_method = 'zst' if load_generator == 'warp' else 'gz'
            if (not only_test_method or only_test_method == method) and os.path.exists(csv_compressed_filename):
                load_steps.append(DF.load(csv_filename, name='{}_{}'.format(method, num), infer_strategy=DF.load.INFER_STRINGS))
                csv_decompress(csv_compression_method, csv_compressed_filename, csv_filename)
                overview_report.setdefault('server_{}_{}'.format(num, method), {}).update(
                    csv_size=os.path.getsize(csv_filename) if os.path.exists(csv_filename) else 0
                )
    assert len(load_steps) > 0, "none of the servers data was loaded"

    stats = defaultdict(int)

    def aggregate_stats(row):
        endpoint = row['endpoint']
        try:
            op = row['op']
            error = row['error']
            bytes = int(row['bytes'])
            duration_ns = int(row['duration_ns'])
            first_byte = row['first_byte']
            end = row['end']
            duration_after_first_byte_ns = get_duration_ns(first_byte, end)
            assert duration_after_first_byte_ns >= 0, row
            assert '~' not in endpoint
            assert '~' not in op
            key_prefix = '{}~{}'.format(endpoint, op)
            if error:
                stats['{}~errors'.format(key_prefix)] += 1
            else:
                stats['{}~successful-requests'.format(key_prefix)] += 1
                stats['{}~bytes'.format(key_prefix)] += bytes
                stats['{}~duration_ns'.format(key_prefix)] += duration_ns
                stats['{}~duration_after_first_byte_ns'.format(key_prefix)] += duration_after_first_byte_ns
                if duration_ns > stats['{}~max-duration_ns'.format(key_prefix)]:
                    stats['{}~max-duration_ns'.format(key_prefix)] = duration_ns
                if stats['{}~min-duration_ns'.format(key_prefix)] == 0 or duration_ns < stats['{}~min-duration_ns'.format(key_prefix)]:
                    stats['{}~min-duration_ns'.format(key_prefix)] = duration_ns
                duration_seconds = duration_ns / 1000000000
                bucket = None
                for bucket_max_seconds in BUCKET_MAX_SECONDS:
                    if duration_seconds <= bucket_max_seconds:
                        bucket = bucket_max_seconds
                        break
                if not bucket:
                    bucket = BUCKET_INF
                stats['{}~request-duration-{}'.format(key_prefix, bucket)] += 1
        except:
            print("Exception in res {} endpoint {}".format(row['source_resource_name'], endpoint))
            raise

    def aggregate_errors(rows):
        for row in rows:
            if row['error'] and len(row['error']) > 0:
                yield row

    concatenate_step = DF.concatenate({"source_resource_name": [],
                                       "op": [],
                                       "bytes": [],
                                       "endpoint": [],
                                       "file": [],
                                       "error": [],
                                       "start": [],
                                       "first_byte": [],
                                       "end": [],
                                       "duration_ns": []},
                                      {"name": "warp_bench_data", "path": "data/warp_bench_data.csv"})

    def add_source_resource_name_step(rows):
        for row in rows:
            row['source_resource_name'] = rows.res.name
            yield row

    DF.Flow(
        *load_steps,
        add_source_resource_name_step,
        concatenate_step,
        aggregate_stats,
        DF.dump_to_path(os.path.join(DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, 'warp_bench_data'))
    ).process()

    try:
        DF.Flow(
            *load_steps,
            concatenate_step,
            aggregate_errors,
            DF.dump_to_path(os.path.join(DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, 'warp_bench_data_errors'))
        ).process()
    except Exception:
        print("No Errors!")

    def dump_stats():
        all_field_names = set()
        normalized_stats = {}
        for k, v in stats.items():
            endpoint, op, *metric = k.split('~')
            metric = '~'.join(metric)
            field_name = '{}-{}'.format(op, metric)
            all_field_names.add(field_name)
            normalized_stats.setdefault(endpoint, {})[field_name] = v
        totals_sum = defaultdict(int)
        for endpoint, row in normalized_stats.items():
            if not endpoint or len(endpoint) < 4:
                continue
            for field_name in all_field_names:
                if field_name not in row:
                    row[field_name] = 0.0
                elif not row[field_name]:
                    row[field_name] = 0.0
            for op in ['STAT', 'DELETE', 'GET', 'PUT']:
                total_bytes = row.get('{}-bytes'.format(op)) or 0
                total_megabytes = total_bytes / 1024 / 1024
                successful_requests = row.get('{}-successful-requests'.format(op)) or 0
                error_requests = row.get('{}-errors'.format(op)) or 0
                total_requests = successful_requests + error_requests
                row['{}-percent-errors'.format(op)] = (error_requests / total_requests * 100) if total_requests > 0 else 0
                row['{}-requests-per-second'.format(op)] = total_requests / total_duration_seconds
                row['{}-successful-requests-per-second'.format(op)] = successful_requests / total_duration_seconds
                if op in ['GET', 'PUT']:
                    row['{}-megabytes'.format(op)] = total_megabytes
                    row['{}-megabytes-per-second'.format(op)] = total_megabytes / total_duration_seconds
                    for bucket in [*BUCKET_MAX_SECONDS, BUCKET_INF]:
                        row['{}-request-duration-percent-{}'.format(op, bucket)] = ((row.get('{}-request-duration-{}'.format(op, bucket)) or 0) / successful_requests * 100) if successful_requests > 0 else 0
            all_ops_total_errors = sum([(row.get('{}-errors'.format(op)) or 0) for op in ['GET', 'PUT', 'STAT', 'DELETE']])
            all_ops_total_successful_requests = sum([(row.get('{}-successful-requests'.format(op)) or 0) for op in ['GET', 'PUT', 'STAT', 'DELETE']])
            all_ops_total_requests = all_ops_total_errors + all_ops_total_successful_requests
            all_ops_total_bytes = sum([(row.get('{}-bytes'.format(op)) or 0) for op in ['GET', 'PUT']])
            all_ops_total_megabytes = all_ops_total_bytes / 1024 / 1024
            output_row = {
                'endpoint': endpoint,
                'datacenter': get_datacenter(endpoint, base_servers_all_zone),
                'total-percent-errors': (all_ops_total_errors / all_ops_total_requests * 100) if all_ops_total_requests > 0 else 0,
                'total-requests-per-second': all_ops_total_requests / total_duration_seconds,
                'total-successful-requests-per-second': all_ops_total_successful_requests / total_duration_seconds,
                'total-megabytes-per-second': all_ops_total_megabytes / total_duration_seconds,
                'total-errors': all_ops_total_errors,
                'total-successful-requests': all_ops_total_successful_requests,
                'total-megabytes': all_ops_total_megabytes,
                'total-bytes': all_ops_total_bytes,
                **{k: row.get(k) or 0 for k in [
                    *['{}-percent-errors'.format(op) for op in ['GET', 'PUT', 'STAT', 'DELETE']],
                    *['{}-requests-per-second'.format(op) for op in ['GET', 'PUT', 'STAT', 'DELETE']],
                    *['{}-successful-requests-per-second'.format(op) for op in ['GET', 'PUT', 'STAT', 'DELETE']],
                    *['{}-megabytes-per-second'.format(op) for op in ['GET', 'PUT']],
                    *['{}-errors'.format(op) for op in ['GET', 'PUT', 'STAT', 'DELETE']],
                    *['{}-successful-requests'.format(op) for op in ['GET', 'PUT', 'STAT', 'DELETE']],
                    *['{}-megabytes'.format(op) for op in ['GET', 'PUT']],
                    *['{}-bytes'.format(op) for op in ['GET', 'PUT']],
                    *['GET-request-duration-percent-{}'.format(bucket) for bucket in [*BUCKET_MAX_SECONDS, BUCKET_INF]],
                    *['PUT-request-duration-percent-{}'.format(bucket) for bucket in [*BUCKET_MAX_SECONDS, BUCKET_INF]],
                ]}
            }
            yield output_row
            for metric in ['errors', 'successful-requests', 'bytes']:
                for op in ['total', 'GET', 'PUT', *(['STAT', 'DELETE'] if metric not in ['bytes'] else [])]:
                    op_metric = '{}-{}'.format(op, metric)
                    totals_sum[op_metric] += (output_row.get(op_metric) or 0)
            for op in ['GET', 'PUT']:
                for bucket in [*BUCKET_MAX_SECONDS, BUCKET_INF]:
                    op_bucket_metric_key = '{}-request-duration-{}'.format(op, bucket)
                    totals_sum[op_bucket_metric_key] += (row.get(op_bucket_metric_key) or 0)
        totals_sum_total_successful_requests = totals_sum['total-successful-requests']
        totals_sum_total_errors = totals_sum['total-errors']
        totals_sum_total_requests = totals_sum_total_successful_requests + totals_sum_total_errors
        totals_sum_total_bytes = totals_sum['total-bytes']
        totals_sum['total-megabytes'] = totals_sum_total_megabytes = totals_sum_total_bytes / 1024 / 1024
        totals_sum['total-percent-errors'] = (totals_sum_total_errors / totals_sum_total_requests * 100) if totals_sum_total_requests > 0 else 0
        totals_sum['total-requests-per-second'] = totals_sum_total_requests / total_duration_seconds
        totals_sum['total-successful-requests-per-second'] = totals_sum_total_successful_requests / total_duration_seconds
        totals_sum['total-megabytes-per-second'] = totals_sum_total_megabytes / total_duration_seconds
        for op in ['GET', 'PUT', 'STAT', 'DELETE']:
            op_errors = totals_sum['{}-errors'.format(op)]
            op_successful_requests = totals_sum['{}-successful-requests'.format(op)]
            op_total_requests = op_errors + op_successful_requests
            totals_sum['{}-percent-errors'.format(op)] = (op_errors / op_total_requests * 100) if op_total_requests > 0 else 0
            totals_sum['{}-requests-per-second'.format(op)] = op_total_requests / total_duration_seconds
            totals_sum['{}-successful-requests-per-second'.format(op)] = op_successful_requests / total_duration_seconds
            if op in ['GET', 'PUT']:
                totals_sum['{}-megabytes'.format(op)] = totals_sum['{}-bytes'.format(op)] / 1024 / 1024
                totals_sum['{}-megabytes-per-second'.format(op)] = totals_sum['{}-megabytes'.format(op)] / total_duration_seconds
                for bucket in [*BUCKET_MAX_SECONDS, BUCKET_INF]:
                    totals_sum['{}-request-duration-percent-{}'.format(op, bucket)] = (totals_sum['{}-request-duration-{}'.format(op, bucket)] / op_successful_requests * 100) if op_successful_requests else 0
        yield {
            'endpoint': '*',
            'datacenter': '*',
            **totals_sum
        }
    DF.Flow(
        dump_stats(),
        DF.dump_to_path(os.path.join(DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR, 'warp_bench_data_stats'))
    ).process()
    pprint(overview_report)
    for num, _ in servers.items():
        for method in ['http', 'https']:
            if not only_test_method or only_test_method == method:
                report = overview_report.get('server_{}_{}'.format(num, method)) or {}
                csv_size = report.get('csv_size') or 0
                is_successful = report.get('is_successful')
                if csv_size < 1:
                    print("csv size for server {}, method {} is missing".format(num, method))
                    return False
                if not is_successful:
                    print("server {}, method {} was not successful".format(num, method))
                    return False
    return True
