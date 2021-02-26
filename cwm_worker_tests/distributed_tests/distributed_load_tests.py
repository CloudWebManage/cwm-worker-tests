import os
import uuid
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

from cwm_worker_cluster import common
from cwm_worker_cluster import config
from cwm_worker_cluster import dummy_api
from cwm_worker_cluster import worker
from cwm_worker_tests.load_generator_custom import prepare_custom_bucket
from cwm_worker_tests.distributed_tests import create_servers


DISTRIBUTED_LOAD_TESTS_OUTPUT_DIR = '.distributed-load-test-output'


def get_domain_name_from_num(domain_num):
    return config.LOAD_TESTING_DOMAIN_NUM_TEMPLATE.format(domain_num if domain_num < 5 else 1)


def prepare_server_for_load_test(tempdir, server_ip):
    scp = 'scp -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(tempdir)
    ssh = 'ssh root@{} -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(server_ip, tempdir)
    ret, out = subprocess.getstatusoutput('''
        {ssh} "docker rm -f load_test_https; docker rm -f load_test_http; rm -rf /root/cwm-worker-cluster/.output" &&\
        {ssh} "mkdir /root/cwm-worker-cluster/.output" &&\
        {scp} $KUBECONFIG root@{ip}:/root/cwm-worker-cluster/.kubeconfig
    '''.format(ssh=ssh, scp=scp, ip=server_ip))
    assert ret == 0, out


def start_server_load_tests(tempdir, server_name, server_ip, load_test_domain_num, objects, duration_seconds, concurrency,
                            obj_size_kb, protocol, custom_load_options, load_generator):
    print("Running {} load tests from server {} load_test_domain_num={} load_generator={}".format(protocol, server_name, load_test_domain_num, load_generator))
    ssh = 'ssh root@{} -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(server_ip, tempdir)
    scp = 'scp -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(tempdir)
    test_load_args = '--objects {objects} --duration-seconds {duration_seconds} --domain-name \\"{domain_name}\\" ' \
                     '--skip-delete-worker --skip-clear-volume --concurrency {concurrency} --obj-size-kb {obj_size_kb} ' \
                     '--benchdatafilename /output/warp-bench-data --skip_add_worker --protocol {protocol} ' \
                     '--load_generator {load_generator} --custom-load-options {custom_load_options}'.format(
        objects=objects,
        duration_seconds=duration_seconds,
        domain_name=get_domain_name_from_num(load_test_domain_num) if not custom_load_options.get('random_domain_names') else "",
        concurrency=concurrency,
        obj_size_kb=obj_size_kb,
        protocol=protocol,
        load_generator=load_generator,
        custom_load_options='b64:' + base64.b64encode(json.dumps(custom_load_options).encode()).decode()
    )
    ret, out = subprocess.getstatusoutput('''
        {ssh} "docker run --name load_test_{protocol} -d --entrypoint bash -e KUBECONFIG=/kube/.config -v /root/cwm-worker-cluster/.kubeconfig:/kube/.config -v /root/cwm-worker-cluster/.output:/output tests -c \'cwm-worker-tests load-test {test_load_args} 2>&1\'"
    '''.format(scp=scp, ssh=ssh, test_load_args=test_load_args, ip=server_ip, num=load_test_domain_num, protocol=protocol))
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


def add_clear_worker(domain_name, node_ip, cluster_zone, root_progress):
    with root_progress.start_sub(__spec__.name, 'add_clear_worker', domain_name) as progress:
        if not node_ip:
            node = common.get_cluster_nodes('worker')[0]
            node_ip = node['ip']
        if not cluster_zone:
            cluster_zone = common.get_cluster_zone()
        with progress.set_start_end('dummy_api_add_example_site_start', 'dummy_api_add_example_site_end'):
            dummy_api.add_example_site(domain_name, domain_name, cluster_zone)
        volume_id = domain_name.replace('.', '--')
        with progress.set_start_end('delete_worker_start', 'delete_worker_end'):
            worker.delete(domain_name)
        with progress.set_start_end('add_clear_worker_volume_start', 'add_clear_worker_volume_end'):
            worker.add_clear_volume(volume_id)
        print("Sleeping 5 seconds to ensure everything is ready...")
        time.sleep(5)
        print("Warming up the site")
        ok = False
        with progress.set_start_end('assert_site_start', 'assert_site_end'):
            for try_num in range(3):
                try:
                    pprint(common.assert_site(domain_name, node_ip))
                    ok = True
                    break
                except Exception:
                    traceback.print_exc()
                    print("Failed to assert_site, will retry up to 3 times")
                    time.sleep(5)
            assert ok, "Failed to assert_site 3 times, giving up"


def add_clear_workers(servers, prepare_domain_names, root_progress):
    with root_progress.start_sub(__spec__.name, 'add_clear_workers') as progress:
        print("Adding and clearing workers")
        cluster_zone = common.get_cluster_zone()
        node = common.get_cluster_nodes('worker')[0]
        node_name = node['name']
        node_ip = node['ip']
        print('node_name={} node_ip={}'.format(node_name, node_ip))
        if not prepare_domain_names:
            prepare_domain_names = set()
            eu_load_test_domain_nums = set()
            for server_num in servers.keys():
                eu_load_test_domain_nums.add(server_num if server_num < 5 else 1)
            for eu_load_test_domain_num in eu_load_test_domain_nums:
                domain_name = get_domain_name_from_num(eu_load_test_domain_num)
                prepare_domain_names.add(domain_name)
        for domain_name in prepare_domain_names:
            add_clear_worker(domain_name, node_ip, cluster_zone, root_progress)


def prepare_custom_load_generator(servers, prepare_domain_names, root_progress):
    with root_progress.start_sub(__spec__.name, 'run_distributed_load_tests') as progress:
        domain_name_servers = {}
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
                    domain_name = get_domain_name_from_num(server_num if server_num < 5 else 1)
                    domain_name_servers.setdefault(domain_name, []).append(server)
        if prepare_domain_names:
            print("preparing custom load generator using the same bucket for all domain names / servers")
            bucket_name = str(uuid.uuid4())
            for domain_name in prepare_domain_names:
                print("domain_name={}".format(domain_name))
                with progress.set_start_end('prepare_custom_bucket_start_{}'.format(domain_name), 'prepare_custom_bucket_end_{}'.format(domain_name)):
                    prepare_custom_bucket('https', domain_name, objects, duration_seconds, concurrency, obj_size_kb, bucket_name=bucket_name)
            for server in servers.values():
                server['custom_load_options']['bucket_name'] = bucket_name
        else:
            for domain_name in domain_name_servers.keys():
                print("preparing custom load generator for domain_name {}".format(domain_name))
                with progress.set_start_end('prepare_custom_bucket_start_{}'.format(domain_name), 'prepare_custom_bucket_end_{}'.format(domain_name)):
                    bucket_name = prepare_custom_bucket('https', domain_name, objects, duration_seconds, concurrency, obj_size_kb)
                for server in domain_name_servers[domain_name]:
                    server['custom_load_options']['bucket_name'] = bucket_name


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


def run_distributed_load_tests(servers, load_generator, prepare_domain_names, root_progress):
    with root_progress.start_sub(__spec__.name, 'run_distributed_load_tests') as progress:
        add_clear_workers(servers, prepare_domain_names, root_progress)
        if load_generator == 'custom':
            prepare_custom_load_generator(servers, prepare_domain_names, root_progress)
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


def aggregate_test_results(servers, total_duration_seconds, base_servers_all_eu, only_test_method, load_generator):
    overview_report = {}
    load_steps = []
    for num, server in servers.items():
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
            stats['{}~requests'.format(key_prefix)] += 1
            stats['{}~bytes'.format(key_prefix)] += bytes
            stats['{}~duration_ns'.format(key_prefix)] += duration_ns
            stats['{}~duration_after_first_byte_ns'.format(key_prefix)] += duration_after_first_byte_ns
            if duration_ns > stats['{}~max-duration_ns'.format(key_prefix)]:
                stats['{}~max-duration_ns'.format(key_prefix)] = duration_ns
            if stats['{}~min-duration_ns'.format(key_prefix)] == 0 or duration_ns < stats['{}~min-duration_ns'.format(key_prefix)]:
                stats['{}~min-duration_ns'.format(key_prefix)] = duration_ns

    def aggregate_errors(rows):
        for row in rows:
            if row['error'] and len(row['error']) > 0:
                yield row

    concatenate_step = DF.concatenate({"op": [],
                                       "bytes": [],
                                       "endpoint": [],
                                       "file": [],
                                       "error": [],
                                       "start": [],
                                       "first_byte": [],
                                       "end": [],
                                       "duration_ns": []},
                                      {"name": "warp_bench_data", "path": "data/warp_bench_data.csv"})

    DF.Flow(
        *load_steps,
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
                total_requests = row.get('{}-requests'.format(op)) or 0
                error_requests = row.get('{}-errors'.format(op)) or 0
                row['{}-percent-errors'.format(op)] = error_requests / total_requests if total_requests > 0 else 0
                row['{}-requests-per-second'.format(op)] = (total_requests - error_requests) / total_duration_seconds
                row['{}-megabytes-per-second'.format(op)] = total_megabytes / total_duration_seconds
            output_row = {
                'endpoint': endpoint,
                'datacenter': {
                    'http://{}'.format(config.LOAD_TESTING_DOMAIN_NUM_TEMPLATE.format(1)): 'EU',
                    'https://{}'.format(config.LOAD_TESTING_DOMAIN_NUM_TEMPLATE.format(1)): 'EU',
                    'http://{}'.format(config.LOAD_TESTING_DOMAIN_NUM_TEMPLATE.format(2)): 'IL',
                    'https://{}'.format(config.LOAD_TESTING_DOMAIN_NUM_TEMPLATE.format(2)): 'IL',
                    'http://{}'.format(config.LOAD_TESTING_DOMAIN_NUM_TEMPLATE.format(3)): 'CA-TR',
                    'https://{}'.format(config.LOAD_TESTING_DOMAIN_NUM_TEMPLATE.format(3)): 'CA-TR',
                    'http://{}'.format(config.LOAD_TESTING_DOMAIN_NUM_TEMPLATE.format(4)): 'EU-LO',
                    'https://{}'.format(config.LOAD_TESTING_DOMAIN_NUM_TEMPLATE.format(4)): 'EU-LO',
                }[endpoint] if not base_servers_all_eu else 'EU',
                'total-percent-errors': sum([(row.get('{}-percent-errors'.format(op)) or 0) for op in ['GET', 'PUT', 'STAT', 'DELETE']]),
                'total-requests-per-second': sum([(row.get('{}-requests-per-second'.format(op)) or 0) for op in ['GET', 'PUT', 'STAT', 'DELETE']]),
                'total-megabytes-per-second': sum([(row.get('{}-megabytes-per-second'.format(op)) or 0) for op in ['GET', 'PUT']]),
                'total-errors': sum([(row.get('{}-errors'.format(op)) or 0) for op in ['GET', 'PUT', 'STAT', 'DELETE']]),
                'total-requests': sum([(row.get('{}-requests'.format(op)) or 0) for op in ['GET', 'PUT', 'STAT', 'DELETE']]),
                'total-bytes': sum([(row.get('{}-bytes'.format(op)) or 0) for op in ['GET', 'PUT']]),
                **{k: row.get(k) or 0 for k in [
                    'GET-percent-errors',
                    'PUT-percent-errors',
                    'STAT-percent-errors',
                    'DELETE-percent-errors',
                    'GET-requests-per-second',
                    'PUT-requests-per-second',
                    'STAT-requests-per-second',
                    'DELETE-requests-per-second',
                    'GET-megabytes-per-second',
                    'PUT-megabytes-per-second',
                    'GET-errors',
                    'PUT-errors',
                    'STAT-errors',
                    'DELETE-errors',
                    'GET-requests',
                    'PUT-requests',
                    'STAT-requests',
                    'DELETE-requests',
                    'GET-bytes',
                    'PUT-bytes',
                ]}
            }
            yield output_row
            for metric in ['percent-errors', 'requests-per-second', 'megabytes-per-second', 'errors', 'requests', 'bytes']:
                for op in ['total', 'GET', 'PUT', *(['STAT', 'DELETE'] if metric not in ['megabytes-per-second', 'bytes'] else [])]:
                    op_metric = '{}-{}'.format(op, metric)
                    if metric in ['errors', 'requests', 'bytes']:
                        totals_sum[op_metric] += (output_row.get(op_metric) or 0)
                    else:
                        totals_sum[op_metric] = 0
        totals_sum['total-percent-errors'] = (totals_sum['total-errors'] / totals_sum['total-requests']) if totals_sum['total-requests'] > 0 else 0
        totals_sum['total-requests-per-second'] = (totals_sum['total-requests'] - totals_sum['total-errors']) / total_duration_seconds
        totals_sum['total-megabytes-per-second'] = totals_sum['total-bytes'] / 1024 / 1024 / total_duration_seconds
        for op in ['GET', 'PUT', 'STAT', 'DELETE']:
            totals_sum['{}-percent-errors'.format(op)] = (totals_sum['{}-errors'.format(op)] / totals_sum['{}-requests'.format(op)]) if totals_sum['{}-requests'.format(op)] > 0 else 0
            totals_sum['{}-requests-per-second'.format(op)] = totals_sum['{}-requests'.format(op)] / total_duration_seconds
            totals_sum['{}-megabytes-per-second'.format(op)] = totals_sum['{}-bytes'.format(op)] / 1024 / 1024 / total_duration_seconds
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
    for num, server in servers.items():
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
