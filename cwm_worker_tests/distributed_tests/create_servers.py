import os
import uuid
import json
import time
import tempfile
import subprocess
from contextlib import contextmanager


from cwm_worker_cluster import config


def generate_new_ssh_key(tempdir):
    print("Generating new SSH KEY")
    ret, out = subprocess.getstatusoutput(
        'ssh-keygen -t rsa -b 4096 -C distributed_load_test -N "" -f {}/id_rsa'.format(tempdir))
    assert ret == 0, out


def start_create_server_process(tempdir, name, password, datacenter):
    print("Creating server {} (datacenter={}, password={})".format(name, datacenter, password))
    return subprocess.Popen(
        """
            cloudcli --config {tempdir}/cloudcli.yaml server create \
            --name {server_name} \
            --datacenter {datacenter} \
            --password {password} \
            --ssh-key {tempdir}/id_rsa.pub \
            --cpu 8B --ram 16384 \
            --image ubuntu_server_18.04_64-bit --wait
        """.format(
            tempdir=tempdir, server_name=name, datacenter=datacenter, password=password
        ),
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )


def wait_for_create_server_process(create_server_process):
    create_server_process.wait(60 * 30)
    return create_server_process.returncode == 0, create_server_process.stdout.read().decode(), create_server_process.stderr.read().decode()


def terminate_create_server_process(create_server_process):
    create_server_process.terminate()


def get_server_ip(tempdir, name):
    ret, out = subprocess.getstatusoutput('cloudcli --config {}/cloudcli.yaml server info --name {} --format json'.format(tempdir, name))
    assert ret == 0, out
    return json.loads(out)[0]['networks'][0]['ips'][0]


def install_docker(tempdir, name, ip):
    print("Installing docker on server {}".format(name))
    ret, out = subprocess.getstatusoutput('''
                                ssh root@{} -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "
                                    wget https://download.docker.com/linux/ubuntu/dists/bionic/pool/stable/amd64/docker-ce_19.03.9~3-0~ubuntu-bionic_amd64.deb -O docker.deb &&\
                                    wget https://download.docker.com/linux/ubuntu/dists/bionic/pool/stable/amd64/containerd.io_1.3.9-1_amd64.deb -O containerd.deb &&\
                                    wget https://download.docker.com/linux/ubuntu/dists/bionic/pool/stable/amd64/docker-ce-cli_19.03.9~3-0~ubuntu-bionic_amd64.deb -O docker-cli.deb &&\
                                    dpkg -i containerd.deb
                                    dpkg -i docker.deb
                                    dpkg -i docker-cli.deb
                                    apt-get update && apt-get  --fix-broken install -y && docker version
                                "
                            '''.format(ip, tempdir))
    return ret == 0, out


def build_test_docker_image(tempdir, name, ip):
    print("Building tests docker image on server {}".format(name))
    ssh = 'ssh root@{} -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(ip, tempdir)
    scp = 'scp -i {}/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'.format(tempdir)
    cwm_worker_cluster_path = '.' if os.path.exists('./clusters') else '../cwm-worker-cluster'
    ret, out = subprocess.getstatusoutput('{ssh} "df --output=pcent /"'.format(ssh=ssh))
    assert ret == 0, out
    disk_space_used_percent = int(out.strip().split("\n")[-1].strip().replace('%', ''))
    if disk_space_used_percent > 60:
        print("Used disk space is more than 60%, running docker system prune")
        ret, out = subprocess.getstatusoutput('{ssh} "docker system prune --force"'.format(ssh=ssh))
        assert ret == 0, out
    ret, out = subprocess.getstatusoutput('''
                            {ssh} "rm -rf /root/cwm-worker-cluster" &&\
                            {ssh} "mkdir /root/cwm-worker-cluster" &&\
                            {scp} -r {cwm_worker_cluster_path}/bin root@{ip}:/root/cwm-worker-cluster/bin &&\
                            {scp} -r {cwm_worker_cluster_path}/clusters root@{ip}:/root/cwm-worker-cluster/clusters &&\
                            {scp} -r {cwm_worker_cluster_path}/cwm_worker_cluster root@{ip}:/root/cwm-worker-cluster/cwm_worker_cluster &&\
                            {scp} -r {cwm_worker_cluster_path}/test_instances root@{ip}:/root/cwm-worker-cluster/test_instances &&\
                            {scp} -r {cwm_worker_cluster_path}/tests root@{ip}:/root/cwm-worker-cluster/tests &&\
                            {scp} {cwm_worker_cluster_path}/setup.py root@{ip}:/root/cwm-worker-cluster/setup.py &&\
                            {scp} {cwm_worker_cluster_path}/requirements-prod.txt root@{ip}:/root/cwm-worker-cluster/requirements-prod.txt &&\
                            {ssh} "cd /root/cwm-worker-cluster && docker build -t tests -f tests/Dockerfile --build-arg CWM_WORKER_TESTS_VERSION=`date +%s` ."
                        '''.format(ip=ip, ssh=ssh, scp=scp, cwm_worker_cluster_path=cwm_worker_cluster_path))
    assert ret == 0, out


def delete_server(tempdir, name):
    ret, out = subprocess.getstatusoutput(
        """cloudcli --config {tempdir}/cloudcli.yaml server terminate --force --name {name}""".format(tempdir=tempdir, name=name))
    if ret != 0:
        print(out)
        return False
    else:
        return True


@contextmanager
def create_servers(servers, post_delete_cleanup, create_servers_stats, root_progress):
    with root_progress.start_sub(__spec__.name, 'create_servers') as progress:
        KEEP_SERVERS_JSON_PATH = os.environ.get('KEEP_SERVERS_JSON_PATH')
        DELETE_KEPT_SERVERS = os.environ.get('DELETE_KEPT_SERVERS') == 'true'
        extra_keep_servers = {}
        with tempfile.TemporaryDirectory() as tempdir:
            # tempdir = '.temp'
            print('tempdir={}'.format(tempdir))
            config.get_cloudcli_yaml(tempdir)
            if KEEP_SERVERS_JSON_PATH and os.path.exists(KEEP_SERVERS_JSON_PATH):
                with open(KEEP_SERVERS_JSON_PATH) as f:
                    keep_server_config = json.load(f)
                    for i, server in keep_server_config['servers'].items():
                        tmp_servers = servers if int(i) in servers else extra_keep_servers
                        tmp_server = tmp_servers.setdefault(int(i), {})
                        tmp_server['created'] = server['created']
                        tmp_server['name'] = server['name']
                        tmp_server['password'] = server['password']
                        tmp_server['ip'] = server['ip']
                        tmp_server['installed_docker'] = server['installed_docker']
                    ssh_id_rsa = keep_server_config['ssh_id_rsa']
                    ssh_id_rsa_pub = keep_server_config['ssh_id_rsa_pub']
            else:
                ssh_id_rsa = None
                ssh_id_rsa_pub = None
            if ssh_id_rsa and ssh_id_rsa_pub:
                print("Using existing SSH KEY")
                with open(os.path.join(tempdir, 'id_rsa.pub'), 'w') as f:
                    f.write(ssh_id_rsa_pub)
                with open(os.path.join(tempdir, 'id_rsa'), 'w') as f:
                    f.write(ssh_id_rsa)
                ret, out = subprocess.getstatusoutput('chmod 400 {}/id_rsa'.format(tempdir))
                assert ret == 0, out
            else:
                generate_new_ssh_key(tempdir)
            created_servers = len([True for server in servers.values() if server.get('created')])
            for i, server in servers.items():
                server['load_test_domain_num'] = i
            try:
                if DELETE_KEPT_SERVERS:
                    raise Exception('Forcing deletion of kept servers')
                while created_servers < len(servers):
                    for server in servers.values():
                        if server.get('created'):
                            continue
                        datacenter = server['datacenter']
                        server_name = server['name'] = str(uuid.uuid4())
                        password = server['password'] = str("Aa1!%s" % os.urandom(12).hex())
                        server['create_server_process'] = start_create_server_process(tempdir, server_name, password, datacenter)
                    try:
                        for server in servers.values():
                            if server.get('created'):
                                continue
                            print("Waiting for server {}".format(server['name']))
                            created, stdout, stderr = wait_for_create_server_process(
                                server['create_server_process'])
                            if created:
                                server['created'] = True
                                created_servers += 1
                            else:
                                print('Server create failed, will retry: {}\n{}'.format(stdout, stderr))
                    finally:
                        for server in servers.values():
                            if server.get('create_server_process'):
                                terminate_create_server_process(server['create_server_process'])
                for server in servers.values():
                    if not server.get('ip'):
                        server['ip'] = get_server_ip(tempdir, server['name'])
                    print("server {} ip {}".format(server['name'], server['ip']))
                num_installed = len([True for server in servers.values() if server.get('installed_docker')])
                while num_installed < len(servers):
                    time.sleep(5)
                    for server in servers.values():
                        if server.get('installed_docker'):
                            continue
                        installed, out = install_docker(tempdir, server['name'], server['ip'])
                        if installed:
                            server['installed_docker'] = True
                            num_installed += 1
                        else:
                            print(out)
                for server in servers.values():
                    if server.get('built_docker'):
                        continue
                    build_test_docker_image(tempdir, server['name'], server['ip'])
                    server['built_docker'] = True
                yield tempdir, servers
            finally:
                if KEEP_SERVERS_JSON_PATH and not DELETE_KEPT_SERVERS:
                    print("Saving server configs to {}".format(KEEP_SERVERS_JSON_PATH))
                    with open(os.path.join(tempdir, 'id_rsa'), 'r') as f:
                        ssh_id_rsa = f.read()
                    with open(os.path.join(tempdir, 'id_rsa.pub'), 'r') as f:
                        ssh_id_rsa_pub = f.read()
                    tmp_keep_servers = {}
                    for tmp_servers in servers, extra_keep_servers:
                        for i, server in tmp_servers.items():
                            tmp_keep_servers[i] = {
                                'created': server.get('created'),
                                'name': server.get('name'),
                                'password': server.get('password'),
                                'ip': server.get('ip'),
                                'installed_docker': server.get('installed_docker')
                            }
                    with open(KEEP_SERVERS_JSON_PATH, 'w') as f:
                        json.dump({
                            'ssh_id_rsa': ssh_id_rsa,
                            'ssh_id_rsa_pub': ssh_id_rsa_pub,
                            'servers': tmp_keep_servers
                        }, f)
                else:
                    _delete_servers(servers, tempdir)
                post_delete_cleanup(servers, create_servers_stats)


def _delete_servers(servers, tempdir):
    print("Deleting servers")
    for server in servers.values():
        if server.get('name'):
            delete_server(tempdir, server['name'])


def delete_kept_servers():
    KEEP_SERVERS_JSON_PATH = os.environ['KEEP_SERVERS_JSON_PATH']
    with tempfile.TemporaryDirectory() as tempdir:
        # tempdir = '.temp'
        print('tempdir={}'.format(tempdir))
        config.get_cloudcli_yaml(tempdir)
        with open(KEEP_SERVERS_JSON_PATH) as f:
            servers = json.load(f)['servers']
        _delete_servers(servers, tempdir)
