import socket

from cwm_worker_cluster import config
from cwm_worker_cluster import common


def check_domain():
    _, _, ipaddrlist = socket.gethostbyname_ex(config.LOAD_TESTING_DOMAIN)
    nodes = {node['ip']: node['name'] for node in common.get_cluster_nodes(role='worker')}
    dns_ips_missing_in_nodes = []
    for ip in ipaddrlist:
        if ip not in nodes:
            dns_ips_missing_in_nodes.append(ip)
    nodes_missing_in_dns_ips = []
    for node_ip, node_name in nodes.items():
        if node_ip not in ipaddrlist:
            nodes_missing_in_dns_ips.append({'ip': node_ip, 'name': node_name})
    return {
        'dns_ips_missing_in_nodes': dns_ips_missing_in_nodes,
        'nodes_missing_in_dns_ips': nodes_missing_in_dns_ips
    }
