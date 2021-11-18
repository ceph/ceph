import logging
import os
import tempfile
import time

from jinja2 import Template
from kcli_handler import execute_kcli_cmd, execute_scp_cmd, \
    execute_ssh_cmd, execute_local_cmd

from typing import Dict

KCLI_PLANS_DIR = "generated_plans"
KCLI_PLAN_NAME = "behave_test_plan"

Kcli_Config = {
    "nodes": 3,
    "pool": "default",
    "network": "default",
    "prefix": "ceph",
    "numcpus": 1,
    "memory": 1024,
    "image": "fedora33",
    "notify": False,
    "admin_password": "password",
    "disks": "\n" + (" - 150\n" * 3),
    "ceph_dev_folder": '/'.join(os.getcwd().split('/')[:-3]),
}


def _write_file(file_path, data):
    with open(file_path, "w") as file:
        file.write(data)


def _read_file(file_path):
    file = open(file_path, "r")
    data = "".join(file.readlines())
    file.close()
    return data


def _kcli_template():
    temp_dir = os.path.join(os.getcwd(), "template")
    logging.info("Loading templates")
    kcli = _read_file(os.path.join(temp_dir, "kcli_plan_template"))
    return Template(kcli)


def _clean_generated(dir_path):
    logging.info("Deleting generated files")
    for file in os.listdir(dir_path):
        os.remove(os.path.join(dir_path, file))
    os.rmdir(dir_path)


def _handle_kcli_plan(context, command_type, plan_file_path=None):
    """
    Executes the kcli vm create and delete command according
    to the provided configuration.
    """
    out = None
    if command_type == "create":
        # TODO : Before creating kcli plan check for exisitng kcli plans
        out, err, code = execute_kcli_cmd(
            context,
            f"create plan -f {plan_file_path} {KCLI_PLAN_NAME}"
        )
        if code:
            print(f"Failed to create kcli plan\n Message: {out}\n{err}")
            exit(1)
    elif command_type == "delete":
        out, err, code = execute_kcli_cmd(context, f"delete plan {KCLI_PLAN_NAME} -y")
    print(out)


def _verify_host_ready(context, host, deps: bool = True) -> bool:
    out, err, code = execute_ssh_cmd(context, host, '', 'ps aux | grep -e "podman python3 chrony lvm2"')
    check_for = ['Connection refused']
    if deps:
        check_for.append('install podman')
    for s in check_for:
        for cmd_output in [out, err]:
            if s in cmd_output:
                return False
    return True


def _wait_host_ready(context, host, deps: bool = True) -> None:
    # check every 10 seconds for up to 10 minutes
    sleep_count = 0
    while not _verify_host_ready(context, host, deps):
        if sleep_count > 60:
            raise Exception(f'Host {host} failed to start up in and install deps in expected time')
        time.sleep(10)
        print('. . .')
        sleep_count += 1


def _init_context(context) -> None:
    # setup initial values for context fields we use, typing
    context.config = {}
    context.bootstrap_node = ''
    context.fsid = ''
    context.available_nodes = {}  # node -> ip mapping
    context.cluster_nodes = []
    context.bootstrap_node = ''
    context.last_executed = {}


def _copy_cephadm_binary_to_host(context, host) -> None:
    binary_path = os.path.join(str(Kcli_Config["ceph_dev_folder"]), "src/cephadm/cephadm")
    out, err, code = execute_scp_cmd(context, host, f'{binary_path}', '/usr/bin/')
    out, err, code = execute_ssh_cmd(context, host, '', 'chmod +x /usr/bin/cephadm')


def _copy_ceph_files_to_host(context, host) -> None:
    local_base = Kcli_Config['ceph_dev_folder']
    vm_base = '/root/shared'
    dirs_to_copy = [
        '/src/ceph-volume/ceph_volume',
        '/src/cephadm',
        '/src/pybind/mgr/cephadm',
        '/src/pybind/mgr/orchestrator',
        '/src/pybind/mgr/nfs',
        '/src/pybind/mgr/prometheus',
        '/src/python-common/ceph',
        '/monitoring/grafana/dashboards',
        '/monitoring/prometheus/alerts',
    ]
    for dir in dirs_to_copy:
        dest = '/'.join(f'{dir}'.split('/')[:-1])
        out, err, code = execute_ssh_cmd(context, host, '', f'mkdir -p {vm_base}{dir}')
        out, err, code = execute_scp_cmd(context, host,
                                         f'{local_base}{dir}', f'{vm_base}{dest}', recursive=True)


def _prepare_custom_image(context, host) -> None:
    file_locations: Dict[str, str] = {
        '/src/ceph-volume/ceph_volume': '/usr/lib/python3.6/site-packages/ceph_volume',
        '/src/cephadm/cephadm': '/usr/sbin/cephadm',
        '/src/pybind/mgr/': '/usr/share/ceph/mgr/',
        '/src/python-common/ceph': '/usr/lib/python3.6/site-packages/ceph',
        '/monitoring/grafana/dashboards': '/etc/grafana/dashboards/ceph-dashboard',
        '/monitoring/prometheus/alerts': '/etc/prometheus/ceph',
    }
    dockerfile_str = 'FROM quay.io/ceph/daemon-base:latest-master-devel\n'
    for local_location, container_location in file_locations.items():
        dockerfile_str += f'COPY shared{local_location} {container_location}\n'
    tmp_dockerfile = tempfile.NamedTemporaryFile()
    tmp_dockerfile.write(dockerfile_str.encode('utf-8'))
    tmp_dockerfile.flush()
    dockerfile_fname = tmp_dockerfile.name
    out, err, code = execute_scp_cmd(context, host,
                                     f'{dockerfile_fname}', '/root/Dockerfile')
    out, err, code = execute_ssh_cmd(context, host, '',
                                     ('podman run -d -p 5000:5000 --restart=always --name reg '
                                      + '-e REGISTRY_HTTP_ADDR=0.0.0.0:5000 registry:2'))
    out, err, code = execute_ssh_cmd(context, host, '',
                                     f'podman build --tag {context.available_nodes[host]}:5000/ceph:testing /root/')
    out, err, code = execute_ssh_cmd(context, host, '',
                                     (f'podman push {context.available_nodes[host]}:5000/ceph:testing '
                                      + '--tls-verify=false'))


def _add_host_to_cluster(context, host) -> None:
    print(f'Adding host {host} to the cluster')
    print(f'Waiting for host {host} to start and install deps (python3, podman lvm2, chrony) . . .')
    _wait_host_ready(context, host, deps=True)
    bootstrap_ip = context.available_nodes[context.bootstrap_node]
    out, err, code = execute_ssh_cmd(context, host, '', f'hostname {host}')
    out, err, code = execute_ssh_cmd(context, host, '', f'sudo hostnamectl set-hostname {host}')
    out, err, code = execute_ssh_cmd(context, host, '', ('podman pull '
                                                         + f'{bootstrap_ip}:5000/ceph:testing --tls-verify=false'))
    print(f'Host {host} ready')
    # copy ceph pub key to host's authorized keys
    out, err, code = execute_ssh_cmd(context, context.bootstrap_node, '',
                                     ('cat /etc/ceph/ceph.pub | ssh -oStrictHostKeyChecking=no '
                                      + f'root@{context.available_nodes[host]} '
                                      + '"cat >> /root/.ssh/authorized_keys"'))
    out, err, code = execute_ssh_cmd(context, context.bootstrap_node, 'cephadm_shell',
                                     f'ceph orch host add {host}')
    _copy_cephadm_binary_to_host(context, host)
    print(f'Host {host} added to cluster')


def _create_initial_ceph_config_file_on_host(context, host) -> bool:
    conf_str = ''
    for section, options in context.config['CEPH_CONFIG'].items():
        conf_str += f'[{section}]\n'
        for param, value in options:
            conf_str += f'{param} = {value}\n'
    if not conf_str:
        return False
    # easiest to just write a copy of the config file into the kcli plans dir
    # since this directory is cleaned up at the end
    conf_path = os.path.join(
        os.getcwd(),
        KCLI_PLANS_DIR,
        'config.conf'
    )
    _write_file(conf_path, conf_str)
    out, err, code = execute_scp_cmd(context, host, conf_path, '/root/')
    return True


def _create_bootstrap_cmd(context) -> str:
    use_initial_config = _create_initial_ceph_config_file_on_host(context, context.bootstrap_node)
    bootstrap_cmd = (f'cephadm --image {context.available_nodes[context.bootstrap_node]}:5000/ceph:testing bootstrap '
                     + f'--mon-ip {context.available_nodes[context.bootstrap_node]} '
                     + f'--initial-dashboard-password {Kcli_Config["admin_password"]} '
                     + '--allow-fqdn-hostname '
                     + '--dashboard-password-noupdate')
    for flag, value in context.config['BOOTSTRAP_FLAG'].items():
        bootstrap_cmd += f' --{flag.strip()} {value.strip()}'
    if use_initial_config:
        bootstrap_cmd += ' --config /root/config.conf'
    return bootstrap_cmd


def _bootstrap_cluster(context) -> None:
    """
    Bootstrap a cluster
    """
    context.cluster_nodes = []
    print('Waiting for bootstrap host to start and install deps (python3, podman lvm2, chrony) . . .')
    _wait_host_ready(context, context.bootstrap_node)
    print('Bootstrap host ready.')
    _copy_cephadm_binary_to_host(context, context.bootstrap_node)
    print('Bootstrapping cluster . . .')
    bootstrap_cmd = _create_bootstrap_cmd(context)
    out, err, code = execute_ssh_cmd(context, context.bootstrap_node, '', bootstrap_cmd)
    try:
        context.fsid = [s for s in (out + '\n' + err).split('\n') if 'Cluster fsid' in s][0].split(' ')[-1]
    except IndexError:
        raise Exception(('Failed to find cluster fsid from bootstrap output. Perhaps an error occurred '
                        + f'during bootstrap.\n\nBootstrap out:\n\n{out}\n\nBootstrap err:\n\n{err}'))
    print(f'Bootstrapped cluster with fsid {context.fsid}')

    context.cluster_nodes.append(context.bootstrap_node)
    for host in [h for h in context.available_nodes.keys() if h != context.bootstrap_node]:
        if len(context.cluster_nodes) >= context.config['NODES']:
            break
        _add_host_to_cluster(context, host)
        context.cluster_nodes.append(host)


def _purge_cluster(context) -> None:
    print(f'Tearing down cluster with fsid {context.fsid}')
    if not context.cluster_nodes:
        print('No known cluster hosts to teardown. Skipping teardown.')
        context.config = {}
        context.bootstrap_node = ''
        context.fsid = ''
        return
    print('Pausing orchestator . . .')
    out, err, code = execute_ssh_cmd(context, context.bootstrap_node, 'cephadm_shell', 'ceph orch pause')
    for host in context.cluster_nodes.copy():
        print(f'Cleaning up cluster daemons and files on host {host} . . .')
        out, err, code = execute_ssh_cmd(context, host, '',
                                         f'cephadm rm-cluster --fsid {context.fsid} --zap-osds --force')
        context.cluster_nodes.remove(host)
    print(f'Cluster with fsid {context.fsid} purged')
    context.config = {}
    context.bootstrap_node = ''
    context.fsid = ''


def _find_container_engine(context):
    out, err, code = execute_local_cmd(context, 'which docker')
    if not code:
        context.container_engine = 'docker'
        return
    out, err, code = execute_local_cmd(context, 'which podman')
    if not code:
        context.container_engine = 'podman'
        return
    raise Exception(('Could not find valid container engine (podman, docker). '
                    + 'Please install a container engine to run these tests.'))


def _setup_initial_config(context, desc) -> None:
    """
    Sets up initial cluster configuration from feature description
    """

    """
    Curent known feature description formats

    -------------------------------------------------------------------------
    NODES, used to specify number of nodes in ceph cluster

    NODES | <value>

    Ex:
        NODES | 2
    -------------------------------------------------------------------------

    -------------------------------------------------------------------------
    BOOTSTRAP_FLAG, for setting miscellaneous bootstrap flags for cluster

    BOOTSTRAP_FLAG | <flag-name> | <value>

    Ex:
        BOOTSTRAP_FLAG | skip-monitoring-stack | True
    -------------------------------------------------------------------------

    -------------------------------------------------------------------------
    CEPH_CONFIG, for setting initial ceph config values to be assimilated
    during bootstrap

    CEPH_CONFIG | <section> | <param> | <value>

    Ex:
        CEPH_CONFIG | mgr | mgr/cephadm/use_agent | false

        would translate to

        [mgr]
        mgr/cephadm/use_agent = false
    -------------------------------------------------------------------------

    All config option will be added to context.config which is a mapping from
    strings which correspond to config option types (e.g. CLUSTER, BOOTSTRAP_FLAG)
    to an Any type that can vary based on the config type. For example,
    BOOTSTRAP_FLAG would be a str->str mappings (flag name -> value)
    """

    # initialize known config options
    context.config['NODES'] = 1
    context.config['BOOTSTRAP_FLAG'] = {}
    context.config['CEPH_CONFIG'] = {}

    for line in desc:
        line = line.strip()
        if line.startswith('NODES'):
            _, node_count = line.split('|')
            context.config['NODES'] = int(node_count.strip())
        elif line.startswith('BOOTSTRAP_FLAG'):
            _, param, value = line.split('|')
            if value.strip().lower() == 'true':
                value = ''
            context.config['BOOTSTRAP_FLAG'][param.strip()] = value.strip()
        elif line.startswith('CEPH_CONFIG'):
            _, section, param, value = line.split('|')
            if section.strip() not in context.config['CEPH_CONFIG']:
                context.config['CEPH_CONFIG'][section.strip()] = []
            context.config['CEPH_CONFIG'][section.strip()].append((param.strip(), value.strip()))

    # Add necessary config options
    context.config['BOOTSTRAP_FLAG']['skip-pull'] = ''
    if 'mgr' not in context.config['CEPH_CONFIG']:
        context.config['CEPH_CONFIG']['mgr'] = []
    context.config['CEPH_CONFIG']['mgr'].append(('mgr/cephadm/default_registry', 'localhost:5000'))
    context.config['CEPH_CONFIG']['mgr'].append(('mgr/cephadm/registry_insecure', 'true'))
    context.config['CEPH_CONFIG']['mgr'].append(('mgr/cephadm/use_repo_digest', 'false'))


def _kcli_initial_setup(context) -> str:
    kcli_plans_dir_path = os.path.join(
        os.getcwd(),
        KCLI_PLANS_DIR,
    )
    if not os.path.exists(kcli_plans_dir_path):
        os.mkdir(kcli_plans_dir_path)
    if not os.path.exists(os.path.join(str(os.getenv("HOME")), '.kcli')):
        os.mkdir(os.path.join(str(os.getenv("HOME")), '.kcli'))
    kcli_plan_path = os.path.join(kcli_plans_dir_path, "gen_kcli_plan.yml")
    loaded_kcli = _kcli_template()
    _write_file(
        kcli_plan_path,
        loaded_kcli.render(Kcli_Config)
    )

    return kcli_plan_path


def _create_vms(context):
    kcli_plan_path = _kcli_initial_setup(context)

    print('Creating VMs . . .')
    _handle_kcli_plan(context, "create", os.path.relpath(kcli_plan_path))

    context.available_nodes: Dict[str, str] = {}  # node -> ip mapping
    context.bootstrap_node = ''
    out, err, code = execute_kcli_cmd(context, 'list vms')
    for line in out.split('\n'):
        info = line.split('|')
        info = [s.strip() for s in info]
        if 'behave_test_plan' in info:
            if not context.bootstrap_node:
                context.bootstrap_node = info[1]
            context.available_nodes[info[1]] = info[3]
    print(f'\n{len(context.available_nodes.keys())} VMs created:')
    for node, ip in context.available_nodes.items():
        print(f'| {node} | {ip} |')
    print('\n')
    print('Waiting for bootstrap node to be running . . .')
    _wait_host_ready(context, context.bootstrap_node, deps=False)
    out, err, code = execute_ssh_cmd(context, context.bootstrap_node, '', f'hostname {context.bootstrap_node}')
    out, err, code = execute_ssh_cmd(context, context.bootstrap_node, '',
                                     f'sudo hostnamectl set-hostname {context.bootstrap_node}')
    print('Copying local ceph files to bootstrap node . . .')
    _copy_ceph_files_to_host(context, context.bootstrap_node)
    print('Local ceph files successfully copied to bootstrap node.')
    print('Waiting for bootstrap node to install dependencies . . .')
    _wait_host_ready(context, context.bootstrap_node, deps=True)
    print('Preparing custom ceph image on on bootstrap node . . .')
    _prepare_custom_image(context, context.bootstrap_node)
    print('Custom image prepared. Starting tests . . .')


def _clean_up(context):
    # clean up VMs and created files
    if os.path.exists(KCLI_PLANS_DIR):
        _clean_generated(os.path.abspath(KCLI_PLANS_DIR))
    _handle_kcli_plan(context, "delete")


def before_all(context):
    _init_context(context)
    _find_container_engine(context)
    _create_vms(context)


def before_feature(context, feature):
    _setup_initial_config(context, feature.description)
    _bootstrap_cluster(context)


def after_feature(context, feature):
    _purge_cluster(context)


def after_all(context):
    _clean_up(context)
