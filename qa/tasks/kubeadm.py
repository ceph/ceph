"""
Kubernetes cluster task, deployed via kubeadm
"""
import argparse
import contextlib
import ipaddress
import json
import logging
import random
import yaml
from io import BytesIO

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.config import config as teuth_config
from teuthology.orchestra import run

log = logging.getLogger(__name__)


def _kubectl(ctx, config, args, **kwargs):
    cluster_name = config['cluster']
    ctx.kubeadm[cluster_name].bootstrap_remote.run(
        args=['kubectl'] + args,
        **kwargs,
    )


def kubectl(ctx, config):
    if isinstance(config, str):
        config = [config]
    assert isinstance(config, list)
    for c in config:
        if isinstance(c, str):
            _kubectl(ctx, config, c.split(' '))
        else:
            _kubectl(ctx, config, c)


@contextlib.contextmanager
def preflight(ctx, config):
    run.wait(
        ctx.cluster.run(
            args=[
                'sudo', 'modprobe', 'br_netfilter',
                run.Raw('&&'),
                'sudo', 'sysctl', 'net.bridge.bridge-nf-call-ip6tables=1',
                run.Raw('&&'),
                'sudo', 'sysctl', 'net.bridge.bridge-nf-call-iptables=1',
                run.Raw('&&'),
                'sudo', 'sysctl', 'net.ipv4.ip_forward=1',
                run.Raw('&&'),
                'sudo', 'swapoff', '-a',
            ],
            wait=False,
        )
    )

    # set docker cgroup driver = systemd
    #  see https://kubernetes.io/docs/setup/production-environment/container-runtimes/#docker
    #  see https://github.com/kubernetes/kubeadm/issues/2066
    for remote in ctx.cluster.remotes.keys():
        try:
            orig = remote.read_file('/etc/docker/daemon.json', sudo=True)
            j = json.loads(orig)
        except Exception as e:
            log.info(f'Failed to pull old daemon.json: {e}')
            j = {}
        j["exec-opts"] = ["native.cgroupdriver=systemd"]
        j["log-driver"] = "json-file"
        j["log-opts"] = {"max-size": "100m"}
        j["storage-driver"] = "overlay2"
        remote.write_file('/etc/docker/daemon.json', json.dumps(j), sudo=True)
    run.wait(
        ctx.cluster.run(
            args=[
                'sudo', 'systemctl', 'restart', 'docker',
                run.Raw('||'),
                'true',
            ],
            wait=False,
        )
    )
    yield


@contextlib.contextmanager
def kubeadm_install(ctx, config):
    version = config.get('version', '1.21')

    os_type = teuthology.get_distro(ctx)
    os_version = teuthology.get_distro_version(ctx)

    try:
        if os_type in ['centos', 'rhel']:
            os = f"CentOS_{os_version.split('.')[0]}"
            log.info('Installing cri-o')
            run.wait(
                ctx.cluster.run(
                    args=[
                        'sudo',
                        'curl', '-L', '-o',
                        '/etc/yum.repos.d/devel:kubic:libcontainers:stable.repo',
                        f'https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/{os}/devel:kubic:libcontainers:stable.repo',
                        run.Raw('&&'),
                        'sudo',
                        'curl', '-L', '-o',
                        f'/etc/yum.repos.d/devel:kubic:libcontainers:stable:cri-o:{version}.repo',
                        f'https://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable:/cri-o:/{version}/{os}/devel:kubic:libcontainers:stable:cri-o:{version}.repo',
                        run.Raw('&&'),
                        'sudo', 'dnf', 'install', '-y', 'cri-o',
                    ],
                    wait=False,
                )
            )

            log.info('Installing kube{adm,ctl,let}')
            repo = """[kubernetes]
name=Kubernetes
baseurl=https://packages.cloud.google.com/yum/repos/kubernetes-el7-$basearch
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
"""
            for remote in ctx.cluster.remotes.keys():
                remote.write_file(
                    '/etc/yum.repos.d/kubernetes.repo',
                    repo,
                    sudo=True,
                )
            run.wait(
                ctx.cluster.run(
                    args=[
                        'sudo', 'dnf', 'install', '-y',
                        'kubelet', 'kubeadm', 'kubectl',
                        'iproute-tc', 'bridge-utils',
                    ],
                    wait=False,
                )
            )

            # fix cni config
            for remote in ctx.cluster.remotes.keys():
                conf = """# from https://github.com/cri-o/cri-o/blob/master/tutorials/kubernetes.md#flannel-network
{
    "name": "crio",
    "type": "flannel"
}
"""
                remote.write_file('/etc/cni/net.d/10-crio-flannel.conf', conf, sudo=True)
                remote.run(args=[
                    'sudo', 'rm', '-f',
                    '/etc/cni/net.d/87-podman-bridge.conflist',
                    '/etc/cni/net.d/100-crio-bridge.conf',
                ])

            # start crio
            run.wait(
                ctx.cluster.run(
                    args=[
                        'sudo', 'systemctl', 'daemon-reload',
                        run.Raw('&&'),
                        'sudo', 'systemctl', 'enable', 'crio', '--now',
                    ],
                    wait=False,
                )
            )

        elif os_type == 'ubuntu':
            os = f"xUbuntu_{os_version}"
            log.info('Installing kube{adm,ctl,let}')
            run.wait(
                ctx.cluster.run(
                    args=[
                        'sudo', 'apt', 'update',
                        run.Raw('&&'),
                        'sudo', 'apt', 'install', '-y',
                        'apt-transport-https', 'ca-certificates', 'curl',
                        run.Raw('&&'),
                        'sudo', 'curl', '-fsSLo',
                        '/usr/share/keyrings/kubernetes-archive-keyring.gpg',
                        'https://packages.cloud.google.com/apt/doc/apt-key.gpg',
                        run.Raw('&&'),
                        'echo', 'deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main',
                        run.Raw('|'),
                        'sudo', 'tee', '/etc/apt/sources.list.d/kubernetes.list',
                        run.Raw('&&'),
                        'sudo', 'apt', 'update',
                        run.Raw('&&'),
                        'sudo', 'apt', 'install', '-y',
                        'kubelet', 'kubeadm', 'kubectl',
                        'bridge-utils',
                    ],
                    wait=False,
                )
            )

        else:
            raise RuntimeError(f'unsupported distro {os_type} for cri-o')

        run.wait(
            ctx.cluster.run(
                args=[
                    'sudo', 'systemctl', 'enable', '--now', 'kubelet',
                    run.Raw('&&'),
                    'sudo', 'kubeadm', 'config', 'images', 'pull',
                ],
                wait=False,
            )
        )

        yield

    finally:
        if config.get('uninstall', True):
            log.info('Uninstalling kube{adm,let,ctl}')
            if os_type in ['centos', 'rhel']:
                run.wait(
                    ctx.cluster.run(
                        args=[
                            'sudo', 'rm', '-f',
                            '/etc/yum.repos.d/kubernetes.repo',
                            run.Raw('&&'),
                            'sudo', 'dnf', 'remove', '-y',
                            'kubeadm', 'kubelet', 'kubectl', 'cri-o',
                        ],
                        wait=False
                    )
                )
            elif os_type == 'ubuntu' and False:
                run.wait(
                    ctx.cluster.run(
                        args=[
                            'sudo', 'rm', '-f',
                            '/etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list',
                            f'/etc/apt/sources.list.d/devel:kubic:libcontainers:stable:cri-o:{version}.list',
                            '/etc/apt/trusted.gpg.d/libcontainers-cri-o.gpg',
                            run.Raw('&&'),
                            'sudo', 'apt', 'remove', '-y',
                            'kkubeadm', 'kubelet', 'kubectl', 'cri-o', 'cri-o-runc',
                        ],
                        wait=False,
                    )
                )


@contextlib.contextmanager
def kubeadm_init_join(ctx, config):
    cluster_name = config['cluster']

    bootstrap_remote = None
    remotes = {}      # remote -> ip
    for remote, roles in ctx.cluster.remotes.items():
        for role in roles:
            if role.startswith('host.'):
                if not bootstrap_remote:
                    bootstrap_remote = remote
                if remote not in remotes:
                    remotes[remote] = remote.ssh.get_transport().getpeername()[0]
    if not bootstrap_remote:
        raise RuntimeError('must define at least one host.something role')
    ctx.kubeadm[cluster_name].bootstrap_remote = bootstrap_remote
    ctx.kubeadm[cluster_name].remotes = remotes
    ctx.kubeadm[cluster_name].token = 'abcdef.' + ''.join([
        random.choice('0123456789abcdefghijklmnopqrstuvwxyz') for _ in range(16)
    ])
    log.info(f'Token: {ctx.kubeadm[cluster_name].token}')
    log.info(f'Remotes: {ctx.kubeadm[cluster_name].remotes}')

    try:
        # init
        cmd = [
            'sudo', 'kubeadm', 'init',
            '--node-name', ctx.kubeadm[cluster_name].bootstrap_remote.shortname,
            '--token', ctx.kubeadm[cluster_name].token,
            '--pod-network-cidr', str(ctx.kubeadm[cluster_name].pod_subnet),
        ]
        bootstrap_remote.run(args=cmd)

        # join additional nodes
        joins = []
        for remote, ip in ctx.kubeadm[cluster_name].remotes.items():
            if remote == bootstrap_remote:
                continue
            cmd = [
                'sudo', 'kubeadm', 'join',
                ctx.kubeadm[cluster_name].remotes[ctx.kubeadm[cluster_name].bootstrap_remote] + ':6443',
                '--node-name', remote.shortname,
                '--token', ctx.kubeadm[cluster_name].token,
                '--discovery-token-unsafe-skip-ca-verification',
            ]
            joins.append(remote.run(args=cmd, wait=False))
        run.wait(joins)
        yield

    except Exception as e:
        log.exception(e)
        raise

    finally:
        log.info('Cleaning up node')
        run.wait(
            ctx.cluster.run(
                args=['sudo', 'kubeadm', 'reset', 'cleanup-node', '-f'],
                wait=False,
            )
        )


@contextlib.contextmanager
def kubectl_config(ctx, config):
    cluster_name = config['cluster']
    bootstrap_remote = ctx.kubeadm[cluster_name].bootstrap_remote

    ctx.kubeadm[cluster_name].admin_conf = \
        bootstrap_remote.read_file('/etc/kubernetes/admin.conf', sudo=True)

    log.info('Setting up kubectl')
    try:
        ctx.cluster.run(args=[
            'mkdir', '-p', '.kube',
            run.Raw('&&'),
            'sudo', 'mkdir', '-p', '/root/.kube',
        ])
        for remote in ctx.kubeadm[cluster_name].remotes.keys():
            remote.write_file('.kube/config', ctx.kubeadm[cluster_name].admin_conf)
            remote.sudo_write_file('/root/.kube/config',
                                   ctx.kubeadm[cluster_name].admin_conf)
        yield

    except Exception as e:
        log.exception(e)
        raise

    finally:
        log.info('Deconfiguring kubectl')
        ctx.cluster.run(args=[
            'rm', '-rf', '.kube',
            run.Raw('&&'),
            'sudo', 'rm', '-rf', '/root/.kube',
        ])


def map_vnet(mip):
    for mapping in teuth_config.get('vnet', []):
        mnet = ipaddress.ip_network(mapping['machine_subnet'])
        vnet = ipaddress.ip_network(mapping['virtual_subnet'])
        if vnet.prefixlen >= mnet.prefixlen:
            log.error(f"virtual_subnet {vnet} prefix >= machine_subnet {mnet} prefix")
            return None
        if mip in mnet:
            pos = list(mnet.hosts()).index(mip)
            log.info(f"{mip} is in {mnet} at pos {pos}")
            sub = list(vnet.subnets(32 - mnet.prefixlen))[pos]
            return sub
    return None


@contextlib.contextmanager
def allocate_pod_subnet(ctx, config):
    """
    Allocate a private subnet that will not collide with other test machines/clusters
    """
    cluster_name = config['cluster']
    assert cluster_name == 'kubeadm', 'multiple subnets not yet implemented'

    log.info('Identifying pod subnet')
    remote = list(ctx.cluster.remotes.keys())[0]
    ip = remote.ssh.get_transport().getpeername()[0]
    mip = ipaddress.ip_address(ip)
    vnet = map_vnet(mip)
    assert vnet
    log.info(f'Pod subnet: {vnet}')
    ctx.kubeadm[cluster_name].pod_subnet = vnet
    yield


@contextlib.contextmanager
def pod_network(ctx, config):
    cluster_name = config['cluster']
    pnet = config.get('pod_network', 'calico')
    if pnet == 'flannel':
        r = ctx.kubeadm[cluster_name].bootstrap_remote.run(
            args=[
                'curl',
                'https://raw.githubusercontent.com/coreos/flannel/master/Documentation/kube-flannel.yml',
            ],
            stdout=BytesIO(),
        )
        assert r.exitstatus == 0
        flannel = list(yaml.load_all(r.stdout.getvalue(), Loader=yaml.FullLoader))
        for o in flannel:
            if o.get('data', {}).get('net-conf.json'):
                log.info(f'Updating {o}')
                o['data']['net-conf.json'] = o['data']['net-conf.json'].replace(
                    '10.244.0.0/16',
                    str(ctx.kubeadm[cluster_name].pod_subnet)
                )
                log.info(f'Now {o}')
        flannel_yaml = yaml.dump_all(flannel)
        log.debug(f'Flannel:\n{flannel_yaml}')
        _kubectl(ctx, config, ['apply', '-f', '-'], stdin=flannel_yaml)

    elif pnet == 'calico':
        _kubectl(ctx, config, [
            'create', '-f',
            'https://docs.projectcalico.org/manifests/tigera-operator.yaml'
        ])
        cr = {
            'apiVersion': 'operator.tigera.io/v1',
            'kind': 'Installation',
            'metadata': {'name': 'default'},
            'spec': {
                'calicoNetwork': {
                    'ipPools': [
                        {
                            'blockSize': 26,
                            'cidr': str(ctx.kubeadm[cluster_name].pod_subnet),
                            'encapsulation': 'IPIPCrossSubnet',
                            'natOutgoing': 'Enabled',
                            'nodeSelector': 'all()',
                        }
                    ]
                }
            }
        }
        _kubectl(ctx, config, ['create', '-f', '-'], stdin=yaml.dump(cr))

    else:
        raise RuntimeError(f'unrecognized pod_network {pnet}')

    try:
        yield

    finally:
        if pnet == 'flannel':
            _kubectl(ctx, config, [
                'delete', '-f',
                'https://raw.githubusercontent.com/coreos/flannel/master/Documentation/kube-flannel.yml',
            ])

        elif pnet == 'calico':
            _kubectl(ctx, config, ['delete', 'installation', 'default'])
            _kubectl(ctx, config, [
                'delete', '-f',
                'https://docs.projectcalico.org/manifests/tigera-operator.yaml'
            ])


@contextlib.contextmanager
def setup_pvs(ctx, config):
    """
    Create PVs for all scratch LVs and set up a trivial provisioner
    """
    log.info('Scanning for scratch devices')
    crs = []
    for remote in ctx.cluster.remotes.keys():
        ls = remote.read_file('/scratch_devs').decode('utf-8').strip().splitlines()
        log.info(f'Scratch devices on {remote.shortname}: {ls}')
        for dev in ls:
            devname = dev.split('/')[-1].replace("_", "-")
            crs.append({
                'apiVersion': 'v1',
                'kind': 'PersistentVolume',
                'metadata': {'name': f'{remote.shortname}-{devname}'},
                'spec': {
                    'volumeMode': 'Block',
                    'accessModes': ['ReadWriteOnce'],
                    'capacity': {'storage': '100Gi'},  # doesn't matter?
                    'persistentVolumeReclaimPolicy': 'Retain',
                    'storageClassName': 'scratch',
                    'local': {'path': dev},
                    'nodeAffinity': {
                        'required': {
                            'nodeSelectorTerms': [
                                {
                                    'matchExpressions': [
                                        {
                                            'key': 'kubernetes.io/hostname',
                                            'operator': 'In',
                                            'values': [remote.shortname]
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }
            })
            # overwriting first few MB is enough to make k8s happy
            remote.run(args=[
                'sudo', 'dd', 'if=/dev/zero', f'of={dev}', 'bs=1M', 'count=10'
            ])
    crs.append({
        'kind': 'StorageClass',
        'apiVersion': 'storage.k8s.io/v1',
        'metadata': {'name': 'scratch'},
        'provisioner': 'kubernetes.io/no-provisioner',
        'volumeBindingMode': 'WaitForFirstConsumer',
    })
    y = yaml.dump_all(crs)
    log.info('Creating PVs + StorageClass')
    log.debug(y)
    _kubectl(ctx, config, ['create', '-f', '-'], stdin=y)

    yield


@contextlib.contextmanager
def final(ctx, config):
    cluster_name = config['cluster']

    # remove master node taint
    _kubectl(ctx, config, [
        'taint', 'node',
        ctx.kubeadm[cluster_name].bootstrap_remote.shortname,
        'node-role.kubernetes.io/master-',
        run.Raw('||'),
        'true',
    ])

    yield


@contextlib.contextmanager
def task(ctx, config):
    if not config:
        config = {}
    assert isinstance(config, dict), \
        "task only supports a dictionary for configuration"

    log.info('Kubeadm start')

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('kubeadm', {}))
    log.info('Config: ' + str(config))

    # set up cluster context
    if not hasattr(ctx, 'kubeadm'):
        ctx.kubeadm = {}
    if 'cluster' not in config:
        config['cluster'] = 'kubeadm'
    cluster_name = config['cluster']
    if cluster_name not in ctx.kubeadm:
        ctx.kubeadm[cluster_name] = argparse.Namespace()

    with contextutil.nested(
            lambda: preflight(ctx, config),
            lambda: allocate_pod_subnet(ctx, config),
            lambda: kubeadm_install(ctx, config),
            lambda: kubeadm_init_join(ctx, config),
            lambda: kubectl_config(ctx, config),
            lambda: pod_network(ctx, config),
            lambda: setup_pvs(ctx, config),
            lambda: final(ctx, config),
    ):
        try:
            log.info('Kubeadm complete, yielding')
            yield

        finally:
            log.info('Tearing down kubeadm')
