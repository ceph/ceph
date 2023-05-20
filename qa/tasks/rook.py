"""
Rook cluster task
"""
import argparse
import configobj
import contextlib
import json
import logging
import os
import yaml
from io import BytesIO

from tarfile import ReadError
from tasks.ceph_manager import CephManager
from teuthology import misc as teuthology
from teuthology.config import config as teuth_config
from teuthology.contextutil import safe_while
from teuthology.orchestra import run
from teuthology import contextutil
from tasks.ceph import healthy
from tasks.cephadm import update_archive_setting

log = logging.getLogger(__name__)

def path_to_examples(ctx, cluster_name : str) -> str:
    for p in ['rook/deploy/examples/', 'rook/cluster/examples/kubernetes/ceph/']:
        try: 
           ctx.rook[cluster_name].remote.get_file(p + 'operator.yaml')
           return p
        except:
            pass 
    assert False, 'Path to examples not found'

def _kubectl(ctx, config, args, **kwargs):
    cluster_name = config.get('cluster', 'ceph')
    return ctx.rook[cluster_name].remote.run(
        args=['kubectl'] + args,
        **kwargs
    )


def shell(ctx, config):
    """
    Run command(s) inside the rook tools container.

      tasks:
      - kubeadm:
      - rook:
      - rook.shell:
          - ceph -s

    or

      tasks:
      - kubeadm:
      - rook:
      - rook.shell:
          commands:
          - ceph -s

    """
    if isinstance(config, list):
        config = {'commands': config}
    for cmd in config.get('commands', []):
        if isinstance(cmd, str):
            _shell(ctx, config, cmd.split(' '))
        else:
            _shell(ctx, config, cmd)


def _shell(ctx, config, args, **kwargs):
    cluster_name = config.get('cluster', 'ceph')
    return _kubectl(
        ctx, config,
        [
            '-n', 'rook-ceph',
            'exec',
            ctx.rook[cluster_name].toolbox, '--'
        ] + args,
        **kwargs
    )


@contextlib.contextmanager
def rook_operator(ctx, config):
    cluster_name = config['cluster']
    rook_branch = config.get('rook_branch', 'master')
    rook_git_url = config.get('rook_git_url', 'https://github.com/rook/rook')

    log.info(f'Cloning {rook_git_url} branch {rook_branch}')
    ctx.rook[cluster_name].remote.run(
        args=[
            'rm', '-rf', 'rook',
            run.Raw('&&'),
            'git',
            'clone',
            '--single-branch',
            '--branch', rook_branch,
            rook_git_url,
            'rook',
        ]
    )

    # operator.yaml
    log.info(os.path.abspath(os.getcwd()))
    object_methods = [method_name for method_name in dir(ctx.rook[cluster_name].remote)
                  if callable(getattr(ctx.rook[cluster_name].remote, method_name))]
    log.info(object_methods)
    operator_yaml = ctx.rook[cluster_name].remote.read_file(
        (path_to_examples(ctx, cluster_name) + 'operator.yaml')
    )
    rook_image = config.get('rook_image')
    if rook_image:
        log.info(f'Patching operator to use image {rook_image}')
        crs = list(yaml.load_all(operator_yaml, Loader=yaml.FullLoader))
        assert len(crs) == 2
        crs[1]['spec']['template']['spec']['containers'][0]['image'] = rook_image
        operator_yaml = yaml.dump_all(crs)
    ctx.rook[cluster_name].remote.write_file('operator.yaml', operator_yaml)

    op_job = None
    try:
        log.info('Deploying operator')
        _kubectl(ctx, config, [
            'create',
            '-f', (path_to_examples(ctx, cluster_name) + 'crds.yaml'),
            '-f', (path_to_examples(ctx, cluster_name) + 'common.yaml'),
            '-f', 'operator.yaml',
        ])

        # on centos:
        if teuthology.get_distro(ctx) == 'centos':
            _kubectl(ctx, config, [
                '-n', 'rook-ceph',
                'set', 'env', 'deploy/rook-ceph-operator',
                'ROOK_HOSTPATH_REQUIRES_PRIVILEGED=true'
            ])

        # wait for operator
        op_name = None
        with safe_while(sleep=10, tries=90, action="wait for operator") as proceed:
            while not op_name and proceed():
                p = _kubectl(
                    ctx, config,
                    ['-n', 'rook-ceph', 'get', 'pods', '-l', 'app=rook-ceph-operator'],
                    stdout=BytesIO(),
                )
                for line in p.stdout.getvalue().decode('utf-8').strip().splitlines():
                    name, ready, status, _ = line.split(None, 3)
                    if status == 'Running':
                        op_name = name
                        break

        # log operator output
        op_job = _kubectl(
            ctx,
            config,
            ['-n', 'rook-ceph', 'logs', '-f', op_name],
            wait=False,
            logger=log.getChild('operator'),
        )

        yield

    except Exception as e:
        log.exception(e)
        raise

    finally:
        log.info('Cleaning up rook operator')
        _kubectl(ctx, config, [
            'delete',
            '-f', 'operator.yaml',
        ])
        if False:
            # don't bother since we'll tear down k8s anyway (and this mysteriously
            # fails sometimes when deleting some of the CRDs... not sure why!)
            _kubectl(ctx, config, [
                'delete',
                '-f', (path_to_examples() + 'common.yaml'),
            ])
            _kubectl(ctx, config, [
                'delete',
                '-f', (path_to_examples() + 'crds.yaml'),
            ])
        ctx.rook[cluster_name].remote.run(args=['rm', '-rf', 'rook', 'operator.yaml'])
        if op_job:
            op_job.wait()
        run.wait(
            ctx.cluster.run(
                args=[
                    'sudo', 'rm', '-rf', '/var/lib/rook'
                ]
            )
        )


@contextlib.contextmanager
def ceph_log(ctx, config):
    cluster_name = config['cluster']

    log_dir = '/var/lib/rook/rook-ceph/log'
    update_archive_setting(ctx, 'log', log_dir)

    try:
        yield

    except Exception:
        # we need to know this below
        ctx.summary['success'] = False
        raise

    finally:
        log.info('Checking cluster log for badness...')
        def first_in_ceph_log(pattern, excludes):
            """
            Find the first occurrence of the pattern specified in the Ceph log,
            Returns None if none found.

            :param pattern: Pattern scanned for.
            :param excludes: Patterns to ignore.
            :return: First line of text (or None if not found)
            """
            args = [
                'sudo',
                'egrep', pattern,
                f'{log_dir}/ceph.log',
            ]
            if excludes:
                for exclude in excludes:
                    args.extend([run.Raw('|'), 'egrep', '-v', exclude])
            args.extend([
                run.Raw('|'), 'head', '-n', '1',
            ])
            r = ctx.rook[cluster_name].remote.run(
                stdout=BytesIO(),
                args=args,
            )
            stdout = r.stdout.getvalue().decode()
            if stdout:
                return stdout
            return None

        if first_in_ceph_log('\[ERR\]|\[WRN\]|\[SEC\]',
                             config.get('log-ignorelist')) is not None:
            log.warning('Found errors (ERR|WRN|SEC) in cluster log')
            ctx.summary['success'] = False
            # use the most severe problem as the failure reason
            if 'failure_reason' not in ctx.summary:
                for pattern in ['\[SEC\]', '\[ERR\]', '\[WRN\]']:
                    match = first_in_ceph_log(pattern, config['log-ignorelist'])
                    if match is not None:
                        ctx.summary['failure_reason'] = \
                            '"{match}" in cluster log'.format(
                                match=match.rstrip('\n'),
                            )
                        break

        if ctx.archive is not None and \
                not (ctx.config.get('archive-on-error') and ctx.summary['success']):
            # and logs
            log.info('Compressing logs...')
            run.wait(
                ctx.cluster.run(
                    args=[
                        'time',
                        'sudo',
                        'find',
                        log_dir,
                        '-name',
                        '*.log',
                        '-print0',
                        run.Raw('|'),
                        'sudo',
                        'xargs',
                        '--max-args=1',
                        '--max-procs=0',
                        '--verbose',
                        '-0',
                        '--no-run-if-empty',
                        '--',
                        'gzip',
                        '-5',
                        '--verbose',
                        '--',
                    ],
                    wait=False,
                ),
            )

            log.info('Archiving logs...')
            path = os.path.join(ctx.archive, 'remote')
            try:
                os.makedirs(path)
            except OSError:
                pass
            for remote in ctx.cluster.remotes.keys():
                sub = os.path.join(path, remote.name)
                try:
                    os.makedirs(sub)
                except OSError:
                    pass
                try:
                    teuthology.pull_directory(remote, log_dir,
                                              os.path.join(sub, 'log'))
                except ReadError:
                    pass


def build_initial_config(ctx, config):
    path = os.path.join(os.path.dirname(__file__), 'rook-ceph.conf')
    conf = configobj.ConfigObj(path, file_error=True)

    # overrides
    for section, keys in config.get('conf',{}).items():
        for key, value in keys.items():
            log.info(" override: [%s] %s = %s" % (section, key, value))
            if section not in conf:
                conf[section] = {}
            conf[section][key] = value

    return conf


@contextlib.contextmanager
def rook_cluster(ctx, config):
    cluster_name = config['cluster']

    # count how many OSDs we'll create
    num_devs = 0
    num_hosts = 0
    for remote in ctx.cluster.remotes.keys():
        ls = remote.read_file('/scratch_devs').decode('utf-8').strip().splitlines()
        num_devs += len(ls)
        num_hosts += 1
    ctx.rook[cluster_name].num_osds = num_devs

    # config
    ceph_conf = build_initial_config(ctx, config)
    ceph_conf_fp = BytesIO()
    ceph_conf.write(ceph_conf_fp)
    log.info(f'Config:\n{ceph_conf_fp.getvalue()}')
    _kubectl(ctx, ceph_conf, ['create', '-f', '-'], stdin=yaml.dump({
        'apiVersion': 'v1',
        'kind': 'ConfigMap',
        'metadata': {
            'name': 'rook-config-override',
            'namespace': 'rook-ceph'},
        'data': {
            'config': ceph_conf_fp.getvalue()
        }
    }))

    # cluster
    cluster = {
        'apiVersion': 'ceph.rook.io/v1',
        'kind': 'CephCluster',
        'metadata': {'name': 'rook-ceph', 'namespace': 'rook-ceph'},
        'spec': {
            'cephVersion': {
                'image': ctx.rook[cluster_name].image,
                'allowUnsupported': True,
            },
            'dataDirHostPath': '/var/lib/rook',
            'skipUpgradeChecks': True,
            'mgr': {
                'count': 1,
                'modules': [
                    { 'name': 'rook', 'enabled': True },
                ],
            },
            'mon': {
                'count': num_hosts,
                'allowMultiplePerNode': True,
            },
            'storage': {
                'storageClassDeviceSets': [
                    {
                        'name': 'scratch',
                        'count': num_devs,
                        'portable': False,
                        'volumeClaimTemplates': [
                            {
                                'metadata': {'name': 'data'},
                                'spec': {
                                    'resources': {
                                        'requests': {
                                            'storage': '10Gi'  # <= (lte) the actual PV size
                                        }
                                    },
                                    'storageClassName': 'scratch',
                                    'volumeMode': 'Block',
                                    'accessModes': ['ReadWriteOnce'],
                                },
                            },
                        ],
                    }
                ],
            },
        }
    }
    teuthology.deep_merge(cluster['spec'], config.get('spec', {}))
    
    cluster_yaml = yaml.dump(cluster)
    log.info(f'Cluster:\n{cluster_yaml}')
    try:
        ctx.rook[cluster_name].remote.write_file('cluster.yaml', cluster_yaml)
        _kubectl(ctx, config, ['create', '-f', 'cluster.yaml'])
        yield

    except Exception as e:
        log.exception(e)
        raise

    finally:
        _kubectl(ctx, config, ['delete', '-f', 'cluster.yaml'], check_status=False)

        # wait for cluster to shut down
        log.info('Waiting for cluster to stop')
        running = True
        with safe_while(sleep=5, tries=100, action="wait for teardown") as proceed:
            while running and proceed():
                p = _kubectl(
                    ctx, config,
                    ['-n', 'rook-ceph', 'get', 'pods'],
                    stdout=BytesIO(),
                )
                running = False
                for line in p.stdout.getvalue().decode('utf-8').strip().splitlines():
                    name, ready, status, _ = line.split(None, 3)
                    if (
                            name != 'NAME'
                            and not name.startswith('csi-')
                            and not name.startswith('rook-ceph-operator-')
                            and not name.startswith('rook-ceph-tools-')
                    ):
                        running = True
                        break

        _kubectl(
            ctx, config,
            ['-n', 'rook-ceph', 'delete', 'configmap', 'rook-config-override'],
            check_status=False,
        )
        ctx.rook[cluster_name].remote.run(args=['rm', '-f', 'cluster.yaml'])


@contextlib.contextmanager
def rook_toolbox(ctx, config):
    cluster_name = config['cluster']
    try:
        _kubectl(ctx, config, [
            'create',
            '-f', (path_to_examples(ctx, cluster_name) + 'toolbox.yaml'),
        ])

        log.info('Waiting for tools container to start')
        toolbox = None
        with safe_while(sleep=5, tries=100, action="wait for toolbox") as proceed:
            while not toolbox and proceed():
                p = _kubectl(
                    ctx, config,
                    ['-n', 'rook-ceph', 'get', 'pods', '-l', 'app=rook-ceph-tools'],
                    stdout=BytesIO(),
                )
                _kubectl(
                    ctx, config,
                    ['-n', 'rook-ceph', 'get', 'pods'],
                    stdout=BytesIO(),
                )
                for line in p.stdout.getvalue().decode('utf-8').strip().splitlines():
                    name, ready, status, _ = line.split(None, 3)
                    if status == 'Running':
                        toolbox = name
                        break
        ctx.rook[cluster_name].toolbox = toolbox
        yield

    except Exception as e:
        log.exception(e)
        raise

    finally:
        _kubectl(ctx, config, [
            'delete',
            '-f', (path_to_examples(ctx, cluster_name) + 'toolbox.yaml'),
        ], check_status=False)


@contextlib.contextmanager
def wait_for_osds(ctx, config):
    cluster_name = config.get('cluster', 'ceph')

    want = ctx.rook[cluster_name].num_osds
    log.info(f'Waiting for {want} OSDs')
    with safe_while(sleep=10, tries=90, action="check osd count") as proceed:
        while proceed():
            p = _shell(ctx, config, ['ceph', 'osd', 'stat', '-f', 'json'],
                       stdout=BytesIO(),
                       check_status=False)
            if p.exitstatus == 0:
                r = json.loads(p.stdout.getvalue().decode('utf-8'))
                have = r.get('num_up_osds', 0)
                if have == want:
                    break
                log.info(f' have {have}/{want} OSDs')

    yield

@contextlib.contextmanager
def ceph_config_keyring(ctx, config):
    # get config and push to hosts
    log.info('Distributing ceph config and client.admin keyring')
    p = _shell(ctx, config, ['cat', '/etc/ceph/ceph.conf'], stdout=BytesIO())
    conf = p.stdout.getvalue()
    p = _shell(ctx, config, ['cat', '/etc/ceph/keyring'], stdout=BytesIO())
    keyring = p.stdout.getvalue()
    ctx.cluster.run(args=['sudo', 'mkdir', '-p', '/etc/ceph'])
    for remote in ctx.cluster.remotes.keys():
        remote.write_file(
            '/etc/ceph/ceph.conf',
            conf,
            sudo=True,
        )
        remote.write_file(
            '/etc/ceph/keyring',
            keyring,
            sudo=True,
        )

    try:
        yield

    except Exception as e:
        log.exception(e)
        raise

    finally:
        log.info('Cleaning up config and client.admin keyring')
        ctx.cluster.run(args=[
            'sudo', 'rm', '-f',
            '/etc/ceph/ceph.conf',
            '/etc/ceph/ceph.client.admin.keyring'
        ])


@contextlib.contextmanager
def ceph_clients(ctx, config):
    cluster_name = config['cluster']

    log.info('Setting up client nodes...')
    clients = ctx.cluster.only(teuthology.is_type('client', cluster_name))
    for remote, roles_for_host in clients.remotes.items():
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'client',
                                                     cluster_name):
            name = teuthology.ceph_role(role)
            client_keyring = '/etc/ceph/{0}.{1}.keyring'.format(cluster_name,
                                                                name)
            r = _shell(ctx, config,
                args=[
                    'ceph', 'auth',
                    'get-or-create', name,
                    'mon', 'allow *',
                    'osd', 'allow *',
                    'mds', 'allow *',
                    'mgr', 'allow *',
                ],
                stdout=BytesIO(),
            )
            keyring = r.stdout.getvalue()
            remote.write_file(client_keyring, keyring, sudo=True, mode='0644')
    yield


@contextlib.contextmanager
def task(ctx, config):
    """
    Deploy rook-ceph cluster

      tasks:
      - kubeadm:
      - rook:
          branch: wip-foo
          spec:
            mon:
              count: 1

    The spec item is deep-merged against the cluster.yaml.  The branch, sha1, or
    image items are used to determine the Ceph container image.
    """
    if not config:
        config = {}
    assert isinstance(config, dict), \
        "task only supports a dictionary for configuration"

    log.info('Rook start')

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph', {}))
    teuthology.deep_merge(config, overrides.get('rook', {}))
    log.info('Config: ' + str(config))

    # set up cluster context
    if not hasattr(ctx, 'rook'):
        ctx.rook = {}
    if 'cluster' not in config:
        config['cluster'] = 'ceph'
    cluster_name = config['cluster']
    if cluster_name not in ctx.rook:
        ctx.rook[cluster_name] = argparse.Namespace()

    ctx.rook[cluster_name].remote = list(ctx.cluster.remotes.keys())[0]

    # image
    teuth_defaults = teuth_config.get('defaults', {})
    cephadm_defaults = teuth_defaults.get('cephadm', {})
    containers_defaults = cephadm_defaults.get('containers', {})
    container_image_name = containers_defaults.get('image', None)
    if 'image' in config:
        ctx.rook[cluster_name].image = config.get('image')
    else:
        sha1 = config.get('sha1')
        flavor = config.get('flavor', 'default')
        if sha1:
            if flavor == "crimson":
                ctx.rook[cluster_name].image = container_image_name + ':' + sha1 + '-' + flavor
            else:
                ctx.rook[cluster_name].image = container_image_name + ':' + sha1
        else:
            # hmm, fall back to branch?
            branch = config.get('branch', 'master')
            ctx.rook[cluster_name].image = container_image_name + ':' + branch
    log.info('Ceph image is %s' % ctx.rook[cluster_name].image)
    
    with contextutil.nested(
            lambda: rook_operator(ctx, config),
            lambda: ceph_log(ctx, config),
            lambda: rook_cluster(ctx, config),
            lambda: rook_toolbox(ctx, config),
            lambda: wait_for_osds(ctx, config),
            lambda: ceph_config_keyring(ctx, config),
            lambda: ceph_clients(ctx, config),
    ):
        if not hasattr(ctx, 'managers'):
            ctx.managers = {}
        ctx.managers[cluster_name] = CephManager(
            ctx.rook[cluster_name].remote,
            ctx=ctx,
            logger=log.getChild('ceph_manager.' + cluster_name),
            cluster=cluster_name,
            rook=True,
        )
        try:
            if config.get('wait-for-healthy', True):
                healthy(ctx=ctx, config=config)
            log.info('Rook complete, yielding')
            yield

        finally:
            log.info('Tearing down rook')
