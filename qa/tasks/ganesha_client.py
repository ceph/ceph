"""
mount a ganesha client
"""

import os
import json
import logging
from io import StringIO

from teuthology.misc import deep_merge
from teuthology.task import Task
from teuthology import misc

log = logging.getLogger(__name__)

class GaneshaClient(Task):
    def __init__(self, ctx, config):
        super(GaneshaClient, self).__init__(ctx, config)
        self.log = log
        self.mounts = {}

    def setup(self):
        super(GaneshaClient, self).setup()

    def begin(self):
        super(GaneshaClient, self).begin()
        log.info('mounting ganesha client(s)')

        if self.config is None:
            ids = misc.all_roles_of_type(self.ctx.cluster, 'client')
            client_roles = [f'client.{id_}' for id_ in ids]
            self.config = dict([r, dict()] for r in client_rols)
        elif isinstance(self.config, list):
            client_roles = self.config
            self.config = dict([r, dict()] for r in client_roles)
        elif isinstance(self.config, dict):
            client_roles = filter(lambda x: 'client.' in x, self.config.keys())
        else:
            raise ValueError(f"Invalid config object: {self.config} ({self.config.__class__})")
        log.info(f"config is {self.config}")

        mounts = {}
        overrides = self.ctx.config.get('overrides', {}).get('ganesha-client', {})
        top_overrides = dict(filter(lambda x: 'client.' not in x[0], overrides.items()))

        clients = list(misc.get_clients(ctx=self.ctx, roles=client_roles))
        test_dir = misc.get_testdir(self.ctx)

        for id_, remote in clients:
            entity = f'client.{id_}'
            client_config = self.config.get(entity)
            if client_config is None:
                client_config = {}
            # top level overrides
            deep_merge(client_config, top_overrides)
            # mount specific overrides
            client_config_overrides = overrides.get(entity)
            deep_merge(client_config, client_config_overrides)
            log.info(f"{entity} config is {client_config}")

            cluster_id = client_config['cluster_id']
            pseudo_path = client_config['pseudo_path']
            nfs_version = client_config.get('version', 'latest')

            first_mon = misc.get_first_mon(self.ctx, None)
            (mon0_remote,) = self.ctx.cluster.only(first_mon).remotes.keys()

            proc = mon0_remote.run(args=['ceph', 'nfs', 'export', 'info', cluster_id, pseudo_path],
                                   stdout=StringIO(), wait=True)
            res = proc.stdout.getvalue()
            export_json = json.loads(res)
            log.debug(f'export_json: {export_json}')

            proc = mon0_remote.run(args=['ceph', 'nfs', 'cluster', 'info', cluster_id],
                                   stdout=StringIO(), wait=True)
            res = proc.stdout.getvalue()
            cluster_info = json.loads(res)
            log.debug(f'cluster_info: {cluster_info}')

            info_output = cluster_info[cluster_id]['backend'][0]
            port = info_output['port']
            ip = info_output['ip']

            mntpt = os.path.join(test_dir, f'mnt.{id_}')
            remote.run(args=['mkdir', '-p', mntpt], timeout=60)
            if nfs_version == 'latest':
                remote.run(args=['sudo', 'mount', '-t', 'nfs', '-o',
                                 f'port={port}', f'{ip}:{pseudo_path}', mntpt])
            else:
                remote.run(args=['sudo', 'mount', '-t', 'nfs', '-o',
                                 f'port={port},vers={nfs_version}', f'{ip}:{pseudo_path}', mntpt])
            remote.run(args=['sudo', 'chmod', '1777', mntpt], timeout=60)
            remote.run(args=['stat', mntpt])
            mounts[id_] = (remote, mntpt)
        self.mounts = mounts

    def end(self):
        super(GaneshaClient, self).end()
        log.debug('unmounting ganesha client(s)')
        for (remote, mntpt) in self.mounts.values():
            log.debug(f'unmounting {mntpt}')
            remote.run(args=['sudo', 'umount', mntpt])
        self.mounts = {}

task = GaneshaClient
