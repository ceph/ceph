"""
reconfigure a ganesha server
"""

import json
import logging
from io import StringIO

from teuthology.misc import deep_merge
from teuthology.task import Task
from teuthology import misc

log = logging.getLogger(__name__)

class GaneshaReconf(Task):
    def __init__(self, ctx, config):
        super(GaneshaReconf, self).__init__(ctx, config)
        self.log = log

    def setup(self):
        super(GaneshaReconf, self).setup()

    def begin(self):
        super(GaneshaReconf, self).begin()
        log.info('reconfiguring ganesha server')

        ganesha_config = self.config
        log.info(f'ganesha_config is {ganesha_config}')
        overrides = self.ctx.config.get('overrides', {}).get('ganesha-reconf', {})
        log.info(f'overrides is {overrides}')

        deep_merge(ganesha_config, overrides)
        log.info(f'ganesha_config is {ganesha_config}')

        first_mon = misc.get_first_mon(self.ctx, None)
        (mon0_remote,) = self.ctx.cluster.only(first_mon).remotes.keys()

        cluster_id = ganesha_config['cluster_id']
        pseudo_path = ganesha_config['pseudo_path']

        proc = mon0_remote.run(args=['ceph', 'nfs', 'export', 'info', cluster_id, pseudo_path],
                               stdout=StringIO(), wait=True)
        res = proc.stdout.getvalue()
        export_json = json.loads(res)
        log.debug(f'export_json: {export_json}')

        ceph_section = {'async': False, 'zerocopy': False}
        is_async = ganesha_config.get('async', False)
        if is_async:
            ceph_section["async"] = True
        is_zerocopy = ganesha_config.get('zerocopy', False)
        if is_zerocopy:
            ceph_section["zerocopy"] = True

        nfsv4_block = {}
        delegations = ganesha_config.get('delegations', 'none')
        nfsv4_block['delegations'] = False if delegations == 'none' else True

        new_export = {}
        if "export" in export_json.keys():
            new_export = export_json
        else:
            new_export["export"] = export_json
        new_export["export"]["delegations"] = delegations
        new_export["ceph"] = ceph_section
        new_export["nfsv4"] = nfsv4_block

        log.debug(f'new_export is {json.dumps(new_export)}')
        mon0_remote.run(args=['ceph', 'nfs', 'export', 'apply', cluster_id, "-i", "-"],
                        stdin=json.dumps(new_export))

    def end(self):
        super(GaneshaReconf, self).end()

task = GaneshaReconf
