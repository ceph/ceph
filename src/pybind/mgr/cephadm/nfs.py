from typing import Dict, Optional

import cephadm

class NFSGanesha(object):
    def __init__(self,
                 mgr,
                 daemon_id,
                 pool,
                 namespace=None):
        # type: (cephadm.CephadmOrchestrator, str, str, Optional[str]) -> None
        self.daemon_type = 'nfs'
        self.daemon_id = daemon_id

        self.mgr = mgr

        # rados pool config
        self.pool = pool
        self.namespace = namespace

    def get_daemon_name(self):
        # type: () -> str
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def get_rados_user(self):
        # type: () -> str
        return 'admin' # TODO: 'nfs.' + self.daemon_id

    def create_keyring(self):
        # type: () -> str
        ret, keyring, err = self.mgr.mon_command({
            'prefix': 'auth get',
            'entity': 'client.' + self.get_rados_user(),
        })
        return keyring

    def get_cephadm_config(self):
        # type: () -> Dict
        config = {'pool' : self.pool} # type: Dict
        if self.namespace:
            config['namespace'] = self.namespace
        config['files'] = {
            'ganesha.conf' : '', # TODO: add ganesha.conf
        }
        return config
