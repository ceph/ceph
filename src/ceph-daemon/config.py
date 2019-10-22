from utils import get_hostname, make_fsid

class Config(object):

    def __init__(self, config):
        self.image = config.image
        self.mon_ip = config.mon_ip
        self.mon_addrv = config.mon_addrv
        self.output_config = config.output_config
        self.output_keyring = config.output_keyring
        self.ceph_conf = config.config
        self.data_dir = config.data_dir
        self.log_dir = config.log_dir
        self.unit_dir = config.unit_dir
        self.fsid = config.fsid or make_fsid()
        self.mon_id = config.mon_id or get_hostname()
        self.mgr_id = config.mgr_id or get_hostname()
        if self.mon_ip:
            self.addr_arg = '[v2:%s:3300,v1:%s:6789]' % (self.mon_ip, self.mon_ip)
        if self.mon_addrv:
            self.addr_arg = self.mon_addrv
        self._ceph_uid = None
        self._ceph_gid = None

    @property
    def ceph_uid(self):
        return self._ceph_uid

    @ceph_uid.setter
    def ceph_uid(self, value):
        self._ceph_uid = value

    @property
    def ceph_gid(self):
        return self._ceph_gid

    @ceph_gid.setter
    def ceph_gid(self, value):
        self._ceph_gid = value
