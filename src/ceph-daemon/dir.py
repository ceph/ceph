import os

LOG_DIR_MODE = 0o770
DATA_DIR_MODE = 0o700

class Directory(object):
    def __init__(self, config):
        self.config = config
        self.daemon_type = None
        self.daemon_id = None

    def __getattr__(self, attr):
        # Inheritance vs Composition..
        return getattr(self.config, attr)

    def create_daemon_dirs(self, daemon_type=None daemon_id=None, config=None, keyring=None):
        self.daemon_type = daemon_type
        self.daemon_id = daemon_id
        data_dir = self.make_data_dir()
        log_dir = self.make_log_dir()

        if config:
            with open(data_dir + '/config', 'w') as f:
                os.fchown(f.fileno(), uid, gid)
                os.fchmod(f.fileno(), 0o600)
                f.write(config)
        if keyring:
            with open(data_dir + '/keyring', 'w') as f:
                os.fchmod(f.fileno(), 0o600)
                os.fchown(f.fileno(), uid, gid)
                f.write(keyring)

    def get_log_dir(self):
        return os.path.join(self.log_dir, self.fsid)


    def get_data_dir(self):
        return os.path.join(self.data_dir, self.fsid, '%s.%s' % (self.daemon_type, self.daemon_id))


    def make_data_dir(self):
        self.make_data_dir_base()
        data_dir = self.get_data_dir()
        makedirs(self.data_dir, self.ceph_uid, self.ceph_gid, DATA_DIR_MODE)
        return data_dir

    def make_data_dir_base(self):
        data_dir_base = os.path.join(self.data_dir, self.fsid)
        makedirs(data_dir_base, self.ceph_uid, self.ceph_gid, DATA_DIR_MODE)
        return data_dir_base


    def make_log_dir(self):
        log_dir = get_log_dir(self.fsid, self.log_dir)
        makedirs(self.log_dir, self.ceph_uid, self.ceph_gid, LOG_DIR_MODE)
        return log_dir

    def makedirs(dir_name, uid, gid, mode):
        os.makedirs(dir_name, exist_ok=True, mode=mode)
        os.chown(dir_name, self.ceph_uid, self.ceph_gid)
        os.chmod(dir_name, mode)
