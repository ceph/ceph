import json
import errno
import logging
try:
    from typing import Dict, List, Optional
except ImportError:
    pass

import cephfs
import orchestrator
from .fs_util import create_pool

log = logging.getLogger(__name__)

class GaneshaConfParser(object):
    def __init__(self, raw_config):
        self.pos = 0
        self.text = ""

    @staticmethod
    def _indentation(depth, size=4):
        conf_str = ""
        for _ in range(0, depth*size):
            conf_str += " "
        return conf_str

    @staticmethod
    def write_block_body(block, depth=0):
        def format_val(key, val):
            if isinstance(val, list):
                return ', '.join([format_val(key, v) for v in val])
            if isinstance(val, bool):
                return str(val).lower()
            if isinstance(val, int) or (block['block_name'] == 'CLIENT'
                                        and key == 'clients'):
                return '{}'.format(val)
            return '"{}"'.format(val)

        conf_str = ""
        for key, val in block.items():
            if key == 'block_name':
                continue
            elif key == '_blocks_':
                for blo in val:
                    conf_str += GaneshaConfParser.write_block(blo, depth)
            elif val:
                conf_str += GaneshaConfParser._indentation(depth)
                conf_str += '{} = {};\n'.format(key, format_val(key, val))
        return conf_str

    @staticmethod
    def write_block(block, depth):
        if block['block_name'] == "%url":
            return '%url "{}"\n\n'.format(block['value'])

        conf_str = ""
        conf_str += GaneshaConfParser._indentation(depth)
        conf_str += format(block['block_name'])
        conf_str += " {\n"
        conf_str += GaneshaConfParser.write_block_body(block, depth+1)
        conf_str += GaneshaConfParser._indentation(depth)
        conf_str += "}\n\n"
        return conf_str

    @staticmethod
    def write_conf(blocks):
        if not isinstance(blocks, list):
            blocks = [blocks]
        conf_str = ""
        for block in blocks:
            conf_str += GaneshaConfParser.write_block(block, 0)
        return conf_str

class CephFSFSal():
    def __init__(self, name, user_id=None, fs_name=None, sec_label_xattr=None,
                 cephx_key=None):
        self.name = name
        self.fs_name = fs_name
        self.user_id = user_id
        self.sec_label_xattr = sec_label_xattr
        self.cephx_key = cephx_key

    @classmethod
    def from_fsal_block(cls, fsal_block):
        return cls(fsal_block['name'],
                   fsal_block.get('user_id', None),
                   fsal_block.get('filesystem', None),
                   fsal_block.get('sec_label_xattr', None),
                   fsal_block.get('secret_access_key', None))

    def to_fsal_block(self):
        result = {
            'block_name': 'FSAL',
            'name': self.name,
        }
        if self.user_id:
            result['user_id'] = self.user_id
        if self.fs_name:
            result['filesystem'] = self.fs_name
        if self.sec_label_xattr:
            result['sec_label_xattr'] = self.sec_label_xattr
        if self.cephx_key:
            result['secret_access_key'] = self.cephx_key
        return result

    @classmethod
    def from_dict(cls, fsal_dict):
        return cls(fsal_dict['name'], fsal_dict['user_id'],
                   fsal_dict['fs_name'], fsal_dict['sec_label_xattr'], None)

class Client(object):
    def __init__(self, addresses, access_type=None, squash=None):
        self.addresses = addresses
        self.access_type = access_type
        self.squash = GaneshaConf.format_squash(squash)

    @classmethod
    def from_client_block(cls, client_block):
        addresses = client_block['clients']
        if not isinstance(addresses, list):
            addresses = [addresses]
        return cls(addresses,
                   client_block.get('access_type', None),
                   client_block.get('squash', None))

    def to_client_block(self):
        result = {
            'block_name': 'CLIENT',
            'clients': self.addresses,
        }
        if self.access_type:
            result['access_type'] = self.access_type
        if self.squash:
            result['squash'] = self.squash
        return result

    @classmethod
    def from_dict(cls, client_dict):
        return cls(client_dict['addresses'], client_dict['access_type'],
                   client_dict['squash'])

    def to_dict(self):
        return {
            'addresses': self.addresses,
            'access_type': self.access_type,
            'squash': self.squash
        }

class Export(object):
    # pylint: disable=R0902
    def __init__(self, export_id, path, fsal, cluster_id, pseudo,
                 access_type='R', clients=None):
        self.export_id = export_id
        self.path = path
        self.fsal = fsal
        self.cluster_id = cluster_id
        self.pseudo = pseudo
        self.access_type = access_type
        self.squash = 'no_root_squash'
        self.attr_expiration_time = 0
        self.security_label = True
        self.protocols = [4]
        self.transports = ["TCP"]
        self.clients = clients

    def to_export_block(self):
        # pylint: disable=too-many-branches
        result = {
            'block_name': 'EXPORT',
            'export_id': self.export_id,
            'path': self.path,
            'pseudo': self.pseudo,
            'access_type': self.access_type,
            'squash': self.squash,
            'attr_expiration_time': self.attr_expiration_time,
            'security_label': self.security_label,
            'protocols': self.protocols,
            'transports': [self.transports],
        }
        result['_blocks_'] = [self.fsal.to_fsal_block()]
        result['_blocks_'].extend([client.to_client_block()
                                   for client in self.clients])
        return result

    @classmethod
    def from_dict(cls, export_id, ex_dict):
        return cls(export_id,
                   ex_dict['path'],
                   CephFSFSal.from_dict(ex_dict['fsal']),
                   ex_dict['cluster_id'],
                   ex_dict['pseudo'],
                   ex_dict['access_type'],
                   [Client.from_dict(client) for client in ex_dict['clients']])

class GaneshaConf(object):
    # pylint: disable=R0902

    def __init__(self):
        self.mgr = nfs_conf.mgr
        self.cephx_key = nfs_conf.key
        self.cluster_id = nfs_conf.cluster_id
        self.rados_pool = nfs_conf.pool_name
        self.rados_namespace = nfs_conf.pool_ns
        self.export_conf_blocks = []
        self.daemons_conf_blocks = {}
        self.exports = {}

    def check_fs_valid(self):
        fs_map = self.mgr.get('fs_map')
        return [{'id': fs['id'], 'name': fs['mdsmap']['fs_name']}
                for fs in fs_map['filesystems']]

    def _create_user_key(self):
        ret, out, err = self.mgr.mon_command({
            'prefix': 'auth get-or-create',
            'entity': self.cluster_id,
            'caps' : ['mon', 'allow r', 'osd', 'allow rw pool=%s namespace=%s, \
                      allow rw tag cephfs data=a' % (self.rados_pool,
                      self.rados_namespace), 'mds', 'allow rw path=/'],
            'format': 'json',
            })

        if ret!= 0:
            return ret, out, err

        json_res = json.loads(out)
        log.info("Export user is {}".format(json_res[0]['entity']))

        return json_res[0]['entity'], json_res[0]['key']

    def _write_raw_config(self, conf_block, obj):
        raw_config = GaneshaConfParser.write_conf(conf_block)
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            ioctx.write_full(obj, raw_config.encode('utf-8'))
            log.debug(
                    "write configuration into rados object %s/%s/%s:\n%s",
                    self.rados_pool, self.rados_namespace, obj, raw_config)

    @classmethod
    def format_path(cls, path):
        if path is not None:
            path = path.strip()
            if len(path) > 1 and path[-1] == '/':
                path = path[:-1]
        return path

    @classmethod
    def format_squash(cls, squash):
        if squash is None:
            return None
        if squash.lower() in ["no_root_squash", "noidsquash", "none"]:
            return "no_root_squash"
        if squash.lower() in ["rootid", "root_id_squash", "rootidsquash"]:
            return "root_id_squash"
        if squash.lower() in ["root", "root_squash", "rootsquash"]:
            return "root_squash"
        if squash.lower() in ["all", "all_squash", "allsquash",
                              "all_anonymous", "allanonymous"]:
            return "all_squash"
        log.error("could not parse squash value: %s", squash)

    def _gen_export_id(self):
        exports = sorted(self.exports)
        nid = 1
        for e_id in exports:
            if e_id == nid:
                nid += 1
            else:
                break
        return nid

    def _persist_daemon_configuration(self):
        daemon_map = {} # type: Dict[str, List[Dict[str, str]]]
        """
        for daemon_id in self.list_daemons():
            daemon_map[daemon_id] = []
        """
        daemon_map["ganesha.a"] = []

        for _, ex in self.exports.items():
            for daemon in ex.daemons:
                daemon_map[daemon].append({
                    'block_name': "%url",
                    'value': self.make_rados_url(
                        "export-{}".format(ex.export_id))
                })
        for daemon_id, conf_blocks in daemon_map.items():
            self._write_raw_config(conf_blocks, "conf-nfs")

    def _delete_export(self, export_id):
        self._persist_daemon_configuration()
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            ioctx.remove_object("export-{}".format(export_id))

    def _save_export(self, export):
        export.fsal.cephx_key = self.cephx_key
        self.exports[export.export_id] = export
        conf_block = export.to_export_block()
        self._write_raw_config(conf_block, "export-{}".format(export.export_id))
        self._persist_daemon_configuration()

    def create_export(self, ex_dict):
        ex_id = self._gen_export_id()
        export = Export.from_dict(ex_id, ex_dict)
        self._save_export(export)
        return ex_id

    def remove_export(self, export_id):
        if export_id not in self.exports:
            return None
        export = self.exports[export_id]
        del self.exports[export_id]
        self._delete_export(export_id)
        return export

    def has_export(self, export_id):
        return export_id in self.exports

    def list_daemons(self):
        return [daemon_id for daemon_id in self.daemons_conf_blocks]

    def reload_daemons(self, daemons):
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            for daemon_id in daemons:
                ioctx.notify("conf-{}".format(daemon_id))

    def make_rados_url(self, obj):
        if self.rados_namespace:
            return "rados://{}/{}/{}".format(self.rados_pool, self.rados_namespace, obj)
        return "rados://{}/{}".format(self.rados_pool, obj)

class NFSConfig(object):
    exp_num = 0

    def __init__(self, mgr, cluster_id):
        self.cluster_id = "ganesha-%s" % cluster_id
        self.pool_name = 'nfs-ganesha'
        self.pool_ns = cluster_id
        self.mgr = mgr
        self.ganeshaconf = None # type: Optional[GaneshaConf]
        self.key = ''

    def update_user_caps(self):
        if NFSConfig.exp_num > 0:
            ret, out, err = self.mgr.mon_command({
                'prefix': 'auth caps',
                'entity': "client.%s" % (self.cluster_id),
                'caps' : ['mon', 'allow *', 'osd', 'allow * pool=%s namespace=%s, allow rw tag cephfs data=a' % (self.pool_name, self.pool_ns), 'mds', 'allow * path=/'],
                })

            if ret!= 0:
                return ret, out, err

    def create_instance(self):
        self.ganeshaconf = GaneshaConf(self)
        ret, out, err = self.mgr.mon_command({'prefix': 'auth get','entity': "client.%s" % (self.cluster_id), 'format': 'json',})

        if not out:
            json_res = json.loads(out)
            self.key = json_res[0]['key']

    def create_export(self):
        assert self.ganeshaconf is not None
        ex_id = self.ganeshaconf.create_export({
            'path': "/",
            'pseudo': "/cephfs",
            'cluster_id': self.cluster_id,
            'daemons': ["ganesha.a"],
            'access_type': "RW",
            'fsal': {"name": "CEPH", "user_id":self.cluster_id, "fs_name": "a", "sec_label_xattr": ""},
            'clients': []
            })
        log.info("Export ID is {}".format(ex_id))
        NFSConfig.exp_num += 1
        #self.update_user_caps()
        return 0, "", "Export Created Successfully"

    def delete_export(self, ex_id):
        assert self.ganeshaconf is not None
        if not self.ganeshaconf.has_export(ex_id):
            return 0, "No exports available",""
        log.info("Export detected for id:{}".format(ex_id))
        export = self.ganeshaconf.remove_export(ex_id)
        self.ganeshaconf.reload_daemons(export.daemons)
        return 0, "", "Export Deleted Successfully"

    def check_fs_valid(self):
        fs_map = self.mgr.get('fs_map')
        return [{'id': fs['id'], 'name': fs['mdsmap']['fs_name']}
                for fs in fs_map['filesystems']]

    def create_empty_rados_obj(self):
        common_conf = 'conf-nfs'
        result = ''
        with self.mgr.rados.open_ioctx(self.pool_name) as ioctx:
            if self.pool_ns:
                ioctx.set_namespace(self.pool_ns)
            ioctx.write_full(common_conf, result.encode('utf-8'))
            log.debug(
                    "write configuration into rados object %s/%s/%s\n",
                    self.pool_name, self.pool_ns, common_conf)

    def create_nfs_cluster(self, size):
        pool_list = [p['pool_name'] for p in self.mgr.get_osdmap().dump().get('pools', [])]
        client = 'client.%s' % self.cluster_id

        if self.pool_name not in pool_list:
            r, out, err = create_pool(self.mgr, self.pool_name)
            if r != 0:
                return r, out, err
            log.info("{}".format(out))

            command = {'prefix': 'osd pool application enable', 'pool': self.pool_name, 'app': 'nfs'}
            r, out, err = self.mgr.mon_command(command)

            if r != 0:
                return r, out, err

        self.create_empty_rados_obj()
        #TODO Check if cluster exists
        #TODO Call Orchestrator to deploy cluster

        return 0, "", "NFS Cluster Created Successfully"

class FSExport(object):
    def __init__(self, mgr, namespace=None):
        self.mgr = mgr
        self.rados_pool = 'nfs-ganesha'
        self.rados_namespace = namespace #TODO check if cluster exists
        self.export_conf_blocks = []
        self.exports = {}

    def check_fs(self, fs_name):
        fs_map = self.mgr.get('fs_map')
        return fs_name in [fs['mdsmap']['fs_name'] for fs in fs_map['filesystems']]

    def check_pseudo_path(self, pseudo_path):
        for ex in self.exports[self.rados_namespace]:
            if ex.pseudo == pseudo_path:
                return True
        return False

    def _create_user_key(self, entity):
        osd_cap = 'allow rw pool={} namespace={}, allow rw tag cephfs data=a'.format(
                self.rados_pool, self.rados_namespace)
        ret, out, err = self.mgr.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.{}'.format(entity),
            'caps' : ['mon', 'allow r', 'osd', osd_cap, 'mds', 'allow rw path=/'],
            'format': 'json',
            })

        if ret!= 0:
            return ret, err

        json_res = json.loads(out)
        log.info("Export user is {}".format(json_res[0]['entity']))

        return json_res[0]['entity'], json_res[0]['key']

    def format_path(self, path):
        if path is not None:
            path = path.strip()
            if len(path) > 1 and path[-1] == '/':
                path = path[:-1]
        return path

    def _gen_export_id(self):
        exports = sorted([ex.export_id for ex in self.exports[self.rados_namespace]])
        nid = 1
        for e_id in exports:
            if e_id == nid:
                nid += 1
            else:
                break
        return nid

    def _write_raw_config(self, conf_block, obj, append=False):
        raw_config = GaneshaConfParser.write_conf(conf_block)
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            if append:
                ioctx.append(obj, raw_config.encode('utf-8'))
            else:
                ioctx.write_full(obj, raw_config.encode('utf-8'))
            log.debug(
                    "write configuration into rados object %s/%s/%s:\n%s",
                    self.rados_pool, self.rados_namespace, obj, raw_config)

    def _update_common_conf(self, ex_id):
        common_conf = 'conf-nfs'
        conf_blocks = {
                'block_name': '%url',
                'value': self.make_rados_url(
                'export-{}'.format(ex_id))
                }
        self._write_raw_config(conf_blocks, common_conf, True)

    def _save_export(self, export):
        self.exports[self.rados_namespace].append(export)
        conf_block = export.to_export_block()
        self._write_raw_config(conf_block, "export-{}".format(export.export_id))
        self._update_common_conf(export.export_id)

    def create_export(self, fs_name, pseudo_path, read_only, path, cluster_id):
        #TODO Check if valid cluster
        if cluster_id not in self.exports:
            self.exports[cluster_id] = []

        self.rados_namespace = cluster_id
        if not self.check_fs(fs_name) or self.check_pseudo_path(pseudo_path):
            return -errno.EINVAL,"", "Invalid CephFS name or export already exists"

        user_id, key = self._create_user_key(cluster_id)
        if isinstance(user_id, int):
            return user_id, "", key
        access_type = "RW"
        if read_only:
            access_type = "R"

        ex_dict = {
            'path': self.format_path(path),
            'pseudo': self.format_path(pseudo_path),
            'cluster_id': cluster_id,
            'access_type': access_type,
            'fsal': {"name": "CEPH", "user_id":cluster_id, "fs_name": fs_name, "sec_label_xattr": ""},
            'clients': []
            }

        ex_id = self._gen_export_id()
        export = Export.from_dict(ex_id, ex_dict)
        export.fsal.cephx_key = key
        self._save_export(export)

        result = {
            "bind": pseudo_path,
            "fs": fs_name,
            "path": path,
            "cluster": cluster_id,
            "mode": access_type,
            }

        return (0, json.dumps(result, indent=4), '')

    def make_rados_url(self, obj):
        if self.rados_namespace:
            return "rados://{}/{}/{}".format(self.rados_pool, self.rados_namespace, obj)
        return "rados://{}/{}".format(self.rados_pool, obj)

class NFSCluster:
    def __init__(self, mgr, cluster_id):
        self.cluster_id = "ganesha-%s" % cluster_id
        self.pool_name = 'nfs-ganesha'
        self.pool_ns = cluster_id
        self.mgr = mgr

    def create_empty_rados_obj(self):
        common_conf = 'conf-nfs'
        result = ''
        with self.mgr.rados.open_ioctx(self.pool_name) as ioctx:
            if self.pool_ns:
                ioctx.set_namespace(self.pool_ns)
            ioctx.write_full(common_conf, result.encode('utf-8'))
            log.debug(
                    "write configuration into rados object %s/%s/nfs-conf\n",
                    self.pool_name, self.pool_ns)

    def create_nfs_cluster(self, size):
        pool_list = [p['pool_name'] for p in self.mgr.get_osdmap().dump().get('pools', [])]
        client = 'client.%s' % self.cluster_id

        if self.pool_name not in pool_list:
            r, out, err = create_pool(self.mgr, self.pool_name)
            if r != 0:
                return r, out, err
            log.info("{}".format(out))

            command = {'prefix': 'osd pool application enable', 'pool': self.pool_name, 'app': 'nfs'}
            r, out, err = self.mgr.mon_command(command)

            if r != 0:
                return r, out, err

        self.create_empty_rados_obj()
        #TODO Check if cluster exists
        #TODO Call Orchestrator to deploy cluster

        return 0, "", "NFS Cluster Created Successfully"

    def update_nfs_cluster(self, size):
        raise NotImplementedError()

    def delete_nfs_cluster(self):
        raise NotImplementedError()
