import json
import logging

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

class FSal(object):
    def __init__(self, name):
        self.name = name

    @classmethod
    def validate_path(cls, _):
        raise NotImplementedError()

    def create_path(self, path):
        raise NotImplementedError()

    @staticmethod
    def from_fsal_block(fsal_block):
        if fsal_block['name'] == "CEPH":
            return CephFSFSal.from_fsal_block(fsal_block)
        return None

    def to_fsal_block(self):
        raise NotImplementedError()

    @staticmethod
    def from_dict(fsal_dict):
        if fsal_dict['name'] == "CEPH":
            return CephFSFSal.from_dict(fsal_dict)
        return None

class CephFSFSal(FSal):
    def __init__(self, name, user_id=None, fs_name=None, sec_label_xattr=None,
                 cephx_key=None):
        super(CephFSFSal, self).__init__(name)
        self.fs_name = fs_name
        self.user_id = user_id
        self.sec_label_xattr = sec_label_xattr
        self.cephx_key = cephx_key

    @classmethod
    def validate_path(cls, path):
        return re.match(r'^/[^><|&()?]*$', path)

    def create_path(self, path):
        cfs = CephFS(self.fs_name)
        cfs.mk_dirs(path)

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
    def __init__(self, export_id, path, fsal, cluster_id, daemons, pseudo=None,
                 tag=None, access_type=None, squash=None,
                 attr_expiration_time=None, security_label=False,
                 protocols=None, transports=None, clients=None):
        self.export_id = export_id
        self.path = GaneshaConf.format_path(path)
        self.fsal = fsal
        self.cluster_id = cluster_id
        self.daemons = set(daemons)
        self.pseudo = GaneshaConf.format_path(pseudo)
        self.tag = tag
        self.access_type = access_type
        self.squash = GaneshaConf.format_squash(squash)
        if attr_expiration_time is None:
            self.attr_expiration_time = 0
        else:
            self.attr_expiration_time = attr_expiration_time
        self.security_label = security_label
        self.protocols = {GaneshaConf.format_protocol(p) for p in protocols}
        self.transports = set(transports)
        self.clients = clients

    def to_export_block(self, defaults):
        # pylint: disable=too-many-branches
        result = {
            'block_name': 'EXPORT',
            'export_id': self.export_id,
            'path': self.path
        }
        if self.pseudo:
            result['pseudo'] = self.pseudo
        if self.tag:
            result['tag'] = self.tag
        if 'access_type' not in defaults \
                or self.access_type != defaults['access_type']:
            result['access_type'] = self.access_type
        if 'squash' not in defaults or self.squash != defaults['squash']:
            result['squash'] = self.squash
        if self.fsal.name == 'CEPH':
            result['attr_expiration_time'] = self.attr_expiration_time
            result['security_label'] = self.security_label
        if 'protocols' not in defaults:
            result['protocols'] = [p for p in self.protocols]
        else:
            def_proto = defaults['protocols']
            if not isinstance(def_proto, list):
                def_proto = set([def_proto])
            if self.protocols != def_proto:
                result['protocols'] = [p for p in self.protocols]
        if 'transports' not in defaults:
            result['transports'] = [t for t in self.transports]
        else:
            def_transp = defaults['transports']
            if not isinstance(def_transp, list):
                def_transp = set([def_transp])
            if self.transports != def_transp:
                result['transports'] = [t for t in self.transports]

        result['_blocks_'] = [self.fsal.to_fsal_block()]
        result['_blocks_'].extend([client.to_client_block()
                                   for client in self.clients])
        return result

    @classmethod
    def from_dict(cls, export_id, ex_dict, old_export=None):
        return cls(export_id,
                   ex_dict['path'],
                   FSal.from_dict(ex_dict['fsal']),
                   ex_dict['cluster_id'],
                   ex_dict['daemons'],
                   ex_dict['pseudo'],
                   ex_dict['tag'],
                   ex_dict['access_type'],
                   ex_dict['squash'],
                   old_export.attr_expiration_time if old_export else None,
                   ex_dict['security_label'],
                   ex_dict['protocols'],
                   ex_dict['transports'],
                   [Client.from_dict(client) for client in ex_dict['clients']])

class GaneshaConf(object):
    # pylint: disable=R0902

    def __init__(self, nfs_conf):
        self.mgr = nfs_conf.mgr
        self.cephx_key = nfs_conf.key
        self.cluster_id = nfs_conf.cluster_id
        self.rados_pool = nfs_conf.pool_name
        self.rados_namespace = nfs_conf.pool_ns
        self.export_conf_blocks = []
        self.daemons_conf_blocks = {}
        self.exports = {}
        self.export_defaults = {}

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
        logger.error("could not parse squash value: %s", squash)
        raise NFSException("'{}' is an invalid squash option".format(squash))

    @classmethod
    def format_protocol(cls, protocol):
        if str(protocol) in ["NFSV3", "3", "V3", "NFS3"]:
            return 3
        if str(protocol) in ["NFSV4", "4", "V4", "NFS4"]:
            return 4
        logger.error("could not parse protocol value: %s", protocol)
        raise NFSException("'{}' is an invalid NFS protocol version identifier"
                           .format(protocol))

    @classmethod
    def format_path(cls, path):
        if path is not None:
            path = path.strip()
            if len(path) > 1 and path[-1] == '/':
                path = path[:-1]
        return path

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
        daemon_map = {}
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
            self._write_raw_config(conf_blocks, "conf-{}".format(daemon_id))

    def _delete_export(self, export_id):
        self._persist_daemon_configuration()
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            ioctx.remove_object("export-{}".format(export_id))

    def _save_export(self, export):
        export.fsal.cephx_key = self.cephx_key
        self.exports[export.export_id] = export
        conf_block = export.to_export_block(self.export_defaults)
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
        self.ganeshaconf = ''
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

    def create_common_config(self, nodeid):
        # TODO change rados url to "%url rados://{}/{}/{}".format(self.pool_name, self.pool_ns, nodeid)
        result = """NFS_CORE_PARAM {{
        Enable_NLM = false;
        Enable_RQUOTA = false;
        Protocols = 4;
        }}

        CACHEINODE {{
        Dir_Chunk = 0;
        NParts = 1;
        Cache_Size = 1;
        }}

        NFSv4 {{
        RecoveryBackend = rados_cluster;
        Minor_Versions = 1, 2;
        }}

        RADOS_URLS {{
        userid = {2};
        }}

        %url rados://{0}/{1}/export-1

        RADOS_KV {{
        pool = {0};
        namespace = {1};
        UserId = {2};
        nodeid = {3};
        }}""".format(self.pool_name, self.pool_ns, self.cluster_id, nodeid)
        #self.ganeshaconf._write_raw_config(result, nodeid)

        with self.mgr.rados.open_ioctx(self.pool_name) as ioctx:
            if self.pool_ns:
                ioctx.set_namespace(self.pool_ns)
            ioctx.write_full(nodeid, result.encode('utf-8'))
            log.debug(
                    "write configuration into rados object %s/%s/%s:\n%s",
                    self.pool_name, self.pool_ns, nodeid, result)

    def create_instance(self):
        self.ganeshaconf = GaneshaConf(self)

    def create_export(self):
        ex_id = self.ganeshaconf.create_export({
            'path': "/",
            'pseudo': "/cephfs",
            'cluster_id': self.cluster_id,
            'daemons': ["ganesha.a"],
            'tag': "",
            'access_type': "RW",
            'squash': "no_root_squash",
            'security_label': True,
            'protocols': [4],
            'transports': ["TCP"],
            'fsal': {"name": "CEPH", "user_id":self.cluster_id, "fs_name": "a", "sec_label_xattr": ""},
            'clients': []
            })
        log.info("Export ID is {}".format(ex_id))
        NFSConfig.exp_num += 1
        #self.update_user_caps()
        return 0, "", "Export Created Successfully"

    def delete_export(self, ex_id):
        if not self.ganeshaconf.has_export(ex_id):
            return 0, "No exports available",""
        log.info("Export detected for id:{}".format(ex_id))
        export = self.ganeshaconf.remove_export(ex_id)
        self.ganeshaconf.reload_daemons(export.daemons)
        return 0, "", "Export Deleted Successfully"

    def check_fsal_valid(self):
        fs_map = self.mgr.get('fs_map')
        return [{'id': fs['id'], 'name': fs['mdsmap']['fs_name']}
                for fs in fs_map['filesystems']]

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

        ret, out, err = self.mgr.mon_command({
            'prefix': 'auth get-or-create',
            'entity': client,
            'caps' : ['mon', 'allow r', 'osd', 'allow rw pool=%s namespace=%s, allow rw tag cephfs data=a' % (self.pool_name, self.pool_ns), 'mds', 'allow rw path=/'],
            'format': 'json',
            })

        if ret!= 0:
            return ret, out, err

        json_res = json.loads(out)
        self.key = json_res[0]['key']
        log.info("The user created is {}".format(json_res[0]['entity']))

        """
        Not required, this just gives mgr keyring location.
        keyring = self.mgr.rados.conf_get("keyring")
        log.info("The keyring location is {}".format(keyring))
        """

        log.info("Calling up common config")
        self.create_common_config("a")

        return 0, "", "NFS Cluster Created Successfully"
