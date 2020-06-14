import errno
import json
import logging
from typing import List

from ceph.deployment.service_spec import NFSServiceSpec, PlacementSpec
from rados import TimedOut

import orchestrator

from .fs_util import create_pool

log = logging.getLogger(__name__)

def available_clusters(mgr):
    completion = mgr.describe_service(service_type='nfs')
    mgr._orchestrator_wait([completion])
    orchestrator.raise_if_exception(completion)
    return [cluster.spec.service_id for cluster in completion.result]

class GaneshaConfParser(object):
    def __init__(self, raw_config):
        self.pos = 0
        self.text = ""
        self.clean_config(raw_config)

    def clean_config(self, raw_config):
        for line in raw_config.split("\n"):
            self.text += line
            if line.startswith("%"):
                self.text += "\n"

    def remove_whitespaces_quotes(self):
        if self.text.startswith("%url"):
            self.text = self.text.replace('"', "")
        else:
            self.text = "".join(self.text.split())

    def stream(self):
        return self.text[self.pos:]

    def parse_block_name(self):
        idx = self.stream().find('{')
        if idx == -1:
            raise Exception("Cannot find block name")
        block_name = self.stream()[:idx]
        self.pos += idx+1
        return block_name

    def parse_block_or_section(self):
        if self.stream().startswith("%url "):
            # section line
            self.pos += 5
            idx = self.stream().find('\n')
            if idx == -1:
                value = self.stream()
                self.pos += len(value)
            else:
                value = self.stream()[:idx]
                self.pos += idx+1
            block_dict = {'block_name': '%url', 'value': value}
            return block_dict

        block_dict = {'block_name': self.parse_block_name().upper()}
        self.parse_block_body(block_dict)
        if self.stream()[0] != '}':
            raise Exception("No closing bracket '}' found at the end of block")
        self.pos += 1
        return block_dict

    def parse_parameter_value(self, raw_value):
        if raw_value.find(',') != -1:
            return [self.parse_parameter_value(v.strip())
                    for v in raw_value.split(',')]
        try:
            return int(raw_value)
        except ValueError:
            if raw_value == "true":
                return True
            if raw_value == "false":
                return False
            if raw_value.find('"') == 0:
                return raw_value[1:-1]
            return raw_value

    def parse_stanza(self, block_dict):
        equal_idx = self.stream().find('=')
        if equal_idx == -1:
            raise Exception("Malformed stanza: no equal symbol found.")
        semicolon_idx = self.stream().find(';')
        parameter_name = self.stream()[:equal_idx].lower()
        parameter_value = self.stream()[equal_idx+1:semicolon_idx]
        block_dict[parameter_name] = self.parse_parameter_value(parameter_value)
        self.pos += semicolon_idx+1

    def parse_block_body(self, block_dict):
        while True:
            if self.stream().find('}') == 0:
                # block end
                return

            last_pos = self.pos
            semicolon_idx = self.stream().find(';')
            lbracket_idx = self.stream().find('{')
            is_semicolon = (semicolon_idx != -1)
            is_lbracket = (lbracket_idx != -1)
            is_semicolon_lt_lbracket = (semicolon_idx < lbracket_idx)

            if is_semicolon and ((is_lbracket and is_semicolon_lt_lbracket) or not is_lbracket):
                self.parse_stanza(block_dict)
            elif is_lbracket and ((is_semicolon and not is_semicolon_lt_lbracket) or
                                  (not is_semicolon)):
                if '_blocks_' not in block_dict:
                    block_dict['_blocks_'] = []
                block_dict['_blocks_'].append(self.parse_block_or_section())
            else:
                raise Exception("Malformed stanza: no semicolon found.")

            if last_pos == self.pos:
                raise Exception("Infinite loop while parsing block content")

    def parse(self):
        self.remove_whitespaces_quotes()
        blocks = []
        while self.stream():
            blocks.append(self.parse_block_or_section())
        return blocks

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
    def write_block(block, depth=0):
        if block['block_name'] == "%url":
            return '%url "{}"\n\n'.format(block['value'])

        conf_str = ""
        conf_str += GaneshaConfParser._indentation(depth)
        conf_str += format(block['block_name'])
        conf_str += " {\n"
        conf_str += GaneshaConfParser.write_block_body(block, depth+1)
        conf_str += GaneshaConfParser._indentation(depth)
        conf_str += "}\n"
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

    def to_dict(self):
        return {
            'name': self.name,
            'user_id': self.user_id,
            'fs_name': self.fs_name,
            'sec_label_xattr': self.sec_label_xattr
        }


class Client(object):
    def __init__(self, addresses, access_type=None, squash=None):
        self.addresses = addresses
        self.access_type = access_type
        self.squash = squash

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

    @classmethod
    def from_export_block(cls, export_block, cluster_id):
        log.debug("parsing export block: %s", export_block)

        fsal_block = [b for b in export_block['_blocks_']
                      if b['block_name'] == "FSAL"]

        client_blocks = [b for b in export_block['_blocks_']
                         if b['block_name'] == "CLIENT"]

        return cls(export_block['export_id'],
                   export_block['path'],
                   CephFSFSal.from_fsal_block(fsal_block[0]),
                   cluster_id,
                   export_block['pseudo'],
                   export_block['access_type'],
                   [Client.from_client_block(client)
                    for client in client_blocks])

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
            'transports': self.transports,
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

    def to_dict(self):
        return {
            'export_id': self.export_id,
            'path': self.path,
            'fsal': self.fsal.to_dict(),
            'cluster_id': self.cluster_id,
            'pseudo': self.pseudo,
            'access_type': self.access_type,
            'squash': self.squash,
            'security_label': self.security_label,
            'protocols': sorted([p for p in self.protocols]),
            'transports': sorted([t for t in self.transports]),
            'clients': [client.to_dict() for client in self.clients]
        }


class FSExport(object):
    def __init__(self, mgr, namespace=None):
        self.mgr = mgr
        self.rados_pool = 'nfs-ganesha'
        self.rados_namespace = namespace
        self.exports = {}

        try:
            log.info("Begin export parsing")
            for cluster_id in available_clusters(self.mgr):
                # Removes 'ganesha-' prefixes from cluster ids.
                cluster_id = cluster_id[cluster_id.index('-')+1:]
                self.export_conf_objs = []  # type: List[Export]
                self._read_raw_config(cluster_id)
                self.exports[cluster_id] = self.export_conf_objs
            log.info(f"Exports parsed successfully {self.exports.items()}")
        except orchestrator.NoOrchestrator:
            # Pass it for vstart
            log.info("Orchestrator not found")
            pass

    @staticmethod
    def _check_rados_notify(ioctx, obj):
        try:
            ioctx.notify(obj)
        except TimedOut:
            log.exception(f"Ganesha timed out")

    def check_fs(self, fs_name):
        fs_map = self.mgr.get('fs_map')
        return fs_name in [fs['mdsmap']['fs_name'] for fs in fs_map['filesystems']]

    def _fetch_export(self, pseudo_path):
        for ex in self.exports[self.rados_namespace]:
            if ex.pseudo == pseudo_path:
                return ex

    def _create_user_key(self, entity, path, fs_name):
        osd_cap = 'allow rw pool={} namespace={}, allow rw tag cephfs data={}'.format(
                self.rados_pool, self.rados_namespace, fs_name)

        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.{}'.format(entity),
            'caps' : ['mon', 'allow r', 'osd', osd_cap, 'mds', 'allow rw path={}'.format(path)],
            'format': 'json',
            })

        json_res = json.loads(out)
        log.info("Export user created is {}".format(json_res[0]['entity']))
        return json_res[0]['entity'], json_res[0]['key']

    def _delete_user(self, entity):
        self.mgr.check_mon_command({
            'prefix': 'auth rm',
            'entity': 'client.{}'.format(entity),
            })
        log.info(f"Export user deleted is {entity}")

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

    def _read_raw_config(self, rados_namespace):
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if rados_namespace:
                ioctx.set_namespace(rados_namespace)

            for obj in ioctx.list_objects():
                if obj.key.startswith("export-"):
                    size, _ = obj.stat()
                    raw_config = obj.read(size)
                    raw_config = raw_config.decode("utf-8")
                    log.debug("read export configuration from rados "
                            "object %s/%s/%s:\n%s", self.rados_pool,
                            rados_namespace, obj.key, raw_config)
                    self.export_conf_objs.append(Export.from_export_block(
                        GaneshaConfParser(raw_config).parse()[0], rados_namespace))

    def _write_raw_config(self, conf_block, obj, append=False):
        raw_config = GaneshaConfParser.write_block(conf_block)
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            if append:
                ioctx.append(obj, raw_config.encode('utf-8'))
                FSExport._check_rados_notify(ioctx, obj)
            else:
                ioctx.write_full(obj, raw_config.encode('utf-8'))
            log.debug(
                    "write configuration into rados object %s/%s/%s:\n%s",
                    self.rados_pool, self.rados_namespace, obj, raw_config)

    def _delete_export_url(self, obj, ex_id):
        export_name = 'export-{}'.format(ex_id)
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)

            export_urls = ioctx.read(obj)
            url = '%url "{}"\n\n'.format(self.make_rados_url(export_name))
            export_urls = export_urls.replace(url.encode('utf-8'), b'')
            ioctx.remove_object(export_name)
            ioctx.write_full(obj, export_urls)
            FSExport._check_rados_notify(ioctx, obj)
            log.debug("Export deleted: {}".format(url))

    def _update_common_conf(self, cluster_id, ex_id):
        common_conf = 'conf-nfs.ganesha-{}'.format(cluster_id)
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
        self._update_common_conf(export.cluster_id, export.export_id)

    def create_export(self, fs_name, cluster_id, pseudo_path, read_only, path):
        try:
            if not self.check_fs(fs_name):
                return -errno.ENOENT, "", f"filesystem {fs_name} not found"

            cluster_check = f"ganesha-{cluster_id}" in available_clusters(self.mgr)
            if not cluster_check:
                return -errno.ENOENT, "", "Cluster does not exists"

            if cluster_id not in self.exports:
                self.exports[cluster_id] = []

            self.rados_namespace = cluster_id

            if not self._fetch_export(pseudo_path):
                ex_id = self._gen_export_id()
                user_id = f"{cluster_id}{ex_id}"
                user_out, key = self._create_user_key(user_id, path, fs_name)
                access_type = "RW"
                if read_only:
                    access_type = "RO"
                ex_dict = {
                        'path': self.format_path(path),
                        'pseudo': self.format_path(pseudo_path),
                        'cluster_id': cluster_id,
                        'access_type': access_type,
                        'fsal': {"name": "CEPH", "user_id": user_id,
                            "fs_name": fs_name, "sec_label_xattr": ""},
                        'clients': []
                        }
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
            return 0, "", "Export already exists"
        except Exception as e:
            log.warning("Failed to create exports")
            return -errno.EINVAL, "", str(e)

    def delete_export(self, cluster_id, pseudo_path, export_obj=None):
        try:
            self.rados_namespace = cluster_id
            if export_obj:
                export = export_obj
            else:
                export = self._fetch_export(pseudo_path)

            if export:
                common_conf = 'conf-nfs.ganesha-{}'.format(cluster_id)
                self._delete_export_url(common_conf, export.export_id)
                self.exports[cluster_id].remove(export)
                self._delete_user(export.fsal.user_id)
                return 0, "Successfully deleted export", ""
            return 0, "", "Export does not exist"
        except KeyError:
            return -errno.EINVAL, "", "Cluster does not exist"
        except Exception as e:
            log.warning("Failed to delete exports")
            return -errno.EINVAL, "", str(e)

    def delete_all_exports(self, cluster_id):
        try:
            export_list = list(self.exports[cluster_id])
            for export in export_list:
               ret, out, err = self.delete_export(cluster_id, None, export)
               if ret != 0:
                   raise Exception(f"Failed to delete exports: {err} and {ret}")
            log.info(f"All exports successfully deleted for cluster id: {cluster_id}")
        except KeyError:
            log.info("No exports to delete")

    def list_exports(self, cluster_id, detailed):
        if not f"ganesha-{cluster_id}" in available_clusters(self.mgr):
            return -errno.ENOENT, "", f"NFS cluster '{cluster_id}' not found"
        if detailed:
            result = [export.to_dict() for export in self.exports[cluster_id]]
        else:
            result = [export.pseudo for export in self.exports[cluster_id]]

        return 0, json.dumps(result, indent=2), ''

    def make_rados_url(self, obj):
        if self.rados_namespace:
            return "rados://{}/{}/{}".format(self.rados_pool, self.rados_namespace, obj)
        return "rados://{}/{}".format(self.rados_pool, obj)

class NFSCluster:
    def __init__(self, mgr):
        self.pool_name = 'nfs-ganesha'
        self.pool_ns = ''
        self.mgr = mgr

    def create_empty_rados_obj(self):
        common_conf = self._get_common_conf_obj_name()
        result = ''
        with self.mgr.rados.open_ioctx(self.pool_name) as ioctx:
            if self.pool_ns:
                ioctx.set_namespace(self.pool_ns)
            ioctx.write_full(common_conf, result.encode('utf-8'))
            log.debug(
                    "write configuration into rados object %s/%s/%s\n",
                    self.pool_name, self.pool_ns, common_conf)

    def delete_common_config_obj(self):
        common_conf = self._get_common_conf_obj_name()
        with self.mgr.rados.open_ioctx(self.pool_name) as ioctx:
            if self.pool_ns:
                ioctx.set_namespace(self.pool_ns)

            ioctx.remove_object(common_conf)
            log.info(f"Deleted object:{common_conf}")

    def _set_cluster_id(self, cluster_id):
        self.cluster_id = f"ganesha-{cluster_id}"

    def _set_pool_namespace(self, cluster_id):
        self.pool_ns = cluster_id

    def _get_common_conf_obj_name(self):
        return 'conf-nfs.{}'.format(self.cluster_id)

    def _call_orch_apply_nfs(self, placement):
        spec = NFSServiceSpec(service_type='nfs', service_id=self.cluster_id,
                              pool=self.pool_name, namespace=self.pool_ns,
                              placement=PlacementSpec.from_string(placement))
        completion = self.mgr.apply_nfs(spec)
        self.mgr._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)

    def create_nfs_cluster(self, export_type, cluster_id, placement):
        if export_type != 'cephfs':
            return -errno.EINVAL, "", f"Invalid export type: {export_type}"
        try:
            pool_list = [p['pool_name'] for p in self.mgr.get_osdmap().dump().get('pools', [])]

            if self.pool_name not in pool_list:
                r, out, err = create_pool(self.mgr, self.pool_name)
                if r != 0:
                    return r, out, err
                log.info(f"Pool Status: {out}")

                self.mgr.check_mon_command({'prefix': 'osd pool application enable',
                    'pool': self.pool_name, 'app': 'nfs'})

            self._set_pool_namespace(cluster_id)
            self._set_cluster_id(cluster_id)
            self.create_empty_rados_obj()

            if self.cluster_id not in available_clusters(self.mgr):
                self._call_orch_apply_nfs(placement)
                return 0, "NFS Cluster Created Successfully", ""
            return 0, "", f"{self.cluster_id} cluster already exists"
        except Exception as e:
            log.warning("NFS Cluster could not be created")
            return -errno.EINVAL, "", str(e)

    def update_nfs_cluster(self, cluster_id, placement):
        try:
            self._set_pool_namespace(cluster_id)
            self._set_cluster_id(cluster_id)
            if self.cluster_id in available_clusters(self.mgr):
                self._call_orch_apply_nfs(placement)
                return 0, "NFS Cluster Updated Successfully", ""
            return -errno.ENOENT, "", "Cluster does not exist"
        except Exception as e:
            log.warning("NFS Cluster could not be updated")
            return -errno.EINVAL, "", str(e)

    def delete_nfs_cluster(self, cluster_id):
        try:
            self._set_pool_namespace(cluster_id)
            self._set_cluster_id(cluster_id)
            cluster_list = available_clusters(self.mgr)
            if self.cluster_id in cluster_list:
                self.mgr.fs_export.delete_all_exports(cluster_id)
                completion = self.mgr.remove_service('nfs.' + self.cluster_id)
                self.mgr._orchestrator_wait([completion])
                orchestrator.raise_if_exception(completion)
                if len(cluster_list) == 1:
                    self.delete_common_config_obj()
                return 0, "NFS Cluster Deleted Successfully", ""
            return 0, "", "Cluster does not exist"
        except Exception as e:
            log.warning("Failed to delete NFS Cluster")
            return -errno.EINVAL, "", str(e)
