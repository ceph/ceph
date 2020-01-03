import json
import logging
import cephfs
import orchestrator
from dashboard.services.cephx import CephX
from dashboard.services.ganesha import Ganesha, NFSException, Export, GaneshaConfParser
from .fs_util import create_pool

log = logging.getLogger(__name__)

exp_num = 0

class GaneshaConf(object):
    # pylint: disable=R0902

    def __init__(self, cluster_id, rados_pool, rados_namespace, mgr):
        self.mgr = mgr
        self.cephx_key = ""
        self.cluster_id = cluster_id
        self.rados_pool = rados_pool
        self.rados_namespace = rados_namespace
        self.export_conf_blocks = []
        self.daemons_conf_blocks = {}
        self._defaults = {}
        self.exports = {}

        self._read_raw_config()

        # load defaults
        def_block = [b for b in self.export_conf_blocks
                     if b['block_name'] == "EXPORT_DEFAULTS"]
        self.export_defaults = def_block[0] if def_block else {}
        self._defaults = self.ganesha_defaults(self.export_defaults)

        for export_block in [block for block in self.export_conf_blocks
                             if block['block_name'] == "EXPORT"]:
            export = Export.from_export_block(export_block, cluster_id,
                                              self._defaults)
            self.exports[export.export_id] = export

        # link daemons to exports
        for daemon_id, daemon_blocks in self.daemons_conf_blocks.items():
            for block in daemon_blocks:
                if block['block_name'] == "%url":
                    rados_url = block['value']
                    _, _, obj = Ganesha.parse_rados_url(rados_url)
                    if obj.startswith("export-"):
                        export_id = int(obj[obj.find('-')+1:])
                        self.exports[export_id].daemons.add(daemon_id)

    def _read_raw_config(self):
        with self.mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            objs = ioctx.list_objects()
            for obj in objs:
                if obj.key.startswith("export-"):
                    size, _ = obj.stat()
                    raw_config = obj.read(size)
                    raw_config = raw_config.decode("utf-8")
                    log.debug("read export configuration from rados "
                              "object %s/%s/%s:\n%s", self.rados_pool,
                              self.rados_namespace, obj.key, raw_config)
                    self.export_conf_blocks.extend(
                        GaneshaConfParser(raw_config).parse())
                elif obj.key.startswith("conf-"):
                    size, _ = obj.stat()
                    raw_config = obj.read(size)
                    raw_config = raw_config.decode("utf-8")
                    log.debug("read daemon configuration from rados "
                              "object %s/%s/%s:\n%s", self.rados_pool,
                              self.rados_namespace, obj.key, raw_config)

                    idx = obj.key.find('-')
                    self.daemons_conf_blocks[obj.key[idx+1:]] = \
                        GaneshaConfParser(raw_config).parse()

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
    def ganesha_defaults(cls, export_defaults):
        """
        According to
        https://github.com/nfs-ganesha/nfs-ganesha/blob/next/src/config_samples/export.txt
        """
        return {
            'access_type': export_defaults.get('access_type', 'NONE'),
            'protocols': export_defaults.get('protocols', [3, 4]),
            'transports': export_defaults.get('transports', ['TCP', 'UDP']),
            'squash': export_defaults.get('squash', 'root_squash')
        }

    def _gen_export_id(self):
        exports = sorted(self.exports)
        nid = 1
        for e_id in exports:
            if e_id == nid:
                nid += 1
            else:
                break
        return nid

    def fill_keys(self, export):
        r, auth_dump, outs = self.mgr.mon_command({'prefix':"auth list", "format":"json"})
        auth_dump_ls = json.loads(auth_dump)
        result = {}
        entity_type = "client"
        for auth_entry in auth_dump_ls['auth_dump']:
            entity = auth_entry['entity']
            if not entity_type or entity.startswith('{}.'.format(entity_type)):
                entity_id = entity[entity.find('.')+1:]
                result[entity_id] = auth_entry
        self.cephx_key = result["admin"]["key"]
        export.fsal.cephx_key = self.cephx_key

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
                    'value': Ganesha.make_rados_url(
                        self.rados_pool, self.rados_namespace,
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
        self.fill_keys(export)
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

def create_instance(orch, pool_name):
    return GaneshaConf("a", pool_name, "ganesha", orch)

def create_export(ganesha_conf):
    ex_id = ganesha_conf.create_export({
        'path': "/",
        'pseudo': "/cephfs",
        'cluster_id': "cluster1",
        'daemons': ["ganesha.a"],
        'tag': "",
        'access_type': "RW",
        'squash': "no_root_squash",
        'security_label': True,
        'protocols': [4],
        'transports': ["TCP"],
        'fsal': {"name": "CEPH", "user_id":"admin", "fs_name": "a", "sec_label_xattr": ""},
        'clients': []
        })

    log.info("Export ID is {}".format(ex_id))
    global exp_num
    exp_num += 1
    return 0, "", ""

def delete_export(ganesha_conf, ex_id):
    if not ganesha_conf.has_export(ex_id):
        return 0, "No exports available",""
    log.info("Export detected for id:{}".format(ex_id))
    export = ganesha_conf.remove_export(ex_id)
    ganesha_conf.reload_daemons(export.daemons)
    return 0, "", ""

def check_fsal_valid(fs_map):
    fsmap_res = [{'id': fs['id'], 'name': fs['mdsmap']['fs_name']}
            for fs in fs_map['filesystems']]

    #return 0, json.dumps(fsmap_res, indent=2), ""
    return fsmap_res

def create_rados_pool(vc_mgr, pool_name):
    global exp_num
    if not exp_num:
        r, outb, outs = create_pool(vc_mgr, pool_name)
    """
    if r != 0:
        #return r, outb, outs

    command = {'prefix': 'osd pool application enable', 'pool': pool_name, 'app': 'nfs'}
    r, outb, outs = vc_mgr.mgr.mon_command(command)

    if r != 0:
        #return r, outb, outs
    log.info("pool enable done r: {}".format(r))
    """
