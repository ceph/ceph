# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re

from .cephfs import CephFS
from .cephx import CephX
from .orchestrator import OrchClient
from .rgw_client import RgwClient, RequestException, NoCredentialsException
from .. import mgr, logger
from ..settings import Settings
from ..exceptions import DashboardException


class NFSException(DashboardException):
    def __init__(self, msg):
        super(NFSException, self).__init__(component="nfs", msg=msg)


class Ganesha(object):
    @classmethod
    def _get_clusters_locations(cls):
        result = {}
        location_list_str = Settings.GANESHA_CLUSTERS_RADOS_POOL_NAMESPACE
        if not location_list_str:
            raise NFSException("Ganesha config location is not configured. "
                               "Please set the GANESHA_RADOS_POOL_NAMESPACE "
                               "setting.")
        location_list = [l.strip() for l in location_list_str.split(",")]
        for location in location_list:
            cluster = None
            pool = None
            namespace = None
            if not location:
                raise NFSException("Invalid Ganesha cluster RADOS "
                                   "[cluster_id:]pool/namespace setting: {}"
                                   .format(location))
            if location.count(':') < 1:
                # default cluster_id
                if location.count('/') > 1:
                    raise NFSException("Invalid Ganesha RADOS pool/namespace "
                                       "setting: {}".format(location))
                # in this case accept pool/namespace only
                cluster = "_default_"
                if location.count('/') == 0:
                    pool, namespace = location, None
                else:
                    pool, namespace = location.split('/', 1)
            else:
                cluster = location[:location.find(':')]
                pool_nm = location[location.find(':')+1:]
                if pool_nm.count('/') == 0:
                    pool, namespace = pool_nm, None
                else:
                    pool, namespace = pool_nm.split('/', 1)

            if cluster in result:
                raise NFSException("Duplicate Ganesha cluster definition in "
                                   "the setting: {}".format(location_list_str))
            result[cluster] = (pool, namespace)

        return result

    @classmethod
    def get_ganesha_clusters(cls):
        return [cluster_id for cluster_id in cls._get_clusters_locations()]

    @classmethod
    def get_daemons_status(cls):
        if not OrchClient.instance().available():
            return None
        instances = OrchClient.instance().list_service_info("nfs")

        result = {}
        for instance in instances:
            if instance.service is None:
                instance.service = "_default_"
            if instance.service not in result:
                result[instance.service] = {}
            result[instance.service][instance.nodename] = {
                'status': instance.status,
                'desc': instance.status_desc,
            }
        return result

    @classmethod
    def parse_rados_url(cls, rados_url):
        if not rados_url.startswith("rados://"):
            raise NFSException("Invalid NFS Ganesha RADOS configuration URL: {}"
                               .format(rados_url))
        rados_url = rados_url[8:]
        url_comps = rados_url.split("/")
        if len(url_comps) < 2 or len(url_comps) > 3:
            raise NFSException("Invalid NFS Ganesha RADOS configuration URL: "
                               "rados://{}".format(rados_url))
        if len(url_comps) == 2:
            return url_comps[0], None, url_comps[1]
        return url_comps

    @classmethod
    def make_rados_url(cls, pool, namespace, obj):
        if namespace:
            return "rados://{}/{}/{}".format(pool, namespace, obj)
        return "rados://{}/{}".format(pool, obj)

    @classmethod
    def get_pool_and_namespace(cls, cluster_id):
        if OrchClient.instance().available():
            instances = OrchClient.instance().list_service_info("nfs")
            # we assume that every instance stores there configuration in the
            # same RADOS pool/namespace
            if instances:
                location = instances[0].rados_config_location
                pool, ns, _ = cls.parse_rados_url(location)
                return pool, ns
        locations = cls._get_clusters_locations()
        if cluster_id not in locations:
            raise NFSException("Cluster not found: cluster_id={}"
                               .format(cluster_id))
        return locations[cluster_id]

    @classmethod
    def reload_daemons(cls, cluster_id, daemons_id):
        logger.debug("[NFS] issued reload of daemons: %s", daemons_id)
        if not OrchClient.instance().available():
            logger.debug("[NFS] orchestrator not available")
            return
        reload_list = []
        daemons = cls.get_daemons_status()
        if cluster_id not in daemons:
            raise NFSException("Cluster not found: cluster_id={}"
                               .format(cluster_id))
        for daemon_id in daemons_id:
            if daemon_id not in daemons[cluster_id]:
                continue
            if daemons[cluster_id][daemon_id] == 1:
                reload_list.append((cluster_id, daemon_id))
        OrchClient.instance().reload_service("nfs", reload_list)

    @classmethod
    def fsals_available(cls):
        result = []
        if CephFS.list_filesystems():
            result.append("CEPH")
        try:
            if RgwClient.admin_instance().is_service_online() and \
                    RgwClient.admin_instance().is_system_user():
                result.append("RGW")
        except (NoCredentialsException, RequestException, LookupError):
            pass
        return result


class GaneshaConfParser(object):
    def __init__(self, raw_config):
        self.pos = 0
        self.text = ""
        self.clean_config(raw_config)

    def clean_config(self, raw_config):
        for line in raw_config.split("\n"):
            cardinal_idx = line.find('#')
            if cardinal_idx == -1:
                self.text += line
            else:
                # remove comments
                self.text += line[:cardinal_idx]
            if line.startswith("%"):
                self.text += "\n"

    def remove_all_whitespaces(self):
        new_text = ""
        in_string = False
        in_section = False
        for i, cha in enumerate(self.text):
            if in_section:
                if cha != '"' and self.text[i-1] != '\\':
                    new_text += cha
                elif cha == '\n':
                    new_text += cha
                    in_section = False
                elif i == (len(self.text)-1):
                    if cha != '"' and self.text[i-1] != '\\':
                        new_text += cha
                    in_section = False
            elif not in_section and (i == 0 or self.text[i-1] == '\n') and cha == '%':
                in_section = True
                new_text += cha
            elif in_string or cha not in [' ', '\n', '\t']:
                new_text += cha
            elif cha == '"' and self.text[i-1] != '\\':
                in_string = not in_string
        self.text = new_text

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
                self.pos += len(self.stream())
            else:
                value = self.stream()[:idx]
                self.pos += idx+1
            block_dict = {'block_name': '%url', 'value': value}
            return block_dict

        block_name = self.parse_block_name().upper()
        block_dict = {'block_name': block_name}
        self.parse_block_body(block_dict)
        if self.stream()[0] != '}':
            raise Exception("No closing bracket '}' found at the end of block")
        self.pos += 1
        return block_dict

    def parse_parameter_value(self, raw_value):
        colon_idx = raw_value.find(',')

        if colon_idx == -1:
            try:
                return int(raw_value)
            except ValueError:
                if raw_value == "true":
                    return True
                elif raw_value == "false":
                    return False
                elif raw_value.find('"') == 0:
                    return raw_value[1:-1]
                return raw_value
        else:
            return [self.parse_parameter_value(v.strip())
                    for v in raw_value.split(',')]

    def parse_stanza(self, block_dict):
        equal_idx = self.stream().find('=')
        semicolon_idx = self.stream().find(';')
        if equal_idx == -1:
            raise Exception("Maformed stanza: no equal symbol found.")
        parameter_name = self.stream()[:equal_idx].lower()
        parameter_value = self.stream()[equal_idx+1:semicolon_idx]
        block_dict[parameter_name] = self.parse_parameter_value(
            parameter_value)
        self.pos += semicolon_idx+1

    def parse_block_body(self, block_dict):
        last_pos = self.pos
        while True:
            semicolon_idx = self.stream().find(';')
            lbracket_idx = self.stream().find('{')
            rbracket_idx = self.stream().find('}')

            if rbracket_idx == 0:
                # block end
                return

            if (semicolon_idx != -1 and lbracket_idx != -1
                    and semicolon_idx < lbracket_idx) \
                    or (semicolon_idx != -1 and lbracket_idx == -1):
                self.parse_stanza(block_dict)
            elif (semicolon_idx != -1 and lbracket_idx != -1
                  and semicolon_idx > lbracket_idx) or (
                      semicolon_idx == -1 and lbracket_idx != -1):
                if '_blocks_' not in block_dict:
                    block_dict['_blocks_'] = []
                block_dict['_blocks_'].append(self.parse_block_or_section())
            else:
                raise Exception("Malformed stanza: no semicolon found.")

            if last_pos == self.pos:
                raise Exception("Infinite loop while parsing block content")
            last_pos = self.pos

    def parse(self):
        self.remove_all_whitespaces()
        blocks = []
        while self.stream():
            block_dict = self.parse_block_or_section()
            blocks.append(block_dict)
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
            elif isinstance(val, bool):
                return str(val).lower()
            elif isinstance(val, int) or (block['block_name'] == 'CLIENT'
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

    def validate(self):
        raise NotImplementedError()

    def fill_keys(self):
        raise NotImplementedError()

    def create_path(self, path):
        raise NotImplementedError()

    @staticmethod
    def from_fsal_block(fsal_block):
        if fsal_block['name'] == "CEPH":
            return CephFSFSal.from_fsal_block(fsal_block)
        elif fsal_block['name'] == 'RGW':
            return RGWFSal.from_fsal_block(fsal_block)
        return None

    def to_fsal_block(self):
        raise NotImplementedError()

    @staticmethod
    def from_dict(fsal_dict):
        if fsal_dict['name'] == "CEPH":
            return CephFSFSal.from_dict(fsal_dict)
        elif fsal_dict['name'] == 'RGW':
            return RGWFSal.from_dict(fsal_dict)
        return None

    def to_dict(self):
        raise NotImplementedError()


class RGWFSal(FSal):
    def __init__(self, name, rgw_user_id, access_key, secret_key):
        super(RGWFSal, self).__init__(name)
        self.rgw_user_id = rgw_user_id
        self.access_key = access_key
        self.secret_key = secret_key

    @classmethod
    def validate_path(cls, path):
        return path == "/" or re.match(r'^[^/><|&()#?]+$', path)

    def validate(self):
        if not self.rgw_user_id:
            raise NFSException('RGW user must be specified')

        if not RgwClient.admin_instance().user_exists(self.rgw_user_id):
            raise NFSException("RGW user '{}' does not exist"
                               .format(self.rgw_user_id))

    def fill_keys(self):
        keys = RgwClient.admin_instance().get_user_keys(self.rgw_user_id)
        self.access_key = keys['access_key']
        self.secret_key = keys['secret_key']

    def create_path(self, path):
        if path == '/':  # nothing to do
            return
        rgw = RgwClient.instance(self.rgw_user_id)
        try:
            exists = rgw.bucket_exists(path, self.rgw_user_id)
            logger.debug('Checking existence of RGW bucket "%s" for user "%s": %s',
                         path, self.rgw_user_id, exists)
        except RequestException as exp:
            if exp.status_code == 403:
                raise NFSException('Cannot create bucket "{}" as it already '
                                   'exists, and belongs to other user.'
                                   .format(path))
            else:
                raise exp
        if not exists:
            logger.info('Creating new RGW bucket "%s" for user "%s"', path,
                        self.rgw_user_id)
            rgw.create_bucket(path)

    @classmethod
    def from_fsal_block(cls, fsal_block):
        return cls(fsal_block['name'],
                   fsal_block['user_id'],
                   fsal_block['access_key_id'],
                   fsal_block['secret_access_key'])

    def to_fsal_block(self):
        return {
            'block_name': 'FSAL',
            'name': self.name,
            'user_id': self.rgw_user_id,
            'access_key_id': self.access_key,
            'secret_access_key': self.secret_key
        }

    @classmethod
    def from_dict(cls, fsal_dict):
        return cls(fsal_dict['name'], fsal_dict['rgw_user_id'], None, None)

    def to_dict(self):
        return {
            'name': self.name,
            'rgw_user_id': self.rgw_user_id
        }


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

    def validate(self):
        if self.user_id and self.user_id not in CephX.list_clients():
            raise NFSException("cephx user '{}' does not exist"
                               .format(self.user_id))

    def fill_keys(self):
        if self.user_id:
            self.cephx_key = CephX.get_client_key(self.user_id)

    def create_path(self, path):
        cfs = CephFS()
        if not cfs.dir_exists(path):
            cfs.mkdirs(path)

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
        self.protocols = set([GaneshaConf.format_protocol(p) for p in protocols])
        self.transports = set(transports)
        self.clients = clients

    def validate(self, daemons_list):
        # pylint: disable=R0912
        for daemon_id in self.daemons:
            if daemon_id not in daemons_list:
                raise NFSException("Daemon '{}' does not exist"
                                   .format(daemon_id))

        if not self.fsal.validate_path(self.path):
            raise NFSException("Export path ({}) is invalid.".format(self.path))

        if not self.protocols:
            raise NFSException(
                "No NFS protocol version specified for the export.")

        if not self.transports:
            raise NFSException(
                "No network transport type specified for the export.")

        for t in self.transports:
            match = re.match(r'^TCP$|^UDP$', t)
            if not match:
                raise NFSException(
                    "'{}' is an invalid network transport type identifier"
                    .format(t))

        self.fsal.validate()

        if 4 in self.protocols:
            if not self.pseudo:
                raise NFSException(
                    "Pseudo path is required when NFSv4 protocol is used")
            match = re.match(r'^/[^><|&()]*$', self.pseudo)
            if not match:
                raise NFSException(
                    "Export pseudo path ({}) is invalid".format(self.pseudo))

        if self.tag:
            match = re.match(r'^[^/><|:&()]+$', self.tag)
            if not match:
                raise NFSException(
                    "Export tag ({}) is invalid".format(self.tag))

        if self.fsal.name == 'RGW' and 4 not in self.protocols and not self.tag:
            raise NFSException(
                "Tag is mandatory for RGW export when using only NFSv3")

    @classmethod
    def from_export_block(cls, export_block, cluster_id, defaults):
        logger.debug("[NFS] parsing export block: %s", export_block)

        fsal_block = [b for b in export_block['_blocks_']
                      if b['block_name'] == "FSAL"]

        protocols = export_block.get('protocols', defaults['protocols'])
        if not isinstance(protocols, list):
            protocols = [protocols]

        transports = export_block.get('transports', defaults['transports'])
        if not isinstance(transports, list):
            transports = [transports]

        client_blocks = [b for b in export_block['_blocks_']
                         if b['block_name'] == "CLIENT"]

        return cls(export_block['export_id'],
                   export_block['path'],
                   FSal.from_fsal_block(fsal_block[0]),
                   cluster_id,
                   [],
                   export_block.get('pseudo', None),
                   export_block.get('tag', None),
                   export_block.get('access_type', defaults['access_type']),
                   export_block.get('squash', defaults['squash']),
                   export_block.get('attr_expiration_time', None),
                   export_block.get('security_label', False),
                   protocols,
                   transports,
                   [Client.from_client_block(client)
                    for client in client_blocks])

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

    def to_dict(self):
        return {
            'export_id': self.export_id,
            'path': self.path,
            'fsal': self.fsal.to_dict(),
            'cluster_id': self.cluster_id,
            'daemons': sorted([d for d in self.daemons]),
            'pseudo': self.pseudo,
            'tag': self.tag,
            'access_type': self.access_type,
            'squash': self.squash,
            'security_label': self.security_label,
            'protocols': sorted([p for p in self.protocols]),
            'transports': sorted([t for t in self.transports]),
            'clients': [client.to_dict() for client in self.clients]
        }


class GaneshaConf(object):
    # pylint: disable=R0902

    def __init__(self, cluster_id, rados_pool, rados_namespace):
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

    @classmethod
    def instance(cls, cluster_id):
        pool, ns = Ganesha.get_pool_and_namespace(cluster_id)
        return cls(cluster_id, pool, ns)

    def _read_raw_config(self):
        with mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            objs = ioctx.list_objects()
            for obj in objs:
                if obj.key.startswith("export-"):
                    size, _ = obj.stat()
                    raw_config = obj.read(size)
                    raw_config = raw_config.decode("utf-8")
                    logger.debug("[NFS] read export configuration from rados "
                                 "object %s/%s/%s:\n%s", self.rados_pool,
                                 self.rados_namespace, obj.key, raw_config)
                    self.export_conf_blocks.extend(
                        GaneshaConfParser(raw_config).parse())
                elif obj.key.startswith("conf-"):
                    size, _ = obj.stat()
                    raw_config = obj.read(size)
                    raw_config = raw_config.decode("utf-8")
                    logger.debug("[NFS] read daemon configuration from rados "
                                 "object %s/%s/%s:\n%s", self.rados_pool,
                                 self.rados_namespace, obj.key, raw_config)
                    idx = obj.key.find('-')
                    self.daemons_conf_blocks[obj.key[idx+1:]] = \
                        GaneshaConfParser(raw_config).parse()

    def _write_raw_config(self, conf_block, obj):
        raw_config = GaneshaConfParser.write_conf(conf_block)
        with mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            ioctx.write_full(obj, raw_config.encode('utf-8'))
            logger.debug(
                "[NFS] write configuration into rados object %s/%s/%s:\n%s",
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

    @classmethod
    def format_squash(cls, squash):
        if squash is None:
            return None
        if squash.lower() in ["no_root_squash", "noidsquash", "none"]:
            return "no_root_squash"
        elif squash.lower() in ["rootid", "root_id_squash", "rootidsquash"]:
            return "root_id_squash"
        elif squash.lower() in ["root", "root_squash", "rootsquash"]:
            return "root_squash"
        elif squash.lower() in ["all", "all_squash", "allsquash",
                                "all_anonymous", "allanonymous"]:
            return "all_squash"
        logger.error("[NFS] could not parse squash value: %s", squash)
        raise NFSException("'{}' is an invalid squash option".format(squash))

    @classmethod
    def format_protocol(cls, protocol):
        if str(protocol) in ["NFSV3", "3", "V3", "NFS3"]:
            return 3
        elif str(protocol) in ["NFSV4", "4", "V4", "NFS4"]:
            return 4
        logger.error("[NFS] could not parse protocol value: %s", protocol)
        raise NFSException("'{}' is an invalid NFS protocol version identifier"
                           .format(protocol))

    @classmethod
    def format_path(cls, path):
        path = path.strip()
        if len(path) > 1 and path[-1] == '/':
            path = path[:-1]
        return path

    def validate(self, export):
        export.validate(self.list_daemons())

        if 4 in export.protocols:  # NFSv4 protocol
            len_prefix = 1
            parent_export = None
            for ex in self.list_exports():
                if export.tag and ex.tag == export.tag:
                    raise NFSException(
                        "Another export exists with the same tag: {}"
                        .format(export.tag))

                if export.pseudo and ex.pseudo == export.pseudo:
                    raise NFSException(
                        "Another export exists with the same pseudo path: {}"
                        .format(export.pseudo))

                if not ex.pseudo:
                    continue

                if export.pseudo[:export.pseudo.rfind('/')+1].startswith(ex.pseudo):
                    if export.pseudo[len(ex.pseudo)] == '/':
                        if len(ex.pseudo) > len_prefix:
                            len_prefix = len(ex.pseudo)
                            parent_export = ex

            if len_prefix > 1:
                # validate pseudo path
                idx = len(parent_export.pseudo)
                idx = idx + 1 if idx > 1 else idx
                real_path = "{}/{}".format(parent_export.path
                                           if len(parent_export.path) > 1 else "",
                                           export.pseudo[idx:])
                if export.fsal.name == 'CEPH':
                    cfs = CephFS()
                    if export.path != real_path and not cfs.dir_exists(real_path):
                        raise NFSException(
                            "Pseudo path ({}) invalid, path {} does not exist."
                            .format(export.pseudo, real_path))

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
        for daemon_id in self.list_daemons():
            daemon_map[daemon_id] = []

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

    def _save_export(self, export):
        self.validate(export)
        export.fsal.create_path(export.path)
        export.fsal.fill_keys()
        self.exports[export.export_id] = export
        conf_block = export.to_export_block(self.export_defaults)
        self._write_raw_config(conf_block, "export-{}".format(export.export_id))
        self._persist_daemon_configuration()

    def _delete_export(self, export_id):
        self._persist_daemon_configuration()
        with mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            ioctx.remove_object("export-{}".format(export_id))

    def list_exports(self):
        return [ex for _, ex in self.exports.items()]

    def create_export(self, ex_dict):
        ex_id = self._gen_export_id()
        export = Export.from_dict(ex_id, ex_dict)
        self._save_export(export)
        return ex_id

    def has_export(self, export_id):
        return export_id in self.exports

    def update_export(self, ex_dict):
        if ex_dict['export_id'] not in self.exports:
            return None
        old_export = self.exports[ex_dict['export_id']]
        del self.exports[ex_dict['export_id']]
        export = Export.from_dict(ex_dict['export_id'], ex_dict, old_export)
        self._save_export(export)
        self.exports[export.export_id] = export
        return old_export

    def remove_export(self, export_id):
        if export_id not in self.exports:
            return None
        export = self.exports[export_id]
        del self.exports[export_id]
        self._delete_export(export_id)
        return export

    def get_export(self, export_id):
        if export_id in self.exports:
            return self.exports[export_id]
        return None

    def list_daemons(self):
        return [daemon_id for daemon_id in self.daemons_conf_blocks]

    def reload_daemons(self, daemons):
        with mgr.rados.open_ioctx(self.rados_pool) as ioctx:
            if self.rados_namespace:
                ioctx.set_namespace(self.rados_namespace)
            for daemon_id in daemons:
                ioctx.notify("conf-{}".format(daemon_id))
