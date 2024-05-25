# -*- coding: utf-8 -*-
# pylint: disable=too-many-return-statements,too-many-branches

import errno
import json
import logging
import os
import threading
import warnings
from urllib import parse

from .. import mgr
from ..tools import prepare_url_prefix

logger = logging.getLogger('sso')

try:
    from onelogin.saml2.errors import OneLogin_Saml2_Error as Saml2Error
    from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser as Saml2Parser
    from onelogin.saml2.settings import OneLogin_Saml2_Settings as Saml2Settings

    python_saml_imported = True
except ImportError:
    python_saml_imported = False


class Saml2(object):
    def __init__(self, onelogin_settings):
        self.onelogin_settings = onelogin_settings

    def get_username_attribute(self):
        return self.onelogin_settings['sp']['attributeConsumingService']['requestedAttributes'][0][
            'name']

    def to_dict(self):
        return {
            'onelogin_settings': self.onelogin_settings
        }

    @classmethod
    def from_dict(cls, s_dict):
        return Saml2(s_dict['onelogin_settings'])


class SsoDB(object):
    VERSION = 1
    SSODB_CONFIG_KEY = "ssodb_v"

    def __init__(self, version, protocol, saml2):
        self.version = version
        self.protocol = protocol
        self.saml2 = saml2
        self.lock = threading.RLock()

    def save(self):
        with self.lock:
            db = {
                'protocol': self.protocol,
                'saml2': self.saml2.to_dict(),
                'version': self.version
            }
            mgr.set_store(self.ssodb_config_key(), json.dumps(db))

    @classmethod
    def ssodb_config_key(cls, version=None):
        if version is None:
            version = cls.VERSION
        return "{}{}".format(cls.SSODB_CONFIG_KEY, version)

    def check_and_update_db(self):
        logger.debug("Checking for previous DB versions")
        if self.VERSION != 1:
            raise NotImplementedError()

    @classmethod
    def load(cls):
        logger.info("Loading SSO DB version=%s", cls.VERSION)

        json_db = mgr.get_store(cls.ssodb_config_key(), None)
        if json_db is None:
            logger.debug("No DB v%s found, creating new...", cls.VERSION)
            db = cls(cls.VERSION, '', Saml2({}))
            # check if we can update from a previous version database
            db.check_and_update_db()
            return db

        dict_db = json.loads(json_db)  # type: dict
        return cls(dict_db['version'], dict_db.get('protocol'),
                   Saml2.from_dict(dict_db.get('saml2')))


def load_sso_db():
    mgr.SSO_DB = SsoDB.load()  # type: ignore


SSO_COMMANDS = [
    {
        'cmd': 'dashboard sso enable saml2',
        'desc': 'Enable SAML2 Single Sign-On',
        'perm': 'w'
    },
    {
        'cmd': 'dashboard sso disable',
        'desc': 'Disable Single Sign-On',
        'perm': 'w'
    },
    {
        'cmd': 'dashboard sso status',
        'desc': 'Get Single Sign-On status',
        'perm': 'r'
    },
    {
        'cmd': 'dashboard sso show saml2',
        'desc': 'Show SAML2 configuration',
        'perm': 'r'
    },
    {
        'cmd': 'dashboard sso setup saml2 '
               'name=ceph_dashboard_base_url,type=CephString '
               'name=idp_metadata,type=CephString '
               'name=idp_username_attribute,type=CephString,req=false '
               'name=idp_entity_id,type=CephString,req=false '
               'name=sp_x_509_cert,type=CephFilepath,req=false '
               'name=sp_private_key,type=CephFilepath,req=false',
        'desc': 'Setup SAML2 Single Sign-On',
        'perm': 'w'
    }
]


def _get_optional_attr(cmd, attr, default):
    if attr in cmd:
        if cmd[attr] != '':
            return cmd[attr]
    return default


def handle_sso_command(cmd):
    ret = -errno.ENOSYS, '', ''
    if cmd['prefix'] not in ['dashboard sso enable saml2',
                             'dashboard sso disable',
                             'dashboard sso status',
                             'dashboard sso show saml2',
                             'dashboard sso setup saml2']:
        return -errno.ENOSYS, '', ''

    if not python_saml_imported:
        return -errno.EPERM, '', 'Required library not found: `python3-saml`'

    if cmd['prefix'] == 'dashboard sso disable':
        mgr.SSO_DB.protocol = ''
        mgr.SSO_DB.save()
        return 0, 'SSO is "disabled".', ''

    if cmd['prefix'] == 'dashboard sso enable saml2':
        configured = _is_sso_configured()
        if configured:
            mgr.SSO_DB.protocol = 'saml2'
            mgr.SSO_DB.save()
            return 0, 'SSO is "enabled" with "SAML2" protocol.', ''
        return -errno.EPERM, '', 'Single Sign-On is not configured: ' \
            'use `ceph dashboard sso setup saml2`'

    if cmd['prefix'] == 'dashboard sso status':
        if mgr.SSO_DB.protocol == 'saml2':
            return 0, 'SSO is "enabled" with "SAML2" protocol.', ''

        return 0, 'SSO is "disabled".', ''

    if cmd['prefix'] == 'dashboard sso show saml2':
        return 0, json.dumps(mgr.SSO_DB.saml2.to_dict()), ''

    if cmd['prefix'] == 'dashboard sso setup saml2':
        ret = _handle_saml_setup(cmd)
        return ret

    return -errno.ENOSYS, '', ''


def _is_sso_configured():
    configured = True
    try:
        Saml2Settings(mgr.SSO_DB.saml2.onelogin_settings)
    except Saml2Error:
        configured = False
    return configured


def _handle_saml_setup(cmd):
    err, sp_x_509_cert, sp_private_key, has_sp_cert = _read_saml_files(cmd)
    if err:
        ret = -errno.EINVAL, '', err
    else:
        _set_saml_settings(cmd, sp_x_509_cert, sp_private_key, has_sp_cert)
        ret = 0, json.dumps(mgr.SSO_DB.saml2.onelogin_settings), ''
    return ret


def _read_saml_files(cmd):
    sp_x_509_cert_path = _get_optional_attr(cmd, 'sp_x_509_cert', '')
    sp_private_key_path = _get_optional_attr(cmd, 'sp_private_key', '')
    has_sp_cert = sp_x_509_cert_path != "" and sp_private_key_path != ""
    sp_x_509_cert = ''
    sp_private_key = ''
    err = None
    if sp_x_509_cert_path and not sp_private_key_path:
        err = 'Missing parameter `sp_private_key`.'
    elif not sp_x_509_cert_path and sp_private_key_path:
        err = 'Missing parameter `sp_x_509_cert`.'
    elif has_sp_cert:
        sp_x_509_cert, err = _try_read_file(sp_x_509_cert_path)
        sp_private_key, err = _try_read_file(sp_private_key_path)
    return err, sp_x_509_cert, sp_private_key, has_sp_cert


def _try_read_file(path):
    res = ""
    ret = ""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            res = f.read()
    except FileNotFoundError:
        ret = '`{}` not found.'.format(path)
    return res, ret


def _set_saml_settings(cmd, sp_x_509_cert, sp_private_key, has_sp_cert):
    ceph_dashboard_base_url = cmd['ceph_dashboard_base_url']
    idp_metadata = cmd['idp_metadata']
    idp_username_attribute = _get_optional_attr(
        cmd, 'idp_username_attribute', 'uid')
    idp_entity_id = _get_optional_attr(cmd, 'idp_entity_id', None)
    idp_settings = _parse_saml_settings(idp_metadata, idp_entity_id)

    url_prefix = prepare_url_prefix(
        mgr.get_module_option('url_prefix', default=''))
    settings = {
        'sp': {
            'entityId': '{}{}/auth/saml2/metadata'.format(ceph_dashboard_base_url, url_prefix),
            'assertionConsumerService': {
                'url': '{}{}/auth/saml2'.format(ceph_dashboard_base_url, url_prefix),
                'binding': "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
            },
            'attributeConsumingService': {
                'serviceName': "Ceph Dashboard",
                "serviceDescription": "Ceph Dashboard Service",
                "requestedAttributes": [
                    {
                        "name": idp_username_attribute,
                        "isRequired": True
                    }
                ]
            },
            'singleLogoutService': {
                'url': '{}{}/auth/saml2/logout'.format(ceph_dashboard_base_url, url_prefix),
                'binding': 'urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect'
            },
            "x509cert": sp_x_509_cert,
            "privateKey": sp_private_key
        },
        'security': {
            "nameIdEncrypted": has_sp_cert,
            "authnRequestsSigned": has_sp_cert,
            "logoutRequestSigned": has_sp_cert,
            "logoutResponseSigned": has_sp_cert,
            "signMetadata": has_sp_cert,
            "wantMessagesSigned": has_sp_cert,
            "wantAssertionsSigned": has_sp_cert,
            "wantAssertionsEncrypted": has_sp_cert,
            # Not all Identity Providers support this.
            "wantNameIdEncrypted": False,
            "metadataValidUntil": '',
            "wantAttributeStatement": False
        }
    }
    settings = Saml2Parser.merge_settings(settings, idp_settings)
    mgr.SSO_DB.saml2.onelogin_settings = settings
    mgr.SSO_DB.protocol = 'saml2'
    mgr.SSO_DB.save()


def _parse_saml_settings(idp_metadata, idp_entity_id):
    if os.path.isfile(idp_metadata):
        warnings.warn(
            "Please prepend 'file://' to indicate a local SAML2 IdP file", DeprecationWarning)
        with open(idp_metadata, 'r', encoding='utf-8') as f:
            idp_settings = Saml2Parser.parse(f.read(), entity_id=idp_entity_id)
    elif parse.urlparse(idp_metadata)[0] in ('http', 'https', 'file'):
        idp_settings = Saml2Parser.parse_remote(
            url=idp_metadata, validate_cert=False, entity_id=idp_entity_id)
    else:
        idp_settings = Saml2Parser.parse(idp_metadata, entity_id=idp_entity_id)
    return idp_settings
