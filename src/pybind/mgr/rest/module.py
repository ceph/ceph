
"""
A RESTful API for Ceph
"""

# We must share a global reference to this instance, because it is the
# gatekeeper to all accesses to data from the C++ side (e.g. the REST API
# request handlers need to see it)
_global_instance = {'plugin': None}
def global_instance():
    assert _global_instance['plugin'] is not None
    return _global_instance['plugin']


import os
import logging
import logging.config
import json
import uuid
import errno
import sys

import cherrypy
from django.core.servers.basehttp import get_internal_wsgi_application

from mgr_module import MgrModule

from rest.app.manager.request_collection import RequestCollection
from rest.app.types import OsdMap, NotFound, Config, FsMap, MonMap, \
    PgSummary, Health, MonStatus

from logger import logger

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "rest.app.settings")

django_log = logging.getLogger("django.request")
django_log.addHandler(logging.StreamHandler())
django_log.setLevel(logging.DEBUG)


def recurse_refs(root, path):
    if isinstance(root, dict):
        for k, v in root.items():
            recurse_refs(v, path + "->%s" % k)
    elif isinstance(root, list):
        for n, i in enumerate(root):
            recurse_refs(i, path + "[%d]" % n)

    logger().info("%s %d (%s)" % (path, sys.getrefcount(root), root.__class__))


class Module(MgrModule):
    COMMANDS = [
            {
                "cmd": "enable_auth "
                       "name=val,type=CephChoices,strings=true|false",
                "desc": "Set whether to authenticate API access by key",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_create "
                       "name=key_name,type=CephString",
                "desc": "Create an API key with this name",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_delete "
                       "name=key_name,type=CephString",
                "desc": "Delete an API key with this name",
                "perm": "rw"
            },
            {
                "cmd": "auth_key_list",
                "desc": "List all API keys",
                "perm": "rw"
            },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        _global_instance['plugin'] = self
        self.log.info("Constructing module {0}: instance {1}".format(
            __name__, _global_instance))
        self.requests = RequestCollection()

        self.keys = {}
        self.enable_auth = True

    def notify(self, notify_type, notify_id):
        # FIXME: don't bother going and get_sync_object'ing the map
        # unless there is actually someone waiting for it (find out inside
        # requests.on_map)
        self.log.info("Notify {0}".format(notify_type))
        if notify_type == "command":
            self.requests.on_completion(notify_id)
        elif notify_type == "osd_map":
            self.requests.on_map(OsdMap, self.get_sync_object(OsdMap))
        elif notify_type == "mon_map":
            self.requests.on_map(MonMap, self.get_sync_object(MonMap))
        elif notify_type == "pg_summary":
            self.requests.on_map(PgSummary, self.get_sync_object(PgSummary))
        else:
            self.log.warning("Unhandled notification type '{0}'".format(notify_type))

    def get_sync_object(self, object_type, path=None):
        if object_type == OsdMap:
            data = self.get("osd_map")

            assert data is not None

            data['tree'] = self.get("osd_map_tree")
            data['crush'] = self.get("osd_map_crush")
            data['crush_map_text'] = self.get("osd_map_crush_map_text")
            data['osd_metadata'] = self.get("osd_metadata")
            obj = OsdMap(data['epoch'], data)
        elif object_type == Config:
            data = self.get("config")
            obj = Config(0, data)
        elif object_type == MonMap:
            data = self.get("mon_map")
            obj = MonMap(data['epoch'], data)
        elif object_type == FsMap:
            data = self.get("fs_map")
            obj = FsMap(data['epoch'], data)
        elif object_type == PgSummary:
            data = self.get("pg_summary")
            self.log.debug("JSON: {0}".format(data))
            obj = PgSummary(0, data)
        elif object_type == Health:
            data = self.get("health")
            obj = Health(0, json.loads(data['json']))
        elif object_type == MonStatus:
            data = self.get("mon_status")
            obj = MonStatus(0, json.loads(data['json']))
        else:
            raise NotImplementedError(object_type)

        # TODO: move 'path' handling up into C++ land so that we only
        # Pythonize the part we're interested in
        if path:
            try:
                for part in path:
                    if isinstance(obj, dict):
                        obj = obj[part]
                    else:
                        obj = getattr(obj, part)
            except (AttributeError, KeyError) as e:
                raise NotFound(object_type, path)

        return obj

    def get_authenticators(self):
        """
        For the benefit of django rest_framework APIView classes
        """
        return [self._auth_cls()]

    def shutdown(self):
        cherrypy.engine.stop()

    def serve(self):
        self.keys = self._load_keys()
        self.enable_auth = self.get_config_json("enable_auth")
        if self.enable_auth is None:
            self.enable_auth = True

        app = get_internal_wsgi_application()

        from rest_framework import authentication

        class KeyUser(object):
            def __init__(self, username):
                self.username = username

            def is_authenticated(self):
                return True

        # Take a local reference to use inside the APIKeyAuthentication
        # class definition
        log = self.log

        # Configure django.request logger
        logging.getLogger("django.request").handlers = self.log.handlers
        logging.getLogger("django.request").setLevel(logging.DEBUG)

        class APIKeyAuthentication(authentication.BaseAuthentication):
            def authenticate(self, request):
                if not global_instance().enable_auth:
                    return KeyUser("anonymous"), None

                username = request.META.get('HTTP_X_USERNAME')
                if not username:
                    log.warning("Rejecting: no X_USERNAME")
                    return None

                if username not in global_instance().keys:
                    log.warning("Rejecting: username does not exist")
                    return None

                api_key = request.META.get('HTTP_X_APIKEY')
                expect_key = global_instance().keys[username]
                if api_key != expect_key:
                    log.warning("Rejecting: wrong API key")
                    return None

                log.debug("Accepted for user {0}".format(username))
                return KeyUser(username), None

        self._auth_cls = APIKeyAuthentication

        cherrypy.config.update({
            'server.socket_port': 8002,
            'engine.autoreload.on': False
        })
        cherrypy.tree.graft(app, '/')

        cherrypy.engine.start()
        cherrypy.engine.block()

    def _generate_key(self):
        return uuid.uuid4().__str__()

    def _load_keys(self):
        loaded_keys = self.get_config_json("keys")
        self.log.debug("loaded_keys: {0}".format(loaded_keys))
        if loaded_keys is None:
            return {}
        else:
            return loaded_keys

    def _save_keys(self):
        self.set_config_json("keys", self.keys)

    def handle_command(self, cmd):
        self.log.info("handle_command: {0}".format(json.dumps(cmd, indent=2)))
        prefix = cmd['prefix']
        if prefix == "enable_auth":
            enable = cmd['val'] == "true"
            self.set_config_json("enable_auth", enable)
            self.enable_auth = enable
            return 0, "", ""
        elif prefix == "auth_key_create":
            if cmd['key_name'] in self.keys:
                return 0, self.keys[cmd['key_name']], ""
            else:
                self.keys[cmd['key_name']] = self._generate_key()
                self._save_keys()

            return 0, self.keys[cmd['key_name']], ""
        elif prefix == "auth_key_delete":
            if cmd['key_name'] in self.keys:
                del self.keys[cmd['key_name']]
                self._save_keys()

            return 0, "", ""
        elif prefix == "auth_key_list":
            return 0, json.dumps(self._load_keys(), indent=2), ""
        else:
            return -errno.EINVAL, "", "Command not found '{0}'".format(prefix)
