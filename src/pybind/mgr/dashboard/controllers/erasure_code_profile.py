# -*- coding: utf-8 -*-
from __future__ import absolute_import

from cherrypy import NotFound

from . import ApiController, ControllerDoc, RESTController, Endpoint, ReadPermission, \
    UiApiController
from ..security import Scope
from ..services.ceph_service import CephService
from .. import mgr


@ApiController('/erasure_code_profile', Scope.POOL)
class ErasureCodeProfile(RESTController):
    """
    create() supports additional key-value arguments that are passed to the
    ECP plugin.
    """

    def list(self):
        return CephService.get_erasure_code_profiles()

    def get(self, name):
        profiles = CephService.get_erasure_code_profiles()
        for p in profiles:
            if p['name'] == name:
                return p
        raise NotFound('No such erasure code profile')

    def create(self, name, **kwargs):
        profile = ['{}={}'.format(key, value) for key, value in kwargs.items()]
        CephService.send_command('mon', 'osd erasure-code-profile set', name=name,
                                 profile=profile)

    def delete(self, name):
        CephService.send_command('mon', 'osd erasure-code-profile rm', name=name)


@UiApiController('/erasure_code_profile', Scope.POOL)
@ControllerDoc("Dashboard UI helper function; not part of the public API", "ErasureCodeProfileUi")
class ErasureCodeProfileUi(ErasureCodeProfile):
    @Endpoint()
    @ReadPermission
    def info(self):
        """
        Used for profile creation and editing
        """
        config = mgr.get('config')
        return {
            # Because 'shec' is experimental it's not included
            'plugins': config['osd_erasure_code_plugins'].split() + ['shec'],
            'directory': config['erasure_code_dir'],
            'nodes': mgr.get('osd_map_tree')['nodes'],
            'names': [name for name, _ in
                      mgr.get('osd_map').get('erasure_code_profiles', {}).items()]
        }
