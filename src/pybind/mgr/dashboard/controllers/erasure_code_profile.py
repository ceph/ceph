# -*- coding: utf-8 -*-

from cherrypy import NotFound

from .. import mgr
from ..security import Scope
from ..services.ceph_service import CephService
from . import APIDoc, APIRouter, Endpoint, EndpointDoc, ReadPermission, RESTController, UIRouter

LIST_CODE__SCHEMA = {
    "crush-failure-domain": (str, ''),
    "k": (int, 'Number of data chunks'),
    "m": (int, 'Number of coding chunks'),
    "plugin": (str, 'Plugin Info'),
    "technique": (str, ''),
    "name": (str, 'Name of the profile')
}


@APIRouter('/erasure_code_profile', Scope.POOL)
@APIDoc("Erasure Code Profile Management API", "ErasureCodeProfile")
class ErasureCodeProfile(RESTController):
    """
    create() supports additional key-value arguments that are passed to the
    ECP plugin.
    """
    @EndpointDoc("List Erasure Code Profile Information",
                 responses={'200': [LIST_CODE__SCHEMA]})
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


@UIRouter('/erasure_code_profile', Scope.POOL)
@APIDoc("Dashboard UI helper function; not part of the public API", "ErasureCodeProfileUi")
class ErasureCodeProfileUi(ErasureCodeProfile):
    @Endpoint()
    @ReadPermission
    def info(self):
        """
        Used for profile creation and editing
        """
        config = mgr.get('config')
        return {
            # Because 'shec' and 'clay' are experimental they're not included
            'plugins': config['osd_erasure_code_plugins'].split() + ['shec', 'clay'],
            'directory': config['erasure_code_dir'],
            'nodes': mgr.get('osd_map_tree')['nodes'],
            'names': [name for name, _ in
                      mgr.get('osd_map').get('erasure_code_profiles', {}).items()]
        }
