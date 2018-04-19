# -*- coding: utf-8 -*-
from __future__ import absolute_import

from cherrypy import NotFound

from . import ApiController, AuthRequired, RESTController
from ..services.ceph_service import CephService
from .. import mgr


def _serialize_ecp(name, ecp):
    ecp['name'] = name
    ecp['k'] = int(ecp['k'])
    ecp['m'] = int(ecp['m'])
    return ecp


@ApiController('erasure_code_profile')
@AuthRequired()
class ErasureCodeProfile(RESTController):
    """
    create() supports additional key-value arguments that are passed to the
    ECP plugin.
    """

    def list(self):
        ret = []
        for name, ecp in mgr.get('osd_map').get('erasure_code_profiles', {}).items():
            ret.append(_serialize_ecp(name, ecp))
        return ret

    def get(self, name):
        try:
            ecp = mgr.get('osd_map')['erasure_code_profiles'][name]
            return _serialize_ecp(name, ecp)
        except KeyError:
            raise NotFound('No such erasure code profile')

    def create(self, name, k, m, plugin=None, ruleset_failure_domain=None, **kwargs):
        kwargs['k'] = k
        kwargs['m'] = m
        if plugin:
            kwargs['plugin'] = plugin
        if ruleset_failure_domain:
            kwargs['ruleset_failure_domain'] = ruleset_failure_domain

        profile = ['{}={}'.format(key, value) for key, value in kwargs.items()]
        CephService.send_command('mon', 'osd erasure-code-profile set', name=name,
                                 profile=profile)

    def delete(self, name):
        CephService.send_command('mon', 'osd erasure-code-profile rm', name=name)
