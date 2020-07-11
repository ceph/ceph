# -*- coding: utf-8 -*-
from __future__ import absolute_import

from cherrypy import NotFound

from . import ApiController, ControllerDoc, RESTController, Endpoint, ReadPermission, \
    UiApiController
from ..security import Scope
from ..services.ceph_service import CephService
from .. import mgr


@ApiController('/crush_rule', Scope.POOL)
class CrushRule(RESTController):
    def list(self):
        return mgr.get('osd_map_crush')['rules']

    def get(self, name):
        rules = mgr.get('osd_map_crush')['rules']
        for r in rules:
            if r['rule_name'] == name:
                return r
        raise NotFound('No such crush rule')

    def create(self, name, root, failure_domain, device_class=None):
        rule = {
            'name': name,
            'root': root,
            'type': failure_domain,
            'class': device_class
        }
        CephService.send_command('mon', 'osd crush rule create-replicated', **rule)

    def delete(self, name):
        CephService.send_command('mon', 'osd crush rule rm', name=name)


@UiApiController('/crush_rule', Scope.POOL)
@ControllerDoc("Dashboard UI helper function; not part of the public API", "CrushRuleUi")
class CrushRuleUi(CrushRule):
    @Endpoint()
    @ReadPermission
    def info(self):
        '''Used for crush rule creation modal'''
        return {
            'names': [r['rule_name'] for r in mgr.get('osd_map_crush')['rules']],
            'nodes': mgr.get('osd_map_tree')['nodes']
        }
