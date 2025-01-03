
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from cherrypy import NotFound

from .. import mgr
from ..security import Scope
from ..services.ceph_service import CephService
from . import APIDoc, APIRouter, Endpoint, EndpointDoc, ReadPermission, RESTController, UIRouter
from ._version import APIVersion

LIST_SCHEMA = {
    "rule_id": (int, 'Rule ID'),
    "rule_name": (str, 'Rule Name'),
    "ruleset": (int, 'RuleSet related to the rule'),
    "type": (int, 'Type of Rule'),
    "min_size": (int, 'Minimum size of Rule'),
    "max_size": (int, 'Maximum size of Rule'),
    'steps': ([{str}], 'Steps included in the rule')
}


@APIRouter('/crush_rule', Scope.POOL)
@APIDoc("Crush Rule Management API", "CrushRule")
class CrushRule(RESTController):
    @EndpointDoc("List Crush Rule Configuration",
                 responses={200: LIST_SCHEMA})
    @RESTController.MethodMap(version=APIVersion(2, 0))
    def list(self):
        return mgr.get('osd_map_crush')['rules']

    @RESTController.MethodMap(version=APIVersion(2, 0))
    def get(self, name):
        rules = mgr.get('osd_map_crush')['rules']
        for r in rules:
            if r['rule_name'] == name:
                return r
        raise NotFound('No such crush rule')

    def create(self, name, failure_domain, device_class=None, root=None, profile=None,
               pool_type='replication'):
        if pool_type == 'erasure':
            rule = {
                'name': name,
                'profile': profile,
                'type': failure_domain,
                'class': device_class
            }
            CephService.send_command('mon', 'osd crush rule create-erasure', **rule)
        else:
            rule = {
                'name': name,
                'root': root,
                'type': failure_domain,
                'class': device_class
            }
            CephService.send_command('mon', 'osd crush rule create-replicated', **rule)

    def delete(self, name):
        CephService.send_command('mon', 'osd crush rule rm', name=name)


@UIRouter('/crush_rule', Scope.POOL)
@APIDoc("Dashboard UI helper function; not part of the public API", "CrushRuleUi")
class CrushRuleUi(CrushRule):
    @Endpoint()
    @ReadPermission
    def info(self):
        '''Used for crush rule creation modal'''
        osd_map = mgr.get_osdmap()
        crush = osd_map.get_crush()
        crush.dump()
        return {
            'names': [r['rule_name'] for r in mgr.get('osd_map_crush')['rules']],
            'nodes': mgr.get('osd_map_tree')['nodes'],
            'roots': crush.find_roots()
        }
