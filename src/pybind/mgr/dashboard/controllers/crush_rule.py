# -*- coding: utf-8 -*-

from cherrypy import NotFound

from .. import mgr
from ..security import Scope
from ..services.ceph_service import CephService
from . import ApiController, ControllerDoc, Endpoint, EndpointDoc, \
    ReadPermission, RESTController, UiApiController

LIST_SCHEMA = {
    "rule_id": (int, 'Rule ID'),
    "rule_name": (str, 'Rule Name'),
    "ruleset": (int, 'RuleSet related to the rule'),
    "type": (int, 'Type of Rule'),
    "min_size": (int, 'Minimum size of Rule'),
    "max_size": (int, 'Maximum size of Rule'),
    'steps': ([{str}], 'Steps included in the rule')
}


@ApiController('/crush_rule', Scope.POOL)
@ControllerDoc("Crush Rule Management API", "CrushRule")
class CrushRule(RESTController):
    @EndpointDoc("List Crush Rule Configuration",
                 responses={200: LIST_SCHEMA})
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
        osd_map = mgr.get_osdmap()
        crush = osd_map.get_crush()
        crush.dump()
        return {
            'names': [r['rule_name'] for r in mgr.get('osd_map_crush')['rules']],
            'nodes': mgr.get('osd_map_tree')['nodes'],
            'roots': crush.find_roots()
        }
