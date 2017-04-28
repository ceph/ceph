from pecan import expose
from pecan.rest import RestController

from restful import common, module
from collections import defaultdict

from restful.decorators import auth


class CrushRule(RestController):
    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show crush rules
        """
        rules = module.instance.get('osd_map_crush')['rules']
        nodes = module.instance.get('osd_map_tree')['nodes']

        for rule in rules:
            rule['osd_count'] = len(common.crush_rule_osds(nodes, rule))

        return rules

class Crush(RestController):
    rule = CrushRule()
