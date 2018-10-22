from pecan import expose
from pecan.rest import RestController

from restful import common, context
from collections import defaultdict

from restful.decorators import auth


class CrushRule(RestController):
    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show crush rules
        """
        rules = context.instance.get('osd_map_crush')['rules']
        nodes = context.instance.get('osd_map_tree')['nodes']

        for rule in rules:
            rule['osd_count'] = len(common.crush_rule_osds(nodes, rule))

        return rules

class Crush(RestController):
    rule = CrushRule()
