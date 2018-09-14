from __future__ import absolute_import

import time


class NodeInfo(object):
    """ Neo4j Node information """
    def __init__(self, label, domain_id, name, meta):
        self.label = label
        self.domain_id = domain_id
        self.name = name
        self.meta = meta


class CypherOP(object):
    """ Cypher Operation """

    @staticmethod
    def update(node, key, value, timestamp=int(time.time()*(1000**3))):
        result = ''
        if isinstance(node, NodeInfo):
            if key != 'time':
                cy_value = '\'%s\'' % value
            else:
                cy_value = value
            result = \
                'set %s.%s=case when %s.time >= %s then %s.%s ELSE %s end' % (
                    node.label, key, node.label, timestamp, node.label, key,
                    cy_value)
        return result

    @staticmethod
    def create_or_merge(node, timestamp=int(time.time()*(1000**3))):
        result = ''
        if isinstance(node, NodeInfo):
            meta_list = []
            if isinstance(node.meta, dict):
                for key, value in node.meta.items():
                    meta_list.append(CypherOP.update(node, key, value, timestamp))
            domain_id = '{domainId:\'%s\'}' % node.domain_id
            if meta_list:
                result = 'merge (%s:%s %s) %s %s %s' % (
                    node.label, node.label,
                    domain_id,
                    CypherOP.update(node, 'name', node.name, timestamp),
                    ' '.join(meta_list),
                    CypherOP.update(node, 'time', timestamp, timestamp))
            else:
                result = 'merge (%s:%s %s) %s %s' % (
                    node.label, node.label,
                    domain_id,
                    CypherOP.update(node, 'name', node.name, timestamp),
                    CypherOP.update(node, 'time', timestamp, timestamp))
        return result

    @staticmethod
    def add_link(snode, dnode, relationship, timestamp=None):
        result = ''
        if timestamp is None:
            timestamp = int(time.time()*(1000**3))
        if isinstance(snode, NodeInfo) and isinstance(dnode, NodeInfo):
            cy_snode = CypherOP.create_or_merge(snode, timestamp)
            cy_dnode = CypherOP.create_or_merge(dnode, timestamp)
            target = snode.label + dnode.label
            link = 'merge (%s)-[%s:%s]->(%s) set %s.time=case when %s.time >= %s then %s.time ELSE %s end' % (
                snode.label, target, relationship,
                dnode.label, target,
                target, timestamp,
                target, timestamp)
            result = '%s %s %s' % (cy_snode, cy_dnode, link)
        return result
