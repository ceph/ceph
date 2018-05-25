from __future__ import absolute_import


class NodeInfo(object):
    """ Neo4j Node information """
    def __init__(self, label, domain_id, name, meta={}):
        self.label = label
        self.domain_id = domain_id
        self.name = name
        self.meta = meta


class CypherOP(object):
    """ Cypher Operation """

    @staticmethod
    def update(node, timestamp, name, value):
        result = ""
        if isinstance(node, NodeInfo):
            cy_value = value
            if name != "time":
                cy_value = "\'%s\'" % value
            result = "set %s.%s=case when %s.time >= %s then %s.%s ELSE %s end" % (
                node.label, name,
                node.label, timestamp,
                node.label, name,
                cy_value)
        return result

    @staticmethod
    def create_or_merge(node, timestamp):
        result = ""
        if isinstance(node, NodeInfo):
            meta_list = []
            if isinstance(node.meta, dict):
                for key, value in node.meta.items():
                    meta_list.append(CypherOP.update(node, timestamp, key, value))
            domain_id = "{domainId:\'%s\'}" % node.domain_id
            if meta_list:
                result = "merge (%s:%s %s) %s %s %s" % (
                    node.label, node.label,
                    domain_id,
                    CypherOP.update(node, timestamp, 'name', node.name),
                    " ".join(meta_list),
                    CypherOP.update(node, timestamp, 'time', timestamp))
            else:
                result = "merge (%s:%s %s) %s %s" % (
                    node.label, node.label,
                    domain_id,
                    CypherOP.update(node, timestamp, 'name', node.name),
                    CypherOP.update(node, timestamp, 'time', timestamp))
        return result

    @staticmethod
    def add_link(snode, dnode, relationship, timestamp):
        result = ""
        if isinstance(snode, NodeInfo) and isinstance(dnode, NodeInfo):
            cy_snode = CypherOP.create_or_merge(snode, timestamp)
            cy_dnode = CypherOP.create_or_merge(dnode, timestamp)
            target = snode.label + dnode.label
            link = "merge (%s)-[%s:%s]->(%s) set %s.time=case when %s.time >= %s then %s.time ELSE %s end" % (
                snode.label, target, relationship,
                dnode.label, target,
                target, timestamp,
                target, timestamp)
            result = "%s %s %s" % (cy_snode, cy_dnode, link)
        return result
