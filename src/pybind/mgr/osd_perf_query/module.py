
"""
osd_perf_query module
"""

from itertools import groupby
from time import time
import errno
import prettytable

from mgr_module import MgrModule

def get_human_readable(bytes, precision=2):
    suffixes = ['', 'Ki', 'Mi', 'Gi', 'Ti']
    suffix_index = 0
    while bytes > 1024 and suffix_index < 4:
        # increment the index of the suffix
        suffix_index += 1
        # apply the division
        bytes = bytes / 1024.0
    return '%.*f%s' % (precision, bytes, suffixes[suffix_index])

class OSDPerfQuery(MgrModule):
    COMMANDS = [
        {
            "cmd": "osd perf query add "
                   "name=query,type=CephChoices,"
                   "strings=client_id|rbd_image_id|all_subkeys",
            "desc": "add osd perf query",
            "perm": "w"
        },
        {
            "cmd": "osd perf query remove "
                   "name=query_id,type=CephInt,req=true",
            "desc": "remove osd perf query",
            "perm": "w"
        },
        {
            "cmd": "osd perf counters get "
                   "name=query_id,type=CephInt,req=true",
            "desc": "fetch osd perf counters",
            "perm": "w"
        },
    ]

    CLIENT_ID_QUERY = {
        'key_descriptor': [
            {'type': 'client_id', 'regex': '^(.+)$'},
        ],
        'performance_counter_descriptors': [
            'bytes', 'write_ops', 'read_ops', 'write_bytes', 'read_bytes',
            'write_latency', 'read_latency',
        ],
        'limit': {'order_by': 'bytes', 'max_count': 10},
    }

    RBD_IMAGE_ID_QUERY = {
        'key_descriptor': [
            {'type': 'pool_id', 'regex': '^(.+)$'},
            {'type': 'object_name',
             'regex': '^(?:rbd|journal)_data\.(?:([0-9]+)\.)?([^.]+)\.'},
        ],
        'performance_counter_descriptors': [
            'bytes', 'write_ops', 'read_ops', 'write_bytes', 'read_bytes',
            'write_latency', 'read_latency',
        ],
        'limit': {'order_by': 'bytes', 'max_count': 10},
    }

    ALL_SUBKEYS_QUERY = {
        'key_descriptor': [
            {'type': 'client_id', 'regex': '^(.*)$'},
            {'type': 'client_address', 'regex': '^(.*)$'},
            {'type': 'pool_id', 'regex': '^(.*)$'},
            {'type': 'namespace', 'regex': '^(.*)$'},
            {'type': 'osd_id', 'regex': '^(.*)$'},
            {'type': 'pg_id', 'regex': '^(.*)$'},
            {'type': 'object_name', 'regex': '^(.*)$'},
            {'type': 'snap_id', 'regex': '^(.*)$'},
        ],
        'performance_counter_descriptors': [
            'write_ops', 'read_ops',
        ],
    }

    queries = {}

    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == "osd perf query add":
            if cmd['query'] == 'rbd_image_id':
                query = self.RBD_IMAGE_ID_QUERY
            elif cmd['query'] == 'client_id':
                query = self.CLIENT_ID_QUERY
            else:
                query = self.ALL_SUBKEYS_QUERY
            query_id = self.add_osd_perf_query(query)
            if query_id is None:
                return -errno.EINVAL, "", "Invalid query"
            self.queries[query_id] = [query, time()]
            return 0, str(query_id), "added query " + cmd['query'] + " with id " + str(query_id)
        elif cmd['prefix'] == "osd perf query remove":
            if cmd['query_id'] not in self.queries:
                return -errno.ENOENT, "", "unknown query id " + str(cmd['query_id'])
            self.remove_osd_perf_query(cmd['query_id'])
            del self.queries[cmd['query_id']]
            return 0, "", "removed query with id " + str(cmd['query_id'])
        elif cmd['prefix'] == "osd perf counters get":
            if cmd['query_id'] not in self.queries:
                return -errno.ENOENT, "", "unknown query id " + str(cmd['query_id'])

            query = self.queries[cmd['query_id']][0]
            res = self.get_osd_perf_counters(cmd['query_id'])
            now = time()
            last_update = self.queries[cmd['query_id']][1]
            descriptors = query['performance_counter_descriptors']

            if query == self.RBD_IMAGE_ID_QUERY:
                column_names = ["pool_id", "rbd image_id"]
            else:
                column_names = [sk['type'] for sk in query['key_descriptor']]
            for d in descriptors:
                desc = d
                if d in ['bytes']:
                    continue
                elif d in ['write_bytes', 'read_bytes']:
                    desc += '/sec'
                elif d in ['write_latency', 'read_latency']:
                    desc += '(msec)'
                column_names.append(desc)

            table = prettytable.PrettyTable(tuple(column_names),
                                            hrules=prettytable.FRAME)

            if query == self.RBD_IMAGE_ID_QUERY:
                # typical output:
                #  {'k': [['3'], ['', '16fe5b5a8435e']],
                #   'c': [[1024, 0], [1, 0], ...]}
                # pool id fixup: if the object_name regex has matched pool id
                # use it as the image pool id
                for c in res['counters']:
                    if c['k'][1][0]:
                        c['k'][0][0] = c['k'][1][0]
                # group by (pool_id, image_id)
                processed = []
                res['counters'].sort(key=lambda c: [c['k'][0][0], c['k'][1][1]])
                for key, group in groupby(res['counters'],
                                          lambda c: [c['k'][0][0], c['k'][1][1]]):
                    counters = [[0, 0] for x in descriptors]
                    for c in group:
                        for i in range(len(counters)):
                            counters[i][0] += c['c'][i][0]
                            counters[i][1] += c['c'][i][1]
                    processed.append({'k' : key, 'c' : counters})
            else:
                # typical output:
                #  {'k': [['client.94348']], 'c': [[1024, 0], [1, 0], ...]}
                processed = res['counters']

            max_count = len(processed)
            if 'limit' in query:
                if 'max_count' in query['limit']:
                    max_count = query['limit']['max_count']
                if 'order_by' in query['limit']:
                    i = descriptors.index(query['limit']['order_by'])
                    processed.sort(key=lambda x: x['c'][i][0], reverse=True)
            for c in processed[:max_count]:
                if query == self.RBD_IMAGE_ID_QUERY:
                    row = c['k']
                else:
                    row = [sk[0] for sk in c['k']]
                counters = c['c']
                for i in range(len(descriptors)):
                    if descriptors[i] in ['bytes']:
                        continue
                    elif descriptors[i] in ['write_bytes', 'read_bytes']:
                        bps = counters[i][0] / (now - last_update)
                        row.append(get_human_readable(bps))
                    elif descriptors[i] in ['write_latency', 'read_latency']:
                        lat = 0
                        if counters[i][1] > 0:
                            lat = 1.0 * counters[i][0] / counters[i][1] / 1000000
                        row.append("%.2f" % lat)
                    else:
                        row.append("%d" % counters[i][0])
                table.add_row(row)

            msg = "counters for the query id %d for the last %d sec" % \
                (cmd['query_id'], now - last_update)
            self.queries[cmd['query_id']][1] = now

            return 0, table.get_string() + "\n", msg
        else:
            raise NotImplementedError(cmd['prefix'])

