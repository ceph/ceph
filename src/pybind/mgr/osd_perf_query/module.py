
"""
osd_perf_query module
"""

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
                   "name=query,type=CephChoices,strings=client_id|rbd_image_id",
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
            {'type': 'client_id', 'regex': '^.+$'},
        ],
        'performance_counter_descriptors': [
            'write_ops', 'read_ops', 'write_bytes', 'read_bytes',
            'write_latency', 'read_latency',
        ],
    }

    RBD_IMAGE_ID_QUERY = {
        'key_descriptor': [
            {'type': 'pool_id', 'regex': '^.+$'},
            {'type': 'object_name', 'regex': '^rbd_data\.([^.]+)\.'},
        ],
        'performance_counter_descriptors': [
            'write_ops', 'read_ops', 'write_bytes', 'read_bytes',
            'write_latency', 'read_latency',
        ],
    }

    queries = {}

    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == "osd perf query add":
            if cmd['query'] == 'rbd_image_id':
                query = self.RBD_IMAGE_ID_QUERY
            else:
                query = self.CLIENT_ID_QUERY
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
                column_names = ["client_id"]
            for d in descriptors:
                desc = d
                if d in ['write_bytes', 'read_bytes']:
                    desc += '/sec'
                elif d in ['write_latency', 'read_latency']:
                    desc += '(msec)'
                column_names.append(desc)

            table = prettytable.PrettyTable(tuple(column_names),
                                            hrules=prettytable.FRAME)
            for c in res['counters']:
                if query == self.RBD_IMAGE_ID_QUERY:
                    row = [c['k'][0][0], c['k'][1][1]]
                else:
                    row = [c['k'][0][0]]
                counters = c['c']
                for i in range(len(descriptors)):
                    if descriptors[i] in ['write_bytes', 'read_bytes']:
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

