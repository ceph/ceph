
from mgr_module import MgrModule


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "mgr iostat",
            "desc": "Get IO rates",
            "perm": "r"
        }
    ]


    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)


    def handle_command(self, command):
        rd = 0
        wr = 0
        ops = 0

        if command['prefix'] == 'mgr iostat':
            r = self.get('io_rate')

            stamp_delta = float(r['pg_stats_delta']['stamp_delta'])
            if (stamp_delta > 0):
                rd = int(r['pg_stats_delta']['stat_sum']['num_read_kb']) / stamp_delta
                wr = int(r['pg_stats_delta']['stat_sum']['num_write_kb']) / stamp_delta
                ops = ( int(r['pg_stats_delta']['stat_sum']['num_write']) + int(r['pg_stats_delta']['stat_sum']['num_read']) ) / stamp_delta

        ret = "wr: {0} kB/s, rd: {1} kB/s, iops: {2}".format(str(int(wr)), str(int(rd)), str(int(ops)))

        return 0, '', ret
