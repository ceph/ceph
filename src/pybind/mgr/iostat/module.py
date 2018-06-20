
from mgr_module import MgrModule


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "iostat",
            "desc": "Get IO rates",
            "perm": "r",
            "poll": "true"
        },
        {
            "cmd": "iostat self-test",
            "desc": "Run a self test the iostat module",
            "perm": "r"
        }
    ]


    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)


    def handle_command(self, inbuf, command):
        rd = 0
        wr = 0
        total = 0
        rd_ops = 0
        wr_ops = 0
        total_ops = 0
        ret = ''

        if command['prefix'] == 'iostat':
            r = self.get('io_rate')

            stamp_delta = float(r['pg_stats_delta']['stamp_delta'])
            if (stamp_delta > 0):
                rd = int(r['pg_stats_delta']['stat_sum']['num_read_kb']) / stamp_delta
                wr = int(r['pg_stats_delta']['stat_sum']['num_write_kb']) / stamp_delta
                # The values are in kB, but to_pretty_iec() requires them to be in bytes
                rd = int(rd) << 10
                wr = int(wr) << 10
                total = rd + wr

                rd_ops = int(r['pg_stats_delta']['stat_sum']['num_read']) / stamp_delta
                wr_ops = int(r['pg_stats_delta']['stat_sum']['num_write']) / stamp_delta
                total_ops = rd_ops + wr_ops

            if 'width' in command:
                width = command['width']
            else:
                width = 80

            if command.get('print_header', False):
                elems = ['Read', 'Write', 'Total', 'Read IOPS', 'Write IOPS', 'Total IOPS']
                ret += self.get_pretty_header(elems, width)

            elems = [
                self.to_pretty_iec(rd) + 'B/s',
                self.to_pretty_iec(wr) + 'B/s',
                self.to_pretty_iec(total) + 'B/s',
                int(rd_ops),
                int(wr_ops),
                int(total_ops)
            ]
            ret += self.get_pretty_row(elems, width)

        elif command['prefix'] == 'iostat self-test':
            r = self.get('io_rate')
            assert('pg_stats_delta' in r)
            assert('stamp_delta' in r['pg_stats_delta'])
            assert('stat_sum' in r['pg_stats_delta'])
            assert('num_read_kb' in r['pg_stats_delta']['stat_sum'])
            assert('num_write_kb' in r['pg_stats_delta']['stat_sum'])
            assert('num_write' in r['pg_stats_delta']['stat_sum'])
            assert('num_read' in r['pg_stats_delta']['stat_sum'])
            ret = 'iostat self-test OK'

        return 0, '', ret
