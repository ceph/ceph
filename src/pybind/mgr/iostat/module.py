from typing import Any

from mgr_module import CLIReadCommand, HandleCommandResult, MgrModule


class Module(MgrModule):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def self_test(self) -> None:
        r = self.get('io_rate')
        assert 'pg_stats_delta' in r
        assert 'stamp_delta' in r['pg_stats_delta']
        assert 'stat_sum' in r['pg_stats_delta']
        assert 'num_read_kb' in r['pg_stats_delta']['stat_sum']
        assert 'num_write_kb' in r['pg_stats_delta']['stat_sum']
        assert 'num_write' in r['pg_stats_delta']['stat_sum']
        assert 'num_read' in r['pg_stats_delta']['stat_sum']

    @CLIReadCommand('iostat', poll=True)
    def iostat(self, width: int = 80, print_header: bool = False) -> HandleCommandResult:
        """
        Get IO rates
        """
        rd = 0
        wr = 0
        total = 0
        rd_ops = 0
        wr_ops = 0
        total_ops = 0
        ret = ''

        r = self.get('io_rate')

        stamp_delta = int(float(r['pg_stats_delta']['stamp_delta']))
        if stamp_delta > 0:
            rd = r['pg_stats_delta']['stat_sum']['num_read_kb'] // stamp_delta
            wr = r['pg_stats_delta']['stat_sum']['num_write_kb'] // stamp_delta
            # The values are in kB, but to_pretty_iec() requires them to be in bytes
            rd = rd << 10
            wr = wr << 10
            total = rd + wr

            rd_ops = r['pg_stats_delta']['stat_sum']['num_read'] // stamp_delta
            wr_ops = r['pg_stats_delta']['stat_sum']['num_write'] // stamp_delta
            total_ops = rd_ops + wr_ops

        if print_header:
            elems = ['Read', 'Write', 'Total', 'Read IOPS', 'Write IOPS', 'Total IOPS']
            ret += self.get_pretty_header(elems, width)

        elems = [
            self.to_pretty_iec(rd) + 'B/s',
            self.to_pretty_iec(wr) + 'B/s',
            self.to_pretty_iec(total) + 'B/s',
            str(rd_ops),
            str(wr_ops),
            str(total_ops)
        ]
        ret += self.get_pretty_row(elems, width)

        return HandleCommandResult(stdout=ret)
