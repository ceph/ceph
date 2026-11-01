"""
Check the out-of-band transports of the OSDs (S3 over RDMA): that they are
up before a workload, and that nothing went wrong during it.
"""
import contextlib
import json
import logging
import os
import shlex
import time

from io import StringIO

from teuthology import misc as teuthology
from teuthology.exceptions import ConfigError

log = logging.getLogger(__name__)

# the counters of "<executor> status" that must stay 0
ZERO = ['gather_crc_mismatch', 'cutoffs_failed']

# OSD log lines that fail the check: a gather window that did not hold
# what its shard pushed, and an executor that stopped because it could not
# cut off its writes in time
LOG_PATTERNS = [
    'do not match its checksum',
    'libfabric delivery is unsafe',
]

# health checks that fail the check
HEALTH_CHECKS = ['OOB_DELIVERY_UNSAFE']


def parse_status(text):
    """ The JSON of an admin socket command, or None """
    try:
        return json.loads(text) if text and text.strip() else None
    except ValueError:
        return None


def transport_up(status, transport=None):
    """
    Whether the status of an OSD's executor says that its transport is up,
    and, when transport is given, names it
    """
    if not status or not status.get('available'):
        return False
    return not transport or transport in status.get('transport', '')


def status_failures(statuses, zero):
    """
    What is wrong in the executor statuses of the OSDs, a dict of each OSD's
    id to its status, or to None for an OSD that did not answer
    """
    failures = []
    for osd in sorted(statuses):
        status = statuses[osd]
        if status is None:
            continue
        if status.get('unsafe'):
            failures.append('osd.%s is unsafe: %s' %
                            (osd, status.get('last_error', '')))
        for counter in zero:
            value = status.get(counter, 0)
            if value:
                failures.append('osd.%s: %s is %s' % (osd, counter, value))
    return failures


def health_failures(health, checks):
    """ The health checks among checks that the health report raises """
    raised = health.get('checks', {})
    return ['health check %s: %s' %
            (check, raised[check].get('summary', {}).get('message', ''))
            for check in checks if check in raised]


# the OSD logs of a host
OSD_LOGS = '/var/log/ceph/{cluster}-osd.*.log'


def log_grep_cmd(logs, patterns, limit=20):
    """
    The command that prints the first lines of the logs, a shell glob,
    that hold any of patterns
    """
    expr = ' '.join('-e %s' % shlex.quote(p) for p in patterns)
    return ['sudo', 'sh', '-c',
            'grep -h -F %s %s 2>/dev/null | head -n %d' % (expr, logs, limit)]


class OOBCheck(object):

    def __init__(self, ctx, config):
        self.ctx = ctx
        self.config = config
        self.cluster = config.get('cluster', 'ceph')
        self.manager = ctx.managers[self.cluster]
        self.executor = config.get('executor', 'ofi')
        self.transport = config.get('transport')
        self.timeout = config.get('timeout', 300)
        self.zero = config.get('zero', ZERO)
        self.patterns = config.get('log_patterns', LOG_PATTERNS)
        self.health_checks = config.get('health_checks', HEALTH_CHECKS)
        self.osd_logs = config.get('osd_logs', OSD_LOGS).format(
            cluster=self.cluster)

    def asok(self, osd, command):
        proc = self.manager.osd_admin_socket(osd, command, check_status=False,
                                             timeout=60, stdout=StringIO())
        if proc.exitstatus != 0:
            return None
        return proc.stdout.getvalue()

    def status(self, osd):
        return parse_status(self.asok(osd, [self.executor, 'status']))

    def up_osds(self):
        """ The OSDs that the OSD map has up """
        return sorted(o['osd'] for o in self.manager.get_osd_dump()
                      if o.get('up'))

    def wait_for_transports(self):
        """ Wait until every OSD that is up reports its transport up """
        pending = set(self.up_osds())
        log.info('waiting for the %s transport of osds %s',
                 self.executor, sorted(pending))
        deadline = time.time() + self.timeout
        last = {}
        while pending:
            for osd in sorted(pending):
                last[osd] = self.status(osd)
                if transport_up(last[osd], self.transport):
                    log.info('osd.%s: %s', osd, last[osd].get('transport'))
                    pending.discard(osd)
            if not pending:
                break
            if time.time() > deadline:
                raise RuntimeError(
                    'the %s transport of %s is not up after %s s: %s' %
                    (self.executor,
                     ', '.join('osd.%s' % o for o in sorted(pending)),
                     self.timeout,
                     {o: last.get(o) for o in sorted(pending)}))
            time.sleep(5)

    def archive(self):
        """
        Save each OSD's executor status and out-of-band counters in the
        job's archive, and return the statuses
        """
        statuses = {}
        path = None
        if getattr(self.ctx, 'archive', None):
            path = os.path.join(self.ctx.archive, 'oob_check')
            os.makedirs(path, exist_ok=True)
        for osd in self.up_osds():
            record = {}
            for name, command in [
                    ('status', [self.executor, 'status']),
                    ('perf_oob_xfer', ['perf', 'dump', 'oob_xfer']),
                    ('transfers', ['dump_oob_transfers'])]:
                record[name] = parse_status(self.asok(osd, command))
            statuses[osd] = record['status']
            if path:
                with open(os.path.join(path, 'osd.%s.json' % osd), 'w') as f:
                    json.dump(record, f, indent=2, sort_keys=True)
        return statuses

    def log_failures(self):
        failures = []
        osds = self.ctx.cluster.only(teuthology.is_type('osd', self.cluster))
        for remote in osds.remotes.keys():
            out = remote.sh(log_grep_cmd(self.osd_logs, self.patterns),
                            check_status=False)
            failures.extend('%s: %s' % (remote.hostname, line)
                            for line in out.splitlines() if line.strip())
        return failures

    def check(self):
        statuses = self.archive()
        failures = status_failures(statuses, self.zero)
        failures += health_failures(self.manager.get_mon_health(detail=True),
                                    self.health_checks)
        failures += self.log_failures()
        return failures


@contextlib.contextmanager
def task(ctx, config):
    """
    Wait until the out-of-band transport of every OSD is up: its "ofi
    status" admin socket command answers, and says available. At the end,
    check that no OSD is unsafe, that no OSD's gather_crc_mismatch and
    cutoffs_failed are above 0, that the cluster raises no
    OOB_DELIVERY_UNSAFE, and that no OSD log has a gather window whose data
    did not match its checksum. The counters of an OSD go back to 0 when
    it restarts, as when a thrasher kills it, so the OSD logs cover those.
    Save each OSD's "ofi status", "perf dump oob_xfer" and
    "dump_oob_transfers" in the archive, in oob_check/osd.N.json.

        tasks:
        - ceph:
        - rgw: [client.0]
        - oob_check:
            # the executor whose "<executor> status" to read (default ofi)
            executor: ofi
            # a word that each OSD's transport must name, such as
            # "provider tcp" (default: any)
            transport: verbs
            # how long to wait for the transports (default 300 s)
            timeout: 300
            # the counters that must be 0 (default
            # [gather_crc_mismatch, cutoffs_failed])
            zero: [gather_crc_mismatch, cutoffs_failed]
            # the OSD logs of each host, a shell glob (default
            # /var/log/ceph/{cluster}-osd.*.log)
            osd_logs: /var/log/ceph/ceph-osd.*.log
        - thrashosds:
        - workunit: ...

    Put it before a thrasher, so that it checks after the thrasher has
    brought every OSD back up.
    """
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise ConfigError('oob_check takes a dict')
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('oob_check', {}))
    check = OOBCheck(ctx, config)
    check.wait_for_transports()
    try:
        yield
    except BaseException:
        try:
            check.archive()
        except Exception:
            log.exception('saving the out-of-band counters')
        raise
    failures = check.check()
    for failure in failures:
        log.error('oob_check: %s', failure)
    if failures:
        raise RuntimeError('oob_check: %s' % '; '.join(failures[:10]))
    log.info('oob_check: no out-of-band failure')
