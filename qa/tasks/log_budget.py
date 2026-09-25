"""
Measure the debug-log volume of the daemons of a job and warn (or fail)
when the log that would be written at the lean debug level (10 by default)
exceeds a budget.

Every Ceph log line records the level of the dout() that produced it, so a
job run at debug_osd=20 tells us what the same job would have logged at
level 10.  At the end of the job, before the ceph task compresses the logs,
this task runs src/script/ceph_log_budget.py on every OSD log on every
remote, merges the per-OSD reports, stores them in the job archive as
log_budget.json, and checks the budgets:

  max_kept_bytes_per_op  level<=N bytes per client op (summed over all
                         OSDs / client ops dequeued by all primaries)
  max_kept_fraction      level<=N bytes / all bytes (only evaluated when the
                         logs were captured at level 20)
  max_kept_mb_per_s      level<=N MB per second per daemon

The warning names the top level<=N message templates and the source lines
they most likely come from, so a regression can be reported against the
component that caused it.  See doc/dev/osd_internals/debug_log_levels.rst.

The task must be listed AFTER the ceph task (so that it is torn down before
ceph compresses and archives the logs) and BEFORE the workload tasks (so
that the workload runs inside the measured interval)::

    tasks:
    - install:
    - ceph:
    - log_budget:
        mode: warn            # warn (default), fail or off
        level: 10
        max_kept_bytes_per_op: 20480
        max_kept_fraction: 0.2
        max_kept_mb_per_s: 12
    - radosbench:
        ...

Other options: daemon_types (default [osd]), cluster (default ceph),
whole_job (default false: only lines logged after this task started are
analysed), min_client_ops (default 1000), top (default 50),
sample_target_bytes (default 2GiB of log per daemon analysed for
templates; totals are always exact), offenders (default 5).
Options can also be set with 'overrides: log_budget: ...'.
"""
import contextlib
import importlib.util
import json
import logging
import os
from io import StringIO

from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)

TOOL = 'ceph_log_budget.py'
DEFAULTS = {
    'mode': 'warn',
    'level': 10,
    'daemon_types': ['osd'],
    'cluster': 'ceph',
    'whole_job': False,
    'max_kept_bytes_per_op': 20480,
    'max_kept_fraction': 0.2,
    'max_kept_mb_per_s': 12.0,
    'min_client_ops': 1000,
    'top': 50,
    'sample_target_bytes': 2 << 30,
    'offenders': 5,
}


def _find_tool(ctx):
    """Path of src/script/ceph_log_budget.py in the qa suite checkout."""
    candidates = []
    suite_path = ctx.config.get('suite_path')
    if suite_path:
        candidates.append(os.path.join(os.path.dirname(suite_path),
                                       'src', 'script', TOOL))
    candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   '..', '..', 'src', 'script', TOOL))
    for path in candidates:
        if os.path.isfile(path):
            return os.path.normpath(path)
    return None


def _load_tool(path):
    spec = importlib.util.spec_from_file_location('ceph_log_budget', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _find_logs(remote, cluster, daemon_types):
    """Return {daemon: [log files]} for the given daemon types on remote,
    including rotated/compressed files and cephadm's /var/log/ceph/<fsid>/."""
    logs = {}
    for daemon_type in daemon_types:
        out = remote.sh(['sudo', 'find', '/var/log/ceph', '-type', 'f',
                         '-name', '%s-%s.*.log*' % (cluster, daemon_type)],
                        check_status=False)
        for path in out.split():
            base = os.path.basename(path)
            daemon = base[:base.index('.log')]
            logs.setdefault(daemon, []).append(path)
    return logs


def _analyse(ctx, config, tool_path, since):
    tool = _load_tool(tool_path)
    testdir = teuthology.get_testdir(ctx)
    remote_tool = os.path.join(testdir, TOOL)
    remotes = {}
    for daemon_type in config['daemon_types']:
        sel = ctx.cluster.only(teuthology.is_type(daemon_type,
                                                  config['cluster']))
        for remote in sel.remotes.keys():
            remotes[remote.name] = remote

    procs = []
    for name, remote in sorted(remotes.items()):
        logs = _find_logs(remote, config['cluster'], config['daemon_types'])
        if not logs:
            continue
        remote.put_file(tool_path, remote_tool)
        for daemon, files in sorted(logs.items()):
            args = ['sudo', 'python3', remote_tool, '--json',
                    '--level', str(config['level']),
                    '--top', str(config['top']),
                    '--sample', 'auto',
                    '--sample-target-bytes',
                    str(config['sample_target_bytes'])]
            if since.get(name):
                args += ['--since', since[name]]
            args += sorted(files)
            proc = remote.run(args=args, stdout=StringIO(), stderr=StringIO(),
                              wait=False, check_status=False)
            procs.append((name, daemon, proc))
    run.wait([p for _, _, p in procs])

    reports = {}
    for name, daemon, proc in procs:
        if proc.exitstatus not in (0, 1):
            log.warning('log_budget: analysis of %s on %s failed (%s): %s',
                        daemon, name, proc.exitstatus,
                        proc.stderr.getvalue().strip()[-2000:])
            continue
        try:
            reports['%s:%s' % (name, daemon)] = json.loads(
                proc.stdout.getvalue())
        except ValueError as e:
            log.warning('log_budget: bad report for %s on %s: %s',
                        daemon, name, e)
    for remote in remotes.values():
        remote.run(args=['rm', '-f', '--', remote_tool], check_status=False)
    if not reports:
        log.warning('log_budget: no logs analysed')
        return []

    summary = tool.merge_reports(list(reports.values()), top=config['top'])
    source_root = os.path.dirname(os.path.dirname(os.path.dirname(tool_path)))
    idx = tool.attribute_sources(summary['top_kept_templates'], source_root)
    tool.attribute_sources(summary['top_templates'], source_root, index=idx)
    violations = tool.check_budgets(
        summary,
        max_kept_bytes_per_op=config['max_kept_bytes_per_op'],
        max_kept_fraction=config['max_kept_fraction'],
        max_kept_mb_per_s=config['max_kept_mb_per_s'],
        min_client_ops=config['min_client_ops'],
        offenders=config['offenders'])
    summary['violations'] = violations

    kept = summary['kept']
    log.info('log_budget: %d daemon logs, %d client ops, %.1f MB total; '
             'level<=%d: %.1f MB (%.1f%%), %s bytes/op, %s MB/s per daemon',
             len(reports), summary['client_ops'], summary['bytes'] / 1e6,
             config['level'], kept['bytes'] / 1e6, 100.0 * kept['fraction'],
             '%.0f' % summary['kept_bytes_per_op']
             if summary['kept_bytes_per_op'] is not None else '-',
             '%.2f' % (kept['bytes_per_s'] / 1e6)
             if kept['bytes_per_s'] is not None else '-')
    for note in summary.get('budget_notes', []):
        log.info('log_budget: %s', note)
    for v in violations:
        log.warning('log_budget: %s exceeded: %.4g > %.4g', v['budget'],
                    v['actual'], v['limit'])
    if violations:
        log.warning('log_budget: top level<=%d templates:', config['level'])
        for o in violations[0]['offenders']:
            log.warning('log_budget:   %s %d %.1f MB %s %s', o['component'],
                        o['level'], o['bytes'] / 1e6, o['template'],
                        ' '.join(o.get('source', [])))

    if ctx.archive is not None:
        path = os.path.join(ctx.archive, 'log_budget.json')
        with open(path, 'w') as f:
            json.dump({'config': config, 'since': since, 'summary': summary,
                       'daemons': reports}, f, indent=1, sort_keys=True)
        log.info('log_budget: report written to %s', path)
    return violations


@contextlib.contextmanager
def task(ctx, config):
    """
    Check the debug-log volume budget of the job's daemon logs at teardown.
    See the module docstring for the options.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'log_budget task only supports a dictionary for configuration'
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('log_budget', {}))
    for k, v in DEFAULTS.items():
        config.setdefault(k, v)
    assert config['mode'] in ('warn', 'fail', 'off'), \
        'log_budget: mode must be warn, fail or off'

    since = {}
    if config['mode'] != 'off' and not config['whole_job']:
        for remote in ctx.cluster.remotes.keys():
            # local time, like the timestamps in the ceph logs
            since[remote.name] = remote.sh(
                ['date', '+%Y-%m-%dT%H:%M:%S.%3N']).strip()

    violations = []
    try:
        yield
    finally:
        if config['mode'] != 'off':
            tool_path = _find_tool(ctx)
            if tool_path is None:
                log.warning('log_budget: %s not found in the suite checkout; '
                            'skipping', TOOL)
            else:
                try:
                    violations = _analyse(ctx, config, tool_path, since)
                except Exception:
                    log.exception('log_budget: analysis failed')
    if violations and config['mode'] == 'fail':
        raise RuntimeError('log_budget: debug log budget exceeded: ' +
                           '; '.join('%s %.4g > %.4g' % (v['budget'],
                                                         v['actual'],
                                                         v['limit'])
                                     for v in violations))
