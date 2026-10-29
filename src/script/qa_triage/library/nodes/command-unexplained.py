import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at, failure

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-09-30_21:49:09-rados-wip-bharath8-testing-20260930.133801-main-distro-default-trial/597283': 'command/unexplained/cbt-perf-keyerror-pid',
    'skanta-2026-09-30_04:36:41-rados-wip-bharath14-testing-20260930.003013-main-distro-default-trial/593732': 'command/unexplained/radosbench-undersized-peered',
    'yuriw-2026-10-22_20:33:51-rados-wip-yuri12-testing-20261022.154156-squid-distro-default-trial/668927': 'command/unexplained/crush-location-json-mismatch',
    'pdonnell-2026-08-27_01:45:31-rados-wip-pdonnell-testing-20260826.223123-squid-distro-default-trial/518577': 'command/unexplained/not-found',
    'rkachach-2026-06-27_08:00:42-orch:cephadm-wip-rkachach-testing-3-2026-06-26-1442-distro-default-trial/319284': 'command/unexplained/download-cephadm-failed',
    'skanta-2026-04-09_03:57:47-rados-wip-bharath16-testing-2026-04-08-1405-distro-default-trial/141085': 'command/unexplained/git-clone-ci-dns-fallback-missing-tree',
    'nmordech-2026-06-03_18:29:30-rados-wip-bharath2-testing-2026-06-03-0438-distro-default-trial/260829': 'command/unexplained/git-clone-ci-dns-fallback-missing-tree',
    'skanta-2026-06-02_04:58:02-rados-wip-bharath3-testing-2026-06-02-0837-distro-default-trial/253529': 'command/unexplained/git-clone-dns-both-failed',
    'lflores-2026-01-21_20:56:39-rados-main-distro-default-trial/11956': 'command/unexplained/prometheus-module-inactive',
    'nmordech-2026-01-25_11:10:14-rados-wip-rocky10-branch-of-the-day-2026-01-23-1769128778-distro-default-trial/17059': 'command/unexplained/prometheus-module-inactive',
    'nmordech-2026-01-28_16:11:20-rados-wip-rocky10-branch-of-the-day-2026-01-23-1769128778-distro-default-trial/23326': 'command/unexplained/prometheus-module-inactive',
    'nmordech-2026-01-28_16:11:20-rados-wip-rocky10-branch-of-the-day-2026-01-23-1769128778-distro-default-trial/23463': 'command/unexplained/prometheus-module-inactive',
    'nmordech-2026-02-04_08:13:49-rados-wip-rocky10-branch-of-the-day-2026-02-03-1770151121-distro-default-trial/33967': 'command/unexplained/prometheus-module-inactive',
    'yaarit-2026-02-05_03:52:11-rados-wip-rocky10-branch-of-the-day-2026-02-03-1770151121-distro-default-trial/35766': 'command/unexplained/prometheus-module-inactive',
    'yaarit-2026-02-05_03:52:11-rados-wip-rocky10-branch-of-the-day-2026-02-03-1770151121-distro-default-trial/35903': 'command/unexplained/prometheus-module-inactive',
    'lflores-2026-04-10_20:57:58-orch:cephadm:upgrade-tentacle-distro-default-trial/144426': 'command/unexplained/mgr-modules-not-loaded-upgrade-status',
}


@node('command/unexplained', 'Why did the command fail?')
def command_unexplained(ctx):
    command = ctx.facts.get('command', '')
    if (command.startswith('egrep ') or command.startswith('fgrep ') or command.startswith('grep ')) and ctx.facts.get('status') == '1':
        fail = failure(ctx)
        return done('not-found', f'"{command}" found nothing', fail,
                    key=f'{command}: not found')
    if command.startswith('rm -rf clone.client') and 'git clone' in command:
        failed_checkout = ctx.search(
            'teuthology.log',
            r"failed to check out '(\w+)' from https://\S+/ceph-ci\.git; will also try in https://\S+/ceph\.git",
            'failed to check out', 'ceph-ci.git',
        )
        unresolved = ctx.search(
            'teuthology.log',
            r'fatal: unable to access \S+: Could not resolve host: (\S+)',
            'Could not resolve host',
        )
        unable_tree = ctx.search(
            'teuthology.log',
            r'fatal: unable to read tree \((\w+)\)',
            'fatal: unable to read tree',
        )
        if failed_checkout and unresolved and unable_tree:
            dns_host = re.search(r'Could not resolve host: (\S+)', unresolved[0].text).group(1)
            ref = re.search(r"failed to check out '(\w+)'", failed_checkout[0].text).group(1)
            return done(
                'git-clone-ci-dns-fallback-missing-tree',
                f'workunit git clone of ceph-ci ref {ref} failed{at(unresolved[0])} due to DNS failure '
                f'("Could not resolve host: {dns_host}"); the fallback clone from ceph.git then failed '
                f'at checkout because ref {ref} was only on ceph-ci',
                unresolved[0], failed_checkout[0], unable_tree[0],
                key=f'git clone ceph-ci: Could not resolve host: {dns_host}, fallback ceph.git missing tree',
                dns_host=dns_host, ref=ref,
            )
        if failed_checkout and not unable_tree:
            ci_dns = ctx.search(
                'teuthology.log',
                r"fatal: unable to access 'https://github\.com/ceph/ceph-ci\.git/': Could not resolve host: (\S+)",
                'Could not resolve host', 'ceph-ci.git/',
            )
            ceph_dns = ctx.search(
                'teuthology.log',
                r"fatal: unable to access 'https://github\.com/ceph/ceph\.git/': Could not resolve host: (\S+)",
                'Could not resolve host', 'ceph/ceph.git/',
            )
            if ci_dns and ceph_dns:
                dns_host = re.search(r'Could not resolve host: (\S+)', ceph_dns[0].text).group(1)
                ref = re.search(r"failed to check out '(\w+)'", failed_checkout[0].text).group(1)
                return done(
                    'git-clone-dns-both-failed',
                    f'workunit git clone of ref {ref} failed{at(ci_dns[0])} from ceph-ci.git and again{at(ceph_dns[0])} '
                    f'from ceph.git, both because DNS could not resolve {dns_host} '
                    f'("Could not resolve host: {dns_host}")',
                    ci_dns[0], failed_checkout[0], ceph_dns[0],
                    key=f'git clone ceph-ci and ceph: Could not resolve host: {dns_host}',
                    dns_host=dns_host, ref=ref,
                )
    if command == 'test -s cephadm && test $(stat -c%s cephadm)':
        download = ctx.search('teuthology.log',
                              r'Downloading cephadm \(repo \S+ ref (\S+)\)\.\.\.',
                              'Downloading cephadm')
        size_check = ctx.search('teuthology.log',
                                r'\.stdout:[-rwx.]+\s+\d+\s+\S+\s+\S+\s+(\d+)\s+.*cephadm',
                                '.stdout:')
        if download and size_check:
            ref = re.search(r'Downloading cephadm \(repo \S+ ref (\S+)\)\.\.\.', download[0].text).group(1)
            size = re.search(r'\.stdout:[-rwx.]+\s+\d+\s+\S+\s+\S+\s+(\d+)\s+', size_check[0].text).group(1)
            return done('download-cephadm-failed',
                        f'downloading cephadm ref {ref} from github failed: '
                        f'the downloaded file was only {size} bytes long (expected > 1000 bytes)',
                        download[0], size_check[0],
                        key='download cephadm: file too small')
    if command.startswith('cbt-venv/bin/python cbt/cbt.py'):
        pid_err = ctx.search('teuthology.log', r"KeyError: 'pid'", "KeyError: 'pid'")
        if pid_err:
            return done('cbt-perf-keyerror-pid',
                        f'cbt perf_monitoring.py raised KeyError: pid{at(pid_err[0])}: '
                        "the perf args template contains {pid} but only perf_dir is "
                        'passed to format(), raising KeyError when building the perf command',
                        pid_err[0],
                        key='cbt perf_monitoring.py: KeyError: pid')
    if command.startswith('rados ') and ' bench' in command:
        timed_out = ctx.search('teuthology.log',
                               r'stderr:error during benchmark: \(110\) Connection timed out',
                               'error during benchmark')
        recovery = ctx.search('teuthology.log',
                              r'^AssertionError: wait_for_recovery: failed before timeout expired',
                              'wait_for_recovery: failed before timeout expired')
        peered = ctx.search('teuthology.log', r"'state': 'undersized\+peered'",
                            'undersized+peered')
        if timed_out and recovery and peered:
            return done('radosbench-undersized-peered',
                        f'rados bench timed out (110){at(timed_out[0])} because PGs of its pool '
                        'were stuck undersized+peered and never went active; the thrasher '
                        'then failed wait_for_recovery on the same PGs',
                        timed_out[0], peered[0], recovery[0],
                        key='rados bench: timed out, PGs undersized+peered, wait_for_recovery failed')
    if command.startswith('cephadm ') and ' shell' in command:
        inactive = ctx.search('teuthology.log', r'CephMgrPrometheusModuleInactive', 'CephMgrPrometheusModuleInactive')
        if inactive:
            return done('prometheus-module-inactive',
                        f'the command failed because the "CephMonDown" alert was not firing, '
                        f'but instead "CephMgrPrometheusModuleInactive" was firing{at(inactive[0])}: '
                        f'the mgr/prometheus module was deadlocked or inactive',
                        inactive[0],
                        key='prometheus module inactive: CephMgrPrometheusModuleInactive is firing')
        compared = ctx.search('teuthology.log',
                              r"stderr:\+ jq .*--arg crushloc '\{[^']*\}'.*\.crush_location == \$crushloc",
                              'crush_location == $crushloc')
        listed = ctx.search('teuthology.log',
                            r'"crush_location":\[\{"key":"[^"]*","val":"',
                            '"crush_location":[{"key"')
        if compared and listed:
            return done('crush-location-json-mismatch',
                        f'a jq check compared a mon crush_location with a string{at(compared[0])}, '
                        'but ceph mon dump --format json gives crush_location as a list of '
                        'key/val objects, so the comparison is false although the location was set',
                        compared[0], listed[0],
                        key='mon dump json: crush_location list compared to a string')
        if (re.search(r'--image quay\.io/ceph/ceph:v\d+\.\d+\.\d+ ', command + ' ')
                and 'ceph versions | jq' in ctx.job.reason
                and 'overall | length == 1' in ctx.job.reason):
            unloaded = ctx.search('teuthology.log',
                                  r"Module '([a-z_]+)' is not enabled/loaded \(required by command "
                                  r"'orch upgrade status'\)",
                                  'is not enabled/loaded', 'orch upgrade status')
            running = ctx.search('teuthology.log',
                                 r'stdout:\s+"message": "Currently upgrading \w+ daemons"',
                                 'Currently upgrading')
            paused = ctx.search('teuthology.log', r'Upgrade: Paused due to (UPGRADE_[A-Z_]+)',
                                'Upgrade: Paused due to')
            malformed = ctx.search('teuthology.log',
                                   r'auth: error parsing file \S+/keyring: error setting modifier '
                                   r'for \[[^\]]+\] type=key val=Ag\S*: Malformed input',
                                   'Malformed input')
            if unloaded and running and not paused and not malformed:
                module = re.search(r"Module '([a-z_]+)'", unloaded[0].text).group(1)
                return done('mgr-modules-not-loaded-upgrade-status',
                            f'the version check "ceph versions | jq .overall | length == 1" was false '
                            'because the upgrade was still running: the wait loop\'s '
                            f'"ceph orch upgrade status" failed{at(unloaded[0])} because the mgr '
                            f'that had just restarted had not loaded its {module} module yet '
                            '("is not enabled/loaded"), so the loop took the upgrade to be over '
                            'and the check ran while mons and osds were still on the old release: '
                            'the mgr advertises itself as active before its modules have loaded',
                            unloaded[0], running[0],
                            key='ceph versions check: upgrade status failed, mgr module not loaded just after a mgr restart',
                            module=module)
    return unknown('none of the failures this node knows')
