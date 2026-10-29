import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-10-17_23:46:38-rados-wip-bharath17-testing-20261016.230103-main-distro-default-trial/650981': 'other/osd-recovery-incomplete-assert',
    'lflores-2026-06-25_15:37:19-rados-wip-lflores-testing-4-2026-06-24-1644-umbrella-distro-default-trial/317066': 'other/injected-leak-suppressed',
    'pdonnell-2026-08-26_21:30:54-upgrade-wip-pdonnell-testing-20260826.182613-umbrella-distro-default-trial/516663': 'other/chacra-build-not-ready',
    'pdonnell-2026-08-03_19:49:29-upgrade-wip-pdonnell-testing-20260802.185424-distro-default-trial/424717': 'other/workunit-keyring-attribute',
    'yuriw-2026-01-23_21:16:01-rados-wip-yuri13-testing-2026-01-23-1852-reef-distro-default-trial/15588': 'other/saw-valgrind-issues',
    'skanta-2026-04-06_10:26:47-rados-wip-bharath14-testing-2026-04-06-1412-squid-distro-default-trial/133240': 'other/ec-inconsistent-hinfo-assert',
}

# rados/valgrind-leaks/2-inject-leak/{mon,osd} make one daemon leak on purpose.
LEAK = r"bash -c 'ceph tell ([\w.]+) leak_some_memory'"
# The suppression in qa/valgrind.supp that hides any leak, of any kind, with a
# frame valgrind prints as UnknownInlinedFun anywhere in its stack.
BROAD = 'ignore *all* ceph-mgr python crap'
# qa/tasks/util/chacra.py get_binary_url() takes the newest shaman result, whatever
# its status, and raises if it is not ready.
CHACRA = r'qa/tasks/util/chacra\.py", line \d+, in get_binary_url'
NOT_READY = r'RuntimeError: cannot pull file with status: (\w+)'
# qa/tasks/workunit.py _run_tests reads ctx.ceph[cluster].keyring, which only the
# ceph task sets; the cephadm task's ctx.ceph[cluster] is a bare Namespace.
NO_KEYRING = r"AttributeError: 'Namespace' object has no attribute 'keyring'"
WORKUNIT = r'qa/tasks/workunit\.py", line \d+, in _run_tests'


@node('other', 'What failed when the failure reason was not recognised?')
def other(ctx):
    recovered = ctx.search(
        'teuthology.log',
        r'assert not manager\.is_recovered\(\)',
        'is_recovered',
    )
    if recovered:
        return done(
            'osd-recovery-incomplete-assert',
            f'test_incomplete_pgs: assert not manager.is_recovered() failed{at(recovered[0])}: '
            'recovery completed unexpectedly despite osd_recovery_delay_start injection',
            recovered[0],
            key='osd_recovery.test_incomplete_pgs: assert not manager.is_recovered()',
        )
    hinfo = ctx.search(
        'teuthology.log',
        r"assert 'clean' in pg\['state'\]\.split\('\+'\)",
        'clean',
    )
    if hinfo:
        task_frame = ctx.search(
            'teuthology.log',
            r'qa/tasks/ec_inconsistent_hinfo\.py", line \d+, in task',
            'ec_inconsistent_hinfo.py',
            max_hits=1,
        )
        if task_frame:
            return done(
                'ec-inconsistent-hinfo-assert',
                f"ec_inconsistent_hinfo: assert 'clean' in pg['state'].split('+') failed{at(hinfo[0])}: "
                'the PG was peering right after backfilling instead of active+clean',
                task_frame[0], hinfo[0],
                key="ec_inconsistent_hinfo: assert 'clean' in pg['state'].split('+')",
            )
    if "'Namespace' object has no attribute 'keyring'" in ctx.job.reason:
        missing = ctx.search('teuthology.log', NO_KEYRING, 'has no attribute', max_hits=1)
        where = ctx.search('teuthology.log', WORKUNIT, 'workunit.py', max_hits=1)
        if missing and where:
            return done(
                'workunit-keyring-attribute',
                'the workunit task read ctx.ceph[cluster].keyring in _run_tests'
                f'{at(missing[0])}, but only the ceph task sets it; with the cephadm task '
                'ctx.ceph[cluster] is a bare Namespace, so it raised AttributeError '
                'before the workunit ran',
                where[0], missing[0],
                key='workunit _run_tests: ctx.ceph[cluster].keyring, Namespace has no attribute',
            )
    if 'cannot pull file with status' in ctx.job.reason:
        refused = ctx.search('teuthology.log', NOT_READY, 'cannot pull file with status',
                             max_hits=1)
        where = ctx.search('teuthology.log', CHACRA, 'get_binary_url', max_hits=1)
        if refused and where:
            status = re.search(NOT_READY, refused[0].text).group(1)
            return done(
                'chacra-build-not-ready',
                'qa/tasks/util/chacra.py get_binary_url() took the newest shaman result, '
                f'a build still in status "{status}", and raised instead of using the '
                'newest ready build',
                where[0], refused[0],
                key='chacra get_binary_url: cannot pull file, newest build not ready',
                status=status,
            )
    if 'expected valgrind issues and found none' in ctx.job.reason:
        told = ctx.search('teuthology.log', LEAK, 'leak_some_memory', max_hits=1)
        who = re.search(LEAK, told[0].text).group(1) if told else ''
        hidden = ctx.search(f'remote/*/log/valgrind/{who}.log*', r'<name>' + re.escape(BROAD),
                            BROAD, max_hits=1) if who else []
        if hidden:
            return done(
                'injected-leak-suppressed',
                f'{who} was made to leak memory{at(told[0])}, but valgrind reported no error '
                f'for it: its leak check matched the over-broad "{BROAD}" suppression '
                '(fun:UnknownInlinedFun in qa/valgrind.supp), which hides the injected leak '
                'when the compiler inlines CephContext::do_command',
                told[0], hidden[0],
                key='expected valgrind issues and found none: injected leak hidden by the '
                    'UnknownInlinedFun suppression',
                daemon=who,
            )
    if 'saw valgrind issues' in ctx.job.reason:
        vhits = ctx.hits('valgrind')
        first_hit = vhits[0] if vhits else None
        if first_hit:
            m = ctx.match('valgrind', first_hit)
            if m:
                text = m.group(2) or ''
                km = re.search(r'<kind>(\w+)</kind>', text)
                dm = re.search(r'valgrind/([a-z]+\.\w+)\.log', text)
                kind = km.group(1) if km else ''
                daemon = dm.group(1) if dm else ''
                if kind:
                    return done(
                        'saw-valgrind-issues',
                        f'valgrind found {kind} errors (saw valgrind issues) in '
                        f'{daemon or "a daemon log"}',
                        first_hit,
                        key=f'saw valgrind issues: {kind}',
                        kind=kind, daemon=daemon,
                    )
    return unknown('none of the failures this node knows')
