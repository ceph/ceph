import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-09-20_00:25:34-rados-wip-bharath25-testing-20260918.001212-main-distro-default-trial/570014': 'crash/abort/specific/cls-2pc-queue-concurrent-op-order',
    'skanta-2026-06-02_04:58:02-rados-wip-bharath3-testing-2026-06-02-0837-distro-default-trial/253356': 'crash/abort/specific/remote-reservation-rejected-too-full',
}


@node('crash/abort/specific', 'What caused the abort?')
def crash_abort_specific(ctx):
    if ctx.facts.get('source') == 'osd/PrimaryLogPG.cc' and 'out of order op' in ctx.facts.get('message', ''):
        bad_order = ctx.search(
            'teuthology.log',
            r'bad op order, already applied (\d+) > this (\d+)',
            'bad op order',
        )
        if bad_order:
            runs = ctx.search('teuthology.log', r'\[ RUN\s+\] (\S+)', '[ RUN', max_hits=200)
            before = [h for h in runs if h.line < bad_order[0].line]
            test = re.search(r'\[ RUN\s+\] (\S+)', before[-1].text).group(1) if before else ''
            if test.startswith('TestCls2PCQueue.'):
                applied, this_tid = re.search(
                    r'bad op order, already applied (\d+) > this (\d+)', bad_order[0].text
                ).groups()
                return done(
                    'cls-2pc-queue-concurrent-op-order',
                    f'{test} ran concurrent client threads without op ordering, and osd_debug_op_order aborted '
                    f'when op {this_tid} arrived after {applied}{at(bad_order[0])}',
                    bad_order[0],
                    key=f'{test}: bad op order from concurrent threads with osd_debug_op_order',
                    test=test,
                )
    if (ctx.facts.get('source') == 'osd/PeeringState.cc' and
            'we got a bad state machine event' in ctx.facts.get('message', '')):
        rejected = ctx.search(
            'remote/*/log/ceph-osd.*.log.gz',
            r'RemoteReservationRejectedTooFull',
            'RemoteReservationRejectedTooFull',
        )
        if rejected:
            return done(
                'remote-reservation-rejected-too-full',
                f'{ctx.facts.get("daemon", "osd")} crashed: a late RemoteReservationRejectedTooFull '
                f'arrived in WaitLocalBackfillReserved state, hitting the bad state machine event '
                f'abort{at(rejected[0])}',
                rejected[0],
                key='PeeringState::Crashed: RemoteReservationRejectedTooFull in WaitLocalBackfillReserved',
            )
    return unknown('none of the abort causes this node knows')
