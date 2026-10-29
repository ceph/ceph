import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

EXAMPLES = {
    'skanta-2026-09-03_10:37:47-rados-wip-bharath10-testing-20260903.050751-main-distro-default-trial/534759': 'crash/assert/specific/coroutine-obc-leak-on-interval-change',
}


@node('crash/assert/specific', 'What caused the assert?')
def crash_assert_specific(ctx):
    if (ctx.facts.get('source') == 'osd/PrimaryLogPG.cc'
            and ctx.facts.get('message') == 'FAILED ceph_assert(object_contexts.empty())'
            and ctx.facts.get('function') == 'virtual void PrimaryLogPG::on_flushed()'):
        who = ctx.facts.get('daemon', '')
        log = f'remote/*/log/ceph-{who}.log.gz'
        alive = ctx.search(log, r'on_flushed: object .+ obc still alive',
                           'on_flushed: object ')
        if alive:
            obj_match = re.search(r'on_flushed: object (.+) obc still alive', alive[0].text)
            obj_name = obj_match.group(1) if obj_match else '?'
            return done(
                'coroutine-obc-leak-on-interval-change',
                f'{who} asserted object_contexts.empty() in on_flushed(): '
                f'a coroutine EC op held the obc for {obj_name} across a peering '
                f'interval change without releasing it{at(alive[0])}',
                alive[0],
                key='PrimaryLogPG::on_flushed: object_contexts not empty after interval '
                    'change, coroutine EC op obc not released',
                object=obj_name,
            )
    return unknown('none of the assert causes this node knows')
