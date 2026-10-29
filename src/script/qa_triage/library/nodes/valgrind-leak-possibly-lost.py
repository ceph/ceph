import re

from qa_triage.tree.core import done, node, unknown

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-08-11_22:46:54-rados-wip-bharath7-testing-20260811.152323-main-distro-default-trial/458958':
        'valgrind/Leak_PossiblyLost/ec-call-coroutine-stack',
}


@node('valgrind/Leak_PossiblyLost', 'What leaked, and where was it allocated?')
def valgrind_leak_possibly_lost(ctx):
    # valgrind.yaml lists the daemons' errors, the reported one first, with its top frames.
    first = ctx.search('valgrind.yaml', r'^- file_path: |create_control_block', max_hits=2)
    if (len(first) == 2 and 'file_path:' in first[0].text
            and 'create_control_block' in first[1].text):
        osd = re.search(r'valgrind/(osd\.\d+)\.log', first[0].text)
        if osd:
            who = osd.group(1)
            caller = ctx.search(f'remote/*/log/valgrind/{who}.log.gz',
                                r'<fn>PrimaryLogPG::do_op\(', '<fn>PrimaryLogPG::do_op(',
                                max_hits=1)
            if caller:
                return done('ec-call-coroutine-stack',
                            f'{who} leaked the 128 KiB stack of the boost coroutine that '
                            'PrimaryLogPG::do_op starts for a CALL op on an EC pool: the '
                            'coroutine resets coro_resumer, its own owner, while still running '
                            'on that stack, so the stack is never freed',
                            first[0], first[1], caller[0],
                            key='valgrind leak: EC CALL op coroutine stack from '
                                'PrimaryLogPG::do_op',
                            leak='ec-call-coroutine-stack', daemon=who)
    return unknown('a leak this node does not know')
