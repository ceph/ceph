import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'jjperez-2026-09-01_13:33:40-crimson-rados-wip-perezjos-crimson-only-01-09-2026-PR71057-distro-debug-trial/527954': 'crash/client/ceph/ping-monitor-abort',
}

# A Python frame in faulthandler's dump after a fatal error, innermost first.
FRAME = re.compile(r'stderr:\s+File "([^"]+)", line (\d+) in (\w+)')


@node('crash/client/ceph', 'Where in the program did it abort?')
def crash_client_ceph(ctx):
    fatal = ctx.search('teuthology.log', r'stderr:Fatal Python error: Aborted',
                       'Fatal Python error: Aborted', max_hits=1)
    if not fatal:
        return unknown('no Python program aborted')
    dump = [h for h in ctx.between(fatal[0].line + 1, fatal[0].line + 8)
            if FRAME.search(h.text)]
    if not dump:
        return unknown('faulthandler printed no Python frame after the abort')
    path, line, function = FRAME.search(dump[0].text).groups()
    if 'ping_monitor' not in function:
        return unknown('none of the aborts this node knows')
    script = path.rsplit('/', 1)[-1]
    return done('ping-monitor-abort',
                f'python3 aborted{at(fatal[0])} inside librados ping_monitor(), called by '
                f'{function} at {script}:{line}: librados aborted on the temporary '
                'MonClientPinger connection to the monitor; it was not a check of the test',
                fatal[0], dump[0],
                key=f'python3 abort in rados ping_monitor from {script} {function}',
                test=function, call='ping_monitor', script=script)
