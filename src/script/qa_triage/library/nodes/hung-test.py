from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

EXAMPLES = {
    'skanta-2026-07-29_01:45:42-rados-wip-bharath23-testing-2026-07-28-1848-distro-default-trial/407437': 'hung/test/rados-api-tests-valgrind-slowdown',
    'lflores-2026-07-17_23:28:16-rados-wip-lflores-testing-3-2026-07-17-1417-distro-default-trial/374705': 'hung/test/rados-api-tests-valgrind-slowdown',
}

# The neorados tests of qa/workunits/rados/test.sh, run in parallel with the api_* ones.
NEORADOS = ('cls', 'cmd', 'completions', 'handler_error', 'io', 'ec_io', 'list', 'ec_list',
            'misc', 'pool', 'read_operations', 'snapshots', 'watch_notify',
            'write_operations')


@node('hung/test', 'Why did the rados API test hang?')
def hung_test(ctx):
    test = ctx.facts.get('test', '')
    if (test.startswith('api_') or test in NEORADOS) and ctx.facts.get('hung_task') == 'workunit':
        valgrind = ctx.search('teuthology.log', r'running \w+\.\w+ under valgrind',
                              'under valgrind', max_hits=1)
        if valgrind:
            return done('rados-api-tests-valgrind-slowdown',
                        f'the rados API test {test} ran too slowly under valgrind{at(valgrind[0])}: '
                        'the cluster daemons run under valgrind memcheck, which slows them so '
                        'much that the rados_api_tests workunit hits the job timeout',
                        valgrind[0],
                        key='rados_api_tests: api_* test timed out under valgrind',
                        test=test)
    return unknown('none of the test hangs this node knows')
