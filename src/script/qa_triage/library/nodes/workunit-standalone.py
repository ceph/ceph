from qa_triage.tree.core import done, node, unknown

EXAMPLES = {
    'skanta-2026-09-30_04:36:41-rados-wip-bharath14-testing-20260930.003013-main-distro-default-trial/593536': 'workunit/standalone/mclock-overrides-sleep',
    'lflores-2026-07-08_14:24:37-rados-wip-lflores-testing-2026-07-07-1045-tentacle-distro-default-trial/346141': 'workunit/standalone/nvmeof-beacon-diff-count',
}


@node('workunit/standalone', 'Why did the standalone test fail?')
def workunit_standalone(ctx):
    if ctx.facts.get('script') == 'mon/misc.sh' and ctx.facts.get('test') == 'TEST_mon_features':
        count_filter = ctx.search(
            'teuthology.log',
            r"jqfilter='[^']*persistent \| length == \d+'",
            'length == ',
        )
        count_fail = ctx.search(
            'teuthology.log',
            r'jq_success:\s+ret=false',
            'ret=false',
        )
        if count_filter and count_fail:
            nvmeof = ctx.search(
                'teuthology.log',
                r'"nvmeof_beacon_diff"',
                '"nvmeof_beacon_diff"',
            )
            if nvmeof:
                return done(
                    'nvmeof-beacon-diff-count',
                    "TEST_mon_features failed because the hardcoded count check "
                    "('.monmap.features.persistent | length == 12') returned false: "
                    'nvmeof_beacon_diff was added as a 13th persistent feature',
                    count_filter[0], count_fail[0],
                    key='TEST_mon_features: persistent feature count wrong, nvmeof_beacon_diff added',
                    test='TEST_mon_features',
                )
        return unknown('TEST_mon_features failed but not for the nvmeof_beacon_diff count reason this node knows')

    if ctx.facts.get('script') == 'osd/osd-recovery-space.sh':
        # When osd_mclock_override_recovery_settings=true, mClock overrides osd_recovery_sleep.
        # The PR #71393 fix injects --osd_recovery_sleep 0.05, but the OSD responds with
        # "osd_recovery_sleep = ''" (empty string), confirming the setting was silently ignored.
        # Without the throttle, recovery completes before the OSD full-status check fires.
        sleep_ignored = ctx.search(
            'teuthology.log',
            r"osd_recovery_sleep = ''",
            "osd_recovery_sleep = ''",
        )
        if sleep_ignored:
            return done(
                'mclock-overrides-sleep',
                'the osd_recovery_sleep throttle (from PR #71393) was silently overridden by '
                'mClock (osd_mclock_override_recovery_settings=true), so recovery still '
                'completed before the OSD full-status check fired and recovery_toofull '
                'never triggered',
                sleep_ignored[0],
                key='osd-recovery-space.sh TEST_recovery_test_simple: mClock overrides osd_recovery_sleep',
            )
        return unknown('osd_recovery_sleep was not overridden by mClock; '
                       'likely a pre-fix build or a timing flake')

    return unknown('none of the failures this node knows')
