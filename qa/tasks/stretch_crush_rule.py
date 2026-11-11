"""
Recreates the hand-written replicated rule of a stretch test when the
monitor has removed it, for tasks.stretch_cluster,
tasks.stretch_mode_disable_enable and tasks.test_netsplit_3az_stretch_pool.
"""
import logging

log = logging.getLogger(__name__)


def ensure_custom_crush_rule(mgr_cluster, rule_name, datacenters=3):
    """
    Make sure that the CRUSH rule rule_name exists.

    The monitor removes a rule when its last pool leaves it, so the rule
    that the job's workunit wrote (mon-stretch-pool.sh or
    mon-stretch-mode-5-mons-8-osds.sh) may be gone by the time setUp or a
    test uses it. If it is gone, write it again as the workunit did: take
    default, choose 'datacenters' datacenters, then chooseleaf 2 hosts in
    each.
    """
    existing = mgr_cluster.mon_manager.raw_cluster_cmd(
        'osd', 'crush', 'rule', 'ls')
    if rule_name in existing.split():
        return
    log.debug('ensure_custom_crush_rule: %s missing, recreating', rule_name)
    mgr_cluster.admin_remote.sh(
        'tmpdir=$(mktemp -d) && trap "rm -rf $tmpdir" EXIT && '
        'ceph osd getcrushmap > $tmpdir/cm && '
        'crushtool --decompile $tmpdir/cm > $tmpdir/cm.txt && '
        'sed "s/^# end crush map$//" $tmpdir/cm.txt > $tmpdir/cm_mod.txt && '
        'rule_id=$(ceph osd crush dump -f json '
        '         | python3 -c "import sys,json; '
        '           rules=json.load(sys.stdin)[\'rules\']; '
        '           print(max(r[\'rule_id\'] for r in rules)+1)") && '
        'printf "rule %s {\\n'
        '        id %s\\n'
        '        type replicated\\n'
        '        step take default\\n'
        '        step choose firstn %s type datacenter\\n'
        '        step chooseleaf firstn 2 type host\\n'
        '        step emit\\n'
        '}\\n# end crush map\\n" "' + rule_name + '" "$rule_id" '
        '"' + str(int(datacenters)) + '" '
        '>> $tmpdir/cm_mod.txt && '
        'crushtool --compile $tmpdir/cm_mod.txt -o $tmpdir/cm.bin && '
        'ceph osd setcrushmap -i $tmpdir/cm.bin'
    )
