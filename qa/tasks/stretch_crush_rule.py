import logging

log = logging.getLogger(__name__)


def ensure_custom_crush_rule(mgr_cluster, rule_name, datacenters=3):
    """
    Guarantee that the CRUSH rule rule_name exists.

    The monitor deletes any CRUSH rule as soon as no pool references it,
    so the rule installed by the workunit (mon-stretch-pool.sh) may be
    gone by the time setUp or a test tries to use it.  Recreate it if
    needed, using the same rule text as the workunit: take default, choose
    'datacenters' datacenters, then chooseleaf 2 hosts in each.
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
