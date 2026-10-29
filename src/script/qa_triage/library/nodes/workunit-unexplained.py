import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at, failure

EXAMPLES = {
    'skanta-2026-10-17_23:46:38-rados-wip-bharath17-testing-20261016.230103-main-distro-default-trial/650944': 'workunit/unexplained/iscsi-gwcli-host-not-found',
    'yuriw-2026-10-21_19:23:54-rados-wip-yuri14-testing-20261021.151926-tentacle-distro-default-trial/664818': 'workunit/unexplained/ceph-pg-query-hung/osd-killed-pg-query-no-timeout',
    'nmordech-2026-02-25_11:36:23-rados-wip-rocky10-branch-of-the-day-2026-02-24-1771941190-distro-default-trial/70083': 'workunit/unexplained/ceph-pg-query-hung/osd-killed-pg-query-no-timeout',
    'yuriw-2026-10-21_19:23:54-rados-wip-yuri14-testing-20261021.151926-tentacle-distro-default-trial/664947': 'workunit/unexplained/backfill-prio-not-in-progress',
    'yuriw-2026-10-21_19:23:54-rados-wip-yuri14-testing-20261021.151926-tentacle-distro-default-trial/664703': 'workunit/unexplained/grafana-invalid-ip-address',
    'skanta-2026-10-03_22:27:10-rados-wip-bharath5-testing-20261003.131043-main-distro-default-trial/605162': 'workunit/unexplained/cephtool-hang',
    'lflores-2026-07-17_23:28:16-rados-wip-lflores-testing-3-2026-07-17-1417-distro-default-trial/374619': 'workunit/unexplained/cephtool-osd-epoch-race',
    'skanta-2026-06-24_04:55:40-rados-wip-bharath14-testing-2026-06-19-0657-tentacle-distro-default-trial/311608': 'workunit/unexplained/cephtool-mds-not-configured',
    'yuriw-2026-02-24_16:22:43-rados-wip-yuri6-testing-2026-02-22-1629-squid-distro-default-trial/68630': 'workunit/unexplained/cephtool-tmp-foo-rm-not-permitted',
    'skanta-2026-01-28_09:27:17-rados-wip-bharath2-testing-2026-01-27-0639-squid-distro-default-trial/23144': 'workunit/unexplained/cephtool-tmp-foo-rm-not-permitted',
    'yuriw-2026-02-24_16:22:43-rados-wip-yuri6-testing-2026-02-22-1629-squid-distro-default-trial/68636': 'workunit/unexplained/cephtool-tmp-foo-rm-not-permitted',
    'yuriw-2026-02-24_16:22:43-rados-wip-yuri6-testing-2026-02-22-1629-squid-distro-default-trial/68640': 'workunit/unexplained/cephtool-tmp-foo-rm-not-permitted',
    'skanta-2026-07-12_15:01:05-rados-wip-bharath4-testing-2026-07-09-1342-distro-default-trial/358456': 'workunit/unexplained/test-repos-gpg-404',
    'skanta-2026-07-02_01:10:52-rados-wip-bharath2-testing-2026-07-01-2021-tentacle-distro-default-trial/335065': 'workunit/unexplained/test-repos-release-file-missing',
    'skanta-2026-03-25_22:46:45-rados-wip-bharath8-testing-2026-03-25-1848-distro-default-trial/122165': 'workunit/unexplained/ceph-post-file-drop-unreachable',
    'jayaprakash-2026-02-06_12:54:34-rados-jaya-bs-testing-05-02-2026-distro-default-trial/38187': 'workunit/unexplained/dencoder-decode-failure',
    'jayaprakash-2026-03-20_14:58:50-rados-aclamk-fcm-2026-03-20-1106-distro-default-trial/113276': 'workunit/unexplained/dencoder-decode-failure',
    'skanta-2026-06-17_03:50:08-rados-wip-yuri7-testing-2026-06-16-0346-distro-default-trial/296715': 'workunit/unexplained/test-localpool-unrecognized-config-option',
    'skanta-2026-04-13_14:09:46-rados-wip-bharath11-testing-2026-04-13-1517-distro-default-trial/148464': 'workunit/unexplained/test-librados-build-git-ceph-com-html',
    'jayaprakash-2026-07-24_10:10:00-rados-wip-jaya-bs-testing-umbrella-23-07-2026-distro-default-trial/396304': 'workunit/unexplained/ganesha-nfsv3-tcp-register',
    'skanta-2026-05-27_06:21:57-rados-wip-bharath4-testing-2026-05-27-1003-distro-default-trial/240019': 'workunit/unexplained/ganesha-nfsv3-tcp-register',
    'yuriw-2026-05-15_14:18:16-rados-wip-yuri-testing-2026-05-14-1648-distro-default-trial/213609': 'workunit/unexplained/ganesha-nfsv3-udp-register',
}


@node('workunit/unexplained', 'Why did the workunit fail?')
def workunit_unexplained(ctx):
    if ctx.facts.get('workunit', '').startswith('post-file.sh'):
        unreachable = ctx.search('teuthology.log',
                                 r'ssh: connect to host drop\.ceph\.com port \d+: .+',
                                 'ssh: connect to host drop.ceph.com')
        if unreachable:
            return done('ceph-post-file-drop-unreachable',
                        f'ceph-post-file could not upload to drop.ceph.com{at(unreachable[0])}: '
                        'the ssh connection failed because the host is unreachable, so ssh '
                        'exited with status 255 and the workunit failed',
                        unreachable[0],
                        key='post-file.sh: ssh to drop.ceph.com failed',
                        script=ctx.facts.get('script', ''))
    if ctx.facts.get('workunit', '').startswith('cephadm/test_cephadm.sh'):
        err = ctx.search('teuthology.log', r'grafana-apiserver.*invalid IP address: [^\s\]]+',
                         'invalid IP address')
        gone = ctx.search('teuthology.log', r'grafana is not available', 'grafana is not available')
        if err and gone:
            address = re.search(r'invalid IP address: ([^\s\]]+)', err[0].text).group(1)
            return done('grafana-invalid-ip-address',
                        f'grafana.a exited at startup{at(err[0])} because its grafana-apiserver '
                        f'rejected the address "{address}" as an invalid IP address, so the '
                        'workunit never reached grafana on port 3000',
                        err[0], gone[0],
                        key='test_cephadm.sh: grafana invalid IP address', address=address)
        nfs_gone = ctx.search('teuthology.log', r'nfs is not available', 'nfs is not available')
        nfs_v3_udp = ctx.search(
            'teuthology.log',
            r'ganesha\.nfsd.*__Register_program :DISP :MAJ :Cannot register NFS V3 on UDP',
            'Cannot register NFS V3 on UDP',
        )
        if nfs_v3_udp and nfs_gone:
            return done(
                'ganesha-nfsv3-udp-register',
                f'test_cephadm.sh deployed nfs.a but ganesha.nfsd could not register '
                f'NFS V3 on UDP{at(nfs_v3_udp[0])}: no portmapper (rpcbind) was running '
                'and Enable_UDP = false was missing from samples/nfs.json, so Ganesha '
                'exited with status 2 (INVALIDARGUMENT) and the NFS service never '
                'became available',
                nfs_v3_udp[0], nfs_gone[0],
                key='test_cephadm.sh: ganesha Cannot register NFS V3 on UDP',
                script=ctx.facts.get('script', ''),
            )
        nfs_v3_tcp = ctx.search(
            'teuthology.log',
            r'ganesha\.nfsd.*__Register_program :DISP :MAJ :Cannot register NFS V3 on TCP',
            'Cannot register NFS V3 on TCP',
        )
        if nfs_v3_tcp and nfs_gone:
            return done(
                'ganesha-nfsv3-tcp-register',
                f'test_cephadm.sh deployed nfs.a but ganesha.nfsd could not register '
                f'NFS V3 on TCP{at(nfs_v3_tcp[0])}: no rpcbind/portmapper was running on Rocky 10, '
                'so Ganesha exited with status 2 (INVALIDARGUMENT) and the NFS service '
                'never became available',
                nfs_v3_tcp[0], nfs_gone[0],
                key='test_cephadm.sh: ganesha Cannot register NFS V3 on TCP',
                script=ctx.facts.get('script', ''),
            )
    if ctx.facts.get('workunit', '').startswith('cephadm/test_repos.sh'):
        gpg_404 = ctx.search(
            'teuthology.log',
            r'Status code: 404 for https://download\.ceph\.com/keys/release\.gpg',
            'Status code: 404', 'release.gpg',
        )
        if gpg_404:
            return done(
                'test-repos-gpg-404',
                f'test_repos.sh failed{at(gpg_404[0])}: download.ceph.com/keys/release.gpg '
                'returned HTTP 404 (the file was missing from the server), so DNF aborted '
                'the cephadm package install',
                gpg_404[0],
                key='test_repos.sh: release.gpg 404 from download.ceph.com',
                script=ctx.facts.get('script', ''),
            )
        no_release = ctx.search(
            'teuthology.log',
            r"The repository 'https://download\.ceph\.com/debian-\S+ \S+ Release' does not have a Release file",
            'does not have a Release file', 'download.ceph.com',
        )
        if no_release:
            return done(
                'test-repos-release-file-missing',
                f'test_repos.sh failed{at(no_release[0])}: cephadm add-repo configured a '
                'download.ceph.com apt repository that was never published for this Ubuntu '
                'release, so apt-get update found no Release file (404)',
                no_release[0],
                key='test_repos.sh: download.ceph.com apt repo has no Release file',
                script=ctx.facts.get('script', ''),
            )
    fail = failure(ctx)
    if fail and ('status 124' in ctx.job.reason or 'status 124' in fail.text):
        window = ctx.between(max(1, fail.line - 200), fail.line)
        hung = [h for h in window if 'is_pg_clean' in h.text and 'ceph pg' in h.text and 'query' in h.text]
        if not hung:
            hung = ctx.search('teuthology.log', r'is_pg_clean:\s+ceph pg \S+ query', 'is_pg_clean', 'ceph pg')
        if hung:
            return done('ceph-pg-query-hung',
                        f'ceph pg query hung during is_pg_clean{at(hung[0])}',
                        hung[0],
                        key='ceph pg query hung in is_pg_clean',
                        command='ceph pg query')
    not_in_progress = ctx.search(
        'teuthology.log',
        r"didn't become the in progress item",
        "didn't become the in progress item",
    )
    if not_in_progress:
        return done('backfill-prio-not-in-progress',
                    f'TEST_backfill_pool_priority: primary PG did not become '
                    f'the in-progress backfill reservation item{at(not_in_progress[0])}',
                    not_in_progress[0],
                    key='osd-backfill-prio: primary PG not in-progress item',
                    script=ctx.facts.get('script', ''))
    no_such_path = ctx.search(
        'teuthology.log',
        r'No such path /iscsi-targets/\S+/hosts/\S+',
        'No such path /iscsi-targets/',
    )
    if no_such_path:
        return done(
            'iscsi-gwcli-host-not-found',
            f'gwcli reported No such path when configuring target host{at(no_such_path[0])}',
            no_such_path[0],
            key='gwcli: No such path /iscsi-targets/.../hosts/...',
            script=ctx.facts.get('script', ''),
        )
    if ctx.facts.get('workunit', '').startswith('cephtool/test.sh') and 'status 124' in ctx.job.reason:
        bad_caps = ctx.search(
            'teuthology.log',
            r'overall HEALTH_ERR \d+ auth entities have invalid capabilities',
            'auth entities have invalid capabilities',
        )
        if len(bad_caps) >= 2:
            return done('cephtool-hang',
                        f'cephtool/test.sh hung until its timeout killed it (status 124) while the '
                        f'mon kept logging HEALTH_ERR for invalid auth capabilities{at(bad_caps[0])}',
                        bad_caps[0], bad_caps[1],
                        key='cephtool/test.sh hung with AUTH_BAD_CAPS HEALTH_ERR',
                        script=ctx.facts.get('script', ''))
    if ctx.facts.get('workunit', '').startswith('cephtool/test.sh') and 'status 124' not in ctx.job.reason:
        mds_enoent = ctx.search(
            'teuthology.log',
            r'Error ENOENT: problem getting command descriptions from mds\.a',
            'problem getting command descriptions from mds',
        )
        if mds_enoent:
            return done('cephtool-mds-not-configured',
                        f'do_messenger_dump_basics_test ran "ceph tell mds.a messenger dump"'
                        f'{at(mds_enoent[0])} but got Error ENOENT: no MDS is configured in '
                        'the rados/singleton-bluestore suite',
                        mds_enoent[0],
                        key='cephtool/test.sh do_messenger_dump_basics_test: mds.a ENOENT',
                        script=ctx.facts.get('script', ''))
        tmp_foo = ctx.search(
            'teuthology.log',
            r"rm: cannot remove '/tmp/foo': Operation not permitted",
            "cannot remove '/tmp/foo'",
        )
        if tmp_foo:
            return done('cephtool-tmp-foo-rm-not-permitted',
                        f'test_tell_output_file passed its checks but "rm -f /tmp/foo" failed'
                        f'{at(tmp_foo[0])} with Operation not permitted, because the file the '
                        'daemon wrote was not removable by the test user, so the workunit '
                        'exited with status 1 under set -e',
                        tmp_foo[0],
                        key='cephtool/test.sh test_tell_output_file: rm /tmp/foo not permitted',
                        script=ctx.facts.get('script', ''))
        osd_epoch = ctx.search(
            'teuthology.log',
            r'test_mon_osd:.*\[\[ osd\.',
            'test_mon_osd:  [[',
        )
        if osd_epoch:
            return done('cephtool-osd-epoch-race',
                        f'test_mon_osd compared info_plain and dump_plain from two separate '
                        f'ceph commands{at(osd_epoch[0])}: the OSD map advanced an epoch between '
                        'the calls so their up_thru values differed and the [[ == ]] check '
                        'at test.sh:1656 failed with status 1',
                        osd_epoch[0],
                        key='cephtool/test.sh test_mon_osd: info_plain != dump_plain (epoch race)',
                        script=ctx.facts.get('script', ''))
    dencoder = ctx.search('teuthology.log',
                          r"Error encountered in subprocess\. Command: \['ceph-dencoder'",
                          'Error encountered in subprocess', 'ceph-dencoder')
    if dencoder:
        return done('dencoder-decode-failure',
                    f'test_readable.py ran ceph-dencoder on a ceph-object-corpus object{at(dencoder[0])} '
                    'and it exited with an error: an older decoder rejects corpus objects whose '
                    'min_compat was raised, and the test has no backward-incompat check to skip them',
                    dencoder[0],
                    key='test_readable.py: ceph-dencoder subprocess error',
                    script=ctx.facts.get('script', ''))
    decode_failed = ctx.search('teuthology.log',
                               r'\*\*\*\* failed to decode type \S+ from archive \S+',
                               'failed to decode type', 'from archive')
    if decode_failed:
        return done('dencoder-decode-failure',
                    f'readable.sh could not decode a ceph-object-corpus object with ceph-dencoder'
                    f'{at(decode_failed[0])}: an older decoder rejects corpus objects whose '
                    'encoding version was raised, and the test has no backward-incompat check '
                    'to skip them',
                    decode_failed[0],
                    key='readable.sh: ceph-dencoder failed to decode corpus object',
                    script=ctx.facts.get('script', ''))
    localpool = ctx.search('teuthology.log',
                           r"Error EINVAL: unrecognized config option 'mgr/localpool/subtree'",
                           'unrecognized config option', 'mgr/localpool/subtree')
    if localpool:
        return done('test-localpool-unrecognized-config-option',
                    f'mgr/test_localpool.sh could not set its config{at(localpool[0])}: the mgr '
                    'localpool module was missing from the build, so the option was unrecognized '
                    '(EINVAL)',
                    localpool[0],
                    key='test_localpool.sh: unrecognized config option mgr/localpool/subtree',
                    script=ctx.facts.get('script', ''))
    if ctx.facts.get('workunit', '').startswith('rados/test_librados_build.sh'):
        missing_sep = ctx.search('teuthology.log',
                                 r'Makefile:\d+: \*\*\* missing separator',
                                 'missing separator')
        if missing_sep:
            return done('test-librados-build-git-ceph-com-html',
                        f'test_librados_build.sh ran make on a Makefile downloaded from '
                        f'git.ceph.com that contained HTML instead of Makefile content'
                        f'{at(missing_sep[0])}: make failed with "missing separator" because '
                        'the server returned an HTML error or gateway-timeout page',
                        missing_sep[0],
                        key='test_librados_build.sh: Makefile missing separator (git.ceph.com HTML)',
                        script=ctx.facts.get('script', ''))
    return unknown('none of the failures this node knows')


@node('workunit/unexplained/ceph-pg-query-hung', 'What left ceph pg query without an answer?')
def pg_query_hung(ctx):
    kill = ctx.search('teuthology.log', r'test_pg_scrub:\s+kill_daemons \S+ KILL osd',
                      'test_pg_scrub', 'KILL osd')
    bounded = ctx.search('teuthology.log', r'test_pg_scrub:\s+WAIT_FOR_CLEAN_TIMEOUT=10',
                         'test_pg_scrub', 'WAIT_FOR_CLEAN_TIMEOUT=10')
    if kill and bounded and kill[0].line < bounded[0].line:
        return done('osd-killed-pg-query-no-timeout',
                    f'test_pg_scrub killed its only OSD{at(kill[0])} and ran pg_scrub again with '
                    'the timeouts of #74004, but is_pg_clean runs ceph pg query without a '
                    'timeout, so the query to the dead OSD never returned',
                    kill[0], bounded[0],
                    key='test_pg_scrub: ceph pg query hung after OSD kill',
                    command='ceph pg query')
    return unknown('none of the ceph pg query hangs this node knows')
