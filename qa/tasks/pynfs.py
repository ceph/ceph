"""
run the pynfs NFSv4 server conformance tests against a ganesha export

pynfs talks NFSv4 to the server directly (no kernel mount), building its
own test tree under <pseudo_path>/pynfs-<version>. pynfs always exits 0,
so results are read from its JSON output: a test that fails and is not
listed in known_failures fails the task.

  - pynfs:
      client.0:
        cluster_id: <nfs cluster id>
        pseudo_path: <export pseudo path>
        repo: <pynfs git repo>
        sha1: <pynfs commit>
        timeout: 3600              # seconds, per version
        versions: ['4.0', '4.1', '4.2']  # the default
        tests:                     # test codes or flags, per version;
          '4.0': [all, ganesha]    # defaults to DEFAULT_TESTS
        known_failures:            # test codes, per version
          '4.0': []
          '4.1': []
          '4.2': []
"""

import json
import logging
import os
from io import StringIO

from teuthology import misc
from teuthology.orchestra import run
from teuthology.task import Task

log = logging.getLogger(__name__)

DEFAULT_REPO = 'git://git.linux-nfs.org/projects/cdmackay/pynfs.git'

# test codes or flags passed to testserver.py. 'ganesha' selects tests
# carrying Ganesha specific variants which 'all' leaves out; COPY5 (a
# synchronous NFSv4.2 COPY) is not part of 'all' either.
DEFAULT_TESTS = {
    '4.0': ['all', 'ganesha'],
    '4.1': ['all', 'ganesha'],
    '4.2': ['all', 'ganesha', 'COPY5'],
}

class Pynfs(Task):
    def __init__(self, ctx, config):
        super(Pynfs, self).__init__(ctx, config)
        self.log = log
        self.srcdirs = []

    def _server_address(self, cluster_id):
        first_mon = misc.get_first_mon(self.ctx, None)
        (mon0_remote,) = self.ctx.cluster.only(first_mon).remotes.keys()
        proc = mon0_remote.run(args=['ceph', 'nfs', 'cluster', 'info', cluster_id],
                               stdout=StringIO(), wait=True)
        backend = json.loads(proc.stdout.getvalue())[cluster_id]['backend'][0]
        return backend['ip'], backend['port']

    def _fetch(self, remote, srcdir, repo, sha1):
        remote.run(args=['sudo', 'rm', '-rf', srcdir])
        remote.run(args=['git', 'clone', repo, srcdir], timeout=600)
        remote.run(args=['git', '-C', srcdir, 'checkout', '-q', sha1])
        # setup.py builds each component by shelling out to python3 and
        # ignores failures, so a broken build only shows up as import
        # errors. Check that the testers load.
        remote.run(args=f'cd {srcdir} && python3 setup.py build', timeout=600)
        for tester in ('nfs4.0', 'nfs4.1'):
            remote.run(args=f'cd {srcdir}/{tester} && python3 testserver.py --showflags',
                       stdout=StringIO())

    def _run(self, remote, srcdir, url, version, tests, outfile, timeout):
        if version == '4.0':
            tester, minorversion = 'nfs4.0', []
        else:
            tester, minorversion = 'nfs4.1', ['--minorversion', version.split('.')[1]]
        remote.run(args=['cd', f'{srcdir}/{tester}', run.Raw('&&'),
                         'sudo', 'python3', 'testserver.py', url,
                         '--maketree', '--hidepass', '--json', outfile,
                         *minorversion, *tests],
                   timeout=timeout)
        return json.loads(remote.read_file(outfile, sudo=True))

    def begin(self):
        super(Pynfs, self).begin()

        if not isinstance(self.config, dict):
            raise ValueError(f"Invalid config object: {self.config} ({self.config.__class__})")
        client_roles = [r for r in self.config.keys() if 'client.' in r]
        clients = list(misc.get_clients(ctx=self.ctx, roles=client_roles))
        test_dir = misc.get_testdir(self.ctx)
        archive_dir = os.path.join(test_dir, 'archive')

        unexpected = {}
        for id_, remote in clients:
            entity = f'client.{id_}'
            client_config = self.config.get(entity) or {}
            log.info(f"{entity} config is {client_config}")

            cluster_id = client_config['cluster_id']
            pseudo_path = client_config['pseudo_path']
            repo = client_config.get('repo', DEFAULT_REPO)
            sha1 = client_config['sha1']
            timeout = client_config.get('timeout', 3600)
            versions = [str(v) for v in client_config.get('versions', DEFAULT_TESTS)]
            tests = dict(DEFAULT_TESTS, **(client_config.get('tests') or {}))
            known_failures = client_config.get('known_failures') or {}

            ip, port = self._server_address(cluster_id)
            srcdir = os.path.join(test_dir, f'pynfs.{id_}')
            self.srcdirs.append((remote, srcdir))
            self._fetch(remote, srcdir, repo, sha1)
            remote.run(args=['mkdir', '-p', archive_dir])

            for version in versions:
                url = f'{ip}:{port}{pseudo_path}/pynfs-{version}'
                if version == '4.0':
                    # the 4.0 tester makes its tree next to the test
                    # directory, keep both under pynfs-4.0
                    url += '/tmp'
                outfile = os.path.join(archive_dir, f'pynfs-{entity}-{version}.json')
                log.info(f'{entity}: running pynfs {version} tests {tests[version]} against {url}')
                results = self._run(remote, srcdir, url, version, tests[version],
                                    outfile, timeout)

                failed = sorted(t['code'] for t in results['testcase'] if 'failure' in t)
                known = set(known_failures.get(version) or [])
                log.info(f'{entity}: pynfs {version}: failed {failed}')
                for code in sorted(known - set(failed)):
                    log.info(f'{entity}: pynfs {version}: known failure {code} did not fail')
                new = [code for code in failed if code not in known]
                if new:
                    log.error(f'{entity}: pynfs {version}: unexpected failures: {new}')
                    unexpected[f'{entity} {version}'] = new

        if unexpected:
            raise RuntimeError(f'pynfs unexpected failures: {unexpected}')

    def end(self):
        super(Pynfs, self).end()
        # the testers run as root and leave root owned bytecode behind
        for remote, srcdir in self.srcdirs:
            remote.run(args=['sudo', 'rm', '-rf', srcdir])
        self.srcdirs = []

task = Pynfs
