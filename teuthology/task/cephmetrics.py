import logging
import os
import pexpect
import time

from teuthology.config import config as teuth_config
from teuthology.exceptions import CommandFailedError

from teuthology.ansible import Ansible, LoggerFile

log = logging.getLogger(__name__)


class CephMetrics(Ansible):
    def __init__(self, ctx, config):
        super(CephMetrics, self).__init__(ctx, config)
        if 'repo' not in self.config:
            self.config['repo'] = os.path.join(
                teuth_config.ceph_git_base_url, 'cephmetrics.git')
        if 'playbook' not in self.config:
            self.config['playbook'] = './ansible/playbook.yml'

    def get_inventory(self):
        return False

    def generate_inventory(self):
        groups_to_roles = {
            'mons': 'mon',
            'mgrs': 'mgr',
            'mdss': 'mds',
            'osds': 'osd',
            'rgws': 'rgw',
            'clients': 'client',
            'ceph-grafana': 'cephmetrics',
        }
        hosts_dict = dict()
        for group in sorted(groups_to_roles.keys()):
            role_prefix = groups_to_roles[group]
            want = lambda role: role.startswith(role_prefix)
            if group not in hosts_dict:
                hosts_dict[group] = dict(hosts=dict())
            group_dict = hosts_dict[group]['hosts']
            for (remote, roles) in self.cluster.only(want).remotes.items():
                hostname = remote.hostname
                group_dict[hostname] = dict(
                    ansible_user=remote.user,
                )
            hosts_dict[group]['hosts'] = group_dict
        # It might be preferable to use a YAML inventory file, but
        # that won't work until an ansible release is out with:
        # https://github.com/ansible/ansible/pull/30730
        # Once that is done, we can simply do this:
        # hosts_str = yaml.safe_dump(hosts_dict, default_flow_style=False)
        # And then pass suffix='.yml' to _write_hosts_file().
        hosts_lines = []
        for group in hosts_dict.keys():
            hosts_lines.append('[%s]' % group)
            for host, vars_ in hosts_dict[group]['hosts'].items():
                host_line = ' '.join(
                    [host] + map(
                        lambda tuple_: '='.join(tuple_),
                        vars_.items(),
                    )
                )
                hosts_lines.append(host_line)
            hosts_lines.append('')
        hosts_str = '\n'.join(hosts_lines)
        self.inventory = self._write_inventory_files(hosts_str)
        self.generated_inventory = True

    def begin(self):
        super(CephMetrics, self).begin()
        wait_time = 5 * 60
        self.log.info(
            "Waiting %ss for data collection before running tests...",
            wait_time,
        )
        time.sleep(wait_time)
        self.run_tests()

    def run_tests(self):
        self.log.info("Running tests...")
        command = "tox -e integration %s" % self.inventory
        out, status = pexpect.run(
            command,
            cwd=self.repo_path,
            logfile=LoggerFile(self.log.getChild('tests'), logging.INFO),
            withexitstatus=True,
            timeout=None,
        )
        if status != 0:
            raise CommandFailedError(command, status)


task = CephMetrics
