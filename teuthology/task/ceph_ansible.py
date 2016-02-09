import os

from cStringIO import StringIO

from . import ansible

from ..config import config as teuth_config


class CephAnsible(ansible.Ansible):
    name = 'ceph_ansible'

    _default_playbook = [
        dict(
            hosts='mons',
            become=True,
            roles=['ceph-mon'],
        ),
        dict(
            hosts='osds',
            become=True,
            roles=['ceph-osd'],
        ),
        dict(
            hosts='mdss',
            become=True,
            roles=['ceph-mds'],
        ),
        dict(
            hosts='rgws',
            become=True,
            roles=['ceph-rgw'],
        ),
        dict(
            hosts='restapis',
            become=True,
            roles=['ceph-restapi'],
        ),
    ]

    __doc__ = """
    A subclass of Ansible that defaults to:

    - ansible:
        repo: {git_base}ceph-ansible.git
        playbook: {playbook}

    It always uses a dynamic inventory.
    """.format(
        git_base=teuth_config.ceph_git_base_url,
        playbook=_default_playbook,
    )

    def __init__(self, ctx, config):
        config = config or dict()
        if 'playbook' not in config:
            config['playbook'] = self._default_playbook
        if 'repo' not in config:
            config['repo'] = os.path.join(teuth_config.ceph_git_base_url,
                                          'ceph-ansible.git')
        super(CephAnsible, self).__init__(ctx, config)

    def get_inventory(self):
        """
        Stub this method so we always generate the hosts file
        """
        pass

    def generate_hosts_file(self):
        groups_to_roles = dict(
            mons='mon',
            mdss='mds',
            osds='osd',
        )
        hosts_dict = dict()
        for group in sorted(groups_to_roles.keys()):
            role_prefix = groups_to_roles[group]
            want = lambda role: role.startswith(role_prefix)
            for (remote, roles) in self.cluster.only(want).remotes.iteritems():
                hostname = remote.hostname
                if group not in hosts_dict:
                    hosts_dict[group] = [hostname]
                elif hostname not in hosts_dict[group]:
                    hosts_dict[group].append(hostname)

        hosts_stringio = StringIO()
        for group in sorted(hosts_dict.keys()):
            hosts = hosts_dict[group]
            hosts_stringio.write('[%s]\n' % group)
            hosts_stringio.write('\n'.join(hosts))
            hosts_stringio.write('\n\n')
        hosts_stringio.seek(0)
        self.inventory = self._write_hosts_file(hosts_stringio.read().strip())
        self.generated_inventory = True

task = CephAnsible
