import logging
import requests
import os
import pexpect
import yaml

from cStringIO import StringIO
from tempfile import NamedTemporaryFile

from teuthology.config import config as teuth_config
from teuthology.exceptions import CommandFailedError
from teuthology.repo_utils import fetch_repo

from . import Task

log = logging.getLogger(__name__)


class LoggerFile(object):
    """
    A thin wrapper around a logging.Logger instance that provides a file-like
    interface.

    Used by Ansible.execute_playbook() when it calls pexpect.run()
    """
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, string):
        self.logger.log(self.level, string)

    def flush(self):
        pass


class Ansible(Task):
    """
    A task to run ansible playbooks

    Required configuration parameters:
        playbook:   Required; can either be a list of plays, or a path/URL to a
                    playbook. In the case of a path, it may be relative to the
                    repo's on-disk location (if a repo is provided), or
                    teuthology's working directory.

    Optional configuration parameters:
        repo:       A path or URL to a repo (defaults to '.'). Given a repo
                    value of 'foo', ANSIBLE_ROLES_PATH is set to 'foo/roles'
        branch:     If pointing to a remote git repo, use this branch. Defaults
                    to 'master'.
        hosts:      A list of teuthology roles or partial hostnames (or a
                    combination of the two). ansible-playbook will only be run
                    against hosts that match.
        inventory:  A path to be passed to ansible-playbook with the
                    --inventory-file flag; useful for playbooks that also have
                    vars they need access to. If this is not set, we check for
                    /etc/ansible/hosts and use that if it exists. If it does
                    not, we generate a temporary file to use.
        tags:       A string including any (comma-separated) tags to be passed
                    directly to ansible-playbook.

    Examples:

    tasks:
    - ansible:
        repo: https://github.com/ceph/ceph-cm-ansible.git
        playbook:
          - roles:
            - some_role
            - another_role
        hosts:
          - client.0
          - host1

    tasks:
    - ansible:
        repo: /path/to/repo
        inventory: /path/to/inventory
        playbook: /path/to/playbook.yml
        tags: my_tags

    """
    def __init__(self, ctx, config):
        super(Ansible, self).__init__(ctx, config)
        self.log = log
        self.generated_inventory = False
        self.generated_playbook = False

    def setup(self):
        super(Ansible, self).setup()
        self.find_repo()
        self.get_playbook()
        self.get_inventory() or self.generate_hosts_file()
        if not hasattr(self, 'playbook_file'):
            self.generate_playbook()

    def find_repo(self):
        """
        Locate the repo we're using; cloning it from a remote repo if necessary
        """
        repo = self.config.get('repo', '.')
        if repo.startswith(('http://', 'https://', 'git@', 'git://')):
            repo_path = fetch_repo(
                repo,
                self.config.get('branch', 'master'),
            )
        else:
            repo_path = os.path.abspath(os.path.expanduser(repo))
        self.repo_path = repo_path

    def get_playbook(self):
        """
        If necessary, fetch and read the playbook file
        """
        playbook = self.config['playbook']
        if isinstance(playbook, list):
            # Multiple plays in a list
            self.playbook = playbook
        elif isinstance(playbook, str) and playbook.startswith(('http://',
                                                               'https://')):
            response = requests.get(playbook)
            response.raise_for_status()
            self.playbook = yaml.safe_load(response.text)
        elif isinstance(playbook, str):
            try:
                playbook_path = os.path.expanduser(playbook)
                if not playbook_path.startswith('/'):
                    # If the path is not absolute at this point, look for the
                    # playbook in the repo dir. If it's not there, we assume
                    # the path is relative to the working directory
                    pb_in_repo = os.path.join(self.repo_path, playbook_path)
                    if os.path.exists(pb_in_repo):
                        playbook_path = pb_in_repo
                self.playbook_file = file(playbook_path)
                playbook_yaml = yaml.safe_load(self.playbook_file)
                self.playbook = playbook_yaml
            except Exception:
                log.error("Unable to read playbook file %s", playbook)
                raise
        else:
            raise TypeError(
                "playbook value must either be a list, URL or a filename")
        log.info("Playbook: %s", self.playbook)

    def get_inventory(self):
        """
        Determine whether or not we're using an existing inventory file
        """
        self.inventory = self.config.get('inventory')
        etc_ansible_hosts = '/etc/ansible/hosts'
        if self.inventory:
            self.inventory = os.path.expanduser(self.inventory)
        elif os.path.exists(etc_ansible_hosts):
            self.inventory = etc_ansible_hosts
        return self.inventory

    def generate_hosts_file(self):
        """
        Generate a hosts (inventory) file to use. This should not be called if
        we're using an existing file.
        """
        hosts = self.cluster.remotes.keys()
        hostnames = [remote.hostname for remote in hosts]
        hostnames.sort()
        hosts_str = '\n'.join(hostnames + [''])
        hosts_file = NamedTemporaryFile(prefix="teuth_ansible_hosts_",
                                        delete=False)
        hosts_file.write(hosts_str)
        hosts_file.flush()
        self.generated_inventory = True
        self.inventory = hosts_file.name

    def generate_playbook(self):
        """
        Generate a playbook file to use. This should not be called if we're
        using an existing file.
        """
        for play in self.playbook:
            # Ensure each play is applied to all hosts mentioned in the --limit
            # flag we specify later
            play['hosts'] = 'all'
        pb_buffer = StringIO()
        pb_buffer.write('---\n')
        yaml.safe_dump(self.playbook, pb_buffer)
        pb_buffer.seek(0)
        playbook_file = NamedTemporaryFile(prefix="teuth_ansible_playbook_",
                                           delete=False)
        playbook_file.write(pb_buffer.read())
        playbook_file.flush()
        self.playbook_file = playbook_file
        self.generated_playbook = True

    def begin(self):
        super(Ansible, self).begin()
        self.execute_playbook()

    def execute_playbook(self, _logfile=None):
        """
        Execute ansible-playbook

        :param _logfile: Use this file-like object instead of a LoggerFile for
                         testing
        """
        environ = os.environ
        environ['ANSIBLE_SSH_PIPELINING'] = '1'
        environ['ANSIBLE_ROLES_PATH'] = "%s/roles" % self.repo_path
        args = self._build_args()
        command = ' '.join(args)
        log.debug("Running %s", command)

        out_log = self.log.getChild('out')
        out, status = pexpect.run(
            command,
            logfile=_logfile or LoggerFile(out_log, logging.INFO),
            withexitstatus=True,
            timeout=None,
        )
        if status != 0:
            raise CommandFailedError(command, status)

    def _build_args(self):
        """
        Assemble the list of args to be executed
        """
        fqdns = [r.hostname for r in self.cluster.remotes.keys()]
        args = [
            'ansible-playbook', '-v',
            '-i', self.inventory,
            '--limit', ','.join(fqdns),
            self.playbook_file.name,
        ]
        tags = self.config.get('tags')
        if tags:
            args.extend(['--tags', tags])
        return args

    def teardown(self):
        if self.generated_inventory:
            os.remove(self.inventory)
        if self.generated_playbook:
            os.remove(self.playbook_file.name)
        super(Ansible, self).teardown()


class CephLab(Ansible):
    __doc__ = """
    A very simple subclass of Ansible that defaults to:

    - ansible:
        repo: {git_base}ceph-cm-ansible.git
        playbook: cephlab.yml
    """.format(git_base=teuth_config.ceph_git_base_url)

    def __init__(self, ctx, config):
        config = config or dict()
        if 'playbook' not in config:
            config['playbook'] = 'cephlab.yml'
        if 'repo' not in config:
            config['repo'] = os.path.join(teuth_config.ceph_git_base_url,
                                          'ceph-cm-ansible.git')
        super(CephLab, self).__init__(ctx, config)


task = Ansible
cephlab = CephLab
