import json
import logging
import requests
import os
import pexpect
import yaml
import shutil

from cStringIO import StringIO
from tempfile import NamedTemporaryFile

from teuthology.config import config as teuth_config
from teuthology.exceptions import CommandFailedError, AnsibleFailedError
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
        self.logger.log(self.level, string.decode('utf-8', 'ignore'))

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
        vars:       A dict of vars to be passed to ansible-playbook via the
                    --extra-vars flag
        cleanup:    If present, the given or generated playbook will be run
                    again during teardown with a 'cleanup' var set to True.
                    This will allow the playbook to clean up after itself,
                    if the playbook supports this feature.
        reconnect:  If set to True (the default), then reconnect to hosts after
                    ansible-playbook completes. This is in case the playbook
                    makes changes to the SSH configuration, or user accounts -
                    we would want to reflect those changes immediately.

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
        vars:
            var1: string_value
            var2:
                - list_item
            var3:
                key: value

    """
    # set this in subclasses to provide a group to
    # assign hosts to for dynamic inventory creation
    inventory_group = None

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

    @property
    def failure_log(self):
        if not hasattr(self, '_failure_log'):
            self._failure_log = NamedTemporaryFile(
                prefix="teuth_ansible_failures_",
                delete=False,
            )
        return self._failure_log

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
        inventory = []
        if self.inventory_group:
            inventory.append('[{0}]'.format(self.inventory_group))
        inventory.extend(hostnames + [''])
        hosts_str = '\n'.join(inventory)
        self.inventory = self._write_hosts_file(hosts_str)
        self.generated_inventory = True

    def _write_hosts_file(self, content):
        """
        Actually write the hosts file
        """
        hosts_file = NamedTemporaryFile(prefix="teuth_ansible_hosts_",
                                        delete=False)
        hosts_file.write(content)
        hosts_file.flush()
        return hosts_file.name

    def generate_playbook(self):
        """
        Generate a playbook file to use. This should not be called if we're
        using an existing file.
        """
        pb_buffer = StringIO()
        pb_buffer.write('---\n')
        yaml.safe_dump(self.playbook, pb_buffer)
        pb_buffer.seek(0)
        playbook_file = NamedTemporaryFile(
            prefix="teuth_ansible_playbook_",
            dir=self.repo_path,
            delete=False,
        )
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
        environ['ANSIBLE_FAILURE_LOG'] = self.failure_log.name
        environ['ANSIBLE_ROLES_PATH'] = "%s/roles" % self.repo_path
        args = self._build_args()
        command = ' '.join(args)
        log.debug("Running %s", command)

        out_log = self.log.getChild('out')
        out, status = pexpect.run(
            command,
            cwd=self.repo_path,
            logfile=_logfile or LoggerFile(out_log, logging.INFO),
            withexitstatus=True,
            timeout=None,
        )
        if status != 0:
            self._handle_failure(command, status)

        if self.config.get('reconnect', True) is True:
            remotes = self.cluster.remotes.keys()
            log.debug("Reconnecting to %s", remotes)
            for remote in remotes:
                remote.reconnect()

    def _handle_failure(self, command, status):
        failures = None
        with open(self.failure_log.name, 'r') as fail_log:
            try:
                failures = yaml.safe_load(fail_log)
            except yaml.parser.ParserError:
                log.error(
                    "Failed to parse ansible failure log: {0}".format(
                        self.failure_log.name,
                    )
                )
                failures = fail_log.read().replace('\n', '')

        if failures:
            self._archive_failures()
            raise AnsibleFailedError(failures)
        raise CommandFailedError(command, status)

    def _archive_failures(self):
        if self.ctx.archive:
            archive_path = "{0}/ansible_failures.yaml".format(self.ctx.archive)
            log.info("Archiving ansible failure log at: {0}".format(
                archive_path,
            ))
            shutil.move(
                self.failure_log.name,
                archive_path
            )
            os.chmod(archive_path, 0664)

    def _build_args(self):
        """
        Assemble the list of args to be executed
        """
        fqdns = [r.hostname for r in self.cluster.remotes.keys()]
        # Assume all remotes use the same username
        user = self.cluster.remotes.keys()[0].user
        extra_vars = dict(ansible_ssh_user=user)
        extra_vars.update(self.config.get('vars', dict()))
        args = [
            'ansible-playbook', '-v',
            "--extra-vars", "'%s'" % json.dumps(extra_vars),
            '-i', self.inventory,
            '--limit', ','.join(fqdns),
            self.playbook_file.name,
        ]
        tags = self.config.get('tags')
        if tags:
            args.extend(['--tags', tags])
        return args

    def teardown(self):
        self._cleanup()
        if self.generated_inventory:
            os.remove(self.inventory)
        if self.generated_playbook:
            os.remove(self.playbook_file.name)
        super(Ansible, self).teardown()

    def _cleanup(self):
        """
        If the ``cleanup`` key exists in config the same playbook will be
        run again during the teardown step with the var ``cleanup`` given with
        a value of ``True``.  If supported, this will allow the playbook to
        cleanup after itself during teardown.
        """
        if self.config.get("cleanup"):
            log.info("Running ansible cleanup...")
            extra = dict(cleanup=True)
            if self.config.get('vars'):
                self.config.get('vars').update(extra)
            else:
                self.config['vars'] = extra
            self.execute_playbook()
        else:
            log.info("Skipping ansible cleanup...")


class CephLab(Ansible):
    __doc__ = """
    A very simple subclass of Ansible that defaults to:

    - ansible:
        repo: {git_base}ceph-cm-ansible.git
        playbook: cephlab.yml

    If a dynamic inventory is used, all hosts will be assigned to the
    group 'testnodes'.
    """.format(git_base=teuth_config.ceph_git_base_url)

    # Set the name so that Task knows to look up overrides for
    # 'ansible.cephlab' instead of just 'cephlab'
    name = 'ansible.cephlab'
    inventory_group = 'testnodes'

    def __init__(self, ctx, config):
        config = config or dict()
        if 'playbook' not in config:
            config['playbook'] = 'cephlab.yml'
        if 'repo' not in config:
            config['repo'] = os.path.join(teuth_config.ceph_git_base_url,
                                          'ceph-cm-ansible.git')
        super(CephLab, self).__init__(ctx, config)

    def begin(self):
        # Emulate 'touch ~/.vault_pass.txt' to avoid ansible failing;
        # in almost all cases we don't need the actual vault password
        vault_pass_path = os.path.expanduser('~/.vault_pass.txt')
        if not os.path.exists(vault_pass_path):
            with open(vault_pass_path, 'a'):
                pass
        super(CephLab, self).begin()


task = Ansible
cephlab = CephLab
