"""
Component IOs
"""

import yaml
import logging
import os
import pwd
import time

from teuthology.orchestra import run

log = logging.getLogger(__name__)


cephqe_scripts = {
    "DIR": {"v1":
                {"script": "rgw/v1/tests/s3/",
                 "config": "rgw/v1/tests/s3/yamls"},
            "v2":
                {"script": "rgw/v2/tests/s3_swift/",
                 "config": "rgw/v2/tests/s3_swift/configs"}
            },
    "MASTER_BRANCH": "master",
    "REPO_NAME": "ceph-qe-scripts",
    "WIP_BRANCH": None
}


class rgw_ios:
    """
    RGW IOS using ceph-qe-scripts
    """

    def __init__(self, ctx, config):
        self.ctx = ctx
        self.config = config

    def __enter__(self):
        log.info('starting rgw-tests')
        log.info('config %s' % self.config)
        if self.config is None:
            self.config = {}
        assert isinstance(self.config, dict), \
            "task set-repo only supports a dictionary for configuration"
        config_file_name = self.config['test'] + ".yaml"
        log.info('test_version: %s' % self.config.get('test_version', 'v2'))
        log.info('test: %s' % self.config['test'])
        log.info('script: %s' % self.config.get('script', self.config['test'] + ".py"))
        time.sleep(2)
        test_root_dir = self.config['test'] + "_%s" % "_".join(str(time.time()).split("."))
        test_venv = os.path.join(test_root_dir, "venv")
        script = os.path.join(cephqe_scripts['REPO_NAME'],
                              cephqe_scripts['DIR'][self.config.get('test_version', 'v2')]['script'],
                              self.config.get('script', self.config['test'] + ".py"))
        config_file = os.path.join(cephqe_scripts['REPO_NAME'],
                                   cephqe_scripts['DIR'][self.config.get('test_version', 'v2')]['config'],
                                   config_file_name)
        log.info('script: %s' % script)
        log.info('config_file: %s' % config_file)
        self.soot = [test_venv, test_root_dir, 'io_info.yaml', '*.json', 'Download.*',
                     'Download', '*.mpFile', 'x*', 'key.*', 'Mp.*', '*.key.*']
        self.cleanup = lambda x: remote.run(args=[run.Raw('sudo rm -rf %s' % x)])
        log.info('listing all clients: %s' % self.config.get('clients'))
        for role in self.config.get('clients', ['client.0']):
            wip_branch = cephqe_scripts["WIP_BRANCH"]
            master_branch = cephqe_scripts["MASTER_BRANCH"]
            assert isinstance(role, basestring)
            prefix = 'client.'
            assert role.startswith(prefix)
            id_ = role[len(prefix):]
            (remote,) = self.ctx.cluster.only(role).remotes.iterkeys()
            map(self.cleanup, self.soot)
            remote.run(args=['mkdir', test_root_dir])
            log.info('cloning the repo to %s' % remote.hostname)
            remote.run(
                args=[
                    'cd',
                    '%s' % test_root_dir,
                    run.Raw(';'),
                    'git',
                    'clone',
                    'https://github.com/red-hat-storage/ceph-qe-scripts.git',
                    '-b',
                    '%s' % master_branch if wip_branch is None else wip_branch
                ])
            if self.config.get('config', None) is not None:
                test_config = {'config': self.config.get('config')}
                log.info('config: %s' % test_config)
                log.info('creating configuration from data: %s' % test_config)
                local_file = os.path.join('/tmp/',
                                          config_file_name +
                                          "_" + str(os.getpid()) +
                                          pwd.getpwuid(os.getuid()).pw_name)
                with open(local_file, 'w') as outfile:
                    outfile.write(yaml.dump(test_config, default_flow_style=False))
                out = remote.run(args=[run.Raw('sudo echo $HOME')],
                                 wait=False,
                                 stdout=run.PIPE)
                out = out.stdout.read().strip()
                conf_file = os.path.join(out, test_root_dir, config_file)
                log.info('local_file: %s' % local_file)
                log.info('config_file: %s' % conf_file)
                log.info('copying temp yaml to the client node')
                remote.put_file(local_file, conf_file)
                remote.run(args=['ls', '-lt', os.path.dirname(conf_file)])
                remote.run(args=['cat', conf_file])
                os.remove(local_file)
            remote.run(args=['python3', '-m', 'venv', test_venv])
            remote.run(
                args=[
                    'source',
                    '{}/bin/activate'.format(test_venv),
                    run.Raw(';'),
                    run.Raw('pip3 install boto boto3 names PyYaml ConfigParser python-swiftclient'),
                    run.Raw(';'),
                    'deactivate'])

            time.sleep(60)
            log.info('trying to restart rgw service after sleep 60 secs')
            out = remote.run(args=[run.Raw('sudo systemctl is-active ceph-radosgw.target')],
                             wait=False,
                             stdout=run.PIPE)
            try:
                out = out.stdout.read().strip()
            except AttributeError:
                out = "inactive"
            if "inactive" in out:
                log.info('Restarting RGW service')
                remote.run(args=[run.Raw('sudo systemctl restart ceph-radosgw.target')])
            log.info('starting the tests after sleep of 60 secs')
            time.sleep(60)
            remote.run(
                args=[run.Raw('sudo cd %s ' % test_root_dir)])
            remote.run(args=[
                run.Raw('cd %s; sudo venv/bin/python3 %s -c %s ' % (test_root_dir,
                                                                    script,
                                                                    config_file))])

    def __exit__(self):
        for role in self.config.get('clients', ['client.0']):
            (remote,) = self.ctx.cluster.only(role).remotes.iterkeys()
            log.info('Test completed')
            log.info("Deleting leftovers")
            map(self.cleanup, self.soot)
