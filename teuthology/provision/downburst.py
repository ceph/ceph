import json
import logging
import os
import subprocess
import tempfile
import yaml

from teuthology.config import config
from teuthology.contextutil import safe_while
from teuthology.misc import decanonicalize_hostname
from teuthology.lock import query

log = logging.getLogger(__name__)


def downburst_executable():
    """
    First check for downburst in the user's path.
    Then check in ~/src, ~ubuntu/src, and ~teuthology/src.
    Return '' if no executable downburst is found.
    """
    if config.downburst:
        return config.downburst
    path = os.environ.get('PATH', None)
    if path:
        for p in os.environ.get('PATH', '').split(os.pathsep):
            pth = os.path.join(p, 'downburst')
            if os.access(pth, os.X_OK):
                return pth
    import pwd
    little_old_me = pwd.getpwuid(os.getuid()).pw_name
    for user in [little_old_me, 'ubuntu', 'teuthology']:
        pth = os.path.expanduser(
            "~%s/src/downburst/virtualenv/bin/downburst" % user)
        if os.access(pth, os.X_OK):
            return pth
    return ''


class Downburst(object):
    """
    A class that provides methods for creating and destroying virtual machine
    instances using downburst: https://github.com/ceph/downburst
    """
    def __init__(self, name, os_type, os_version, status=None, user='ubuntu',
                 logfile=None):
        self.name = name
        self.shortname = decanonicalize_hostname(self.name)
        self.os_type = os_type
        self.os_version = os_version
        self.status = status or query.get_status(self.name)
        self.config_path = None
        self.user_path = None
        self.user = user
        self.logfile = logfile
        self.host = decanonicalize_hostname(self.status['vm_host']['name'])
        self.executable = downburst_executable()

    def create(self):
        """
        Launch a virtual machine instance.

        If creation fails because an instance with the specified name is
        already running, first destroy it, then try again. This process will
        repeat two more times, waiting 60s between tries, before giving up.
        """
        if not self.executable:
            log.error("No downburst executable found.")
            return False
        self.build_config()
        success = None
        with safe_while(sleep=60, tries=3,
                        action="downburst create") as proceed:
            while proceed():
                (returncode, stdout, stderr) = self._run_create()
                log.info(stdout)
                log.info(stderr)
                if returncode == 0:
                    log.info("Downburst created %s: %s" % (self.name,
                                                           stdout.strip()))
                    success = True
                    break
                elif stderr:
                    # If the guest already exists first destroy then re-create:
                    if 'exists' in stderr:
                        success = False
                        log.info("Guest files exist. Re-creating guest: %s" %
                                 (self.name))
                        self.destroy()
                    else:
                        success = False
                        log.info("Downburst failed on %s: %s" % (
                            self.name, stderr.strip()))
                        break
            return success

    def _run_create(self):
        """
        Used by create(), this method is what actually calls downburst when
        creating a virtual machine instance.
        """
        if not self.config_path:
            raise ValueError("I need a config_path!")
        if not self.user_path:
            raise ValueError("I need a user_path!")

        args = [self.executable, '-v', '-c', self.host]
        if self.logfile:
            args.extend(['-l', self.logfile])
        args.extend([
            'create',
            '--wait',
            '--meta-data=%s' % self.config_path,
            '--user-data=%s' % self.user_path,
            self.shortname,
        ])
        log.info("Provisioning a {distro} {distroversion} vps".format(
            distro=self.os_type,
            distroversion=self.os_version
        ))
        log.debug(args)
        proc = subprocess.Popen(args, universal_newlines=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        return (proc.returncode, out, err)

    def destroy(self):
        """
        Destroy (shutdown and delete) a virtual machine instance.
        """
        executable = self.executable
        if not executable:
            log.error("No downburst executable found.")
            return False
        args = [executable, '-v', '-c', self.host]
        if self.logfile:
            args.extend(['-l', self.logfile])
        args.extend(['destroy', self.shortname])
        log.debug(args)
        proc = subprocess.Popen(args, universal_newlines=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,)
        out, err = proc.communicate()
        log.info(out)
        log.info(err)
        if proc.returncode != 0:
            not_found_msg = "no domain with matching name '%s'" % self.shortname
            if not_found_msg in err:
                log.warn("Ignoring error during destroy: %s", err)
                return True
            log.error("Error destroying %s: %s", self.name, err)
            return False
        else:
            out_str = ': %s' % out if out else ''
            log.info("Destroyed %s%s" % (self.name, out_str))
            return True

    def build_config(self):
        """
        Assemble a configuration to pass to downburst, and write it to a file.
        """
        config_fd = tempfile.NamedTemporaryFile(delete=False, mode='wt')

        os_type = self.os_type.lower()
        mac_address = self.status['mac_address']

        cpus = int(os.environ.get('DOWNBURST_CPUS', 1))
        ram_size = os.environ.get('DOWNBURST_RAM_SIZE', '3.8G')
        disk_size = os.environ.get('DOWNBURST_DISK_SIZE', '100G')
        extra_disk_size = os.environ.get('DOWNBURST_EXTRA_DISK_SIZE', '100G')
        extra_disk_number = int(os.environ.get('DOWNBURST_EXTRA_DISK_NUMBER', 4))
        file_info = {
            'disk-size': disk_size,
            'ram': ram_size,
            'cpus': cpus,
            'networks': [
                {'source': 'front', 'mac': mac_address}],
            'distro': os_type,
            'distroversion': self.os_version,
            'additional-disks': extra_disk_number,
            'additional-disks-size': extra_disk_size,
            'arch': 'x86_64',
        }
        fqdn = self.name.split('@')[1]
        file_out = {
            'downburst': file_info,
            'local-hostname': fqdn,
        }
        yaml.safe_dump(file_out, config_fd)
        self.config_path = config_fd.name

        user_info = {
            'user': self.user,
            # Remove the user's password so console logins are possible
            'runcmd': [
                ['passwd', '-d', self.user],
            ]
        }
        # Install git on downbursted VMs to clone upstream linux-firmware.
        # Issue #17154
        if 'packages' not in user_info:
            user_info['packages'] = list()
        user_info['packages'].extend([
            'git',
            'wget',
        ])
        # On CentOS/RHEL/Fedora, write the correct mac address and
        # install redhab-lsb-core for `lsb_release`
        if os_type in ['centos', 'rhel', 'fedora']:
            user_info['runcmd'].extend([
                ['sed', '-ie', 's/HWADDR=".*"/HWADDR="%s"/' % mac_address,
                 '/etc/sysconfig/network-scripts/ifcfg-eth0'],
            ])
            user_info['packages'].append('redhat-lsb-core')
        # On Ubuntu, starting with 16.04, and Fedora, starting with 24, we need
        # to install 'python' to get python2.7, which ansible needs
        if os_type in ('ubuntu', 'fedora'):
            user_info['packages'].append('python')
        user_fd = tempfile.NamedTemporaryFile(delete=False, mode='wt')
        user_str = "#cloud-config\n" + yaml.safe_dump(user_info)
        user_fd.write(user_str)
        self.user_path = user_fd.name
        return True

    def remove_config(self):
        """
        Remove the downburst configuration file created by build_config()
        """
        if self.config_path and os.path.exists(self.config_path):
            os.remove(self.config_path)
            self.config_path = None
            return True
        if self.user_path and os.path.exists(self.user_path):
            os.remove(self.user_path)
            self.user_path = None
            return True
        return False

    def __del__(self):
        self.remove_config()


def get_distro_from_downburst():
    """
    Return a table of valid distros.

    If downburst is in path use it.  If either downburst is unavailable,
    or if downburst is unable to produce a json list, then use a default
    table.
    """
    default_table = {u'rhel_minimal': [u'6.4', u'6.5'],
                     u'fedora': [u'17', u'18', u'19', u'20', u'22'],
                     u'centos': [u'6.3', u'6.4', u'6.5', u'7.0',
				 u'7.2'],
                     u'centos_minimal': [u'6.4', u'6.5'],
                     u'ubuntu': [u'8.04(hardy)', u'9.10(karmic)',
                                 u'10.04(lucid)', u'10.10(maverick)',
                                 u'11.04(natty)', u'11.10(oneiric)',
                                 u'12.04(precise)', u'12.10(quantal)',
                                 u'13.04(raring)', u'13.10(saucy)',
                                 u'14.04(trusty)', u'utopic(utopic)',
                                 u'16.04(xenial)'],
                     u'sles': [u'11-sp2'],
                     u'debian': [u'6.0', u'7.0', u'8.0']}
    executable_cmd = downburst_executable()
    if not executable_cmd:
        log.warn("Downburst not found!")
        log.info('Using default values for supported os_type/os_version')
        return default_table
    try:
        output = subprocess.check_output([executable_cmd, 'list-json'])
        downburst_data = json.loads(output)
        return downburst_data
    except (subprocess.CalledProcessError, OSError):
        log.exception("Error calling downburst!")
        log.info('Using default values for supported os_type/os_version')
        return default_table
