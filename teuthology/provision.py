import logging
import os
import subprocess
import tempfile
import yaml

from .config import config
from .misc import decanonicalize_hostname, get_distro, get_distro_version
from .lockstatus import get_status


log = logging.getLogger(__name__)


def _get_downburst_exec():
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
        pth = os.path.expanduser("~%s/src/downburst/virtualenv/bin/downburst"
                                 % user)
        if os.access(pth, os.X_OK):
            return pth
    return ''


def create_if_vm(ctx, machine_name):
    """
    Use downburst to create a virtual machine
    """
    status_info = get_status(machine_name)
    if not status_info.get('is_vm', False):
        return False
    phys_host = decanonicalize_hostname(status_info['vm_host']['name'])
    os_type = get_distro(ctx)
    os_version = get_distro_version(ctx)

    createMe = decanonicalize_hostname(machine_name)
    with tempfile.NamedTemporaryFile() as tmp:
        has_config = hasattr(ctx, 'config') and ctx.config is not None
        if has_config and 'downburst' in ctx.config:
            log.warning(
                'Usage of a custom downburst config has been deprecated.'
            )

        log.info("Provisioning a {distro} {distroversion} vps".format(
            distro=os_type,
            distroversion=os_version
        ))

        file_info = {
            'disk-size': '100G',
            'ram': '1.9G',
            'cpus': 1,
            'networks': [
                {'source': 'front', 'mac': status_info['mac_address']}],
            'distro': os_type.lower(),
            'distroversion': os_version,
            'additional-disks': 3,
            'additional-disks-size': '200G',
            'arch': 'x86_64',
        }
        fqdn = machine_name.split('@')[1]
        file_out = {'downburst': file_info, 'local-hostname': fqdn}
        yaml.safe_dump(file_out, tmp)
        metadata = "--meta-data=%s" % tmp.name
        dbrst = _get_downburst_exec()
        if not dbrst:
            log.error("No downburst executable found.")
            return False
        p = subprocess.Popen([dbrst, '-c', phys_host,
                              'create', metadata, createMe],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
        owt, err = p.communicate()
        if err:
            log.info("Downburst completed on %s: %s" %
                     (machine_name, err))
        else:
            log.info("%s created: %s" % (machine_name, owt))
        # If the guest already exists first destroy then re-create:
        if 'exists' in err:
            log.info("Guest files exist. Re-creating guest: %s" %
                     (machine_name))
            destroy_if_vm(ctx, machine_name)
            create_if_vm(ctx, machine_name)
    return True


def destroy_if_vm(ctx, machine_name, user=None):
    """
    Use downburst to destroy a virtual machine

    Return False only on vm downburst failures.
    """
    status_info = get_status(machine_name)
    if not status_info or not status_info.get('is_vm', False):
        return True
    if user is not None and user != status_info['locked_by']:
        log.error("Tried to destroy {node} as {as_user} but it is locked by {locked_by}".format(
            node=machine_name, as_user=user, locked_by=status_info['locked_by']))
        return False
    phys_host = decanonicalize_hostname(status_info['vm_host']['name'])
    destroyMe = decanonicalize_hostname(machine_name)
    dbrst = _get_downburst_exec()
    if not dbrst:
        log.error("No downburst executable found.")
        return False
    p = subprocess.Popen([dbrst, '-c', phys_host,
                          'destroy', destroyMe],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    owt, err = p.communicate()
    if err:
        log.error(err)
        return False
    else:
        log.info("%s destroyed: %s" % (machine_name, owt))
    return True
