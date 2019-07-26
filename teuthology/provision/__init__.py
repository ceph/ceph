import logging

import teuthology.lock.query
from teuthology.misc import decanonicalize_hostname, get_distro, get_distro_version

from teuthology.provision import cloud
from teuthology.provision import downburst
from teuthology.provision import fog
from teuthology.provision import openstack
from teuthology.provision import pelagos
import os

log = logging.getLogger(__name__)


def _logfile(ctx, shortname):
    if hasattr(ctx, 'config') and ctx.config.get('archive_path'):
        return os.path.join(ctx.config['archive_path'],
                            shortname + '.downburst.log')

def get_reimage_types():
    return pelagos.get_types() + fog.get_types()

def reimage(ctx, machine_name, machine_type):
    os_type = get_distro(ctx)
    os_version = get_distro_version(ctx)

    pelagos_types = pelagos.get_types()
    fog_types = fog.get_types()
    if machine_type in pelagos_types and machine_type in fog_types:
        raise Exception('machine_type can be used with one provisioner only')
    elif machine_type in pelagos_types:
        obj = pelagos.Pelagos(machine_name, os_type, os_version)
    elif machine_type in fog_types:
        obj = fog.FOG(machine_name, os_type, os_version)
    else:
        raise Exception("The machine_type '%s' is not known to any "
                        "of configured provisioners" % machine_type)
    return obj.create()


def create_if_vm(ctx, machine_name, _downburst=None):
    """
    Use downburst to create a virtual machine

    :param _downburst: Only used for unit testing.
    """
    if _downburst:
        status_info = _downburst.status
    else:
        status_info = teuthology.lock.query.get_status(machine_name)
    shortname = decanonicalize_hostname(machine_name)
    machine_type = status_info['machine_type']
    os_type = get_distro(ctx)
    os_version = get_distro_version(ctx)
    if not teuthology.lock.query.is_vm(status=status_info):
        return False

    if machine_type in cloud.get_types():
        return cloud.get_provisioner(
            machine_type,
            shortname,
            os_type,
            os_version,
            conf=getattr(ctx, 'config', dict()),
        ).create()

    has_config = hasattr(ctx, 'config') and ctx.config is not None
    if has_config and 'downburst' in ctx.config:
        log.warning(
            'Usage of a custom downburst config has been deprecated.'
        )

    dbrst = _downburst or \
        downburst.Downburst(name=machine_name, os_type=os_type,
                            os_version=os_version, status=status_info,
                            logfile=_logfile(ctx, shortname))
    return dbrst.create()


def destroy_if_vm(ctx, machine_name, user=None, description=None,
                  _downburst=None):
    """
    Use downburst to destroy a virtual machine

    Return False only on vm downburst failures.

    :param _downburst: Only used for unit testing.
    """
    if _downburst:
        status_info = _downburst.status
    else:
        status_info = teuthology.lock.query.get_status(machine_name)
    if not status_info or not teuthology.lock.query.is_vm(status=status_info):
        return True
    if user is not None and user != status_info['locked_by']:
        msg = "Tried to destroy {node} as {as_user} but it is locked " + \
            "by {locked_by}"
        log.error(msg.format(node=machine_name, as_user=user,
                             locked_by=status_info['locked_by']))
        return False
    if (description is not None and description !=
            status_info['description']):
        msg = "Tried to destroy {node} with description {desc_arg} " + \
            "but it is locked with description {desc_lock}"
        log.error(msg.format(node=machine_name, desc_arg=description,
                             desc_lock=status_info['description']))
        return False
    machine_type = status_info.get('machine_type')
    shortname = decanonicalize_hostname(machine_name)
    if machine_type == 'openstack':
        return openstack.ProvisionOpenStack().destroy(shortname)
    elif machine_type in cloud.get_types():
        return cloud.get_provisioner(
            machine_type, shortname, None, None).destroy()

    dbrst = _downburst or \
        downburst.Downburst(name=machine_name, os_type=None,
                            os_version=None, status=status_info,
                            logfile=_logfile(ctx, shortname))
    return dbrst.destroy()
