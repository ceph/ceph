import logging
from ..misc import decanonicalize_hostname, get_distro, get_distro_version
from ..lockstatus import get_status

from .downburst import Downburst
from .openstack import ProvisionOpenStack


log = logging.getLogger(__name__)


def create_if_vm(ctx, machine_name, _downburst=None):
    """
    Use downburst to create a virtual machine

    :param _downburst: Only used for unit testing.
    """
    if _downburst:
        status_info = _downburst.status
    else:
        status_info = get_status(machine_name)
    if not status_info.get('is_vm', False):
        return False
    os_type = get_distro(ctx)
    os_version = get_distro_version(ctx)

    has_config = hasattr(ctx, 'config') and ctx.config is not None
    if has_config and 'downburst' in ctx.config:
        log.warning(
            'Usage of a custom downburst config has been deprecated.'
        )

    dbrst = _downburst or Downburst(name=machine_name, os_type=os_type,
                                    os_version=os_version, status=status_info)
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
        status_info = get_status(machine_name)
    if not status_info or not status_info.get('is_vm', False):
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
    if status_info.get('machine_type') == 'openstack':
        return ProvisionOpenStack().destroy(
            decanonicalize_hostname(machine_name))

    dbrst = _downburst or Downburst(name=machine_name, os_type=None,
                                    os_version=None, status=status_info)
    return dbrst.destroy()
