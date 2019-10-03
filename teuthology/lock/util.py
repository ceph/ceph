import datetime
import json
import logging

from teuthology import misc
import teuthology.provision.downburst

log = logging.getLogger(__name__)


def vps_version_or_type_valid(machine_type, os_type, os_version):
    """
    Check os-type and os-version parameters when locking a vps.
    Os-type will always be set (defaults to ubuntu).

    In the case where downburst does not handle list-json (an older version
    of downburst, for instance), a message is printed and this checking
    is skipped (so that this code should behave as it did before this
    check was added).
    """
    if not machine_type == 'vps':
        return True
    if os_type is None or os_version is None:
        # we'll use the defaults provided by provision.create_if_vm
        # later on during provisioning
        return True
    valid_os_and_version = \
        teuthology.provision.downburst.get_distro_from_downburst()
    if os_type not in valid_os_and_version:
        log.error("os-type '%s' is invalid. Try one of: %s",
                  os_type,
                  ', '.join(valid_os_and_version.keys()))
        return False
    if not validate_distro_version(os_version,
                                   valid_os_and_version[os_type]):
        log.error(
            "os-version '%s' is invalid for os-type '%s'. Try one of: %s",
            os_version,
            os_type,
            ', '.join(valid_os_and_version[os_type]))
        return False
    return True


def validate_distro_version(version, supported_versions):
    """
    Return True if the version is valid.  For Ubuntu, possible
    supported version values are of the form '12.04 (precise)' where
    either the number of the version name is acceptable.
    """
    if version in supported_versions:
        return True
    for parts in supported_versions:
        part = parts.split('(')
        if len(part) == 2:
            if version == part[0]:
                return True
            if version == part[1][0:len(part[1])-1]:
                return True


def json_matching_statuses(json_file_or_str, statuses):
    """
    Filter statuses by json dict in file or fragment; return list of
    matching statuses.  json_file_or_str must be a file containing
    json or json in a string.
    """
    try:
        open(json_file_or_str, 'r')
    except IOError:
        query = json.loads(json_file_or_str)
    else:
        query = json.load(json_file_or_str)

    if not isinstance(query, dict):
        raise RuntimeError('--json-query must be a dict')

    return_statuses = list()
    for status in statuses:
        for k, v in query.items():
            if not misc.is_in_dict(k, v, status):
                break
        else:
            return_statuses.append(status)

    return return_statuses


def winnow(statuses, arg, status_key, func=None):
    """
    Call with a list of statuses, and the ctx.<key>
    'arg' that you may want to filter by.
    If arg is not None, filter statuses by either:

    1) func=None: filter by status[status_key] == arg
    remove any status that fails

    2) func=<filter function that takes status>: remove any
    status for which func returns False

    Return the possibly-smaller set of statuses.
    """

    if arg is not None:
        if func:
            statuses = [_status for _status in statuses
                        if func(_status)]
        else:
            statuses = [_status for _status in statuses
                       if _status[status_key] == arg]

    return statuses


def locked_since_seconds(node):
    now = datetime.datetime.now()
    since = datetime.datetime.strptime(
        node['locked_since'], '%Y-%m-%d %H:%M:%S.%f')
    return (now - since).total_seconds()


