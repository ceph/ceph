"""
Role-based access permissions decorators
"""
import logging

from ..exceptions import PermissionNotValid
from ..security import Permission

logger = logging.getLogger(__name__)


def _set_func_permissions(func, permissions):
    if not isinstance(permissions, list):
        permissions = [permissions]

    for perm in permissions:
        if not Permission.valid_permission(perm):
            logger.debug("Invalid security permission: %s\n "
                         "Possible values: %s", perm,
                         Permission.all_permissions())
            raise PermissionNotValid(perm)

    # pylint: disable=protected-access
    if not hasattr(func, '_security_permissions'):
        func._security_permissions = permissions
    else:
        permissions.extend(func._security_permissions)
        func._security_permissions = list(set(permissions))


def ReadPermission(func):  # noqa: N802
    """
    :raises PermissionNotValid: If the permission is missing.
    """
    _set_func_permissions(func, Permission.READ)
    return func


def CreatePermission(func):  # noqa: N802
    """
    :raises PermissionNotValid: If the permission is missing.
    """
    _set_func_permissions(func, Permission.CREATE)
    return func


def DeletePermission(func):  # noqa: N802
    """
    :raises PermissionNotValid: If the permission is missing.
    """
    _set_func_permissions(func, Permission.DELETE)
    return func


def UpdatePermission(func):  # noqa: N802
    """
    :raises PermissionNotValid: If the permission is missing.
    """
    _set_func_permissions(func, Permission.UPDATE)
    return func
