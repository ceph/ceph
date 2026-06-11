"""RGW authorization utilities for SMB."""

from typing import List, Optional

import logging

from .proto import MonCommandIssuer

log = logging.getLogger(__name__)


class RGWAuthorizationGrantError(ValueError):
    pass


class RGWAuthorizer:
    """Using the rados APIs provided by the ceph mgr, authorize cephx users for
    RGW access via RADOS.
    """

    def __init__(self, mc: MonCommandIssuer) -> None:
        self._mc = mc

    def authorize_entity(
        self, entity: str, caps: Optional[List[str]] = None
    ) -> None:
        """Create or update a CephX entity with RGW access capabilities."""

        assert entity.startswith('client.')

        # Default caps for RGW access via RADOS
        # These allow the SMB daemon to access RGW data through RADOS
        if not caps:
            caps = [
                'mon',
                'allow r',
                'osd',
                'allow rwx tag rgw *=*',
            ]

        cmd = {
            'prefix': 'auth get-or-create',
            'entity': entity,
            'caps': caps,
        }
        log.info('Requesting RGW authorization: %r', cmd)
        ret, _, status = self._mc.mon_command(cmd)
        if ret != 0:
            raise RGWAuthorizationGrantError(status)
        log.info('RGW authorization request success: %r', status)
