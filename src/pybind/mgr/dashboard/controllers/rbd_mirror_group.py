# -*- coding: utf-8 -*-

import json
import logging
from functools import partial
from typing import Optional

import rbd

from ..exceptions import DashboardException
from ..security import Scope
from ..services.exception import handle_rados_error, handle_rbd_error, serialize_dashboard_exception
from ..services.rbd import RbdMirroringService, rbd_call
from . import APIDoc, APIRouter, Endpoint, EndpointDoc, ReadPermission, \
    RESTController, Task, UpdatePermission, allow_empty_body

logger = logging.getLogger('controllers.rbd_mirror_group')


# pylint: disable=not-callable
def RbdMirrorGroupTask(name, metadata, wait_for):  # noqa: N802
    def composed_decorator(func):
        func = handle_rados_error('pool')(func)
        func = handle_rbd_error()(func)
        return Task("rbd/mirror_group/{}".format(name), metadata, wait_for,
                    partial(serialize_dashboard_exception, include_http_status=True))(func)
    return composed_decorator


RBD_MIRROR_GROUP_SCHEMA = {
    "group_name": (str, "Group name"),
    "group_id": (str, "Group ID"),
    "mirroring": ({
        "state": (str, "Mirror state"),
        "mode": (str, "Mirror mode"),
        "global_id": (str, "Global ID"),
        "primary": (bool, "Is primary")
    }, "Mirroring info")
}

RBD_MIRROR_GROUP_STATUS_SCHEMA = {
    "name": (str, "Group name"),
    "global_id": (str, "Global ID"),
    "state": (str, "Mirror state"),
    "description": (str, "Status description"),
    "last_update": (str, "Last update time"),
    "snapshots": ([{
        "name": (str, "Snapshot name"),
        "state": (str, "Snapshot state")
    }], "Group snapshots"),
    "peer_sites": ([{
        "site_name": (str, "Site name"),
        "mirror_uuid": (str, "Mirror UUID"),
        "state": (str, "Site state"),
        "description": (str, "Site description"),
        "last_update": (str, "Last update time"),
        "mirror_images": ([{
            "global_id": (str, "Image global ID"),
            "state": (str, "Image state"),
            "description": (str, "Image description"),
            "last_update": (str, "Last update time"),
            "up": (bool, "Is up")
        }], "Per-image replication status")
    }], "Peer sites")
}


@APIRouter('/block/mirroring/pool/{pool_name}/group', Scope.RBD_MIRRORING)
@APIDoc("RBD Mirror Group Management API", "RbdMirrorGroup")
class RbdMirrorGroup(RESTController):
    """
    Controller for RBD mirror group operations.
    Provides endpoints for enabling, disabling, promoting, demoting,
    resyncing, and getting status of mirror groups.
    """

    RESOURCE_ID = "group_name"

    @handle_rbd_error()
    @handle_rados_error('pool')
    @EndpointDoc("Get mirror group information",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'namespace': (str, 'Namespace (optional)'),
                     'group_name': (str, 'Group name')
                 },
                 responses={200: RBD_MIRROR_GROUP_SCHEMA})
    def get(self, pool_name: str, group_name: str, namespace: Optional[str] = None):
        """Get mirror group information.  Matches 'rbd group info --format json'."""
        def _get_info(ioctx):
            with rbd.Group(ioctx, group_name) as group:
                info = group.mirror_group_get_info()
                group_id = group.id()

            state_map = {
                rbd.RBD_MIRROR_GROUP_DISABLED: 'disabled',
                rbd.RBD_MIRROR_GROUP_ENABLED: 'enabled',
                rbd.RBD_MIRROR_GROUP_DISABLING: 'disabling'
            }
            mode_map = {
                rbd.RBD_MIRROR_IMAGE_MODE_JOURNAL: 'journal',
                rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT: 'snapshot'
            }

            return {
                'group_name': group_name,
                'group_id': group_id,
                'mirroring': {
                    'state': state_map.get(info.get('state'), 'unknown'),
                    'mode': mode_map.get(info.get('image_mode'), 'unknown'),
                    'global_id': info.get('global_id', ''),
                    'primary': info.get('primary', False)
                }
            }

        return rbd_call(pool_name, namespace, _get_info)

    @RbdMirrorGroupTask('enable', {
        'pool_name': '{pool_name}',
        'group_name': '{group_name}'
    }, 2.0)
    @allow_empty_body
    @EndpointDoc("Enable mirroring for a group",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)'),
                     'mode': (str, "Mirror mode (default: 'snapshot')")
                 })
    def set(self, pool_name: str, group_name: str, namespace: Optional[str] = None,
            mode: str = 'snapshot'):
        """
        Enable mirroring for a group.

        :param pool_name: Pool name
        :param group_name: Group name
        :param namespace: Namespace (optional)
        :param mode: Mirror mode (default: 'snapshot')
        """
        def _enable(ioctx):
            mode_map = {
                'journal': rbd.RBD_MIRROR_IMAGE_MODE_JOURNAL,
                'snapshot': rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT
            }

            if mode not in mode_map:
                raise DashboardException(
                    msg=f"Invalid mirror mode: {mode}",
                    code='invalid_mirror_mode',
                    http_status_code=400,
                    component='rbd'
                )

            with rbd.Group(ioctx, group_name) as group:
                group.mirror_group_enable(mode_map[mode])

        return rbd_call(pool_name, namespace, _enable)

    @Endpoint(method='POST', path='{group_name}/promote')
    @RbdMirrorGroupTask('promote', {
        'pool_name': '{pool_name}',
        'group_name': '{group_name}'
    }, 2.0)
    @UpdatePermission
    @EndpointDoc("Promote a mirror group to primary",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)'),
                     'force': (bool, 'Force promotion (optional)')
                 })
    def promote(self, pool_name: str, group_name: str, namespace: Optional[str] = None,
                force: bool = False):
        """
        Promote a group to primary for RBD mirroring.

        :param pool_name: Pool name
        :param group_name: Group name
        :param namespace: Namespace (optional)
        :param force: Force promotion even if not cleanly demoted
        """
        def _promote(ioctx):
            with rbd.Group(ioctx, group_name) as group:
                group.mirror_group_promote(force)

        return rbd_call(pool_name, namespace, _promote)

    @Endpoint(method='POST', path='{group_name}/demote')
    @RbdMirrorGroupTask('demote', {
        'pool_name': '{pool_name}',
        'group_name': '{group_name}'
    }, 2.0)
    @UpdatePermission
    @EndpointDoc("Demote a mirror group to non-primary",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)')
                 })
    def demote(self, pool_name: str, group_name: str, namespace: Optional[str] = None):
        """
        Demote a group to non-primary for RBD mirroring.

        :param pool_name: Pool name
        :param group_name: Group name
        :param namespace: Namespace (optional)
        """
        def _demote(ioctx):
            with rbd.Group(ioctx, group_name) as group:
                group.mirror_group_demote()

        return rbd_call(pool_name, namespace, _demote)

    @Endpoint(method='POST', path='{group_name}/resync')
    @RbdMirrorGroupTask('resync', {
        'pool_name': '{pool_name}',
        'group_name': '{group_name}'
    }, 2.0)
    @UpdatePermission
    @EndpointDoc("Force resync of a mirror group",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)')
                 })
    def resync(self, pool_name: str, group_name: str, namespace: Optional[str] = None):
        """
        Force resync to primary group for RBD mirroring.

        :param pool_name: Pool name
        :param group_name: Group name
        :param namespace: Namespace (optional)
        """
        def _resync(ioctx):
            with rbd.Group(ioctx, group_name) as group:
                group.mirror_group_resync()

        return rbd_call(pool_name, namespace, _resync)

    @Endpoint(method='GET', path='{group_name}/status')
    @ReadPermission
    @handle_rbd_error()
    @handle_rados_error('pool')
    @EndpointDoc("Get mirror group status",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)')
                 },
                 responses={200: RBD_MIRROR_GROUP_STATUS_SCHEMA})
    def status(self, pool_name: str, group_name: str, namespace: Optional[str] = None):
        """
        Get RBD mirroring status for a group.

        :param pool_name: Pool name
        :param group_name: Group name
        :param namespace: Namespace (optional)
        """
        # Maps rbd.MIRROR_IMAGE_STATUS_STATE_* int → human-readable string
        _MIRROR_STATE_MAP = {
            rbd.MIRROR_IMAGE_STATUS_STATE_UNKNOWN: 'unknown',
            rbd.MIRROR_IMAGE_STATUS_STATE_ERROR: 'error',
            rbd.MIRROR_IMAGE_STATUS_STATE_SYNCING: 'syncing',
            rbd.MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY: 'starting_replay',
            rbd.MIRROR_IMAGE_STATUS_STATE_REPLAYING: 'replaying',
            rbd.MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY: 'stopping_replay',
            rbd.MIRROR_IMAGE_STATUS_STATE_STOPPED: 'stopped',
        }

        # Maps rbd.RBD_GROUP_SNAP_STATE_* int → human-readable string
        _SNAP_STATE_MAP = {
            rbd.RBD_GROUP_SNAP_STATE_CREATING: 'creating',
            rbd.RBD_GROUP_SNAP_STATE_CREATED: 'created',
        }

        def _get_status(ioctx):
            with rbd.Group(ioctx, group_name) as group:
                status = group.mirror_group_get_global_status()
                # Collect group snapshots — matches 'rbd mirror group status --format json'
                raw_snaps = list(group.list_snaps())

            # Build mirror_uuid → site_name lookup from pool peer list
            rbd_inst = rbd.RBD()
            peer_name_map = {}
            try:
                for peer in rbd_inst.mirror_peer_list(ioctx):
                    peer_name_map[peer.get('mirror_uuid', '')] = peer.get('site_name', '')
            except Exception:  # pylint: disable=broad-except
                pass  # peer list is best-effort; status still valid without site names

            # Build snapshots list matching CLI output
            snapshots = [
                {
                    'name': s.get('name', ''),
                    'state': _SNAP_STATE_MAP.get(s.get('state', -1), 'unknown')
                }
                for s in raw_snaps
            ]

            # Determine top-level state from site statuses
            site_statuses = status.get('site_statuses', [])
            top_state = 'unknown'
            top_description = ''
            top_last_update = ''
            if site_statuses:
                first = site_statuses[0]
                top_state = _MIRROR_STATE_MAP.get(first.get('state', 0), 'unknown')
                top_description = first.get('description', '')
                top_last_update = str(first.get('last_update', ''))

            result = {
                'name': status.get('name', group_name),
                'global_id': status.get('info', {}).get('global_id', ''),
                'state': top_state,
                'description': top_description,
                'last_update': top_last_update,
                'snapshots': snapshots,
                'peer_sites': []
            }

            for site in site_statuses:
                mirror_uuid = site.get('mirror_uuid', '')
                # Build per-image replication status for this peer site
                mirror_images = [
                    {
                        'global_id': img.get('global_id', ''),
                        'state': _MIRROR_STATE_MAP.get(img.get('state', 0), 'unknown'),
                        'description': img.get('description', ''),
                        'last_update': str(img.get('last_update', '')),
                        'up': img.get('up', False)
                    }
                    for img in site.get('mirror_images', [])
                ]
                peer_site = {
                    'site_name': peer_name_map.get(mirror_uuid, ''),
                    'mirror_uuid': mirror_uuid,
                    'state': _MIRROR_STATE_MAP.get(site.get('state', 0), 'unknown'),
                    'description': site.get('description', ''),
                    'last_update': str(site.get('last_update', '')),
                    'mirror_images': mirror_images
                }
                result['peer_sites'].append(peer_site)

            return result

        return rbd_call(pool_name, namespace, _get_status)

    @Endpoint(method='POST', path='{group_name}/snapshot')
    @RbdMirrorGroupTask('snapshot', {
        'pool_name': '{pool_name}',
        'group_name': '{group_name}'
    }, 2.0)
    @UpdatePermission
    @EndpointDoc("Create a mirror group snapshot",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)'),
                     'flags': (int, 'Snapshot flags (optional)')
                 })
    def snapshot(self, pool_name: str, group_name: str, namespace: Optional[str] = None,
                 flags: int = 0):
        """
        Create RBD mirroring group snapshot.

        :param pool_name: Pool name
        :param group_name: Group name
        :param namespace: Namespace (optional)
        :param flags: Snapshot creation flags
        """
        def _create_snapshot(ioctx):
            with rbd.Group(ioctx, group_name) as group:
                snap_id = group.mirror_group_create_snapshot(flags)
                return {'snapshot_id': snap_id}

        return rbd_call(pool_name, namespace, _create_snapshot)

    @RbdMirrorGroupTask('disable', {
        'pool_name': '{pool_name}',
        'group_name': '{group_name}'
    }, 2.0)
    @EndpointDoc("Disable mirroring for a group",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)'),
                     'force': (bool, 'Force disable even if not primary')
                 })
    def delete(self, pool_name: str, group_name: str, namespace: Optional[str] = None,
               force: bool = False):
        """
        Disable mirroring for a group.

        :param pool_name: Pool name
        :param group_name: Group name
        :param namespace: Namespace (optional)
        :param force: Force disable even if not primary
        """
        def _disable(ioctx):
            with rbd.Group(ioctx, group_name) as group:
                group.mirror_group_disable(force)

        return rbd_call(pool_name, namespace, _disable)

    @Endpoint(method='GET', path='{group_name}/snapshot/schedule')
    @ReadPermission
    @handle_rbd_error()
    @handle_rados_error('pool')
    @EndpointDoc("List snapshot schedules for a mirror group",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)')
                 },
                 responses={200: [{
                     'spec': (str, 'Group level spec (pool/group or pool/namespace/group)'),
                     'schedule_interval': ([{
                         'interval': (str, 'Schedule interval'),
                         'start_time': (str, 'Start time (optional)')
                     }], 'Schedule intervals')
                 }]})
    def snapshot_schedule_list(self, pool_name: str, group_name: str,
                               namespace: Optional[str] = None):
        """List all snapshot schedules for the specified mirror group."""
        try:
            raw = RbdMirroringService.group_snapshot_schedule_list(
                pool_name, namespace, group_name
            )
            if not raw or len(raw) < 2 or not raw[1]:
                return []
            raw_dict = json.loads(raw[1])
            return RbdMirroringService.transform_schedule_list(raw_dict)
        except Exception as e:
            logger.exception("Failed to list group snapshot schedules")
            raise DashboardException(
                msg=f'Failed to list group snapshot schedules: {str(e)}',
                http_status_code=500,
                component='rbd_mirror_group'
            )

    @Endpoint(method='GET', path='{group_name}/snapshot/schedule/status')
    @ReadPermission
    @handle_rbd_error()
    @handle_rados_error('pool')
    @EndpointDoc("Get snapshot schedule status for a mirror group",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)')
                 },
                 responses={200: [{
                     'schedule_time': (str, 'Next scheduled snapshot time'),
                     'spec': (str, 'Group level spec (pool/group or pool/namespace/group)')
                 }]})
    def snapshot_schedule_status(self, pool_name: str, group_name: str,
                                 namespace: Optional[str] = None):
        """Get snapshot schedule status for the specified mirror group.

        Unwraps the 'scheduled_groups' wrapper to return an array directly,
        matching 'rbd mirror group snapshot schedule status'.
        """
        try:
            raw = RbdMirroringService.group_snapshot_schedule_status(
                pool_name, namespace, group_name
            )
            # raw is (rc, json_string, stderr) — unwrap to parsed JSON
            if not raw or len(raw) < 2 or not raw[1]:
                return []
            parsed = json.loads(raw[1])
            # rbd_support wraps the list in {"scheduled_groups": [...]}
            # unwrap and rename "group" key to "spec" for consistency
            # with the schedule list endpoint which also uses "spec"
            return [
                {'spec': g.get('group', ''),
                 'schedule_time': g.get('schedule_time', '')}
                for g in parsed.get('scheduled_groups', [])
            ]
        except Exception as e:
            logger.exception("Failed to get group snapshot schedule status")
            raise DashboardException(
                msg=f'Failed to get group snapshot schedule status: {str(e)}',
                http_status_code=500,
                component='rbd_mirror_group'
            )

    @Endpoint(method='GET', path='{group_name}/snapshot/schedule/info')
    @ReadPermission
    @handle_rbd_error()
    @handle_rados_error('pool')
    @EndpointDoc("Get combined snapshot schedule info for a mirror group",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)')
                 },
                 responses={200: {
                     'schedules': ([{
                         'interval': (str, 'Schedule interval'),
                         'start_time': (str, 'Start time (optional)')
                     }], 'Schedules'),
                     'status': ({
                         'scheduled_images': ([{
                             'image': (str, 'Image name'),
                             'schedule': (str, 'Schedule interval')
                         }], 'Scheduled images')
                     }, 'Status')
                 }})
    def snapshot_schedule_info(self, pool_name: str, group_name: str,
                               namespace: Optional[str] = None):
        """Get combined snapshot schedule info (list + status) for the specified mirror group."""
        try:
            info = RbdMirroringService.get_group_snapshot_schedule_info(
                pool_name, namespace, group_name
            )
            return info
        except Exception as e:
            logger.exception("Failed to get group snapshot schedule info")
            raise DashboardException(
                msg=f'Failed to get group snapshot schedule info: {str(e)}',
                http_status_code=500,
                component='rbd_mirror_group'
            )

    @Endpoint(method='POST', path='{group_name}/snapshot/schedule')
    @UpdatePermission
    @handle_rbd_error()
    @handle_rados_error('pool')
    @EndpointDoc("Add a snapshot schedule to a mirror group",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'namespace': (str, 'Namespace (optional)'),
                     'interval': (str, 'Schedule interval (e.g., 1m, 5m, 1h)'),
                     'start_time': (str, 'Start time (optional)')
                 },
                 responses={201: None})
    @allow_empty_body
    def snapshot_schedule_add(self, pool_name: str, group_name: str, interval: str,
                              namespace: Optional[str] = None, start_time: Optional[str] = None):
        """Add a snapshot schedule to the specified mirror group."""
        try:
            RbdMirroringService.group_snapshot_schedule_add(
                pool_name, namespace, group_name, interval, start_time
            )
            return None
        except Exception as e:
            logger.exception("Failed to add group snapshot schedule")
            raise DashboardException(
                msg=f'Failed to add group snapshot schedule: {str(e)}',
                http_status_code=500,
                component='rbd_mirror_group'
            )

    @Endpoint(method='DELETE', path='{group_name}/snapshot/schedule/{interval}')
    @UpdatePermission
    @handle_rbd_error()
    @handle_rados_error('pool')
    @EndpointDoc("Remove a snapshot schedule from a mirror group",
                 parameters={
                     'pool_name': (str, 'Pool name'),
                     'group_name': (str, 'Group name'),
                     'interval': (str, 'Schedule interval to remove'),
                     'namespace': (str, 'Namespace (optional)'),
                     'start_time': (str, 'Start time (optional)')
                 },
                 responses={204: None})
    def snapshot_schedule_remove(self, pool_name: str, group_name: str, interval: str,
                                 namespace: Optional[str] = None,
                                 start_time: Optional[str] = None):
        """Remove a snapshot schedule from the specified mirror group."""
        try:
            RbdMirroringService.group_snapshot_schedule_remove(
                pool_name, namespace, group_name, interval, start_time
            )
            return None
        except Exception as e:
            logger.exception("Failed to remove group snapshot schedule")
            raise DashboardException(
                msg=f'Failed to remove group snapshot schedule: {str(e)}',
                http_status_code=500,
                component='rbd_mirror_group'
            )

# Made with Bob
