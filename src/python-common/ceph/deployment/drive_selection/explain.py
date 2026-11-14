"""Explain why devices are, or are not, selected by an OSD spec.

``explain_drive_selection()`` runs the regular ``DriveSelection`` (which
remains the single source of truth for what gets deployed) and, for each
device and each device role defined in the spec, reports:

* whether the device was selected for that role,
* the reason why it was not (unavailable, used by another spec, taken by
  another role, limit reached, filter mismatch, ...),
* the outcome of *every* filter criterion, with the expected and the
  actual value, without short-circuiting on the first mismatch.
"""
from typing import Any, Dict, List, Optional, Tuple

from ..drive_group import DriveGroupSpec, DriveGroupValidationError
from ..inventory import Device
from .filter import FilterGenerator
from .matchers import MatchEvaluation, MATCH
from .selector import DriveSelection

# Roles in the order DriveSelection assigns them: a device selected for a
# role is not available to the following ones.
ROLES = ['data_devices', 'wal_devices', 'db_devices', 'journal_devices']

SELECTED = 'selected'
NOT_IN_PATHS = 'not_in_paths'
BEING_REPLACED = 'being_replaced'
UNAVAILABLE = 'unavailable'
OTHER_SPEC = 'other_spec'
TAKEN_BY_ROLE = 'taken_by_role'
FILTER_MISMATCH = 'filter_mismatch'
LIMIT_REACHED = 'limit_reached'
SELECTION_ERROR = 'selection_error'


class RoleEvaluation(object):
    def __init__(self,
                 role: str,
                 selected: bool,
                 reason: str,
                 detail: Optional[str] = None,
                 filters_match: Optional[bool] = None,
                 criteria: Optional[List[MatchEvaluation]] = None) -> None:
        self.role = role
        self.selected = selected
        self.reason = reason
        self.detail = detail
        #: outcome of the filters alone, regardless of availability etc.
        #: None when the role uses explicit paths
        self.filters_match = filters_match
        self.criteria = criteria or []

    def to_json(self) -> Dict[str, Any]:
        return {
            'role': self.role,
            'selected': self.selected,
            'reason': self.reason,
            'detail': self.detail,
            'filters_match': self.filters_match,
            'criteria': [c.to_json() for c in self.criteria],
        }


class DeviceExplanation(object):
    def __init__(self, device: Device, roles: List[RoleEvaluation]) -> None:
        self.device = device
        self.roles = roles

    def to_json(self) -> Dict[str, Any]:
        d = self.device
        sys_api = d.sys_api or {}
        return {
            'path': d.path,
            'type': d.human_readable_type,
            'size': sys_api.get('human_readable_size'),
            'size_bytes': sys_api.get('size'),
            'model': sys_api.get('model'),
            'vendor': sys_api.get('vendor'),
            'rotational': sys_api.get('rotational'),
            'available': d.available,
            'rejected_reasons': d.rejected_reasons,
            'ceph_device_lvm': bool(d.ceph_device_lvm),
            'roles': [r.to_json() for r in self.roles],
        }


class RoleSummary(object):
    def __init__(self, role: str, selected: List[str], explicit_paths: bool,
                 unsatisfied: bool) -> None:
        self.role = role
        self.selected = selected
        self.explicit_paths = explicit_paths
        #: role defined in the spec but no device selected while new
        #: data devices are: OSDs would be created without this role.
        self.unsatisfied = unsatisfied

    def to_json(self) -> Dict[str, Any]:
        return {
            'role': self.role,
            'selected': self.selected,
            'explicit_paths': self.explicit_paths,
            'unsatisfied': self.unsatisfied,
        }


class SelectionExplanation(object):
    def __init__(self,
                 spec: DriveGroupSpec,
                 devices: List[DeviceExplanation],
                 roles: List[RoleSummary],
                 error: Optional[str] = None,
                 notes: Optional[List[str]] = None) -> None:
        self.spec = spec
        self.devices = devices
        self.roles = roles
        self.error = error
        self.notes = notes or []

    @property
    def new_data_devices(self) -> List[str]:
        return [d.device.path for d in self.devices
                for r in d.roles
                if r.role == 'data_devices' and r.selected and not d.device.ceph_device_lvm]

    def to_json(self) -> Dict[str, Any]:
        return {
            'filter_logic': self.spec.filter_logic,
            'error': self.error,
            'notes': self.notes,
            'roles': [r.to_json() for r in self.roles],
            'new_data_devices': self.new_data_devices,
            'devices': [d.to_json() for d in self.devices],
        }


def _other_spec_affinity(disk: Device, spec: DriveGroupSpec) -> Optional[str]:
    # mirrors DriveSelection.assign_devices()
    if disk.available or not disk.ceph_device_lvm or not disk.lvs:
        return None
    for lv in disk.lvs:
        affinity = lv.get('osdspec_affinity')
        if affinity is not None and affinity != str(spec.service_id):
            return affinity
    return None


def _evaluate_role(spec: DriveGroupSpec,
                   role: str,
                   disk: Device,
                   selected: Optional[Dict[str, List[str]]]) -> RoleEvaluation:
    ds = getattr(spec, role)

    if ds.paths:
        paths = [p.path for p in ds.paths]
        if disk.path in paths:
            return RoleEvaluation(role, True, SELECTED, 'listed in paths')
        return RoleEvaluation(role, False, NOT_IN_PATHS, 'role uses explicit paths')

    criteria = [m.evaluate(disk) for m in FilterGenerator(ds)]
    results = [c.result == MATCH for c in criteria]
    filters_match = all(results) if spec.filter_logic == 'AND' else any(results)

    def ev(is_selected: bool, reason: str, detail: Optional[str] = None) -> RoleEvaluation:
        return RoleEvaluation(role, is_selected, reason, detail, filters_match, criteria)

    if selected is None:
        return ev(False, SELECTION_ERROR)
    if disk.path in selected[role]:
        return ev(True, SELECTED,
                  'already used by an OSD of this spec' if disk.ceph_device_lvm else None)
    # What prevented the selection, besides the filters. The filters express
    # what the role is meant to select, so a filter mismatch is reported
    # first and the other obstacle, if any, as its detail.
    blocker: Optional[Tuple[str, Optional[str]]] = None
    affinity = _other_spec_affinity(disk, spec)
    taken_by = [r for r in ROLES[:ROLES.index(role)] if disk.path in selected.get(r, [])]
    if disk.being_replaced:
        blocker = (BEING_REPLACED, None)
    elif not disk.available and not disk.ceph_device_lvm:
        blocker = (UNAVAILABLE, ', '.join(disk.rejected_reasons) or None)
    elif affinity:
        blocker = (OTHER_SPEC, f'used by osd.{affinity}')
    elif taken_by:
        blocker = (TAKEN_BY_ROLE, f'selected as {taken_by[0]}')

    if not filters_match:
        detail = None
        if blocker:
            reason, reason_detail = blocker
            detail = 'also ' + reason.replace('_', ' ')
            if reason_detail:
                detail += f': {reason_detail}'
        return ev(False, FILTER_MISMATCH, detail)
    if blocker:
        return ev(False, blocker[0], blocker[1])
    if ds.limit:
        return ev(False, LIMIT_REACHED, f'limit: {ds.limit}')
    # should not happen: report it rather than guessing
    return ev(False, FILTER_MISMATCH, 'not selected')


def explain_drive_selection(spec: DriveGroupSpec,
                            disks: List[Device],
                            existing_daemons: Optional[int] = None) -> SelectionExplanation:
    """ Explain the device selection of ``spec`` for one host inventory """
    defined_roles = [r for r in ROLES if getattr(spec, r) is not None]
    error = None
    selected: Optional[Dict[str, List[str]]] = None
    try:
        sel = DriveSelection(spec, disks, existing_daemons=existing_daemons)
        selected = {
            'data_devices': [d.path for d in sel.data_devices()],
            'wal_devices': [d.path for d in sel.wal_devices()],
            'db_devices': [d.path for d in sel.db_devices()],
            'journal_devices': [d.path for d in sel.journal_devices()],
        }
    except DriveGroupValidationError as e:
        error = str(e)

    devices = [
        DeviceExplanation(disk, [_evaluate_role(spec, role, disk, selected)
                                 for role in defined_roles])
        for disk in sorted(disks, key=lambda d: d.path or '')
    ]
    expl = SelectionExplanation(spec, devices, [], error)

    has_new_data = bool(expl.new_data_devices)
    for role in defined_roles:
        role_selected = selected.get(role, []) if selected is not None else []
        expl.roles.append(RoleSummary(
            role,
            role_selected,
            explicit_paths=bool(getattr(spec, role).paths),
            unsatisfied=(role in ('db_devices', 'wal_devices')
                         and selected is not None
                         and not role_selected
                         and has_new_data),
        ))

    if selected is not None and not selected['data_devices']:
        expl.notes.append('data_devices selects no device: no OSD will be created')
    elif selected is not None and not has_new_data:
        expl.notes.append('all selected data devices already back OSDs of this spec: '
                          'no new OSD will be created')
    if 'journal_devices' in defined_roles:
        expl.notes.append('journal_devices is not used by the supported objectstores '
                          'and is ignored when deploying OSDs')
    for summary in expl.roles:
        if summary.unsatisfied:
            expl.notes.append(
                f'{summary.role} is defined but selects no device: new OSDs would be '
                'created without dedicated DB/WAL, collocated on the data devices')
    return expl
