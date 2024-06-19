from typing import Dict, List, Optional, Union, cast

import json

import yaml

from ceph.deployment.service_spec import PlacementSpec

from . import resourcelib, validation
from .enums import (
    AuthMode,
    CephFSStorageProvider,
    Intent,
    JoinSourceType,
    UserGroupSourceType,
)
from .proto import Self, Simplified, checked


def _get_intent(data: Simplified) -> Intent:
    """Helper function that returns the intent value from a data dict."""
    return Intent(data.get('intent', Intent.PRESENT))


def _removed(data: Simplified) -> bool:
    """Condition function returning true when the intent is removed."""
    return _get_intent(data) == Intent.REMOVED


def _present(data: Simplified) -> bool:
    """Condition function returning true when the intent is present."""
    return _get_intent(data) == Intent.PRESENT


class InvalidResourceError(ValueError):
    def __init__(self, msg: str, data: Simplified) -> None:
        super().__init__(msg)
        self.resource_data = data

    @classmethod
    def wrap(cls, err: Exception, data: Simplified) -> Exception:
        if isinstance(err, ValueError) and not isinstance(
            err, resourcelib.ResourceTypeError
        ):
            return cls(str(err), data)
        return err


class _RBase:
    # mypy doesn't currently (well?) support class decorators adding methods
    # so we use a base class to add this method to all our resource classes.
    def to_simplified(self) -> Simplified:
        rc = getattr(self, '_resource_config')
        return rc.object_to_simplified(self)


@resourcelib.component()
class CephFSStorage(_RBase):
    """Description of where in a CephFS file system a share is located."""

    volume: str
    path: str = '/'
    subvolumegroup: str = ''
    subvolume: str = ''
    provider: CephFSStorageProvider = CephFSStorageProvider.SAMBA_VFS

    def __post_init__(self) -> None:
        # Allow a shortcut form of <subvolgroup>/<subvol> in the subvolume
        # field. If that's the case split it here and put the values in
        # their proper locations.
        if '/' in self.subvolume and not self.subvolumegroup:
            try:
                svg, sv = self.subvolume.split('/')
                self.subvolumegroup = svg
                self.subvolume = sv
            except ValueError:
                raise ValueError(
                    'invalid subvolume value: {self.subvolume!r}'
                )
        # remove extra slashes, relative path components, etc.
        self.path = validation.normalize_path(self.path)

    def validate(self) -> None:
        if not self.volume:
            raise ValueError('volume requires a value')
        if '/' in self.subvolumegroup:
            raise ValueError(
                'invalid subvolumegroup value: {self.subvolumegroup!r}'
            )
        if '/' in self.subvolume:
            raise ValueError('invalid subvolume value: {self.subvolume!r}')
        validation.check_path(self.path)
        # TODO: validate volume/subvol/etc name (where defined in ceph?)

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.subvolumegroup.quiet = True
        rc.subvolume.quiet = True
        return rc


@resourcelib.resource('ceph.smb.share')
class RemovedShare(_RBase):
    """Represents a share that has / will be removed."""

    cluster_id: str
    share_id: str
    intent: Intent = Intent.REMOVED

    def validate(self) -> None:
        if not self.cluster_id:
            raise ValueError('cluster_id requires a value')
        validation.check_id(self.cluster_id)
        if not self.share_id:
            raise ValueError('share_id requires a value')
        validation.check_id(self.share_id)

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.on_condition(_removed)
        rc.on_construction_error(InvalidResourceError.wrap)
        return rc


@resourcelib.resource('ceph.smb.share')
class Share(_RBase):
    """Represents a share that should / currently exists."""

    cluster_id: str
    share_id: str
    intent: Intent = Intent.PRESENT

    name: str = ''
    readonly: bool = False
    browseable: bool = True
    cephfs: Optional[CephFSStorage] = None
    custom_smb_share_options: Optional[Dict[str, str]] = None

    def __post_init__(self) -> None:
        # if name is not given explicitly, take it from the share_id
        if not self.name:
            self.name = self.share_id

    def validate(self) -> None:
        if not self.cluster_id:
            raise ValueError('cluster_id requires a value')
        if not self.share_id:
            raise ValueError('share_id requires a value')
        validation.check_id(self.cluster_id)
        validation.check_id(self.share_id)
        validation.check_share_name(self.name)
        if self.intent != Intent.PRESENT:
            raise ValueError('Share must have present intent')
        # currently only cephfs is supported
        if self.cephfs is None:
            raise ValueError('a cephfs configuration is required')
        validation.check_custom_options(self.custom_smb_share_options)

    @property
    def checked_cephfs(self) -> CephFSStorage:
        """Return the .cephfs storage object or raise ValueError if None."""
        return checked(self.cephfs)

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.on_condition(_present)
        rc.on_construction_error(InvalidResourceError.wrap)
        return rc

    @property
    def cleaned_custom_smb_share_options(self) -> Optional[Dict[str, str]]:
        return validation.clean_custom_options(self.custom_smb_share_options)


@resourcelib.component()
class JoinAuthValues(_RBase):
    """Represents user/password values used to join to Active Directory."""

    username: str
    password: str


@resourcelib.component()
class JoinSource(_RBase):
    """Represents data that can be used to join a system to Active Directory."""

    source_type: JoinSourceType = JoinSourceType.RESOURCE
    ref: str = ''

    def validate(self) -> None:
        if not self.ref:
            raise ValueError('reference value must be specified')
        else:
            validation.check_id(self.ref)

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.ref.quiet = True
        return rc


@resourcelib.component()
class UserGroupSettings(_RBase):
    """Represents user and group data for a non-AD instance."""

    users: List[Dict[str, str]]
    groups: List[Dict[str, str]]


@resourcelib.component()
class UserGroupSource(_RBase):
    """Represents data used to set up user/group settings for an instance."""

    source_type: UserGroupSourceType = UserGroupSourceType.RESOURCE
    ref: str = ''

    def validate(self) -> None:
        if self.source_type == UserGroupSourceType.RESOURCE:
            if not self.ref:
                raise ValueError('reference value must be specified')
            else:
                validation.check_id(self.ref)
        else:
            if self.ref:
                raise ValueError('ref may not be specified')

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.ref.quiet = True
        return rc


@resourcelib.component()
class DomainSettings(_RBase):
    """Represents general settings for a system joined to Active Directory."""

    realm: str
    join_sources: List[JoinSource]


@resourcelib.resource('ceph.smb.cluster')
class RemovedCluster(_RBase):
    """Represents a cluster (instance) that is / should be removed."""

    cluster_id: str
    intent: Intent = Intent.REMOVED

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.on_condition(_removed)
        rc.on_construction_error(InvalidResourceError.wrap)
        return rc

    def validate(self) -> None:
        if not self.cluster_id:
            raise ValueError('cluster_id requires a value')
        validation.check_id(self.cluster_id)


class WrappedPlacementSpec(PlacementSpec):
    """A shim class allowing smb.resourcelib to structure/unstructure the
    placement spec type and avoid re-implementing it here.
    """

    @classmethod
    def from_simplified(cls, data: Simplified) -> Self:
        # N.B. The resourcelib (re)structuring code explictly does not support
        # methods named from_json, although in theory it could be argued
        # they're the same.  The issue is that many of ceph's {to,from}_json
        # calls take *dict*s, not actual JSON (strings). However, some do,
        # making these both a misnomer and creating ambiguity. resourcelib
        # refuses to guess.
        #
        # this cast is needed because a lot of classmethods in ceph are
        # improperly typed. They are improperly typed because typing.Self
        # didn't exist and the old correct way is a PITA to write (and
        # remember).  Thus a lot of classmethods are return the exact class
        # which is technically incorrect. This fine class is guilty of the same
        # sin. :-)
        return cast(Self, cls.from_json(data))

    @classmethod
    def wrap(cls, value: Optional[PlacementSpec]) -> Optional[Self]:
        if value is None:
            return None
        value.__class__ = cls
        return cast(Self, value)

    def to_simplified(self) -> Simplified:
        return self.to_json()


@resourcelib.resource('ceph.smb.cluster')
class Cluster(_RBase):
    """Represents a cluster (instance) that is / should be present."""

    cluster_id: str
    auth_mode: AuthMode
    intent: Intent = Intent.PRESENT
    domain_settings: Optional[DomainSettings] = None
    user_group_settings: Optional[List[UserGroupSource]] = None
    custom_dns: Optional[List[str]] = None
    custom_smb_global_options: Optional[Dict[str, str]] = None
    # embedded orchestration placement spec
    placement: Optional[WrappedPlacementSpec] = None

    def validate(self) -> None:
        if not self.cluster_id:
            raise ValueError('cluster_id requires a value')
        validation.check_id(self.cluster_id)
        if self.intent != Intent.PRESENT:
            raise ValueError('cluster requires present intent')
        if self.auth_mode == AuthMode.ACTIVE_DIRECTORY:
            if not self.domain_settings:
                raise ValueError(
                    'domain settings are required for active directory mode'
                )
            if self.user_group_settings:
                raise ValueError(
                    'user & group settings not supported for active directory mode'
                )
        if self.auth_mode == AuthMode.USER:
            if not self.user_group_settings:
                raise ValueError(
                    'user & group settings required for user auth mode'
                )
            if self.domain_settings:
                raise ValueError(
                    'domain settings not supported for user auth mode'
                )
        validation.check_custom_options(self.custom_smb_global_options)

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.on_condition(_present)
        rc.on_construction_error(InvalidResourceError.wrap)
        return rc

    @property
    def cleaned_custom_smb_global_options(self) -> Optional[Dict[str, str]]:
        return validation.clean_custom_options(self.custom_smb_global_options)


@resourcelib.resource('ceph.smb.join.auth')
class JoinAuth(_RBase):
    """Represents metadata used to join a system to Active Directory."""

    auth_id: str
    intent: Intent = Intent.PRESENT
    auth: Optional[JoinAuthValues] = None
    # linked resources can only be used by the resource they are linked to
    # and are automatically removed when the "parent" resource is removed
    linked_to_cluster: Optional[str] = None

    def validate(self) -> None:
        if not self.auth_id:
            raise ValueError('auth_id requires a value')
        validation.check_id(self.auth_id)
        if self.linked_to_cluster is not None:
            validation.check_id(self.linked_to_cluster)

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.linked_to_cluster.quiet = True
        rc.on_construction_error(InvalidResourceError.wrap)
        return rc


@resourcelib.resource('ceph.smb.usersgroups')
class UsersAndGroups(_RBase):
    """Represents metadata used to set up users/groups for an instance."""

    users_groups_id: str
    intent: Intent = Intent.PRESENT
    values: Optional[UserGroupSettings] = None
    # linked resources can only be used by the resource they are linked to
    # and are automatically removed when the "parent" resource is removed
    linked_to_cluster: Optional[str] = None

    def validate(self) -> None:
        if not self.users_groups_id:
            raise ValueError('users_groups_id requires a value')
        validation.check_id(self.users_groups_id)
        if self.linked_to_cluster is not None:
            validation.check_id(self.linked_to_cluster)

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.linked_to_cluster.quiet = True
        rc.on_construction_error(InvalidResourceError.wrap)
        return rc


# SMBResource is a union of all valid top-level smb resource types.
SMBResource = Union[
    Cluster,
    JoinAuth,
    RemovedCluster,
    RemovedShare,
    Share,
    UsersAndGroups,
]


def load_text(blob: str) -> List[SMBResource]:
    """Given JSON or YAML return a list of SMBResource objects deserialized
    from the input.
    """
    try:
        data = yaml.safe_load(blob)
    except ValueError:
        pass
    try:
        data = json.loads(blob)
    except ValueError:
        pass
    return load(data)


def load(data: Simplified) -> List[SMBResource]:
    """Given simple python types (unstructured data) return a list of
    SMBResource objects that can be produced by mapping that data into
    structured types.
    """
    return resourcelib.load(data)
