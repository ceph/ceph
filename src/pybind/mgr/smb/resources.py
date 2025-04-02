from typing import Dict, List, Optional, Tuple, Union, cast

import errno
import json

import yaml

from ceph.deployment.service_spec import (
    PlacementSpec,
    SMBClusterPublicIPSpec,
    SpecValidationError,
)
from object_format import ErrorResponseBase

from . import resourcelib, validation
from .enums import (
    AuthMode,
    CephFSStorageProvider,
    Intent,
    JoinSourceType,
    LoginAccess,
    LoginCategory,
    SMBClustering,
    UserGroupSourceType,
)
from .proto import Self, Simplified
from .utils import checked


def _get_intent(data: Simplified) -> Intent:
    """Helper function that returns the intent value from a data dict."""
    return Intent(data.get('intent', Intent.PRESENT))


def _removed(data: Simplified) -> bool:
    """Condition function returning true when the intent is removed."""
    return _get_intent(data) == Intent.REMOVED


def _present(data: Simplified) -> bool:
    """Condition function returning true when the intent is present."""
    return _get_intent(data) == Intent.PRESENT


class InvalidResourceError(ValueError, ErrorResponseBase):
    def __init__(self, msg: str, data: Simplified) -> None:
        super().__init__(msg)
        self.resource_data = data

    def to_simplified(self) -> Simplified:
        return {
            'resource': self.resource_data,
            'msg': str(self),
            'success': False,
        }

    def format_response(self) -> Tuple[int, str, str]:
        data = json.dumps(self.to_simplified())
        return -errno.EINVAL, data, "Invalid resource"

    @classmethod
    def wrap(cls, err: Exception, data: Simplified) -> Exception:
        if isinstance(err, ValueError) and not isinstance(
            err, resourcelib.ResourceTypeError
        ):
            return cls(str(err), data)
        return err


class InvalidInputError(ValueError, ErrorResponseBase):
    summary_max = 1024

    def __init__(self, msg: str, content: str) -> None:
        super().__init__(msg)
        self.content = content

    def to_simplified(self) -> Simplified:
        return {
            'input': self.content[: self.summary_max],
            'truncated_input': len(self.content) > self.summary_max,
            'msg': str(self),
            'success': False,
        }

    def format_response(self) -> Tuple[int, str, str]:
        data = json.dumps(self.to_simplified())
        return -errno.EINVAL, data, "Invalid input"


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


@resourcelib.component()
class LoginAccessEntry(_RBase):
    name: str
    category: LoginCategory = LoginCategory.USER
    access: LoginAccess = LoginAccess.READ_ONLY

    def __post_init__(self) -> None:
        self.access = self.access.expand()

    def validate(self) -> None:
        validation.check_access_name(self.name)


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
    login_control: Optional[List[LoginAccessEntry]] = None
    restrict_access: bool = False

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
        if self.restrict_access and not self.login_control:
            raise ValueError(
                'a share with restricted access must define at least one login_control entry'
            )

    @property
    def checked_cephfs(self) -> CephFSStorage:
        """Return the .cephfs storage object or raise ValueError if None."""
        return checked(self.cephfs)

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.restrict_access.quiet = True
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
        # which is technically incorrect.
        return cast(Self, cls.from_json(data))

    @classmethod
    def wrap(cls, value: Optional[PlacementSpec]) -> Optional[Self]:
        if value is None:
            return None
        value.__class__ = cls
        return cast(Self, value)

    def to_simplified(self) -> Simplified:
        return self.to_json()


# This class is a near 1:1 mirror of the service spec helper class.
@resourcelib.component()
class ClusterPublicIPAssignment(_RBase):
    address: str
    destination: Union[List[str], str, None] = None

    def to_spec(self) -> SMBClusterPublicIPSpec:
        return SMBClusterPublicIPSpec(
            address=self.address,
            destination=self.destination,
        )

    def validate(self) -> None:
        try:
            self.to_spec().validate()
        except SpecValidationError as err:
            raise ValueError(str(err)) from err


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
    # control if the cluster is really a cluster
    clustering: Optional[SMBClustering] = None
    public_addrs: Optional[List[ClusterPublicIPAssignment]] = None

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

    @property
    def clustering_mode(self) -> SMBClustering:
        return self.clustering if self.clustering else SMBClustering.DEFAULT

    def is_clustered(self) -> bool:
        """Return true if smbd instance should use (CTDB) clustering."""
        if self.clustering_mode == SMBClustering.ALWAYS:
            return True
        if self.clustering_mode == SMBClustering.NEVER:
            return False
        # do clustering automatically, based on the placement spec's count value
        count = 0
        if self.placement and self.placement.count:
            count = self.placement.count
        # clustering enabled unless we're deploying a single instance "cluster"
        return count != 1

    def service_spec_public_addrs(
        self,
    ) -> Optional[List[SMBClusterPublicIPSpec]]:
        if self.public_addrs is None:
            return None
        return [a.to_spec() for a in self.public_addrs]


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


def load_text(
    blob: str, *, input_sample_max: int = 1024
) -> List[SMBResource]:
    """Given JSON or YAML return a list of SMBResource objects deserialized
    from the input.
    """
    json_err = None
    try:
        # apparently JSON is not always as strict subset of YAML
        # therefore trying to parse as JSON first is not a waste:
        # https://john-millikin.com/json-is-not-a-yaml-subset
        data = json.loads(blob)
    except ValueError as err:
        json_err = err
    try:
        data = yaml.safe_load(blob) if json_err else data
    except (ValueError, yaml.parser.ParserError) as err:
        raise InvalidInputError(str(err), blob) from err
    if not isinstance(data, (list, dict)):
        raise InvalidInputError("input must be an object or list", blob)
    return load(cast(Simplified, data))


def load(data: Simplified) -> List[SMBResource]:
    """Given simple python types (unstructured data) return a list of
    SMBResource objects that can be produced by mapping that data into
    structured types.
    """
    return resourcelib.load(data)
