from typing import Dict, List, Optional, Tuple, Union, cast

import base64
import dataclasses
import errno
import json

import yaml

from ceph.deployment.service_spec import (
    PlacementSpec,
    SMBClusterBindIPSpec,
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
    KeyBridgePeerPolicy,
    KeyBridgeScopeType,
    LoginAccess,
    LoginCategory,
    PasswordFilter,
    SMBClustering,
    SourceReferenceType,
    TLSCredentialType,
    UserGroupSourceType,
)
from .proto import Self, Simplified
from .utils import checked

ConversionOp = Tuple[PasswordFilter, PasswordFilter]

_MASKED = '*' * 16


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


class BigString(str):
    """A subclass of str that exists specifally to assit the YAML
    formatting of longer strings (SSL/TLS certs). Because the
    python YAML lib makes doing this automatically very awkward.
    """

    @staticmethod
    def yaml_representer(
        dumper: yaml.SafeDumper, data: 'BigString'
    ) -> yaml.ScalarNode:
        _type = 'tag:yaml.org,2002:str'
        data = str(data)
        if '\n' in data or len(data) >= 80:
            return dumper.represent_scalar(_type, data, style='|')
        return dumper.represent_scalar(_type, data)


# thanks yaml lib for your odd api.
# Maybe this should be part of object_format.py? If this could be useful
# elsewhere, perhaps lift this.
yaml.SafeDumper.add_representer(BigString, BigString.yaml_representer)


class KeyBridgeScopeIdentity:
    """Represent a KeyBridge scope's name in a structured manner.
    Helps parse and validate the name of a keybridge scope without encoding a
    more complex type in the JSON/YAML.

    NOTE: Does not need to be serialized by resourcelib.
    """

    _AUTO_SUB = '00'

    def __init__(
        self,
        scope_type: KeyBridgeScopeType,
        subname: str = '',
        *,
        autosub: bool = False,
    ):
        if scope_type.unique() and subname:
            raise ValueError(
                f'invalid scope name {scope_type}.{subname},'
                f' must be {scope_type}'
            )
        if subname:
            # is the subname valid?
            try:
                validation.check_id(subname)
            except ValueError as err:
                raise ValueError(f'invalid scope name: {err}')
        if autosub and not scope_type.unique():
            # used to transform unqualified non-unique to qualified
            subname = self._AUTO_SUB
        elif subname and subname.startswith(self._AUTO_SUB):
            # reserved for auto-naming and other future uses
            raise ValueError(f'invalid scope name: reserved id: {subname}')
        self._scope_type = scope_type
        self._subname = subname

    @property
    def scope_type(self) -> KeyBridgeScopeType:
        return self._scope_type

    def __str__(self) -> str:
        if self._subname:
            return f'{self._scope_type}.{self._subname}'
        return str(self._scope_type)

    def qualified(self) -> Self:
        """Return a qualified version of this scope identity if the scope is
        not unique.
        """
        if self._scope_type.unique() or self._subname:
            return self
        return self.__class__(self._scope_type, autosub=True)

    @classmethod
    def from_name(cls, name: str) -> Self:
        """Parse a scope name string into a scope identity.

        A scope name can be unqalified, consisting only of the scope type, like
        "mem" or "kmip" or qualified where a sub-name follows a dot (.)
        following the type, like "kmip.foo". This allows the common case of
        just one "kmip" scope but allow for >1 if needed (eg. "kmip.1" &
        "kmip.2".

        Subnames starting with "00" are resrved for automatic naming and/or
        future uses.
        """
        typename, subname = name, ''
        if '.' in name:
            typename, subname = name.split('.', 1)
            if not subname:
                raise ValueError(
                    'invalid scope name: no value after delimiter'
                )
        try:
            _type = KeyBridgeScopeType(typename)
        except ValueError:
            scopes = sorted(st.value for st in KeyBridgeScopeType)
            raise ValueError(f'invalid scope type: must be one of {scopes}')
        return cls(_type, subname)


class _RBase:
    # mypy doesn't currently (well?) support class decorators adding methods
    # so we use a base class to add this method to all our resource classes.
    def to_simplified(self) -> Simplified:
        rc = getattr(self, '_resource_config')
        return rc.object_to_simplified(self)

    def convert(self, operation: ConversionOp) -> Self:
        return self


@resourcelib.component()
class FSCryptKeySelector(_RBase):
    """Parameters used to define where a fscrypt key will be acquired."""

    # name of the keybridge scope to use
    scope: str
    # name of the entity (the key) to fetch
    name: str

    def scope_identity(self) -> KeyBridgeScopeIdentity:
        return KeyBridgeScopeIdentity.from_name(self.scope)

    def validate(self) -> None:
        self.scope_identity()  # raises value error if scope invalid
        validation.check_fscrypt_key_name(self.name)


@resourcelib.component()
class CephFSStorage(_RBase):
    """Description of where in a CephFS file system a share is located."""

    volume: str
    path: str = '/'
    subvolumegroup: str = ''
    subvolume: str = ''
    provider: CephFSStorageProvider = CephFSStorageProvider.SAMBA_VFS
    # fscrypt_key is used to identify and obtain fscrypt key material
    # from the keybridge.
    fscrypt_key: Optional[FSCryptKeySelector] = None

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
    comment: Optional[str] = None
    max_connections: Optional[int] = None
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
        if self.max_connections is not None and self.max_connections < 0:
            raise ValueError(
                'max_connections must be 0 or a non-negative integer'
            )
        if self.comment is not None and '\n' in self.comment:
            raise ValueError('Comment cannot contain newlines')
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

    def convert(self, operation: ConversionOp) -> Self:
        return self.__class__(
            username=self.username,
            password=_password_convert(self.password, operation),
        )


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

    def convert(self, operation: ConversionOp) -> Self:
        def _convert_pw_key(dct: Dict[str, str]) -> Dict[str, str]:
            pw = dct.get('password', None)
            if pw is not None:
                data = dict(dct)
                data["password"] = _password_convert(pw, operation)
                return data
            return dct

        return self.__class__(
            users=[_convert_pw_key(u) for u in self.users],
            groups=self.groups,
        )


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


# A resource component wrapper around the service spec class
# SMBClusterBindIPSpec
@resourcelib.component()
class ClusterBindIP(_RBase):
    """Cluster Bind IP address or network.
    Restricts what addresses SMB services and/or containers will bind
    to when run on a cluster node.
    """

    address: str = ''
    network: str = ''

    def to_spec(self) -> SMBClusterBindIPSpec:
        if self.address:
            kwargs = {'address': self.address}
        elif self.network:
            kwargs = {'network': self.network}
        else:
            raise ValueError('ClusterBindIP has no values')
        return SMBClusterBindIPSpec(**kwargs)

    def validate(self) -> None:
        try:
            self.to_spec().validate()
        except SpecValidationError as err:
            raise ValueError(str(err)) from err

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.address.quiet = True
        rc.network.quiet = True
        return rc


@resourcelib.component()
class TLSSource(_RBase):
    """Represents TLS Certificates and Keys used to configure SMB related
    resources.
    """

    source_type: SourceReferenceType = SourceReferenceType.RESOURCE
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
class RemoteControl(_RBase):
    # enabled can be set to explicitly toggle the remote control server
    enabled: Optional[bool] = None
    # cert specifies the ssl/tls certificate to use
    cert: Optional[TLSSource] = None
    # cert specifies the ssl/tls server key to use
    key: Optional[TLSSource] = None
    # ca_cert specifies the ssl/tls ca cert for mTLS auth
    ca_cert: Optional[TLSSource] = None

    def validate(self) -> None:
        if bool(self.cert) ^ bool(self.key):
            raise ValueError('cert and key values must be provided together')

    @property
    def is_enabled(self) -> bool:
        if self.enabled is not None:
            return self.enabled
        return bool(self.cert and self.key)


@resourcelib.component()
class KeyBridgeScope(_RBase):
    """Define and configure scopes for they keybridge service.
    Each each scope is to be named via <type>[.<subname>] and specifies zero or
    configuration parameters depending on the scope type.
    """

    # name of the scope (can be unique, like "mem" or "kmip" or qualified
    # like "kmip.1")
    name: str
    # KMIP fields
    kmip_hosts: Optional[List[str]] = None
    kmip_port: Optional[int] = None
    kmip_cert: Optional[TLSSource] = None
    kmip_key: Optional[TLSSource] = None
    kmip_ca_cert: Optional[TLSSource] = None

    def scope_identity(self) -> KeyBridgeScopeIdentity:
        return KeyBridgeScopeIdentity.from_name(self.name)

    def validate(self) -> None:
        kbsi = self.scope_identity()  # raises value error if scope invalid
        vfn = {
            KeyBridgeScopeType.KMIP: self.validate_kmip,
            KeyBridgeScopeType.MEM: self.validate_mem,
        }
        vfn[kbsi.scope_type]()

    def validate_kmip(self) -> None:
        if not self.kmip_hosts:
            raise ValueError('at least one kmip hostname is required')
        if not (self.kmip_port or all(':' in h for h in self.kmip_hosts)):
            raise ValueError(
                'a kmip default port is required unless all'
                ' hosts include a port'
            )
        # TODO: should tls credentials be always required?
        if not (self.kmip_cert and self.kmip_key and self.kmip_ca_cert):
            raise ValueError('kmip requires a cert, a key, and a ca cert')

    def validate_mem(self) -> None:
        if (
            self.kmip_hosts
            or self.kmip_port
            or self.kmip_cert
            or self.kmip_key
            or self.kmip_ca_cert
        ):
            raise ValueError('mem scope does not support kmip parameters')


@resourcelib.component()
class KeyBridge(_RBase):
    """Configure and enable/disable the keybridge service for this cluster.

    The keybridge can be explicitly enabled or disabled. It will automatically
    be enabled if scopes are defined and is not explictly enabled (or
    disabled).  The peer_policy parameter can be used by devs/testers to relax
    some of the normal access restrictions.
    """

    # enabled can be set to explicitly toggle the keybridge server
    enabled: Optional[bool] = None
    scopes: Optional[List[KeyBridgeScope]] = None
    # peer_policy allows one to change/relax the keybridge server's peer
    # verification policy. generally this is only something a developer
    # should change
    peer_policy: Optional[KeyBridgePeerPolicy] = None

    @property
    def is_enabled(self) -> bool:
        if self.enabled is not None:
            return self.enabled
        return bool(self.scopes)

    @property
    def use_peer_policy(self) -> KeyBridgePeerPolicy:
        if self.peer_policy is None:
            return KeyBridgePeerPolicy.RESTRICTED
        return self.peer_policy

    def validate(self) -> None:
        if self.enabled and not self.scopes:
            raise ValueError(
                'an enabled KeyBridge requires at least one scope'
            )
        for scope in self.scopes or []:
            scope.validate()


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
    custom_ports: Optional[Dict[str, int]] = None
    # bind_addrs are used to restrict what IP addresses instances of this
    # cluster will use
    bind_addrs: Optional[List[ClusterBindIP]] = None
    # configure a remote control sidecar server.
    remote_control: Optional[RemoteControl] = None
    keybridge: Optional[KeyBridge] = None

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
        validation.check_custom_ports(self.custom_ports)
        if self.bind_addrs is not None and not self.bind_addrs:
            raise ValueError(
                'bind_addrs must have at least one value or not be set'
            )

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

    @property
    def remote_control_is_enabled(self) -> bool:
        """Return true if a remote control service should be enabled for this
        cluster.
        """
        if not self.remote_control:
            return False
        return self.remote_control.is_enabled

    @property
    def keybridge_is_enabled(self) -> bool:
        """Return true is a keybridge service should be enabled for this
        cluster.
        """
        if not self.keybridge:
            return False
        return self.keybridge.is_enabled

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

    def service_spec_bind_addrs(self) -> Optional[List[SMBClusterBindIPSpec]]:
        if self.bind_addrs is None:
            return None
        return [b.to_spec() for b in self.bind_addrs]


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

    def convert(self, operation: ConversionOp) -> Self:
        return self.__class__(
            auth_id=self.auth_id,
            intent=self.intent,
            auth=None if not self.auth else self.auth.convert(operation),
            linked_to_cluster=self.linked_to_cluster,
        )


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

    def convert(self, operation: ConversionOp) -> Self:
        values = None if not self.values else self.values.convert(operation)
        return self.__class__(
            users_groups_id=self.users_groups_id,
            intent=self.intent,
            values=values,
            linked_to_cluster=self.linked_to_cluster,
        )


@resourcelib.resource('ceph.smb.tls.credential')
class TLSCredential(_RBase):
    """Contains a TLS certificate or key that can be used to configure
    SMB services that make use of TLS/SSL.
    """

    tls_credential_id: str
    intent: Intent = Intent.PRESENT
    credential_type: Optional[TLSCredentialType] = None
    value: Optional[str] = None
    # linked resources can only be used by the resource they are linked to
    # and are automatically removed when the "parent" resource is removed
    linked_to_cluster: Optional[str] = None

    def validate(self) -> None:
        if not self.tls_credential_id:
            raise ValueError('tls_credential_id requires a value')
        validation.check_id(self.tls_credential_id)
        if self.linked_to_cluster is not None:
            validation.check_id(self.linked_to_cluster)
        if self.intent is Intent.PRESENT:
            if self.credential_type is None:
                raise ValueError('credential_type must be specified')
            if not self.value:
                raise ValueError('a value must be specified')

    @resourcelib.customize
    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
        rc.value.wrapper_type = BigString
        return rc

    def convert(self, operation: ConversionOp) -> Self:
        """When hiding sensitive data hide TLS/SSL certs too. However, the
        BASE64 filter enum will act as a no-op. Our certs are already long
        Base64 encoded strings that are resistant to casual shoulder-surfing.
        """
        if (
            operation == (PasswordFilter.NONE, PasswordFilter.HIDDEN)
            and self.value
        ):
            return dataclasses.replace(self, value=_MASKED)
        return self


# SMBResource is a union of all valid top-level smb resource types.
SMBResource = Union[
    Cluster,
    JoinAuth,
    RemovedCluster,
    RemovedShare,
    Share,
    UsersAndGroups,
    TLSCredential,
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


def _password_convert(pvalue: str, operation: ConversionOp) -> str:
    if operation == (PasswordFilter.NONE, PasswordFilter.BASE64):
        pvalue = base64.b64encode(pvalue.encode("utf8")).decode("utf8")
    elif operation == (PasswordFilter.NONE, PasswordFilter.HIDDEN):
        pvalue = _MASKED
    elif operation == (PasswordFilter.BASE64, PasswordFilter.NONE):
        pvalue = base64.b64decode(pvalue.encode("utf8")).decode("utf8")
    else:
        osrc, odst = operation
        raise ValueError(
            f"can not convert password value encoding from {osrc} to {odst}"
        )
    return pvalue
