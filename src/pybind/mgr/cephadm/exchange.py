# Data exchange formats for communicating more
# complex data structures between the cephadm binary
# an the mgr module.

import json

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    TypeVar,
    Union,
    cast,
)


FuncT = TypeVar("FuncT", bound=Callable)


class _DataField:
    """A descriptor to map object fields into a data dictionary."""

    def __init__(
        self,
        name: Optional[str] = None,
        field_type: Optional[FuncT] = None,
    ):
        self.name = name
        self.field_type = field_type

    def __set_name__(self, _: str, name: str) -> None:
        if not self.name:
            self.name = name

    def __get__(self, obj: Any, objtype: Any = None) -> Any:
        return obj.data[self.name]

    def __set__(self, obj: Any, value: Any) -> None:
        if self.field_type is not None:
            obj.data[self.name] = self.field_type(value)
        else:
            obj.data[self.name] = value


def _get_data(obj: Any) -> Any:
    """Wrapper to get underlying data dicts from objects that
    advertise having them.
    """
    _gd = getattr(obj, "get_data", None)
    if _gd:
        return _gd()
    return obj


def _or_none(field_type: FuncT) -> FuncT:
    def _field_type_or_none(value: Any) -> Any:
        if value is None:
            return None
        return field_type(value)

    return cast(FuncT, _field_type_or_none)


class DeployMeta:
    """Deployment metadata. Child of Deploy. Used by cephadm to
    determine when certain changes have been made.
    """

    service_name = _DataField(field_type=str)
    ports = _DataField(field_type=list)
    ip = _DataField(field_type=_or_none(str))
    deployed_by = _DataField(field_type=_or_none(list))
    rank = _DataField(field_type=_or_none(int))
    rank_generation = _DataField(field_type=_or_none(int))
    extra_container_args = _DataField(field_type=_or_none(list))
    extra_entrypoint_args = _DataField(field_type=_or_none(list))
    init_containers = _DataField(field_type=_or_none(list))

    def __init__(
        self,
        init_data: Optional[Dict[str, Any]] = None,
        *,
        service_name: str = "",
        ports: Optional[List[int]] = None,
        ip: Optional[str] = None,
        deployed_by: Optional[List[str]] = None,
        rank: Optional[int] = None,
        rank_generation: Optional[int] = None,
        extra_container_args: Optional[List[Union[str, Dict[str, Any]]]] = None,
        extra_entrypoint_args: Optional[List[Union[str, Dict[str, Any]]]] = None,
        init_containers: Optional[List[Dict[str, Any]]] = None,
    ):
        self.data = dict(init_data or {})
        # set fields
        self.service_name = service_name
        self.ports = ports or []
        self.ip = ip
        self.deployed_by = deployed_by
        self.rank = rank
        self.rank_generation = rank_generation
        self.extra_container_args = extra_container_args
        self.extra_entrypoint_args = extra_entrypoint_args
        if init_containers:
            self.init_containers = init_containers

    def get_data(self) -> Dict[str, Any]:
        return self.data

    to_simplified = get_data

    @classmethod
    def convert(
        cls,
        value: Union[Dict[str, Any], "DeployMeta", None],
    ) -> "DeployMeta":
        if not isinstance(value, DeployMeta):
            return cls(value)
        return value


class Deploy:
    """Set of fields that instructs cephadm to deploy a
    service/daemon.
    """

    fsid = _DataField(field_type=str)
    name = _DataField(field_type=str)
    image = _DataField(field_type=str)
    deploy_arguments = _DataField(field_type=list)
    params = _DataField(field_type=dict)
    meta = _DataField(field_type=DeployMeta.convert)
    config_blobs = _DataField(field_type=dict)

    def __init__(
        self,
        init_data: Optional[Dict[str, Any]] = None,
        *,
        fsid: str = "",
        name: str = "",
        image: str = "",
        deploy_arguments: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        meta: Optional[DeployMeta] = None,
        config_blobs: Optional[Dict[str, Any]] = None,
    ):
        self.data = dict(init_data or {})
        # set fields
        self.fsid = fsid
        self.name = name
        self.image = image
        self.deploy_arguments = deploy_arguments or []
        self.params = params or {}
        self.meta = DeployMeta.convert(meta)
        self.config_blobs = config_blobs or {}

    def get_data(self) -> Dict[str, Any]:
        """Return the underlying data dict."""
        return self.data

    def to_simplified(self) -> Dict[str, Any]:
        """Return a simplified serializable version of the object."""
        return {k: _get_data(v) for k, v in self.get_data().items()}

    def dump_json_str(self) -> str:
        """Return the object's JSON string representation."""
        return json.dumps(self.to_simplified())
