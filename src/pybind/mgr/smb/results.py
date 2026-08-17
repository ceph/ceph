from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Protocol,
    TypedDict,
    Union,
    cast,
)

import errno

from .proto import Self, Simplified
from .resources import ConversionOp, SMBResource
from .utils import one

if TYPE_CHECKING:
    from .enums import State

_DOMAIN = 'domain'


class Result(Protocol):
    @property
    def success(self) -> bool:
        ...

    def to_simplified(self) -> Simplified:
        ...

    def mgr_return_value(self) -> int:
        ...

    def mgr_status_value(self) -> str:
        ...


class BaseResult:
    success = False

    def to_simplified(self) -> Simplified:
        return {'success': self.success}

    def mgr_return_value(self) -> int:
        return 0 if self.success else -errno.EAGAIN

    def mgr_status_value(self) -> str:
        if self.success:
            return ""
        return "resource failed to apply (see response data for details)"


class ResourceStatus(TypedDict, total=False):
    """Valid fields for updates to smb resources."""

    checked: bool
    state: str
    additional_results: List[Simplified]


class ResourceResult(BaseResult):
    """Result of applying a single smb resource update to the system."""

    # Compatible with object formatter, thus suitable for being returned
    # directly to mgr module.
    def __init__(
        self,
        src: SMBResource,
        success: bool,
        msg: str = '',
        status: Union[ResourceStatus, Simplified, None] = None,
    ) -> None:
        self.src = src
        self.success = success
        self.msg = msg
        self.status = status
        self._check_status()

    _allowed_status: Any = ResourceStatus

    def _check_status(self) -> None:
        """Runtime check that only valid keys are passed via status."""
        if not self.status:
            return
        other_keys = set(self.status) - set(
            self._allowed_status.__optional_keys__
        )
        if other_keys:
            raise KeyError(
                f'unknown keys in status: {", ".join(sorted(other_keys))}'
            )

    def to_simplified(self) -> Simplified:
        ds: Simplified = {}
        ds['resource'] = self.src.to_simplified()
        if self.status:
            ds.update(self.status)
        if self.msg:
            ds['msg'] = self.msg
        ds['success'] = self.success
        return ds

    def replace_resource(self, resource: SMBResource) -> Self:
        return self.__class__(
            src=resource,
            success=self.success,
            msg=self.msg,
            status=self.status,
        )

    @classmethod
    def processed(cls, src: SMBResource, state: 'State') -> Self:
        """Return a new ResourceResult for a resource and the state in the
        store afeter being processed by the mgr module.
        """
        return cls(src, success=True, status={'state': state})

    @classmethod
    def checked(cls, src: SMBResource) -> Self:
        """Return a new ResourceResult with metadata indicating that the
        resource has been checked for validity.
        """
        return cls(src, success=True, status={'checked': True})


class ResourceErrorStatus(TypedDict, total=False):
    """Valid fields for ErrorResult status metadata.

    Use these fields to provide structured metadata about error conditions
    modifying smb resources. We try to constrain the system to known
    predictable fields so that tools have a chance at parsing results. Errors
    can be a bit looser than non-error conditions. If error metadata doesn't
    exist for your condition add it here but do try to reuse whenever possible.

    The type hints are not enforced but you should try to match them to
    avoid unexpected output.
    """

    cluster_id: str
    clusters: List[str]
    conflicting_share_id: str
    credential_ref: str
    existing_auth_mode: str
    existing_domain_realm: str
    hint: Dict[str, str]
    invalid_scope: str
    known_scopes: List[str]
    other_cluster_id: str
    shares: List[str]
    unknown_id: str


class ErrorResult(ResourceResult, Exception):
    """A Result subclass for wrapping an error condition."""

    def __init__(
        self,
        src: SMBResource,
        msg: str = '',
        status: Union[ResourceErrorStatus, Simplified, None] = None,
    ) -> None:
        super().__init__(
            src,
            success=False,
            msg=msg,
            status=cast(Union[Dict, None], status),
        )

    _allowed_status: Any = ResourceErrorStatus


class InvalidResourceResult(BaseResult):
    def __init__(
        self,
        resource_data: Simplified,
        msg: str = '',
    ) -> None:
        self.resource_data = resource_data
        self.success = False
        self.msg = msg

    def to_simplified(self) -> Simplified:
        ds: Simplified = {}
        ds['resource'] = self.resource_data
        ds['success'] = self.success
        if self.msg:
            ds['msg'] = self.msg
        return ds


class ResultGroup:
    """Result of applying multiple smb resource updates to the system."""

    # Compatible with object formatter, thus suitable for being returned
    # directly to mgr module.
    def __init__(
        self, initial_results: Optional[Iterable[Result]] = None
    ) -> None:
        self._contents: List[Result] = list(initial_results or [])

    def append(self, result: Result) -> None:
        self._contents.append(result)

    def one(self) -> Result:
        return one(self._contents)

    def squash(self, target: SMBResource) -> Result:
        match: Optional[ResourceResult] = None
        others: List[Result] = []
        for result in self._contents:
            if isinstance(result, ResourceResult) and result.src == target:
                match = result
            else:
                others.append(result)
        if match:
            match.success = self.success
            match.status = {} if match.status is None else match.status
            match.status['additional_results'] = [
                r.to_simplified() for r in others
            ]
            return match
        raise ValueError('no matching result for resource found')

    def __iter__(self) -> Iterator[Result]:
        return iter(self._contents)

    def resources(self, check: bool = True) -> Iterator[ResourceResult]:
        """Iterate over resource results in this result group.
        If check is true (the default) raise an error if this is not
        a successful result group.
        """
        for res in self._contents:
            # check res.success to avoid iterating over contents twice
            if check and not res.success:
                raise ValueError(
                    "getting resource results from failed result group"
                )
            if isinstance(res, ResourceResult):
                yield res

    @property
    def success(self) -> bool:
        return all(r.success for r in self._contents)

    def to_simplified(self) -> Simplified:
        return {
            'results': [r.to_simplified() for r in self._contents],
            'success': self.success,
        }

    def mgr_return_value(self) -> int:
        return 0 if self.success else -errno.EAGAIN

    def mgr_status_value(self) -> str:
        if self.success:
            return ""
        ct = sum(0 if r.success else 1 for r in self._contents)
        s = '' if ct <= 1 else 's'
        return f"{ct} resource{s} failed to apply (see response data for details)"

    def convert_results(self, operation: ConversionOp) -> Self:
        """Apply a conversion operation to all the resources in the result group
        returning a new result group with all the results updated.
        """
        return self.__class__(
            initial_results=[
                _replace_resource(result, operation)
                for result in self._contents
            ]
        )


def _replace_resource(result: Result, operation: ConversionOp) -> Result:
    if isinstance(result, ResourceResult):
        return result.replace_resource(result.src.convert(operation))
    return result


class _DictValuesResult:
    """Treat a generic dict as a result.
    Special keys:
        success -> success status
        msg -> manager status
        message -> manager status fallback

    IMPORTANT - Try not to use this class in future code. This is added to
    help support existing return values that were using loosely structured
    dicts. Future code should be more consistent (across all return values
    in the mgr module) and more structured & predictable.
    """

    def __init__(self, values: Dict) -> None:
        self.values = values

    @property
    def success(self) -> bool:
        return bool(self.values.get('success'))

    def to_simplified(self) -> Simplified:
        out = dict(self.values)
        out['success'] = self.success  # ensure 'success' key
        return out

    def mgr_return_value(self) -> int:
        return 0 if self.success else -errno.EAGAIN

    def mgr_status_value(self) -> str:
        if self.success:
            return ''
        msg = self.values.get('msg') or self.values.get('message')
        return msg or 'unexpected error (see response data for details)'


class _FailedCluster(TypedDict):
    cluster_id: str
    error: str


class _FailedShare(TypedDict):
    share_id: str
    error: str


class ClientCompatBatchResult(_DictValuesResult):
    _required_keys = (
        'cluster_id',
        'client_compat',
        'cluster_updated',
        'successful_share_updates',
        'failed_share_updates',
        'total_shares',
    )

    @classmethod
    def create(cls, values: Dict) -> Self:
        for key in cls._required_keys:
            if key not in values:
                raise KeyError(f'missing required key: {key}')
        return cls(values | {'success': not values['failed_share_updates']})


class QoSBatchResult(_DictValuesResult):
    _required_keys = (
        'cluster_id',
        'successful_updates',
        'failed_updates',
        'unchanged_shares',
        'total_shares',
    )
    _unchanged_required_keys = (
        'cluster_id',
        'message',
        'unchanged_shares',
        'total_shares',
    )

    @classmethod
    def create(cls, values: Dict) -> Self:
        for key in cls._required_keys:
            if key not in values:
                raise KeyError(f'missing required key: {key}')
        return cls(values | {'success': not values['failed_updates']})

    @classmethod
    def unchanged(cls, values: Dict) -> Self:
        for key in cls._unchanged_required_keys:
            if key not in values:
                raise KeyError(f'missing required key: {key}')
        return cls(values | {'success': True})

    @classmethod
    def unhandled_error(cls, msg: str) -> Self:
        return cls({'success': False, 'msg': msg})


class ClusterShareSummary:
    def __init__(self) -> None:
        self.successful_clusters: List[str] = []
        self.successful_shares: List[str] = []
        self.failed_clusters: List[_FailedCluster] = []
        self.failed_shares: List[_FailedShare] = []

    def build_dict(
        self,
        successful_shares_key: str = '',
        failed_shares_key: str = '',
        cluster_updated_key: str = '',
        check_clusters_ok: bool = True,
    ) -> Dict:
        out: Dict = {}
        if successful_shares_key:
            out[successful_shares_key] = self.successful_shares
        if failed_shares_key:
            out[failed_shares_key] = self.failed_shares
        if check_clusters_ok and self.failed_clusters:
            raise ValueError('cluster failed to update')
        if cluster_updated_key:
            out[cluster_updated_key] = bool(self.successful_clusters)
        return out

    @classmethod
    def from_result_group(cls, rg: ResultGroup) -> 'ClusterShareSummary':
        cssum = cls()
        for result in rg.resources(check=False):
            cluster_id = str(getattr(result.src, 'cluster_id', ''))
            share_id = str(getattr(result.src, 'share_id', ''))
            if not cluster_id and not share_id:
                continue
            if cluster_id and not share_id:
                if result.success:
                    cssum.successful_clusters.append(cluster_id)
                else:
                    cssum.failed_clusters.append(
                        {'cluster_id': cluster_id, 'error': result.msg}
                    )
            if cluster_id and share_id:
                if result.success:
                    cssum.successful_shares.append(share_id)
                else:
                    cssum.failed_shares.append(
                        {'share_id': share_id, 'error': result.msg}
                    )
        return cssum
