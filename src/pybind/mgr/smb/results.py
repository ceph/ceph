from typing import Iterable, Iterator, List, Optional

import errno

from .proto import Simplified, one
from .resources import SMBResource

_DOMAIN = 'domain'


class Result:
    """Result of applying a single smb resource update to the system."""

    # Compatible with object formatter, thus suitable for being returned
    # directly to mgr module.
    def __init__(
        self,
        src: SMBResource,
        success: bool,
        msg: str = '',
        status: Optional[Simplified] = None,
    ) -> None:
        self.src = src
        self.success = success
        self.msg = msg
        self.status = status

    def to_simplified(self) -> Simplified:
        ds: Simplified = {}
        ds['resource'] = self.src.to_simplified()
        if self.status:
            ds.update(self.status)
        if self.msg:
            ds['msg'] = self.msg
        ds['success'] = self.success
        return ds

    def mgr_return_value(self) -> int:
        return 0 if self.success else -errno.EAGAIN

    def mgr_status_value(self) -> str:
        if self.success:
            return ""
        return "resource failed to apply (see response data for details)"


class ErrorResult(Result, Exception):
    """A Result subclass for wrapping an error condition."""

    def __init__(
        self,
        src: SMBResource,
        msg: str = '',
        status: Optional[Simplified] = None,
    ) -> None:
        super().__init__(src, success=False, msg=msg, status=status)


class InvalidResourceResult(Result):
    def __init__(
        self,
        resource_data: Simplified,
        msg: str = '',
        status: Optional[Simplified] = None,
    ) -> None:
        self.resource_data = resource_data
        self.success = False
        self.msg = msg
        self.status = status

    def to_simplified(self) -> Simplified:
        ds: Simplified = {}
        ds['resource'] = self.resource_data
        ds['success'] = self.success
        if self.msg:
            ds['msg'] = self.msg
        if self.status:
            ds.update(self.status)
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
        match: Optional[Result] = None
        others: List[Result] = []
        for result in self._contents:
            if result.src == target:
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
