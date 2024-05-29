from typing import Iterator, List, Optional

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


class ResultGroup:
    """Result of applying multiple smb resource updates to the system."""

    # Compatible with object formatter, thus suitable for being returned
    # directly to mgr module.
    def __init__(self) -> None:
        self._contents: List[Result] = []

    def append(self, result: Result) -> None:
        self._contents.append(result)

    def one(self) -> Result:
        return one(self._contents)

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
