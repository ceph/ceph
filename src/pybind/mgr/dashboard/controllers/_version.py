import re
from typing import NamedTuple


class APIVersion(NamedTuple):
    """
    >>> APIVersion(1,0)
    APIVersion(major=1, minor=0)

    >>> APIVersion._make([1,0])
    APIVersion(major=1, minor=0)

    >>> f'{APIVersion(1, 0)!r}'
    'APIVersion(major=1, minor=0)'
    """
    major: int
    minor: int

    DEFAULT = ...  # type: ignore
    EXPERIMENTAL = ...  # type: ignore
    NONE = ...  # type: ignore

    __MIME_TYPE_REGEX = re.compile(  # type: ignore
        r'^application/vnd\.ceph\.api\.v(\d+\.\d+)\+json$')

    @classmethod
    def from_string(cls, version_string: str) -> 'APIVersion':
        """
        >>> APIVersion.from_string("1.0")
        APIVersion(major=1, minor=0)
        """
        return cls._make(int(s) for s in version_string.split('.'))

    @classmethod
    def from_mime_type(cls, mime_type: str) -> 'APIVersion':
        """
        >>> APIVersion.from_mime_type('application/vnd.ceph.api.v1.0+json')
        APIVersion(major=1, minor=0)

        """
        return cls.from_string(cls.__MIME_TYPE_REGEX.match(mime_type).group(1))

    def __str__(self):
        """
        >>> f'{APIVersion(1, 0)}'
        '1.0'
        """
        return f'{self.major}.{self.minor}'

    def to_mime_type(self, subtype='json'):
        """
        >>> APIVersion(1, 0).to_mime_type(subtype='xml')
        'application/vnd.ceph.api.v1.0+xml'
        """
        return f'application/vnd.ceph.api.v{self!s}+{subtype}'

    def supports(self, client_version: "APIVersion") -> bool:
        """
        >>> APIVersion(1, 1).supports(APIVersion(1, 0))
        True

        >>> APIVersion(1, 0).supports(APIVersion(1, 1))
        False

        >>> APIVersion(2, 0).supports(APIVersion(1, 1))
        False
        """
        return (self.major == client_version.major
                and client_version.minor <= self.minor)


# Sentinel Values
APIVersion.DEFAULT = APIVersion(1, 0)  # type: ignore
APIVersion.EXPERIMENTAL = APIVersion(0, 1)  # type: ignore
APIVersion.NONE = APIVersion(0, 0)  # type: ignore
