from typing import Dict, Tuple

import abc


class CryptoCallError(ValueError):
    pass


class CryptoCaller(abc.ABC):
    """Abstract base class for `CryptoCaller`s - an interface that
    encapsulates basic password and TLS cert related functions
    needed by the Ceph MGR.
    """

    @abc.abstractmethod
    def create_private_key(self) -> str:
        """Create a new TLS private key, returning it as a string."""

    @abc.abstractmethod
    def create_self_signed_cert(
        self, dname: Dict[str, str], pkey: str
    ) -> str:
        """Given TLS certificate subject parameters and a private key,
        create a new self signed certificate - returned as a string.
        """

    @abc.abstractmethod
    def verify_tls(self, crt: str, key: str) -> None:
        """Given a TLS certificate and a private key raise an error
        if the combination is not valid.
        """

    @abc.abstractmethod
    def certificate_days_to_expire(self, crt: str) -> int:
        """Return the number of days until the given TLS certificate expires."""

    @abc.abstractmethod
    def get_cert_issuer_info(self, crt: str) -> Tuple[str, str]:
        """Basic validation of a ca cert"""

    @abc.abstractmethod
    def password_hash(self, password: str, salt_password: str) -> str:
        """Hash a password. Returns the hashed password as a string."""

    @abc.abstractmethod
    def verify_password(self, password: str, hashed_password: str) -> bool:
        """Return true if a password and hash match."""
