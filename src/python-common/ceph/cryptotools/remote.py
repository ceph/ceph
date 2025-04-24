"""Remote execution of cryptographic functions for the ceph mgr
"""
# NB. This module exists to enapsulate the logic around running
# the cryptotools module that are forked off of the parent process
# to avoid the pyo3 subintepreters problem.
#
# The current implementation is simple using the command line and either raw
# blobs or JSON as stdin inputs and raw blobs or JSON as outputs. It is important
# that we avoid putting the sensitive data on the command line as that
# is visible in /proc.
#
# This simple implementation incurs the cost of starting a python process
# for every function call. CryptoCaller is written as a class so that if
# we choose to we can have multiple implementations of the CryptoCaller
# sharing the same protocol.
# For instance we could have a persistent process listening on a unix
# socket accepting the crypto functions as RPCs. For now, we keep it
# simple though :-)

from typing import List, Union, Dict, Any, Optional, Tuple

import json
import logging
import subprocess


_ctmodule = 'ceph.cryptotools.cryptotools'

logger = logging.getLogger('ceph.cryptotools.remote')


class CryptoCallError(ValueError):
    pass


class CryptoCaller:
    """CryptoCaller encapsulates cryptographic functions used by the
    ceph mgr into a suite of functions that can be executed in a
    different process.
    Running the crypto functions in a separate process avoids conflicts
    between the mgr's use of subintepreters and the cryptography module's
    use of PyO3 rust bindings.

    If you want to raise different error types set the json_error_cls
    attribute and/or subclass and override the map_error method.
    """

    def __init__(
        self, errors_from_json: bool = True, module: str = _ctmodule
    ):
        self._module = module
        self.errors_from_json = errors_from_json
        self.json_error_cls = ValueError

    def _run(
        self,
        args: List[str],
        input_data: Union[str, None] = None,
        capture_output: bool = False,
        check: bool = False,
    ) -> subprocess.CompletedProcess:
        if input_data is None:
            _input = None
        else:
            _input = input_data.encode()
        cmd = ['python3', '-m', _ctmodule] + list(args)
        logger.warning('CryptoCaller will run: %r', cmd)
        try:
            return subprocess.run(
                cmd, capture_output=capture_output, input=_input, check=check
            )
        except Exception as err:
            mapped_err = self.map_error(err)
            if mapped_err:
                raise mapped_err from err
            raise

    def _result_json(self, result: subprocess.CompletedProcess) -> Any:
        result_obj = json.loads(result.stdout)
        if self.errors_from_json and 'error' in result_obj:
            raise self.json_error_cls(str(result_obj['error']))
        return result_obj

    def _result_str(self, result: subprocess.CompletedProcess) -> str:
        return result.stdout.decode()

    def map_error(self, err: Exception) -> Optional[Exception]:
        """Convert between error types raised by the subprocesses
        running the crypto functions and what the mgr caller expects.
        """
        if isinstance(err, subprocess.CalledProcessError):
            return CryptoCallError(
                f'failed crypto call: {err.cmd}: {err.stderr}'
            )
        return None

    def create_private_key(self) -> str:
        """Create a new TLS private key, returning it as a string."""
        result = self._run(
            ['create_private_key'],
            capture_output=True,
            check=True,
        )
        return self._result_str(result).strip()

    def create_self_signed_cert(
        self, dname: Dict[str, str], pkey: str
    ) -> str:
        """Given TLS certificate subject parameters and a private key,
        create a new self signed certificate - returned as a string.
        """
        result = self._run(
            ['create_self_signed_cert'],
            input_data=json.dumps({'dname': dname, 'private_key': pkey}),
            capture_output=True,
            check=True,
        )
        return self._result_str(result).strip()

    def verify_tls(self, crt: str, key: str) -> None:
        """Given a TLS certificate and a private key raise an error
        if the combination is not valid.
        """
        result = self._run(
            ['verify_tls'],
            input_data=json.dumps({'crt': crt, 'key': key}),
            capture_output=True,
            check=True,
        )
        self._result_json(result)  # for errors only

    def certificate_days_to_expire(self, crt: str) -> int:
        """Verify a CA Certificate return the number of days until expiration."""
        result = self._run(
            ["certificate_days_to_expire"],
            input_data=crt,
            capture_output=True,
            check=True,
        )
        result_obj = self._result_json(result)
        return int(result_obj['days_until_expiration'])

    def get_cert_issuer_info(self, crt: str) -> Tuple[str, str]:
        """Basic validation of a ca cert"""
        result = self._run(
            ["get_cert_issuer_info"],
            input_data=crt,
            capture_output=True,
            check=True,
        )
        result_obj = self._result_json(result)
        org_name = str(result_obj.get('org_name', ''))
        cn = str(result_obj.get('cn', ''))
        return org_name, cn

    def password_hash(self, password: str, salt_password: str) -> str:
        """Hash a password. Returns the hashed password as a string."""
        pwdata = {"password": password, "salt_password": salt_password}
        result = self._run(
            ["password_hash"],
            input_data=json.dumps(pwdata),
            capture_output=True,
            check=True,
        )
        result_obj = self._result_json(result)
        pw_hash = result_obj.get("hash")
        if not pw_hash:
            raise CryptoCallError('no password hash')
        return pw_hash

    def verify_password(self, password: str, hashed_password: str) -> bool:
        """Verify a password matches the hashed password. Returns true if
        password and hashed_password match.
        """
        pwdata = {"password": password, "hashed_password": hashed_password}
        result = self._run(
            ["verify_password"],
            input_data=json.dumps(pwdata),
            capture_output=True,
            check=True,
        )
        result_obj = self._result_json(result)
        ok = result_obj.get("ok", False)
        return ok
