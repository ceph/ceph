import base64
import hashlib
import json
import logging
import time
import urllib.error
import urllib.request
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Tuple

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.x509.oid import NameOID

from ceph.utils import datetime_now, datetime_to_str, str_to_datetime
from cephadm.tlsobject_types import TLSCredentials

if TYPE_CHECKING:
    from cephadm.cert_mgr import CertInfo, Cert
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class ACMEProtocolError(Exception):
    pass


class ACMEManager:
    """
    ACME lifecycle coordination for cephadm-managed certificates.

    The actual certificate/key material is stored in CertMgr/TLSObjectStore.
    This class owns ACME-specific state such as account keys, orders and
    HTTP-01 challenge tokens.
    """

    ACCOUNT_STORE_PREFIX = 'cert_store.acme.account.'
    ORDER_STORE_PREFIX = 'cert_store.acme.order.'
    CERT_META_STORE_PREFIX = 'cert_store.acme.cert.'
    CHALLENGE_STORE_PREFIX = 'cert_store.acme.challenge.'

    DEFAULT_PROFILE_NAME = 'default'
    DEFAULT_ACCOUNT_NAME = 'default'
    DEFAULT_CERT_KEY_SIZE = 2048
    DEFAULT_ACCOUNT_KEY_SIZE = 4096
    HTTP_TIMEOUT = 30
    ORDER_POLL_SECONDS = 3
    ORDER_POLL_ATTEMPTS = 5

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr

    @staticmethod
    def _b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode('ascii').rstrip('=')

    @staticmethod
    def _json_dumps(data: Any) -> str:
        return json.dumps(data, sort_keys=True, separators=(',', ':'))

    @staticmethod
    def _int_to_bytes(value: int) -> bytes:
        return value.to_bytes((value.bit_length() + 7) // 8, 'big')

    def _kv_get_json(self, key: str) -> Optional[Dict[str, Any]]:
        raw = self.mgr.get_store(key)
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except Exception as e:
            logger.warning('ACME: failed to decode store key %r: %s', key, e)
            return None
        if not isinstance(data, dict):
            logger.warning('ACME: invalid JSON payload for store key %r', key)
            return None
        return data

    def _kv_set_json(self, key: str, payload: Dict[str, Any]) -> None:
        self.mgr.set_store(key, json.dumps(payload, sort_keys=True))

    def _challenge_key(self, token: str) -> str:
        return self.CHALLENGE_STORE_PREFIX + token

    def _order_key(self, service_name: str) -> str:
        return self.ORDER_STORE_PREFIX + service_name

    def _cert_meta_key(self, service_name: str) -> str:
        return self.CERT_META_STORE_PREFIX + service_name

    def _account_key(self, account_name: str) -> str:
        return self.ACCOUNT_STORE_PREFIX + account_name

    def _load_challenge(self, token: str) -> Optional[Dict[str, Any]]:
        data = self._kv_get_json(self._challenge_key(token))
        if not data:
            return None
        return data

    def get_http01_key_authorization(self, token: str) -> Optional[str]:
        """Return key-authorization for a pending HTTP-01 token, or None."""
        if not token:
            return None
        data = self._load_challenge(token)
        if not data:
            return None
        expires = data.get('expires')
        if isinstance(expires, str):
            try:
                if str_to_datetime(expires) < datetime_now():
                    logger.info('ACME: HTTP-01 challenge token %r expired', token)
                    self.remove_http01_challenge(token)
                    return None
            except Exception:
                logger.debug('ACME: ignoring unparsable challenge expiration for token %r', token)
        key_authorization = data.get('key_authorization')
        return key_authorization if isinstance(key_authorization, str) else None

    def set_http01_challenge(
        self,
        token: str,
        key_authorization: str,
        service_name: str,
        expires: Optional[str] = None,
        order_url: Optional[str] = None,
    ) -> None:
        """Persist an HTTP-01 challenge token for the challenge server."""
        payload = {
            'token': token,
            'key_authorization': key_authorization,
            'service_name': service_name,
            'created': datetime_to_str(datetime_now()),
        }
        if expires:
            payload['expires'] = expires
        if order_url:
            payload['order_url'] = order_url
        self._kv_set_json(self._challenge_key(token), payload)

    def remove_http01_challenge(self, token: str) -> None:
        self.mgr.set_store(self._challenge_key(token), None)

    def list_http01_challenges(self, service_name: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        challenges: Dict[str, Dict[str, Any]] = {}
        for key, raw in self.mgr.get_store_prefix(self.CHALLENGE_STORE_PREFIX).items():
            token = key[len(self.CHALLENGE_STORE_PREFIX):]
            try:
                data = json.loads(raw)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            if service_name and data.get('service_name') != service_name:
                continue
            challenges[token] = data
        return challenges

    def clear_service_challenges(self, service_name: str) -> None:
        for token in self.list_http01_challenges(service_name):
            self.remove_http01_challenge(token)

    def cleanup_service(self, service_name: str) -> None:
        self.clear_service_challenges(service_name)
        self.mgr.set_store(self._order_key(service_name), None)
        self.mgr.set_store(self._cert_meta_key(service_name), None)

    def _load_private_key(self, pem: str) -> rsa.RSAPrivateKey:
        key = serialization.load_pem_private_key(pem.encode('utf-8'), password=None)
        if not isinstance(key, rsa.RSAPrivateKey):
            raise ACMEProtocolError('Only RSA ACME keys are supported')
        return key

    def _generate_private_key_pem(self, key_size: int) -> str:
        key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)
        return key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode('utf-8')

    def _jwk_from_private_key(self, key: rsa.RSAPrivateKey) -> Dict[str, str]:
        numbers = key.public_key().public_numbers()
        return {
            'e': self._b64url(self._int_to_bytes(numbers.e)),
            'kty': 'RSA',
            'n': self._b64url(self._int_to_bytes(numbers.n)),
        }

    def _jwk_thumbprint(self, jwk: Dict[str, str]) -> str:
        # RFC 7638 canonical JSON for RSA JWK members.
        canonical = self._json_dumps({
            'e': jwk['e'],
            'kty': jwk['kty'],
            'n': jwk['n'],
        }).encode('utf-8')
        return self._b64url(hashlib.sha256(canonical).digest())

    def _http_request(
        self,
        url: str,
        method: str = 'GET',
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, Dict[str, str], bytes]:
        req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.HTTP_TIMEOUT) as response:
                return response.getcode(), dict(response.headers.items()), response.read()
        except urllib.error.HTTPError as e:
            body = e.read()
            raise ACMEProtocolError(
                'ACME HTTP request failed: method=%s url=%s status=%s body=%s' % (
                    method, url, e.code, body.decode('utf-8', errors='replace')[:500]
                )
            ) from e
        except urllib.error.URLError as e:
            raise ACMEProtocolError(f'ACME HTTP request failed: method={method} url={url} error={e}') from e

    def _get_directory(self, directory_url: str) -> Dict[str, Any]:
        _, _, body = self._http_request(directory_url)
        try:
            directory = json.loads(body.decode('utf-8'))
        except Exception as e:
            raise ACMEProtocolError(f'Cannot parse ACME directory {directory_url}: {e}') from e
        for required in ['newNonce', 'newAccount', 'newOrder']:
            if required not in directory:
                raise ACMEProtocolError(f'ACME directory {directory_url} is missing {required}')
        return directory

    def _get_nonce(self, directory: Dict[str, Any]) -> str:
        nonce_url = directory['newNonce']
        for method in ['HEAD', 'GET']:
            try:
                _, headers, _ = self._http_request(nonce_url, method=method)
                nonce = headers.get('Replay-Nonce') or headers.get('replay-nonce')
                if nonce:
                    return nonce
            except ACMEProtocolError:
                if method == 'GET':
                    raise
        raise ACMEProtocolError('ACME server did not return Replay-Nonce')

    def _jws_body(
        self,
        url: str,
        payload: Optional[Dict[str, Any]],
        account: Dict[str, Any],
        directory: Dict[str, Any],
        use_jwk: bool = False,
    ) -> bytes:
        key = self._load_private_key(account['private_key_pem'])
        protected: Dict[str, Any] = {
            'alg': 'RS256',
            'nonce': self._get_nonce(directory),
            'url': url,
        }
        if use_jwk:
            protected['jwk'] = self._jwk_from_private_key(key)
        else:
            kid = account.get('account_url')
            if not kid:
                raise ACMEProtocolError('Cannot send ACME request without account URL')
            protected['kid'] = kid

        protected64 = self._b64url(self._json_dumps(protected).encode('utf-8'))
        if payload is None:
            payload64 = ''
        else:
            payload64 = self._b64url(self._json_dumps(payload).encode('utf-8'))
        signing_input = f'{protected64}.{payload64}'.encode('ascii')
        signature = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
        body = {
            'protected': protected64,
            'payload': payload64,
            'signature': self._b64url(signature),
        }
        return self._json_dumps(body).encode('utf-8')

    def _post_jws(
        self,
        url: str,
        payload: Optional[Dict[str, Any]],
        account: Dict[str, Any],
        directory: Dict[str, Any],
        use_jwk: bool = False,
        expect_json: bool = True,
    ) -> Tuple[Optional[Any], Dict[str, str]]:
        headers = {
            'Content-Type': 'application/jose+json',
            'Accept': 'application/json',
            'User-Agent': 'cephadm-acme',
        }
        body = self._jws_body(url, payload, account, directory, use_jwk=use_jwk)
        status, response_headers, response_body = self._http_request(url, method='POST', data=body, headers=headers)
        if not response_body:
            return None, response_headers
        if expect_json:
            try:
                return json.loads(response_body.decode('utf-8')), response_headers
            except Exception as e:
                raise ACMEProtocolError(f'Cannot decode ACME JSON response from {url}: {e}') from e
        return response_body.decode('utf-8'), response_headers

    def _load_account(self, account_name: str) -> Optional[Dict[str, Any]]:
        return self._kv_get_json(self._account_key(account_name))

    def _save_account(self, account_name: str, account: Dict[str, Any]) -> None:
        account['updated'] = datetime_to_str(datetime_now())
        self._kv_set_json(self._account_key(account_name), account)

    def _ensure_account(self, acme_cfg: Dict[str, Any], directory: Dict[str, Any]) -> Dict[str, Any]:
        account_name = acme_cfg.get('account_name') or self.DEFAULT_ACCOUNT_NAME
        directory_url = acme_cfg['directory_url']
        account = self._load_account(account_name)
        if account and account.get('directory_url') == directory_url and account.get('account_url'):
            return account

        if not account or account.get('directory_url') != directory_url:
            private_key_pem = self._generate_private_key_pem(self.DEFAULT_ACCOUNT_KEY_SIZE)
            key = self._load_private_key(private_key_pem)
            jwk = self._jwk_from_private_key(key)
            account = {
                'account_name': account_name,
                'directory_url': directory_url,
                'private_key_pem': private_key_pem,
                'jwk_thumbprint': self._jwk_thumbprint(jwk),
                'created': datetime_to_str(datetime_now()),
            }

        payload: Dict[str, Any] = {'termsOfServiceAgreed': True}
        email = acme_cfg.get('email')
        if email:
            payload['contact'] = [f'mailto:{email}']
        response, headers = self._post_jws(
            directory['newAccount'],
            payload,
            account,
            directory,
            use_jwk=True,
            expect_json=True,
        )
        account_url = headers.get('Location') or headers.get('location')
        if not account_url:
            raise ACMEProtocolError('ACME newAccount response did not include Location header')
        account['account_url'] = account_url
        if isinstance(response, dict):
            account['account_status'] = response.get('status')
        self._save_account(account_name, account)
        return account

    def _load_acme_profiles(self) -> Dict[str, Dict[str, Any]]:
        """Return cluster-level ACME profiles from the mgr module option.

        The option accepts either a raw profile map::

            {"default": {"directory_url": "..."}}

        or a wrapper object::

            {"profiles": {"default": {"directory_url": "..."}}}

        The wrapper form leaves room for future top-level ACME settings while
        keeping the first implementation easy to configure through a single
        mgr module option.
        """
        raw_profiles = getattr(self.mgr, 'acme_profiles', '')
        if not raw_profiles:
            return {}
        if isinstance(raw_profiles, str):
            try:
                parsed = json.loads(raw_profiles)
            except Exception as e:
                raise ACMEProtocolError(f'Cannot parse mgr/cephadm/acme_profiles JSON: {e}') from e
        elif isinstance(raw_profiles, Mapping):
            parsed = dict(raw_profiles)
        else:
            raise ACMEProtocolError('mgr/cephadm/acme_profiles must be a JSON object')

        if not isinstance(parsed, dict):
            raise ACMEProtocolError('mgr/cephadm/acme_profiles must decode to a JSON object')
        if any(k in parsed for k in ['directory_url', 'email', 'account_name', 'terms_of_service_agreed']):
            # Convenience form for a single default profile.
            profiles = {self.DEFAULT_PROFILE_NAME: parsed}
        else:
            profiles = parsed.get('profiles', parsed)
        if not isinstance(profiles, dict):
            raise ACMEProtocolError('mgr/cephadm/acme_profiles profiles field must be a JSON object')

        out: Dict[str, Dict[str, Any]] = {}
        for name, profile in profiles.items():
            if not isinstance(name, str) or not name.strip():
                raise ACMEProtocolError('ACME profile names must be non-empty strings')
            if not isinstance(profile, dict):
                raise ACMEProtocolError(f'ACME profile {name!r} must be a JSON object')
            out[name.strip()] = dict(profile)
        return out

    def _resolve_acme_config(self, acme_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Merge cluster-level ACME profile config with service overrides.

        Service specs carry the certificate request details, especially
        ``domains`` and optionally ``profile``. CA/account parameters such as
        directory_url, email, account_name and terms_of_service_agreed normally
        come from the selected cluster-level profile, but the service may
        override them for exceptional cases.
        """
        if not isinstance(acme_cfg, dict):
            raise ACMEProtocolError('ACME service config must be an object')

        profile_name = acme_cfg.get('profile') or self.DEFAULT_PROFILE_NAME
        if not isinstance(profile_name, str) or not profile_name.strip():
            raise ACMEProtocolError('ACME profile name must be a non-empty string')
        profile_name = profile_name.strip()

        profiles = self._load_acme_profiles()
        profile_cfg = profiles.get(profile_name, {})
        if profile_name != self.DEFAULT_PROFILE_NAME and not profile_cfg:
            raise ACMEProtocolError(f'ACME profile {profile_name!r} is not configured')

        merged: Dict[str, Any] = dict(profile_cfg)
        for key, value in acme_cfg.items():
            if key == 'profile':
                continue
            if value is not None:
                merged[key] = value
        merged['profile'] = profile_name
        return self._normalize_acme_config(merged)

    def _normalize_acme_config(self, acme_cfg: Dict[str, Any]) -> Dict[str, Any]:
        domains_raw = acme_cfg.get('domains', [])
        if not isinstance(domains_raw, list):
            raise ACMEProtocolError('ACME config requires domains to be a list')
        domains = sorted(set(d.strip().lower() for d in domains_raw if isinstance(d, str) and d.strip()))
        if not domains:
            raise ACMEProtocolError('ACME config requires at least one domain')

        directory_url = acme_cfg.get('directory_url')
        if not isinstance(directory_url, str) or not directory_url.strip():
            profile = acme_cfg.get('profile') or self.DEFAULT_PROFILE_NAME
            raise ACMEProtocolError(
                f"ACME profile {profile!r} does not define a non-empty directory_url, "
                "and the service did not override it"
            )

        terms_agreed = acme_cfg.get('terms_of_service_agreed')
        if terms_agreed is not True:
            profile = acme_cfg.get('profile') or self.DEFAULT_PROFILE_NAME
            raise ACMEProtocolError(
                f"ACME profile {profile!r} requires terms_of_service_agreed: true, "
                "or the service must override it with true"
            )

        account_name = acme_cfg.get('account_name') or self.DEFAULT_ACCOUNT_NAME
        if not isinstance(account_name, str) or not account_name.strip():
            raise ACMEProtocolError('ACME account_name must be a non-empty string')

        email = acme_cfg.get('email')
        if email is not None and not isinstance(email, str):
            raise ACMEProtocolError('ACME email must be a string when provided')

        return {
            'domains': domains,
            'profile': acme_cfg.get('profile') or self.DEFAULT_PROFILE_NAME,
            'directory_url': directory_url.strip(),
            'email': email,
            'account_name': account_name.strip(),
            'terms_of_service_agreed': True,
        }

    def _save_cert_metadata(self, service_name: str, metadata: Dict[str, Any]) -> None:
        metadata['updated'] = datetime_to_str(datetime_now())
        self._kv_set_json(self._cert_meta_key(service_name), metadata)

    def _load_cert_metadata(self, service_name: str) -> Optional[Dict[str, Any]]:
        return self._kv_get_json(self._cert_meta_key(service_name))

    def ensure_certificate(
        self,
        service_name: str,
        cert_name: str,
        key_name: str,
        acme_cfg: Dict[str, Any],
        force: bool = False,
    ) -> None:
        """Ensure an ACME order is queued for a service if needed.

        This method is intentionally store-only. The network ACME operations
        run later from the serve loop via process_pending_orders().
        """
        cfg = self._resolve_acme_config(acme_cfg)
        metadata = {
            'service_name': service_name,
            'cert_name': cert_name,
            'key_name': key_name,
            'domains': cfg['domains'],
            'profile': cfg['profile'],
            'directory_url': cfg['directory_url'],
            'email': cfg.get('email'),
            'account_name': cfg['account_name'],
            'terms_of_service_agreed': cfg['terms_of_service_agreed'],
        }
        self._save_cert_metadata(service_name, metadata)

        order = self._kv_get_json(self._order_key(service_name))
        if order and not force:
            same_request = (
                order.get('domains') == cfg['domains']
                and order.get('directory_url') == cfg['directory_url']
                and order.get('status') not in ['valid', 'failed']
            )
            if same_request:
                return

        self.clear_service_challenges(service_name)
        private_key_pem = self._generate_private_key_pem(self.DEFAULT_CERT_KEY_SIZE)
        now = datetime_to_str(datetime_now())
        new_order = {
            'service_name': service_name,
            'cert_name': cert_name,
            'key_name': key_name,
            'domains': cfg['domains'],
            'profile': cfg['profile'],
            'directory_url': cfg['directory_url'],
            'email': cfg.get('email'),
            'account_name': cfg['account_name'],
            'terms_of_service_agreed': cfg['terms_of_service_agreed'],
            'private_key_pem': private_key_pem,
            'status': 'needs-order',
            'created': now,
            'updated': now,
        }
        self._kv_set_json(self._order_key(service_name), new_order)
        logger.info('ACME: queued HTTP-01 order for %s domains=%s', service_name, ','.join(cfg['domains']))

    def _create_csr_der(self, private_key_pem: str, domains: List[str]) -> bytes:
        key = self._load_private_key(private_key_pem)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, domains[0])])
        san = x509.SubjectAlternativeName([x509.DNSName(d) for d in domains])
        csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(subject)
            .add_extension(san, critical=False)
            .sign(key, hashes.SHA256())
        )
        return csr.public_bytes(serialization.Encoding.DER)

    def _save_order(self, service_name: str, order: Dict[str, Any]) -> None:
        order['updated'] = datetime_to_str(datetime_now())
        self._kv_set_json(self._order_key(service_name), order)

    def _fail_order(self, service_name: str, order: Dict[str, Any], error: str) -> None:
        order['status'] = 'failed'
        order['error'] = error
        logger.error('ACME: order failed for %s: %s', service_name, error)
        self._save_order(service_name, order)
        self.clear_service_challenges(service_name)

    def _get_order(self, order: Dict[str, Any], account: Dict[str, Any], directory: Dict[str, Any]) -> Dict[str, Any]:
        order_url = order.get('order_url')
        if not order_url:
            raise ACMEProtocolError('Order record is missing order_url')
        response, _ = self._post_jws(order_url, None, account, directory, expect_json=True)
        if not isinstance(response, dict):
            raise ACMEProtocolError('ACME order response is not an object')
        return response

    def _fetch_authorization(self, url: str, account: Dict[str, Any], directory: Dict[str, Any]) -> Dict[str, Any]:
        response, _ = self._post_jws(url, None, account, directory, expect_json=True)
        if not isinstance(response, dict):
            raise ACMEProtocolError(f'ACME authorization response from {url} is not an object')
        return response

    def _create_and_start_order(self, order: Dict[str, Any]) -> None:
        service_name = order['service_name']
        directory = self._get_directory(order['directory_url'])
        account = self._ensure_account(order, directory)
        payload = {
            'identifiers': [{'type': 'dns', 'value': d} for d in order['domains']]
        }
        response, headers = self._post_jws(directory['newOrder'], payload, account, directory, expect_json=True)
        if not isinstance(response, dict):
            raise ACMEProtocolError('ACME newOrder response is not an object')
        order_url = headers.get('Location') or headers.get('location')
        if not order_url:
            raise ACMEProtocolError('ACME newOrder response did not include Location header')

        order.update({
            'order_url': order_url,
            'finalize_url': response.get('finalize'),
            'authorizations': response.get('authorizations', []),
            'status': response.get('status', 'pending'),
        })
        if not order.get('finalize_url'):
            raise ACMEProtocolError('ACME newOrder response did not include finalize URL')
        if not isinstance(order.get('authorizations'), list):
            raise ACMEProtocolError('ACME newOrder authorizations field is not a list')

        thumbprint = account.get('jwk_thumbprint')
        if not thumbprint:
            key = self._load_private_key(account['private_key_pem'])
            thumbprint = self._jwk_thumbprint(self._jwk_from_private_key(key))
            account['jwk_thumbprint'] = thumbprint
            self._save_account(order.get('account_name') or self.DEFAULT_ACCOUNT_NAME, account)

        challenges: Dict[str, Dict[str, Any]] = {}
        for authz_url in order['authorizations']:
            authz = self._fetch_authorization(authz_url, account, directory)
            if authz.get('status') == 'valid':
                continue
            http01 = None
            for challenge in authz.get('challenges', []):
                if isinstance(challenge, dict) and challenge.get('type') == 'http-01':
                    http01 = challenge
                    break
            if not http01:
                raise ACMEProtocolError(f'No http-01 challenge found for authorization {authz_url}')
            token = http01.get('token')
            challenge_url = http01.get('url')
            if not isinstance(token, str) or not isinstance(challenge_url, str):
                raise ACMEProtocolError(f'Invalid http-01 challenge object for authorization {authz_url}')
            key_authorization = f'{token}.{thumbprint}'
            self.set_http01_challenge(
                token,
                key_authorization,
                service_name,
                expires=authz.get('expires') if isinstance(authz.get('expires'), str) else None,
                order_url=order_url,
            )
            # Tell the ACME server that the HTTP-01 resource is ready.
            self._post_jws(challenge_url, {}, account, directory, expect_json=True)
            challenges[token] = {
                'url': challenge_url,
                'authorization_url': authz_url,
            }

        order['challenges'] = challenges
        order['status'] = 'validating'
        self._save_order(service_name, order)
        logger.info('ACME: started HTTP-01 validation for %s', service_name)

    def _finalize_order(self, order: Dict[str, Any], account: Dict[str, Any], directory: Dict[str, Any]) -> Dict[str, Any]:
        finalize_url = order.get('finalize_url')
        if not finalize_url:
            raise ACMEProtocolError('Order record is missing finalize_url')
        csr_der = self._create_csr_der(order['private_key_pem'], order['domains'])
        response, _ = self._post_jws(
            finalize_url,
            {'csr': self._b64url(csr_der)},
            account,
            directory,
            expect_json=True,
        )
        if not isinstance(response, dict):
            raise ACMEProtocolError('ACME finalize response is not an object')
        return response

    def _download_certificate(self, order: Dict[str, Any], account: Dict[str, Any], directory: Dict[str, Any]) -> str:
        cert_url = order.get('certificate_url')
        if not cert_url:
            raise ACMEProtocolError('Order record is missing certificate_url')
        response, _ = self._post_jws(cert_url, None, account, directory, expect_json=False)
        if not isinstance(response, str) or 'BEGIN CERTIFICATE' not in response:
            raise ACMEProtocolError('ACME certificate download did not return a PEM certificate chain')
        return response

    def _continue_order(self, order: Dict[str, Any]) -> Optional[str]:
        service_name = order['service_name']
        directory = self._get_directory(order['directory_url'])
        account = self._ensure_account(order, directory)

        current = self._get_order(order, account, directory)
        status = current.get('status')
        if status == 'invalid':
            self._fail_order(service_name, order, json.dumps(current.get('error', current)))
            return None
        if status == 'pending':
            order['status'] = 'validating'
            self._save_order(service_name, order)
            return None
        if status == 'ready':
            current = self._finalize_order(order, account, directory)
            status = current.get('status')
        if status == 'processing':
            order['status'] = 'processing'
            order['certificate_url'] = current.get('certificate') or order.get('certificate_url')
            self._save_order(service_name, order)
            return None
        if status == 'valid':
            order['status'] = 'valid'
            order['certificate_url'] = current.get('certificate') or order.get('certificate_url')
            if not order.get('certificate_url'):
                self._save_order(service_name, order)
                return None
            cert_pem = self._download_certificate(order, account, directory)
            tls_creds = TLSCredentials(cert=cert_pem, key=order['private_key_pem'])
            self.mgr.cert_mgr.save_acme_cert_key_pair(service_name, tls_creds)
            self.clear_service_challenges(service_name)
            self.mgr.set_store(self._order_key(service_name), None)
            logger.info('ACME: installed certificate for %s', service_name)
            return service_name
        order['status'] = str(status or 'unknown')
        self._save_order(service_name, order)
        return None

    def process_pending_orders(self) -> List[str]:
        """Process ACME orders queued by ensure_certificate()/ensure_renewal()."""
        services_to_reconfig: List[str] = []
        for key, raw in self.mgr.get_store_prefix(self.ORDER_STORE_PREFIX).items():
            service_name = key[len(self.ORDER_STORE_PREFIX):]
            try:
                order = json.loads(raw)
            except Exception as e:
                logger.warning('ACME: cannot decode order %r: %s', service_name, e)
                continue
            if not isinstance(order, dict):
                continue
            if order.get('status') == 'failed':
                continue
            try:
                if order.get('status') == 'needs-order':
                    self._create_and_start_order(order)
                    continue

                # Poll a few times to collapse the common ready->valid path into
                # a single serve-loop iteration, but keep the cap low so we do not
                # block cephadm for long on slow CAs.
                completed: Optional[str] = None
                for _ in range(self.ORDER_POLL_ATTEMPTS):
                    completed = self._continue_order(order)
                    if completed:
                        services_to_reconfig.append(completed)
                        break
                    updated = self._kv_get_json(self._order_key(service_name))
                    if not updated or updated.get('status') in ['failed', 'needs-order']:
                        break
                    order = updated
                    if order.get('status') not in ['validating', 'processing', 'ready', 'pending']:
                        break
                    time.sleep(self.ORDER_POLL_SECONDS)
            except Exception as e:
                self._fail_order(service_name, order, str(e))
        return services_to_reconfig

    def ensure_renewal(self, cert_info: "CertInfo", cert_obj: "Cert") -> bool:
        svc = self.mgr.cert_mgr.get_associated_service(cert_info)
        if not svc:
            logger.error('ACME: cannot renew %s; no associated service found', cert_info.cert_name)
            return False
        metadata = self._load_cert_metadata(svc)
        if not metadata:
            logger.error('ACME: cannot renew %s; no ACME metadata found for %s', cert_info.cert_name, svc)
            return False
        self.ensure_certificate(
            svc,
            metadata['cert_name'],
            metadata['key_name'],
            metadata,
            force=True,
        )
        logger.info('ACME: queued renewal order for %s', svc)
        return False
