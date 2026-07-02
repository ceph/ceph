import json
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ceph.utils import datetime_now, datetime_to_str, str_to_datetime
from cephadm.tlsobject_types import TLSObjectManager

if TYPE_CHECKING:
    from cephadm.cert_mgr import CertInfo, Cert
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class ACMEManager:
    """
    ACME lifecycle coordination for cephadm-managed certificates.

    The actual certificate/key material is stored in CertMgr/TLSObjectStore.
    This class owns ACME-specific state such as HTTP-01 challenge tokens and,
    later, account/order/finalization metadata.
    """

    CHALLENGE_STORE_PREFIX = 'cert_store.acme.challenge.'

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr

    def _challenge_key(self, token: str) -> str:
        return self.CHALLENGE_STORE_PREFIX + token

    def _load_challenge(self, token: str) -> Optional[Dict[str, Any]]:
        raw = self.mgr.get_store(self._challenge_key(token))
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except Exception as e:
            logger.warning('ACME: failed to decode HTTP-01 challenge token %r: %s', token, e)
            return None
        if not isinstance(data, dict):
            logger.warning('ACME: invalid HTTP-01 challenge payload for token %r', token)
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
        self.mgr.set_store(self._challenge_key(token), json.dumps(payload))

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

    def process_pending_orders(self) -> List[str]:
        """
        Process pending ACME orders.

        This patch introduces the HTTP-01 serving/routing and metadata plumbing.
        The ACME order/finalize client is intentionally left isolated here for a
        follow-up implementation.
        """
        return []

    def ensure_renewal(self, cert_info: "CertInfo", cert_obj: "Cert") -> bool:
        logger.warning(
            'ACME: certificate %s is managed_by=%s but ACME order/finalize support is not implemented yet',
            cert_info.cert_name,
            TLSObjectManager.ACME.value,
        )
        return False
