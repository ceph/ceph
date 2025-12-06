import json
import logging
from typing import Dict, Any, List, Optional

import cherrypy
from ceph.deployment.service_spec import ServiceSpec

from .. import mgr
from ..security import Scope
from ..services.exception import handle_orchestrator_error
from ..services.orchestrator import OrchClient, OrchFeature
from ..tools import str_to_bool
from . import APIDoc, APIRouter, EndpointDoc, ReadPermission, RESTController
from ._version import APIVersion
from .orchestrator import raise_if_no_orchestrator

logger = logging.getLogger(__name__)


def _get_certificate_renewal_threshold_days() -> int:
    """
    Get the certificate renewal threshold days from cephadm config.
    Falls back to default value of 30 if config cannot be retrieved.
    
    :return: Number of days before expiration to consider certificate as expiring
    """
    # Get the config value directly from cephadm module
    # Default value is 30 days per cephadm module definition
    threshold = mgr.get_module_option_ex('cephadm', 'certificate_renewal_threshold_days', 30)
    return int(threshold) if threshold is not None else 30


def _determine_certificate_status(remaining_days: int) -> str:
    """
    Determine certificate status based on remaining days until expiration.
    
    :param remaining_days: Number of days remaining until certificate expiration
    :return: Status string ('expired', 'expiring', or 'valid')
    """
    renewal_threshold = _get_certificate_renewal_threshold_days()
    if remaining_days < 0:
        return 'expired'
    elif remaining_days < renewal_threshold:
        return 'expiring'
    else:
        return 'valid'


def _extract_certificate_basic_info(cert_details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract basic certificate information from certificate details.
    
    :param cert_details: Dictionary containing certificate details
    :return: Dictionary with extracted information (validity, remaining_days, expiry_date,
             subject, issuer, common_name, issuer_str)
    """
    validity = cert_details.get('validity', {})
    remaining_days = validity.get('remaining_days', 0)
    expiry_date = validity.get('not_after')
    
    subject = cert_details.get('subject', {})
    issuer = cert_details.get('issuer', {})
    common_name = subject.get('commonName') or subject.get('CN')
    issuer_str = issuer.get('commonName') or issuer.get('CN') or str(issuer) if issuer else None
    
    return {
        'validity': validity,
        'remaining_days': remaining_days,
        'expiry_date': expiry_date,
        'subject': subject,
        'issuer': issuer,
        'common_name': common_name,
        'issuer_str': issuer_str
    }


def _determine_signed_by(cert_name: str) -> str:
    """
    Determine if certificate is signed by cephadm or user based on certificate name.
    
    :param cert_name: Certificate name
    :return: 'cephadm' if cephadm-signed, 'user' otherwise
    """
    return 'cephadm' if cert_name and 'cephadm-signed' in cert_name else 'user'


def _find_certificate_in_data(cert_ls_data: Dict[str, Any], cert_name: str, 
                               cert_scope: str, service_name: str,
                               daemon_hostnames: Optional[List[str]] = None) -> tuple:
    """
    Find certificate details in cert_ls_data for a given certificate name and scope.
    
    :param cert_ls_data: Certificate list data from cert_ls
    :param cert_name: Name of the certificate to find
    :param cert_scope: Scope of the certificate ('SERVICE', 'HOST', or 'GLOBAL')
    :param service_name: Service name for SERVICE scope matching
    :param daemon_hostnames: List of hostnames for HOST scope matching
    :return: Tuple of (cert_details, target_key) or (None, None) if not found
    """
    if not cert_ls_data or cert_name not in cert_ls_data:
        return (None, None)
    
    cert_data = cert_ls_data[cert_name]
    certificates = cert_data.get('certificates', {})
    
    if cert_scope == 'SERVICE':
        # For SERVICE scope, match by service name
        if service_name in certificates:
            return (certificates[service_name], service_name)
    elif cert_scope == 'HOST':
        # For HOST scope, match by hostname
        if daemon_hostnames:
            for hostname in daemon_hostnames:
                if hostname in certificates:
                    return (certificates[hostname], hostname)
        # If not found by hostname, get first available
        if certificates:
            target_key = next(iter(certificates.keys()))
            return (certificates[target_key], target_key)
    elif cert_scope == 'GLOBAL':
        # For GLOBAL scope, certificates dict contains the cert directly
        if isinstance(certificates, dict) and certificates:
            return (certificates, None)
    
    return (None, None)


CERTIFICATE_LIST_SCHEMA = [{
    'cert_name': (str, 'Certificate name'),
    'scope': (str, 'Certificate scope (SERVICE, HOST, or GLOBAL)'),
    'signed_by': (str, 'Certificate issuer (user or cephadm)'),
    'status': (str, 'Certificate status (valid, expiring, expired, invalid)'),
    'days_to_expiration': (int, 'Days remaining until expiration'),
    'expiry_date': (str, 'Certificate expiration date'),
    'issuer': (str, 'Certificate issuer distinguished name'),
    'common_name': (str, 'Certificate common name (CN)'),
    'target': (str, 'Certificate target (service name or hostname)'),
    'subject': (dict, 'Certificate subject details'),
    'key_type': (str, 'Public key type (RSA, ECC, etc.)'),
    'key_size': (int, 'Public key size in bits')
}]


@APIRouter('/certificate', Scope.HOSTS)
@APIDoc("Certificate Management API", "Certificate")
class Certificate(RESTController):

    @EndpointDoc("List All Certificates",
                 parameters={
                     'filter_by': (str, 'Optional filter string (e.g., "service=rgw", "status=expired", "signed-by=user")'),
                     'include_cephadm_signed': (bool, 'Include cephadm-signed certificates in the list')
                 },
                 responses={200: CERTIFICATE_LIST_SCHEMA})
    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    @RESTController.MethodMap(version=APIVersion(2, 0))  # type: ignore
    @ReadPermission
    @handle_orchestrator_error('certificate')
    def list(self, filter_by: Optional[str] = '', include_cephadm_signed: bool = True) -> List[Dict[str, Any]]:
        """
        List all certificates configured in the cluster.
        
        This endpoint returns a list of all certificates managed by certmgr,
        including both user-provided and cephadm-signed certificates.
        
        :param filter_by: Optional filter string. Supported filters:
            - service=<service_type> (e.g., 'service=rgw')
            - status=<status> (e.g., 'status=expired')
            - signed-by=<user|cephadm> (e.g., 'signed-by=user')
            - scope=<SERVICE|HOST|GLOBAL> (e.g., 'scope=SERVICE')
            - name=<cert_name> (e.g., 'name=rgw_ssl_cert')
        :param include_cephadm_signed: If True, include cephadm-signed certificates.
            If False, only user-provided certificates are returned.
        :return: List of certificate objects with their details
        """
        orch = OrchClient.instance()
        
        # Get all certificates
        cert_ls_data = None
        try:
            logger.debug('Calling cert_ls with filter_by=%s, show_details=True, include_cephadm_signed=%s',
                        filter_by or '', include_cephadm_signed)
            cert_ls_result = orch.cert_store.cert_ls(
                filter_by=filter_by or '',
                show_details=True,
                include_cephadm_signed=include_cephadm_signed
            )
            cert_ls_data = cert_ls_result if cert_ls_result else {}
            logger.debug('cert_ls returned %d certificate(s): %s',
                        len(cert_ls_data) if cert_ls_data else 0,
                        json.dumps(cert_ls_data, indent=2, default=str) if cert_ls_data else '{}')
        except Exception as e:
            logger.error('Failed to retrieve certificate list: %s', str(e), exc_info=True)
            raise cherrypy.HTTPError(500, f'Failed to retrieve certificate list: {str(e)}')
        
        # Transform certificate data into a list format
        certificates_list = []
        
        for cert_name, cert_data in cert_ls_data.items():
            cert_scope = cert_data.get('scope', 'UNKNOWN').upper()
            certificates = cert_data.get('certificates', {})
            
            # For GLOBAL scope, certificates dict contains the cert directly
            if cert_scope == 'GLOBAL':
                cert_details = certificates if isinstance(certificates, dict) else {}
                if not isinstance(cert_details, dict) or 'Error' in cert_details:
                    continue
                
                # Extract certificate information
                cert_info = _extract_certificate_basic_info(cert_details)
                remaining_days = cert_info['remaining_days']
                expiry_date = cert_info['expiry_date']
                subject = cert_info['subject']
                common_name = cert_info['common_name']
                issuer_str = cert_info['issuer_str']
                
                # Determine status and signed_by
                status = _determine_certificate_status(remaining_days)
                signed_by = _determine_signed_by(cert_name)
                
                certificates_list.append({
                    'cert_name': cert_name,
                    'scope': cert_scope,
                    'signed_by': signed_by,
                    'status': status,
                    'days_to_expiration': remaining_days,
                    'expiry_date': expiry_date,
                    'issuer': issuer_str,
                    'common_name': common_name,
                    'target': None,  # GLOBAL scope has no target
                    'subject': subject,
                    'key_type': cert_details.get('public_key', {}).get('key_type'),
                    'key_size': cert_details.get('public_key', {}).get('key_size')
                })
            else:
                # For SERVICE and HOST scope, iterate through targets
                for target, cert_details in certificates.items():
                    if isinstance(cert_details, dict) and 'Error' in cert_details:
                        # Skip certificates with errors
                        continue
                    
                    if not isinstance(cert_details, dict):
                        continue
                    
                    # Extract certificate information
                    cert_info = _extract_certificate_basic_info(cert_details)
                    remaining_days = cert_info['remaining_days']
                    expiry_date = cert_info['expiry_date']
                    subject = cert_info['subject']
                    common_name = cert_info['common_name']
                    issuer_str = cert_info['issuer_str']
                    
                    # Determine status and signed_by
                    status = _determine_certificate_status(remaining_days)
                    signed_by = _determine_signed_by(cert_name)
                    
                    certificates_list.append({
                        'cert_name': cert_name,
                        'scope': cert_scope,
                        'signed_by': signed_by,
                        'status': status,
                        'days_to_expiration': remaining_days,
                        'expiry_date': expiry_date,
                        'issuer': issuer_str,
                        'common_name': common_name,
                        'target': target,  # SERVICE or HOST target
                        'subject': subject,
                        'key_type': cert_details.get('public_key', {}).get('key_type'),
                        'key_size': cert_details.get('public_key', {}).get('key_size')
                    })
        
        # Set total count header
        cherrypy.response.headers['X-Total-Count'] = len(certificates_list)
        
        return certificates_list

    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST, OrchFeature.DAEMON_LIST])
    @ReadPermission
    @handle_orchestrator_error('certificate')
    def get(self, service_name: str) -> Dict[str, Any]:
        """
        Get detailed certificate information for a service.
        
        :param service_name: The service name, e.g. 'rgw.myzone'.
        :return: Detailed certificate information including full certificate details
        """
        orch = OrchClient.instance()
        
        # Get service information
        services = orch.services.get(service_name)
        if not services:
            raise cherrypy.HTTPError(404, 'Service {} not found'.format(service_name))
        
        service = services[0]
        service_type = service.service_type()
        service_name_full = service.spec.service_name()
        
        # Get certificate configuration from REQUIRES_CERTIFICATES
        cert_config = ServiceSpec.REQUIRES_CERTIFICATES.get(service_type)
        if not cert_config:
            raise cherrypy.HTTPError(400, 'Service {} does not require certificates'.format(service_name))
        
        # Get all certificates at once
        cert_ls_data = None
        try:
            logger.debug('Calling cert_ls for service %s with filter_by="", show_details=True, include_cephadm_signed=True',
                        service_name)
            cert_ls_result = orch.cert_store.cert_ls(
                filter_by='',
                show_details=True,
                include_cephadm_signed=True
            )
            cert_ls_data = cert_ls_result if cert_ls_result else {}
            logger.debug('cert_ls for service %s returned %d certificate(s): %s',
                        service_name,
                        len(cert_ls_data) if cert_ls_data else 0,
                        json.dumps(cert_ls_data, indent=2, default=str) if cert_ls_data else '{}')
        except Exception as e:
            logger.error('Failed to retrieve certificate information for service %s: %s', service_name, str(e), exc_info=True)
            raise cherrypy.HTTPError(500, f'Failed to retrieve certificate information: {str(e)}')
        
        # Get certificate names - try user-provided first, then cephadm-signed
        user_cert_name = f"{service_type.replace('-', '_')}_ssl_cert"
        cephadm_cert_name = f"cephadm-signed_{service_type}_cert"
        cert_scope_str = cert_config.get('scope', 'service').upper()
        
        # Get daemon hostnames for HOST scope certificates
        daemon_hostnames = []
        target_hostname = None
        try:
            daemons = orch.services.list_daemons(service_name=service_name_full)
            daemon_hostnames = [d.hostname for d in daemons if d.hostname]
            if daemon_hostnames:
                target_hostname = daemon_hostnames[0]
        except Exception:
            pass
        
        # Find the certificate - try user-provided first, then cephadm-signed
        cert_details = None
        target_key = None
        cert_name = None
        
        # Try user-provided certificate first
        if cert_ls_data and user_cert_name in cert_ls_data:
            cert_data = cert_ls_data[user_cert_name]
            cert_scope_from_data = cert_data.get('scope', 'UNKNOWN').upper()
            # Use scope from certificate data if available, otherwise use from config
            actual_scope = cert_scope_from_data if cert_scope_from_data != 'UNKNOWN' else cert_scope_str
            cert_details, target_key = _find_certificate_in_data(
                cert_ls_data, user_cert_name, actual_scope, service_name_full, daemon_hostnames
            )
            if cert_details:
                cert_name = user_cert_name
                cert_scope_str = actual_scope
        
        # If user-provided cert not found, try cephadm-signed certificate
        if not cert_details and cert_ls_data and cephadm_cert_name in cert_ls_data:
            # Cephadm-signed certificates are always HOST scope
            cert_details, target_key = _find_certificate_in_data(
                cert_ls_data, cephadm_cert_name, 'HOST', service_name_full, daemon_hostnames
            )
            if cert_details:
                cert_name = cephadm_cert_name
                cert_scope_str = 'HOST'
        
        # Build response
        if not cert_details:
            return {
                'status': 'not_configured',
                'days_to_expiration': None,
                'cert_name': cert_name or user_cert_name,
                'scope': cert_scope_str,
                'signed_by': None,
                'has_certificate': False,
                'requires_certificate': True,
                'certificate_source': None,
                'expiry_date': None,
                'issuer': None,
                'common_name': None,
                'target': target_key
            }
        
        if isinstance(cert_details, dict) and 'Error' in cert_details:
            return {
                'status': 'invalid',
                'days_to_expiration': None,
                'cert_name': cert_name or user_cert_name,
                'scope': cert_scope_str,
                'signed_by': _determine_signed_by(cert_name or user_cert_name),
                'has_certificate': True,
                'requires_certificate': True,
                'certificate_source': None,
                'expiry_date': None,
                'issuer': None,
                'common_name': None,
                'target': target_key,
                'error': cert_details.get('Error')
            }
        
        # Extract certificate information
        cert_info = _extract_certificate_basic_info(cert_details)
        remaining_days = cert_info['remaining_days']
        expiry_date = cert_info['expiry_date']
        subject = cert_info['subject']
        issuer = cert_info['issuer']
        common_name = cert_info['common_name']
        issuer_str = cert_info['issuer_str']
        
        # Determine status and signed_by
        status = _determine_certificate_status(remaining_days)
        signed_by = _determine_signed_by(cert_name or user_cert_name)
        
        # Get certificate extensions for SAN entries
        extensions = cert_details.get('extensions', {})
        san_entries = extensions.get('subjectAltName', {})
        
        # Try to get the private key information
        key_info = None
        if cert_name:
            key_name = cert_name.replace('_cert', '_key')
        else:
            key_name = user_cert_name.replace('_cert', '_key')
        try:
            key_data = orch.cert_store.get_key(
                key_name,
                service_name=service_name_full if cert_scope_str == 'SERVICE' else None,
                hostname=target_hostname if cert_scope_str == 'HOST' else None,
                ignore_missing_exception=True
            )
            if key_data:
                from cephadm.ssl_cert_utils import get_private_key_info
                key_info = get_private_key_info(key_data)
        except Exception:
            pass
        
        # Build detailed certificate information
        result = {
            'status': status,
            'days_to_expiration': remaining_days,
            'cert_name': cert_name or user_cert_name,
            'scope': cert_scope_str,
            'signed_by': signed_by,
            'has_certificate': True,
            'requires_certificate': True,
            'certificate_source': 'reference',
            'expiry_date': expiry_date,
            'issuer': issuer_str,
            'common_name': common_name,
            'target': target_key,
            'details': {
                'subject': subject,
                'issuer': issuer,
                'san_entries': {
                    'dns_names': san_entries.get('DNS', []),
                    'ip_addresses': san_entries.get('IP', [])
                },
                'key_type': cert_details.get('public_key', {}).get('key_type'),
                'key_size': cert_details.get('public_key', {}).get('key_size'),
                'validity': {
                    'not_before': cert_info['validity'].get('not_before'),
                    'not_after': cert_info['validity'].get('not_after'),
                    'remaining_days': remaining_days
                },
                'extensions': extensions
            }
        }
        
        if key_info:
            result['key_info'] = key_info
        
        return result

    @EndpointDoc("Get Root CA Certificate",
                 parameters={
                     'raw': (str, 'If "true", returns only the certificate in PEM format as plain text (default: "false")')
                 },
                 responses={
                     200: {
                         'certificate': (str, 'Root CA certificate in PEM format')
                     }
                 })
    @RESTController.Collection('GET', path='/root-ca')
    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    @ReadPermission
    @handle_orchestrator_error('certificate')
    def root_ca(self, raw: Optional[str] = None) -> Any:
        """
        Get the cephadm root CA certificate.
        
        This endpoint returns the root Certificate Authority (CA) certificate
        used by cephadm to sign other certificates in the cluster.
        
        :param raw: If "true", returns only the certificate in PEM format as plain text.
                    If "false" or not provided, returns JSON with the certificate field.
        :return: Root CA certificate in PEM format
        """
        orch = OrchClient.instance()
        
        # Root CA certificate name
        root_ca_cert_name = 'cephadm_root_ca_cert'
        
        # Get the root CA certificate
        # Root CA is GLOBAL scope, so no service_name or hostname needed
        try:
            root_ca_cert = orch.cert_store.get_cert(
                root_ca_cert_name,
                service_name=None,
                hostname=None,
                ignore_missing_exception=False
            )
        except Exception as e:
            raise cherrypy.HTTPError(500, f'Failed to retrieve root CA certificate: {str(e)}')
        
        if not root_ca_cert:
            raise cherrypy.HTTPError(404, 'Root CA certificate not found')
        
        # If raw is True, return just the certificate as plain text
        if raw and str_to_bool(raw):
            cherrypy.response.headers['Content-Type'] = 'text/plain'
            return root_ca_cert
        
        # Otherwise return JSON with certificate field
        return {
            'certificate': root_ca_cert
        }

