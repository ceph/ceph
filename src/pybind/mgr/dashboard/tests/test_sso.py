# -*- coding: utf-8 -*-
# pylint: disable=dangerous-default-value,too-many-public-methods

import errno
import tempfile
import unittest

import pytest

from ..controllers.saml2 import check_python_saml
from ..services.sso import load_sso_db
from ..tests import CLICommandTestMixin, CmdException


@pytest.mark.skipif(
    pytest.raises(Exception, check_python_saml),
    reason="SAML dependency is missing"
)
class AccessControlTest(unittest.TestCase, CLICommandTestMixin):
    IDP_METADATA = '''<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
                     xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
                     entityID="https://testidp.ceph.com/simplesamlphp/saml2/idp/metadata.php"
                     ID="pfx8ca6fbd7-6062-d4a9-7995-0730aeb8114f">
  <ds:Signature>
    <ds:SignedInfo>
      <ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
      <ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>
      <ds:Reference URI="#pfx8ca6fbd7-6062-d4a9-7995-0730aeb8114f">
        <ds:Transforms>
          <ds:Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>
          <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
        </ds:Transforms>
        <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
        <ds:DigestValue>v6V8fooEUeq/LO/59JCfJF69Tw3ohN52OGAY6X3jX8w=</ds:DigestValue>
      </ds:Reference>
    </ds:SignedInfo>
    <ds:SignatureValue>IDP_SIGNATURE_VALUE</ds:SignatureValue>
    <ds:KeyInfo>
      <ds:X509Data>
        <ds:X509Certificate>IDP_X509_CERTIFICATE</ds:X509Certificate>
      </ds:X509Data>
    </ds:KeyInfo>
  </ds:Signature>
  <md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <md:KeyDescriptor use="signing">
      <ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
        <ds:X509Data>
          <ds:X509Certificate>IDP_X509_CERTIFICATE</ds:X509Certificate>
        </ds:X509Data>
      </ds:KeyInfo>
    </md:KeyDescriptor>
    <md:KeyDescriptor use="encryption">
      <ds:KeyInfo xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
        <ds:X509Data>
          <ds:X509Certificate>IDP_X509_CERTIFICATE</ds:X509Certificate>
        </ds:X509Data>
      </ds:KeyInfo>
    </md:KeyDescriptor>
    <md:SingleLogoutService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
                            Location="https://testidp.ceph.com/simplesamlphp/saml2/idp/SingleLogoutService.php"/>
    <md:NameIDFormat>urn:oasis:names:tc:SAML:2.0:nameid-format:transient</md:NameIDFormat>
    <md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
                            Location="https://testidp.ceph.com/simplesamlphp/saml2/idp/SSOService.php"/>
  </md:IDPSSODescriptor>
</md:EntityDescriptor>'''

    def setUp(self):
        self.mock_kv_store()
        load_sso_db()

    def validate_onelogin_settings(self, onelogin_settings, ceph_dashboard_base_url, uid,
                                   sp_x509cert, sp_private_key, signature_enabled):
        self.assertIn('sp', onelogin_settings)
        self.assertIn('entityId', onelogin_settings['sp'])
        self.assertEqual(onelogin_settings['sp']['entityId'],
                         '{}/auth/saml2/metadata'.format(ceph_dashboard_base_url))

        self.assertIn('assertionConsumerService', onelogin_settings['sp'])
        self.assertIn('url', onelogin_settings['sp']['assertionConsumerService'])
        self.assertEqual(onelogin_settings['sp']['assertionConsumerService']['url'],
                         '{}/auth/saml2'.format(ceph_dashboard_base_url))

        self.assertIn('attributeConsumingService', onelogin_settings['sp'])
        attribute_consuming_service = onelogin_settings['sp']['attributeConsumingService']
        self.assertIn('requestedAttributes', attribute_consuming_service)
        requested_attributes = attribute_consuming_service['requestedAttributes']
        self.assertEqual(len(requested_attributes), 1)
        self.assertIn('name', requested_attributes[0])
        self.assertEqual(requested_attributes[0]['name'], uid)

        self.assertIn('singleLogoutService', onelogin_settings['sp'])
        self.assertIn('url', onelogin_settings['sp']['singleLogoutService'])
        self.assertEqual(onelogin_settings['sp']['singleLogoutService']['url'],
                         '{}/auth/saml2/logout'.format(ceph_dashboard_base_url))

        self.assertIn('x509cert', onelogin_settings['sp'])
        self.assertEqual(onelogin_settings['sp']['x509cert'], sp_x509cert)

        self.assertIn('privateKey', onelogin_settings['sp'])
        self.assertEqual(onelogin_settings['sp']['privateKey'], sp_private_key)

        self.assertIn('security', onelogin_settings)
        self.assertIn('authnRequestsSigned', onelogin_settings['security'])
        self.assertEqual(onelogin_settings['security']['authnRequestsSigned'], signature_enabled)

        self.assertIn('logoutRequestSigned', onelogin_settings['security'])
        self.assertEqual(onelogin_settings['security']['logoutRequestSigned'], signature_enabled)

        self.assertIn('logoutResponseSigned', onelogin_settings['security'])
        self.assertEqual(onelogin_settings['security']['logoutResponseSigned'], signature_enabled)

        self.assertIn('wantMessagesSigned', onelogin_settings['security'])
        self.assertEqual(onelogin_settings['security']['wantMessagesSigned'], signature_enabled)

        self.assertIn('wantAssertionsSigned', onelogin_settings['security'])
        self.assertEqual(onelogin_settings['security']['wantAssertionsSigned'], signature_enabled)

    def test_sso_saml2_setup(self):
        result = self.exec_cmd('sso setup saml2',
                               ceph_dashboard_base_url='https://cephdashboard.local',
                               idp_metadata=self.IDP_METADATA)
        self.validate_onelogin_settings(result, 'https://cephdashboard.local', 'uid', '', '',
                                        False)

    def test_sso_saml2_setup_error(self):
        default_kwargs = {
            "ceph_dashboard_base_url": 'https://cephdashboard.local',
            "idp_metadata": self.IDP_METADATA
        }
        params = [
            ({"sp_x_509_cert": "some/path"},
             "Missing parameter `sp_private_key`."),
            ({"sp_private_key": "some/path"},
             "Missing parameter `sp_x_509_cert`."),
            ({"sp_private_key": "some/path", "sp_x_509_cert": "invalid/path"},
             "`some/path` not found."),
        ]
        for param in params:
            kwargs = param[0]
            msg = param[1]
            kwargs.update(default_kwargs)
            with self.assertRaises(CmdException) as ctx:
                self.exec_cmd('sso setup saml2', **kwargs)
            self.assertEqual(str(ctx.exception), msg)
            self.assertEqual(ctx.exception.retcode, -errno.EINVAL)

    def test_sso_saml2_setup_with_files(self):
        tmpfile = tempfile.NamedTemporaryFile()
        tmpfile2 = tempfile.NamedTemporaryFile()
        kwargs = {
            "ceph_dashboard_base_url": 'https://cephdashboard.local',
            "idp_metadata": self.IDP_METADATA,
            "sp_private_key": tmpfile.name,
            "sp_x_509_cert": tmpfile2.name,
        }
        result = self.exec_cmd('sso setup saml2', **kwargs)
        self.validate_onelogin_settings(result, 'https://cephdashboard.local', 'uid', '', '',
                                        True)
        tmpfile.close()
        tmpfile2.close()

    def test_sso_enable_saml2(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('sso enable saml2')

        self.assertEqual(ctx.exception.retcode, -errno.EPERM)
        self.assertEqual(str(ctx.exception), 'Single Sign-On is not configured: '
                                             'use `ceph dashboard sso setup saml2`')

        self.exec_cmd('sso setup saml2',
                      ceph_dashboard_base_url='https://cephdashboard.local',
                      idp_metadata=self.IDP_METADATA)

        result = self.exec_cmd('sso enable saml2')
        self.assertEqual(result, 'SSO is "enabled" with "saml2" protocol.')

    def test_sso_disable(self):
        result = self.exec_cmd('sso disable')
        self.assertEqual(result, 'SSO is "disabled".')

    def test_sso_status(self):
        result = self.exec_cmd('sso status')
        self.assertEqual(result, 'SSO is "disabled".')

        self.exec_cmd('sso setup saml2',
                      ceph_dashboard_base_url='https://cephdashboard.local',
                      idp_metadata=self.IDP_METADATA)

        result = self.exec_cmd('sso status')
        self.assertEqual(result, 'SSO is "enabled" with "saml2" protocol.')

    def test_sso_show_saml2(self):
        result = self.exec_cmd('sso show saml2')
        self.assertEqual(result, {
            'onelogin_settings': {}
        })
