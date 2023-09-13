"""
Generates and installs a signed SSL certificate.
"""
import argparse
import logging
import os

from teuthology import misc
from teuthology.exceptions import ConfigError
from teuthology.orchestra import run
from teuthology.task import Task

log = logging.getLogger(__name__)

class OpenSSLKeys(Task):
    name = 'openssl_keys'
    """
    Generates and installs a signed SSL certificate.

    To create a self-signed certificate:

        - openssl_keys:
            # certificate name
            root: # results in root.key and root.crt

              # [required] make the private key and certificate available in this client's test directory
              client: client.0

              # common name, defaults to `hostname`. chained certificates must not share a common name
              cn: teuthology

              # private key type for -newkey, defaults to rsa:2048
              key-type: rsa:4096

              # install the certificate as trusted on these clients:
              install: [client.0, client.1]


    To create a certificate signed by a ca certificate:

        - openssl_keys:
            root: (self-signed certificate as above)
              ...

            cert-for-client1:
              client: client.1

              # use another ssl certificate (by 'name') as the certificate authority
              ca: root  # --CAkey=root.key -CA=root.crt

              # embed the private key in the certificate file
              embed-key: true
    """

    def __init__(self, ctx, config):
        super(OpenSSLKeys, self).__init__(ctx, config)
        self.certs = []
        self.installed = []

    def setup(self):
        # global dictionary allows other tasks to look up certificate paths
        if not hasattr(self.ctx, 'ssl_certificates'):
            self.ctx.ssl_certificates = {}

        # use testdir/ca as a working directory
        self.cadir = '/'.join((misc.get_testdir(self.ctx), 'ca'))
        # make sure self-signed certs get added first, they don't have 'ca' field
        configs = sorted(self.config.items(), key=lambda x: 'ca' in x[1])
        for name, config in configs:
            # names must be unique to avoid clobbering each others files
            if name in self.ctx.ssl_certificates:
                raise ConfigError('ssl: duplicate certificate name {}'.format(name))

            # create the key and certificate
            cert = self.create_cert(name, config)

            self.ctx.ssl_certificates[name] = cert
            self.certs.append(cert)

            # install as trusted on the requested clients
            for client in config.get('install', []):
                installed = self.install_cert(cert, client)
                self.installed.append(installed)

    def teardown(self):
        """
        Clean up any created/installed certificate files.
        """
        for cert in self.certs:
            self.remove_cert(cert)

        for installed in self.installed:
            self.uninstall_cert(installed)

    def create_cert(self, name, config):
        """
        Create a certificate with the given configuration.
        """
        cert = argparse.Namespace()
        cert.name = name
        cert.key_type = config.get('key-type', 'rsa:2048')

        cert.client = config.get('client', None)
        if not cert.client:
            raise ConfigError('ssl: missing required field "client"')

        (cert.remote,) = self.ctx.cluster.only(cert.client).remotes.keys()

        cert.remote.run(args=['mkdir', '-p', self.cadir])

        cert.key = f'{self.cadir}/{cert.name}.key'
        cert.certificate = f'{self.cadir}/{cert.name}.crt'

        san_ext = []
        add_san_default = False
        cn = config.get('cn', '')
        if cn == '':
            cn = cert.remote.hostname
            add_san_default = True
        if config.get('add-san', add_san_default):
            ext = f'{self.cadir}/{cert.name}.ext'
            san_ext = ['-extfile', ext]

        # provide the common name in -subj to avoid the openssl command prompts
        subject = f'/CN={cn}'

        # if a ca certificate is provided, use it to sign the new certificate
        ca = config.get('ca', None)
        if ca:
            # the ca certificate must have been created by a prior ssl task
            ca_cert = self.ctx.ssl_certificates.get(ca, None)
            if not ca_cert:
                raise ConfigError(f'ssl: ca {ca} not found for certificate {cert.name}')

            csr = f'{self.cadir}/{cert.name}.csr'
            srl = f'{self.cadir}/{ca_cert.name}.srl'
            remove_files = ['rm', '-f', csr, srl]

            # these commands are run on the ca certificate's client because
            # they need access to its private key and cert

            # generate a private key and signing request
            ca_cert.remote.run(args=['openssl', 'req', '-nodes',
                '-newkey', cert.key_type, '-keyout', cert.key,
                '-out', csr, '-subj', subject])

            if san_ext:
                remove_files.append(ext)
                ca_cert.remote.write_file(path=ext,
                    data='subjectAltName = DNS:{},IP:{}'.format(
                        cn,
                        config.get('ip', cert.remote.ip_address)))

            # create the signed certificate
            ca_cert.remote.run(args=['openssl', 'x509', '-req', '-in', csr,
                '-CA', ca_cert.certificate, '-CAkey', ca_cert.key, '-CAcreateserial',
                '-out', cert.certificate, '-days', '365', '-sha256'] + san_ext)

            ca_cert.remote.run(args=remove_files) # clean up the signing request and serial

            # verify the new certificate against its ca cert
            ca_cert.remote.run(args=['openssl', 'verify',
                '-CAfile', ca_cert.certificate, cert.certificate])

            if cert.remote != ca_cert.remote:
                # copy to remote client
                self.remote_copy_file(ca_cert.remote, cert.certificate, cert.remote, cert.certificate)
                self.remote_copy_file(ca_cert.remote, cert.key, cert.remote, cert.key)
                # clean up the local copies
                ca_cert.remote.run(args=['rm', cert.certificate, cert.key])
                # verify the remote certificate (requires ca to be in its trusted ca certificate store)
                cert.remote.run(args=['openssl', 'verify', cert.certificate])
        else:
            # otherwise, generate a private key and use it to self-sign a new certificate
            cert.remote.run(args=['openssl', 'req', '-x509', '-nodes',
                '-newkey', cert.key_type, '-keyout', cert.key,
                '-days', '365', '-out', cert.certificate, '-subj', subject])

        if config.get('embed-key', False):
            # append the private key to the certificate file
            cert.remote.run(args=['cat', cert.key, run.Raw('>>'), cert.certificate])

        return cert

    def remove_cert(self, cert):
        """
        Delete all of the files associated with the given certificate.
        """
        # remove the private key and certificate
        cert.remote.run(args=['rm', '-f', cert.certificate, cert.key])

        # remove ca subdirectory if it's empty
        cert.remote.run(args=['rmdir', '--ignore-fail-on-non-empty', self.cadir])

    def install_cert(self, cert, client):
        """
        Install as a trusted ca certificate on the given client.
        """
        (remote,) = self.ctx.cluster.only(client).remotes.keys()

        installed = argparse.Namespace()
        installed.remote = remote

        if remote.os.package_type == 'deb':
            installed.path = '/usr/local/share/ca-certificates/{}.crt'.format(cert.name)
            installed.command = ['sudo', 'update-ca-certificates']
        else:
            installed.path = '/usr/share/pki/ca-trust-source/anchors/{}.crt'.format(cert.name)
            installed.command = ['sudo', 'update-ca-trust']

        cp_or_mv = 'cp'
        if remote != cert.remote:
            # copy into remote cadir (with mkdir if necessary)
            remote.run(args=['mkdir', '-p', self.cadir])
            self.remote_copy_file(cert.remote, cert.certificate, remote, cert.certificate)
            cp_or_mv = 'mv' # move this remote copy into the certificate store

        # install into certificate store as root
        remote.run(args=['sudo', cp_or_mv, cert.certificate, installed.path])
        remote.run(args=installed.command)

        return installed

    def uninstall_cert(self, installed):
        """
        Uninstall a certificate from the trusted certificate store.
        """
        installed.remote.run(args=['sudo', 'rm', installed.path])
        installed.remote.run(args=installed.command)

    def remote_copy_file(self, from_remote, from_path, to_remote, to_path):
        """
        Copies a file from one remote to another.

        The remotes don't have public-key auth for 'scp' or misc.copy_file(),
        so this copies through an intermediate local tmp file.
        """
        log.info('copying from {}:{} to {}:{}...'.format(from_remote, from_path, to_remote, to_path))
        local_path = from_remote.get_file(from_path)
        try:
            to_remote.put_file(local_path, to_path)
        finally:
            os.remove(local_path)

task = OpenSSLKeys
