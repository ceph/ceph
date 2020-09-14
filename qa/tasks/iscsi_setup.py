"""
Run ceph-iscsi tests
"""
import logging
import socket
import re
from io import StringIO
from teuthology.exceptions import CommandFailedError
from teuthology.orchestra import run

log = logging.getLogger(__name__)

class IscsiSetup(object):
    def __init__(self, ctx, config):
        self.ctx = ctx
        self.config = config
        self.target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:ceph-gw"
        self.client_iqn = "iqn.1994-05.com.redhat:client"
        self.trusted_ip_list = []

    # Copied from ceph-iscsi project
    def get_host(self, ip=''):
        """
        If the 'ip' is empty, it will return local node's
        hostname, or will return the specified node's hostname
        """
        return socket.getfqdn(ip)

    # Get the trusted ip list from the ceph.conf
    def _get_trusted_ip_list(self, remote, target):
        if self.trusted_ip_list is not []:
            return
        stdout = StringIO()
        stderr = StringIO()
        try:
            args = ['cat', target]
            remote.run(args=args, stdout=stdout, stderr=stderr)
        except CommandFailedError:
            if "No such file or directory" in stderr.getvalue():
                raise

        ips = re.findall(r'mon host = \[v2:([\d.]+).*v1.*\],\[v2:([\d.]+).*v1.*\],\[v2:([\d.]+).*v1.*\]', stdout.getvalue())
        self.trusted_ip_list = list(ips[0])
        if self.trusted_ip_list is []:
            raise RuntimeError("no trust ip list was found!")

    def _setup_gateway(self, role):
        """Spawned task that setups the gateway"""
        (remote,) = self.ctx.cluster.only(role).remotes.keys()

        # setup the iscsi-gateway.cfg file, we only set the
        # clust_name and trusted_ip_list and all the others
        # as default
        target = "/etc/ceph/iscsi-gateway.cfg"
        self._get_trusted_ip_list(remote, target)

        args = ['sudo', 'echo', 'cluster_name = ceph', run.Raw('>'), target]
        remote.run(args=args)
        args = ['sudo', 'echo', 'pool = rbd', run.Raw('>>'), target]
        remote.run(args=args)
        args = ['sudo', 'echo', 'api_secure = false', run.Raw('>>'), target]
        remote.run(args=args)
        args = ['sudo', 'echo', 'api_port = 5000', run.Raw('>>'), target]
        remote.run(args=args)
        ips = ','.join(self.trusted_ip_list)
        args = ['sudo', 'echo', 'trusted_ip_list = {}'.format(ips), run.Raw('>>'), target]
        remote.run(args=args)

        remote.run(args=['sudo', 'systemctl', 'start', 'tcmu-runner'])
        remote.run(args=['sudo', 'systemctl', 'start', 'rbd-target-gw'])
        remote.run(args=['sudo', 'systemctl', 'start', 'rbd-target-api'])

    def setup_gateways(self):
        for role in self.config['gateways']:
            self._setup_gateway(role)

    def _setup_client(self, role):
        """Spawned task that setups the gateway"""
        (remote,) = self.ctx.cluster.only(role).remotes.keys()

        # setup the iscsi-gateway.cfg file, we only set the
        # clust_name and trusted_ip_list and all the others
        # as default
        target = "/etc/iscsi/initiatorname.iscsi"
        args = ['sudo', 'echo', f'InitiatorName={self.client_iqn}', run.Raw('>'), target]
        remote.run(args=args)

    def setup_clients(self):
        for role in self.config['clients']:
            self._setup_client(role)

    def check_status(self, remote, check='iscsi-targets', reverse=False):
        raise NotImplementedError()

    def run_ls(self, remote):
        self.check_status(remote)

    def run_create(self, remote):
        raise NotImplementedError()

    def run_delete(self, remote):
        raise NotImplementedError()

    def run_test(self):
        """Spawned task that runs the gateway"""
        role = self.config['gateways'][0]
        (remote,) = self.ctx.cluster.only(role).remotes.keys()

        # gwcli ls
        self.run_ls(remote)

        # gwcli create
        self.run_create(remote)

        # gwcli delete
        self.run_delete(remote)
