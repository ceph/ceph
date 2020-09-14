"""
Run ceph-iscsi tests
"""
import contextlib
import logging
import socket
import re
from io import StringIO
from tasks.iscsi_setup import IscsiSetup

log = logging.getLogger(__name__)

class Gwcli(IscsiSetup):
    def __init__(self, ctx, config):
        super(Gwcli, self).__init__(ctx=ctx, config=config)

    def check_status(self, remote, check='iscsi-targets', reverse=False):
        stdout = StringIO()
        args = ['gwcli', 'ls']

        remote.run(args=args, stdout=stdout)
        result = re.findall(r'{}'.format(check), stdout.getvalue())
        if reverse is False and result is []:
            raise RuntimeError("gwcli {} failed!".format(check))
        if reverse is True and result is not []:
            raise RuntimeError("gwcli {} failed!".format(check))

    def run_create(self, remote):
        # create the datapool/blockX disk
        args = ['gwcli', '/disks/', 'create', 'pool=datapool', 'image=blockX', 'size=1M']
        remote.run(args=args)
        check = 'blockX [.]+ \[datapool/blockX \(State.*\]'
        self.check_status(remote, check)

        # create the iscsi-targets
        args = ['gwcli', '/iscsi-targets', 'create', self.target_iqn]
        remote.run(args=args)
        check = '{} [.]+ \[Auth.*\]'.format(self.target_iqn)
        self.check_status(remote, check)

        # the first gateway must be the local host
        hostname = self.get_host()
        ip0 = socket.gethostbyname(hostname)
        args = ['gwcli', '/iscsi-targets/{}/gateways'.foramt(self.target_iqn), 'create', 'ip_addresses={}'.format(ip0), 'gateway_name={}'.format(hostname)]
        remote.run(args=args)
        check = '{0} [.]+ \[{1} \(UP\)\]'.format(hostname, ip0)
        self.check_status(remote, check)

        # for all the other gateways
        for ip in self.trusted_ip_list:
            if ip == ip0:
                continue
            hostname = self.get_host(ip)

            args = ['gwcli', '/iscsi-targets/{}/gateways'.foramt(self.target_iqn), 'create', 'ip_addresses={}'.format(ip), 'gateway_name={}'.format(hostname)]
            remote.run(args=args)
            check = '{0} [.]+ \[{1} \(UP\)\]'.format(hostname, ip)
            self.check_status(remote, check)

        # attach the disk to iscsi-target
        args = ['gwcli', '/iscsi-targets/{}/hosts'.format(self.target_iqn), 'create', self.client_iqn]
        remote.run(args=args)
        check = '{0} [.]+ \[Auth:.*\]'.format(self.client_iqn)
        self.check_status(remote, check)

        # map the disk to the hosts
        args = ['gwcli', '/iscsi-targets/{}/hosts/{}'.format(self.target_iqn, self.client_iqn), 'disk', 'disk=datapool/blockX']
        remote.run(args=args)
        check = 'lun 0 [.]+ \[datapool/blockX.*\]'
        self.check_status(remote, check)

    def run_delete(self, remote):
        # delete the hosts
        args = ['gwcli', '/iscsi-targets/{}/hosts'.format(self.target_iqn), 'delete', 'self.client_iqn={}'.format(self.client_iqn)]
        remote.run(args=args)
        check = '{0} [.]+ \[Auth:.*\]'.format(self.client_iqn)
        self.check_status(remote, check, True)

        # delete the disks
        args = ['gwcli', '/iscsi-targets/{}/disks'.format(self.target_iqn), 'delete', 'disk=datapool/block02']
        remote.run(args=args)
        check = 'blockX [.]+ \[datapool/blockX \(State.*\]'
        self.check_status(remote, check, True)

        # delete the iscsi-targets
        args = ['gwcli', '/iscsi-targets', 'delete', self.target_iqn]
        remote.run(args=args)
        check = '{} [.]+ \[Auth.*\]'.format(self.target_iqn)
        self.check_status(remote, check, True)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run fsx on an rbd image.

    Currently this requires running as client.admin
    to create a pool.

    Specify the list of gateways to run ::

      tasks:
        iscsi_gwcli:
          gateways: [gateway.0, gateway.1]
          clients: [client.0]

    """
    log.info('starting iscsi_gwcli...')
    gwcli = Gwcli(ctx, config)
    gwcli.setup_gateways()

    gwcli.run_test()
    yield
