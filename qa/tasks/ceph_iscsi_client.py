"""
Set up ceph-iscsi client.
"""
import logging
import contextlib
from textwrap import dedent

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
    Set up ceph-iscsi client.

      tasks:
        ceph_iscsi_client:
          clients: [client.1]
    """
    log.info('Setting up ceph-iscsi client...')
    for role in config['clients']:
        (remote,) = (ctx.cluster.only(role).remotes.keys())

        conf = dedent('''
        InitiatorName=iqn.1994-05.com.redhat:client
        ''')
        path = "/etc/iscsi/initiatorname.iscsi"
        remote.sudo_write_file(path, conf, mkdir=True)

        # the restart is needed after the above change is applied
        remote.run(args=['sudo', 'systemctl', 'restart', 'iscsid'])

        remote.run(args=['sudo', 'modprobe', 'dm_multipath'])
        conf = dedent('''
        defaults {
                user_friendly_names yes
                find_multipaths yes
        }

        blacklist {
        }

        devices {
                device {
                        vendor                 "LIO-ORG"
                        product                "TCMU device"
                        hardware_handler       "1 alua"
                        path_grouping_policy   "failover"
                        path_selector          "queue-length 0"
                        failback               60
                        path_checker           tur
                        prio                   alua
                        prio_args              exclusive_pref_bit
                        fast_io_fail_tmo       25
                        no_path_retry          queue
                }
        }
        ''')
        path = "/etc/multipath.conf"
        remote.sudo_write_file(path, conf)
        remote.run(args=['sudo', 'systemctl', 'start', 'multipathd'])

    yield
