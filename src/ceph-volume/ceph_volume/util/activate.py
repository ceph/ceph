import os
from ceph_volume import process, conf


def add_osd_to_mon(osd_id):
    """
    Register the previously prepared OSD to the monitor. A call will look like::

        ceph --cluster ceph --name client.bootstrap-osd
        --keyring /var/lib/ceph/bootstrap-osd/ceph.keyring \
        auth add osd.0 -i /var/lib/ceph/osd/ceph-0/keyring
        osd "allow *" mon "allow profile osd"
    """
    path = '/var/lib/ceph/osd/%s-%s/' % (conf.cluster, osd_id)
    osd_keyring = os.path.join(path, 'keyring')
    bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/ceph.keyring'

    process.run([
        'sudo',
        'ceph',
        '--cluster', conf.cluster,
        '--name', 'client.bootstrap-osd',
        '--keyring', bootstrap_keyring,
        'auth', 'add', 'osd.%s' % osd_id,
        '-i', osd_keyring,
        'osd', 'allow *',
        'mon', 'allow profile osd',
    ])
