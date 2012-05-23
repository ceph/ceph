
========
Security
========

Ceph supports two authentication mechanisms that control which clients
and daemons are allowed to participate in the cluster:

 * none: No authentication is performed of any kind
 * cephx: A cryptographic authentication mechanism with a design very similar to Kerberos.


Cephx features
--------------

* Mutual authentication.  Cephx uses shared secret keys for
authentication, meaning both the client and the monitor cluster have a
copy of the client's secret key.  The authentication protocol is such
that both parties are able to prove to each other they have a copy of
the key without actually revealing it.  This provides mutual
authentication, which means the cluster is sure the user posesses the
secret key, and the user is sure they are talking to a cluster that
has a copy of their secret key.

* No encryption.  Actual data that passes over an authenticated
session is not encrypted.

* Currently no protected from TCP session hijacking.  This should be
  fixed soon.


Enabling cephx
--------------

To enable cephx on a cluster without authentication:

#. Create a ``client.admin`` key, and save a copy for ourselves::

    ceph auth get-or-create client.admin mon 'allow *' mds 'allow *' osd 'allow *' -o /etc/ceph/keyring

Warning: this will clobber any existing /etc/ceph/keyring file; be careful.

#. Generate a secret monitor ``mon.`` key::

    ceph-authtool --create --gen-key -n mon. /tmp/monkey

#. Copy the mon keyring into a ``keyring`` file in every monitor's ``mon data`` directory::

    cp /tmp/monkey /var/lib/ceph/mon/ceph-a/keyring

#. Generate a secret key for every OSD::

    ceph auth get-or-create osd.NNN mon 'allow rwx' osd 'allow *' -o /var/lib/ceph/osd/ceph-NNN/keyring

#. Generate a secret key for every MDS::

    ceph auth get-or-create mds.NNN mon 'allow rwx' osd 'allow *' mds 'allow *' -o /var/lib/ceph/mds/ceph-NNN/keyring

#. Enable cephx authentication by setting the following options in ceph.conf::

    auth cluster required = cephx
    auth service required = cephx
    auth client required = cephx

and remove the setting::

    auth supported = <anything>





     
