================
 Authentication
================

Ceph provides two authentication modes: 

- **None:** Any user can access data without authentication.
- **Cephx**: Ceph requires user authentication in a manner similar to Kerberos.

Cephx uses shared secret keys for authentication, meaning both the client and
the monitor cluster have a copy of the client's secret key.  The authentication
protocol is such that both parties are able to prove to each other they have a
copy of the key without actually revealing it.  This provides mutual
authentication, which means the cluster is sure the user possesses the secret
key, and the user is sure that the cluster has a copy of the secret key.

Default users and pools are suitable for initial testing purposes. For test bed 
and production environments, you should create users and assign pool access to 
the users.

.. important:: The ``cephx`` protocol supports authentication only. Cephx 
   currently **does not** address man-in-the-middle attacks. We will address
   this in an upcoming release.
   
.. important: The ``cephx`` protocol does not address data encryption in transport 
   (e.g., SSL/TLS) or encryption at rest.   



Enabling Authentication
=======================

If you do not turn on authentication, it will be turned off by default. If you
specify ``cephx``, Ceph will look for the keyring in the default search path,
which includes ``/etc/ceph/keyring``.  You can override this location by adding
a ``keyring`` option in the ``[global]`` section of your ``ceph.conf`` file, but
this is not recommended.

To enable ``cephx`` on a cluster without authentication:

#. Create a ``client.admin`` key, and save a copy of the key for your client host::

	ceph auth get-or-create client.admin mon 'allow *' mds 'allow *' osd 'allow *' -o /etc/ceph/keyring

	**Warning:** This will clobber any existing ``/etc/ceph/keyring`` file. Be careful!

#. Generate a secret monitor ``mon.`` key::

    ceph-authtool --create --gen-key -n mon. /tmp/monitor-key

#. Copy the mon keyring into a ``keyring`` file in every monitor's ``mon data`` directory::

    cp /tmp/monitor-key /var/lib/ceph/mon/ceph-a/keyring

#. Generate a secret key for every OSD, where ``{$id}`` is the OSD number::

    ceph auth get-or-create osd.{$id} mon 'allow rwx' osd 'allow *' -o /var/lib/ceph/osd/ceph-{$id}/keyring

#. Generate a secret key for every MDS, where ``{$id}`` is the MDS letter::

    ceph auth get-or-create mds.{$id} mon 'allow rwx' osd 'allow *' mds 'allow *' -o /var/lib/ceph/mds/ceph-{$id}/keyring

#. Enable ``cephx`` authentication for versions ``0.52`` and above by setting
   the following options in the ``[global]`` section of your ``ceph.conf``::

    auth cluster required = cephx
    auth service required = cephx
    auth client required = cephx

#. Or, enable ``cephx`` authentication for versions ``0.51`` and below by
   setting the following option in the ``[global]`` section of your ``ceph.conf``::

    auth supported = cephx

.. deprecated:: 0.52


The ``client.admin`` Key
========================

By default, each Ceph command you execute on the command line assumes
that you are the ``client.admin`` default user. When running Ceph with
``cephx`` enabled, you need to have a ``client.admin`` key to run
``ceph`` commands.

.. important: To continue to run Ceph commands on the command line with
   ``cephx`` enabled, you need to create a key for the ``client.admin`` 
   user, and create a secret file under ``/etc/ceph``. 

The following command will generate and register a ``client.admin``
key on the monitor with admin capabilities and write it to a keyring
on the local file system.  If the key already exists, its current
value will be returned.	::

	sudo ceph auth get-or-create client.admin mds 'allow' osd 'allow *' mon 'allow *' > /etc/ceph/keyring

Generate a Key
==============

Keys enable a specific user to access the monitor, metadata server and
cluster according to capabilities assigned to the key.  Capabilities are
simple strings specifying some access permissions for a given server type.
Each server type has its own string.  All capabilities are simply listed
in ``{type}`` and ``{capability}`` pairs on the command line::

	sudo ceph auth get-or-create-key client.{username} {daemon1} {cap1} {daemon2} {cap2} ...

For example, to create a user ``client.foo`` with access 'rw' for
daemon type 'osd' and 'r' for daemon type 'mon'::

   sudo ceph auth get-or-create-key client.foo osd rw mon r > keyring.foo

.. note: User names are associated to user types, which include ``client``
   ``admin``, ``osd``, ``mon``, and ``mds``. In most cases, you will be 
   creating keys for ``client`` users.


List Keys in your Cluster
=========================

To list the keys registered in your cluster::

	sudo ceph auth list


Daemon Keyrings
===============

With the exception of the monitors, daemon keyrings are generated in
the same way that user keyrings are.  By default, the daemons store
their keyrings inside their data directory.  The default keyring
locations, and the capabilities necessary for the daemon to function,
are shown below.

``ceph-mon``

:Location: ``$mon_data/keyring``
:Capabilities: N/A

``ceph-osd``

:Location: ``$osd_data/keyring``
:Capabilities: ``mon 'allow rwx' osd 'allow *'``

``ceph-mds``

:Location: ``$mds_data/keyring``
:Capabilities: ``mds 'allow rwx' mds 'allow *' osd 'allow *'``

``radosgw``

:Location: ``$rgw_data/keyring``
:Capabilities: mon 'allow r' osd 'allow rwx'


Note that the monitor keyring contains a key but no capabilities, and
is not part of the cluster ``auth`` database.

The daemon data directory locations default to directories of the form::

  /var/lib/ceph/$daemontype/$cluster-$id

For example, ``osd.12`` would be::

  /var/lib/ceph/osd/ceph-12

You can override these locations, but it is not recommended.

Monitor Keyrings
================

Use the ``ceph-authtool`` command to generate a monitor key and kerying. ::

      sudo ceph-authtool {keyring} --create-keyring --gen-key -n mon.

A cluster with multiple monitors must have identical keyrings for all 
monitors. So you must copy the keyring to each monitor host under the
following directory::

  /var/lib/ceph/mon/$cluster-$id

