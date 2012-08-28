================
 Authentication
================

Default users and pools are suitable for initial testing purposes. For test bed 
and production environments, you should create users and assign pool access to 
the users.

Enabling Authentication
-----------------------
In the ``[global]`` settings of your ``ceph.conf`` file, you must enable 
authentication for your cluster. ::

	[global]
		auth supported = cephx

The valid values are ``cephx`` or ``none``. If you specify ``cephx``,
Ceph will look for the keyring in the default search path, which
includes ``/etc/ceph/keyring``.  You can override this location by
adding a ``keyring`` option in the ``[global]`` section of your
``ceph.conf`` file, but this is not recommended.

The ``client.admin`` Key
------------------------

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
value will be returned.

::

        sudo ceph auth get-or-create client.admin mds 'allow' osd 'allow *' mon 'allow *' > /etc/ceph/keyring

Generate a Key
--------------

Keys enable a specific user to access the monitor, metadata server and
cluster according to capabilities assigned to the key.  Capabilities are
simple strings specifying some access permissions for a given server type.
Each server type has its own string.  All capabilities are simply listed
in ``{type}`` and ``{capability}`` pairs on the command line::

	sudo ceph auth get-or-create client.{username} {daemon1} {cap1} {daemon2} {cap2} ...

For example, to create a user ``client.foo`` with access 'rw' for
daemon type 'osd' and 'r' for daemon type 'mon'::

	sudo ceph auth get-or-create client.foo osd rw mon r > keyring.foo

.. note: User names are associated to user types, which include ``client``
   ``admin``, ``osd``, ``mon``, and ``mds``. In most cases, you will be 
   creating keys for ``client`` users.


List Keys in your Cluster
-------------------------

To list the keys registered in your cluster::

	sudo ceph auth list


Daemon keyrings
---------------

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

The monitor key can be created with ``ceph-authtool`` command, and
must be identical across all monitors::

      sudo ceph-authtool {keyring} --create-keyring --gen-key -n mon.
