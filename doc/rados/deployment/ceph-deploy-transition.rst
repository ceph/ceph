==============================
 Transitioning to ceph-deploy
==============================

If you have an existing cluster that you deployed with ``mkcephfs``, 
you will need to make a few changes to your configuration to 
ensure that your cluster will work with ``ceph-deploy``.

Monitor Keyring
===============

You will need to add ``caps mon = "allow *"`` to your monitor keyring if it is
not already in the keyring. By default, the monitor keyring is located under
``/var/lib/ceph/mon/ceph-$id/keyring``. When you have added the ``caps``
setting, your monitor keyring should look something like this::

	[mon.]
		key = AQBJIHhRuHCwDRAAZjBTSJcIBIoGpdOR9ToiyQ==
		caps mon = "allow *" 
		
Adding ``caps mon = "allow *"`` will ease the transition from ``mkcephfs`` to
``ceph-deploy`` by allowing ``ceph-create-keys`` to use the ``mon.`` keyring
file in ``$mon_data`` and get the caps it needs.


Use Default Paths
=================

Under the ``/var/lib/ceph`` directory, the ``mon`` and ``osd`` directories need
to use the default paths.

- **OSDs**: The path should be ``/var/lib/ceph/osd/ceph-$id``
- **MON**: The path should be  ``/var/lib/ceph/mon/ceph-$id``

Under those directories, the keyring should be in a file named ``keyring``.