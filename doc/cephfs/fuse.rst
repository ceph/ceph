=========================
Mount Ceph FS using FUSE
=========================

Before mounting a Ceph File System in User Space (FUSE), ensure that the client
host has a copy of the Ceph configuration file and a keyring with CAPS for the
Ceph metadata server.

#. From your client host, copy the Ceph configuration file from the monitor host 
   to the ``/etc/ceph`` directory. :: 

	sudo mkdir -p /etc/ceph
	sudo scp {user}@{server-machine}:/etc/ceph/ceph.conf /etc/ceph/ceph.conf

#. From your client host, copy the Ceph keyring from the monitor host to 
   to the ``/etc/ceph`` directory. :: 

	sudo scp {user}@{server-machine}:/etc/ceph/ceph.keyring /etc/ceph/ceph.keyring

#. Ensure that the Ceph configuration file and the keyring have appropriate 
   permissions set on your client machine  (e.g., ``chmod 644``).

For additional details on ``cephx`` configuration, see 
`CEPHX Config Reference`_.

To mount the Ceph file system as a FUSE, you may use the ``ceph-fuse`` command.
For example::

	sudo mkdir /home/usernname/cephfs
	sudo ceph-fuse -m 192.168.0.1:6789 /home/username/cephfs

If you have more than one filesystem, specify which one to mount using
the ``--client_mds_namespace`` command line argument, or add a
``client_mds_namespace`` setting to your ``ceph.conf``.

See `ceph-fuse`_ for additional details.

To automate mounting ceph-fuse, you may add an entry to the system fstab_.
Additionally, ``ceph-fuse@.service`` and ``ceph-fuse.target`` systemd units are
available. As usual, these unit files declare the default dependencies and
recommended execution context for ``ceph-fuse``. An example ceph-fuse mount on
``/mnt`` would be::

	sudo systemctl start ceph-fuse@/mnt.service

A persistent mount point can be setup via::

	sudo systemctl enable ceph-fuse@/mnt.service

.. _ceph-fuse: ../../man/8/ceph-fuse/
.. _fstab: ./fstab
.. _CEPHX Config Reference: ../../rados/configuration/auth-config-ref
