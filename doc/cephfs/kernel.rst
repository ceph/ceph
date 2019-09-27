====================================
 Mount CephFS with the Kernel Driver
====================================

It is recommended to install the ``/sbin/mount.ceph`` kernel mount
helper if working with the kernel cephfs driver. To mount CephFS with
the kernel driver you may use the ``mount`` command::

	# mkdir /mnt/mycephfs
	# mount -t ceph :/ /mnt/mycephfs

Omitting the monitor addresses will cue ``mount.ceph`` to look them up
in the local configuration file.  If you know at least one of the monitor
addresses and ports you can specify them directly in a comma-separated
list, and the kernel will avoid looking them up::

        # mount -t ceph 192.168.0.1:6789,192.168.0.2:6789:/ /mnt/mycephfs

To mount a subtree of the cephfs root, append the path to the device
string ::

        # mount -t ceph :/subvolume/dir1/dir2  /mnt/mycephfs -o name=fs

To mount the Ceph file system with ``cephx`` authentication enabled, the
kernel must authenticate with the cluster. The default ``name=`` option
is ``guest``.  The mount.ceph helper will automatically attempt to find
a secret key in a cephx keyring if it's not specified. For example, to
mount the filesystem as the cephx user ``fs``::

        # mount -t ceph :/ /mnt/mycephfs -o name=fs

The secret can also be specified manually with the ``secret=`` option.::

	# mount -t ceph :/ /mnt/mycephfs -o name=fs,secret=AQATSKdNGBnwLhAAnNDKnH65FmVKpXZJVasUeQ==

For legacy usage, mount.ceph can be told to read a lone secret from a
file. For example::

	# mount -t ceph :/ /mnt/mycephfs -o name=fs,secretfile=/etc/ceph/admin.secret

See `User Management`_ for details on cephx.

If you have more than one file system, specify which one to mount using
the ``mds_namespace`` option, e.g.::

        # mount -t ceph :/ -o name=fs,mds_namespace=myfs

To unmount the Ceph file system, you may use the ``umount`` command. For example:: 

	# umount /mnt/mycephfs

.. tip:: Ensure that you are not within the file system directories before
   executing this command.

See `mount.ceph`_ for details. For troubleshooting, see :ref:`kernel_mount_debugging`.

.. _mount.ceph: ../../man/8/mount.ceph/
.. _User Management: ../../rados/operations/user-management/
