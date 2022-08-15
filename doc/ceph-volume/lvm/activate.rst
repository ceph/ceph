.. _ceph-volume-lvm-activate:

``activate``
============
          
Once :ref:`ceph-volume-lvm-prepare` is completed, and all the various steps
that entails are done, the volume is ready to get "activated".

This activation process enables a systemd unit that persists the OSD ID and its
UUID (also called ``fsid`` in Ceph CLI tools), so that at boot time it can
understand what OSD is enabled and needs to be mounted.

.. note:: The execution of this call is fully idempotent, and there is no
          side-effects when running multiple times

For OSDs deployed by cephadm, please refer to :ref:cephadm-osd-activate: 
instead.

New OSDs
--------
To activate newly prepared OSDs with ceph-volume, one must provide the :term:OSD uuid and optionally the :term:OSD id.

    ceph-volume lvm activate --bluestore 0 0263644D-0BF1-4D6D-BC34-28BD98AE3BC8
    
    [root@node1 ~]# ceph-volume lvm list
    
    ====== osd.8 =======

     [block]       /dev/ceph-39a5214f-bdc4-4869-a07e-448f6d21cd31/osd-block-00d4e8cb-282e-41c6-a64c-2590dad3e8be

      block device              /dev/ceph-39a5214f-bdc4-4869-a07e-448f6d21cd31/osd-block-00d4e8cb-282e-41c6-a64c-2590dad3e8be
      block uuid                GuYMbR-bJCH-n3Rm-4BKs-rfyL-VdTd-DyKVMM
      cephx lockbox secret      
      cluster fsid              ba757a9a-01d9-11ed-b2f7-fa163e4c5d9d
      cluster name              ceph
      crush device class        
      encrypted                 0
      osd fsid                  00d4e8cb-282e-41c6-a64c-2590dad3e8be
      osd id                    8
      osdspec affinity          
      type                      block
      vdo                       0
      devices                   /dev/vdb6
      
    [root@node1 ~]# ceph-volume lvm activate 8 00d4e8cb-282e-41c6-a64c-2590dad3e8be
    Running command: /usr/bin/chown -R ceph:ceph /var/lib/ceph/osd/ceph-8
    Running command: /usr/bin/ceph-bluestore-tool --cluster=ceph prime-osd-dir --dev /dev/ceph-39a5214f-bdc4-4869-a07e-448f6d21cd31/osd-block-00d4e8cb-282e-41c6-a64c-2590dad3e8be --path /var/lib/ceph/osd/ceph-8 --no-mon-config
    Running command: /usr/bin/ln -snf /dev/ceph-39a5214f-bdc4-4869-a07e-448f6d21cd31/osd-block-00d4e8cb-282e-41c6-a64c-2590dad3e8be /var/lib/ceph/osd/ceph-8/block
    Running command: /usr/bin/chown -h ceph:ceph /var/lib/ceph/osd/ceph-8/block
    Running command: /usr/bin/chown -R ceph:ceph /dev/dm-7
    Running command: /usr/bin/chown -R ceph:ceph /var/lib/ceph/osd/ceph-8
    Running command: /usr/bin/systemctl enable ceph-volume@lvm-8-00d4e8cb-282e-41c6-a64c-2590dad3e8be
    Running command: /usr/bin/systemctl enable --runtime ceph-osd@8
    Running command: /usr/bin/systemctl start ceph-osd@8
    --> ceph-volume lvm activate successful for osd ID: 8

For example use `OSD uuid` only::

    ceph-volume lvm activate --bluestore "" 0263644D-0BF1-4D6D-BC34-28BD98AE3BC8
    
    [root@node1 ~]# ceph-volume lvm list
    
    ====== osd.8 =======

     [block]       /dev/ceph-39a5214f-bdc4-4869-a07e-448f6d21cd31/osd-block-00d4e8cb-282e-41c6-a64c-2590dad3e8be

      block device              /dev/ceph-39a5214f-bdc4-4869-a07e-448f6d21cd31/osd-block-00d4e8cb-282e-41c6-a64c-2590dad3e8be
      block uuid                GuYMbR-bJCH-n3Rm-4BKs-rfyL-VdTd-DyKVMM
      cephx lockbox secret
      cluster fsid              ba757a9a-01d9-11ed-b2f7-fa163e4c5d9d
      cluster name              ceph
      crush device class
      encrypted                 0
      osd fsid                  00d4e8cb-282e-41c6-a64c-2590dad3e8be
      osd id                    8
      osdspec affinity
      type                      block
      vdo                       0
      devices                   /dev/vdb6
      
    [root@node1 ~]# ceph-volume lvm activate "" 00d4e8cb-282e-41c6-a64c-2590dad3e8be
    Running command: /usr/bin/chown -R ceph:ceph /var/lib/ceph/osd/ceph-8
    Running command: /usr/bin/ceph-bluestore-tool --cluster=ceph prime-osd-dir --dev /dev/ceph-39a5214f-bdc4-4869-a07e-448f6d21cd31/osd-block-00d4e8cb-282e-41c6-a64c-2590dad3e8be --path /var/lib/ceph/osd/ceph-8 --no-mon-config
    Running command: /usr/bin/ln -snf /dev/ceph-39a5214f-bdc4-4869-a07e-448f6d21cd31/osd-block-00d4e8cb-282e-41c6-a64c-2590dad3e8be /var/lib/ceph/osd/ceph-8/block
    Running command: /usr/bin/chown -h ceph:ceph /var/lib/ceph/osd/ceph-8/block
    Running command: /usr/bin/chown -R ceph:ceph /dev/dm-7
    Running command: /usr/bin/chown -R ceph:ceph /var/lib/ceph/osd/ceph-8
    Running command: /usr/bin/systemctl enable ceph-volume@lvm-8-00d4e8cb-282e-41c6-a64c-2590dad3e8be
     stderr: Created symlink /etc/systemd/system/multi-user.target.wants/ceph-volume@lvm-8-00d4e8cb-282e-41c6-a64c-2590dad3e8be.service → /usr/lib/systemd/system/ceph-volume@.service.
    Running command: /usr/bin/systemctl enable --runtime ceph-osd@8
    Running command: /usr/bin/systemctl start ceph-osd@8
    --> ceph-volume lvm activate successful for osd ID: 8
    
    [root@node1 ~]# ceph osd tree
    ID   CLASS  WEIGHT   TYPE NAME         STATUS  REWEIGHT  PRI-AFF
     -1         0.14635  root default
     -3         0.04099      host node1
      0    hdd  0.03119          osd.0         up   1.00000  1.00000
      7    hdd  0.00490          osd.7         up   1.00000  1.00000
      8    hdd  0.00490          osd.8         up   1.00000  1.00000
       
.. note:: The UUID is stored in the ``fsid`` file in the OSD path, which is
          generated when :ref:`ceph-volume-lvm-prepare` is used.

Activating all OSDs
-------------------

.. note:: For OSDs deployed by cephadm, please refer to :ref:cephadm-osd-activate: 
          instead.

It is possible to activate all existing OSDs at once by using the ``--all``
flag. For example::

    ceph-volume lvm activate --all

This call will inspect all the OSDs created by ceph-volume that are inactive
and will activate them one by one. If any of the OSDs are already running, it
will report them in the command output and skip them, making it safe to rerun
(idempotent).

requiring uuids
^^^^^^^^^^^^^^^
The :term:`OSD uuid` is being required as an extra step to ensure that the
right OSD is being activated. It is entirely possible that a previous OSD with
the same id exists and would end up activating the incorrect one.


dmcrypt
^^^^^^^
If the OSD was prepared with dmcrypt by ceph-volume, there is no need to
specify ``--dmcrypt`` on the command line again (that flag is not available for
the ``activate`` subcommand). An encrypted OSD will be automatically detected.


Discovery
---------
With OSDs previously created by ``ceph-volume``, a *discovery* process is
performed using :term:`LVM tags` to enable the systemd units.

The systemd unit will capture the :term:`OSD id` and :term:`OSD uuid` and
persist it. Internally, the activation will enable it like::

    systemctl enable ceph-volume@lvm-$id-$uuid

For example::

    systemctl enable ceph-volume@lvm-0-8715BEB4-15C5-49DE-BA6F-401086EC7B41

Would start the discovery process for the OSD with an id of ``0`` and a UUID of
``8715BEB4-15C5-49DE-BA6F-401086EC7B41``.

.. note:: for more details on the systemd workflow see :ref:`ceph-volume-lvm-systemd`

The systemd unit will look for the matching OSD device, and by looking at its
:term:`LVM tags` will proceed to:

#. Mount the device in the corresponding location (by convention this is
``/var/lib/ceph/osd/<cluster name>-<osd id>/``)

#. Ensure that all required devices are ready for that OSD. In the case of
a journal (when ``--filestore`` is selected) the device will be queried (with
``blkid`` for partitions, and lvm for logical volumes) to ensure that the
correct device is being linked. The symbolic link will *always* be re-done to
ensure that the correct device is linked.

#. Start the ``ceph-osd@0`` systemd unit

.. note:: The system infers the objectstore type (filestore or bluestore) by
          inspecting the LVM tags applied to the OSD devices

Existing OSDs
-------------
For existing OSDs that have been deployed with ``ceph-disk``, they need to be
scanned and activated :ref:`using the simple sub-command <ceph-volume-simple>`.
If a different tool was used then the only way to port them over to the new
mechanism is to prepare them again (losing data). See
:ref:`ceph-volume-lvm-existing-osds` for details on how to proceed.

Summary
-------
To recap the ``activate`` process for :term:`bluestore`:

#. Require both :term:`OSD id` and :term:`OSD uuid`
#. Enable the system unit with matching id and uuid
#. Create the ``tmpfs`` mount at the OSD directory in
   ``/var/lib/ceph/osd/$cluster-$id/``
#. Recreate all the files needed with ``ceph-bluestore-tool prime-osd-dir`` by
   pointing it to the OSD ``block`` device.
#. The systemd unit will ensure all devices are ready and linked
#. The matching ``ceph-osd`` systemd unit will get started

And for :term:`filestore`:

#. Require both :term:`OSD id` and :term:`OSD uuid`
#. Enable the system unit with matching id and uuid
#. The systemd unit will ensure all devices are ready and mounted (if needed)
#. The matching ``ceph-osd`` systemd unit will get started
