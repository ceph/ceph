--------------------------------
Configuring the iSCSI Initiators
--------------------------------

- `iSCSI Initiator for Linux <../iscsi-initiator-linux>`_

- `iSCSI Initiator for Microsoft Windows <../iscsi-initiator-win>`_

- `iSCSI Initiator for VMware ESX <../iscsi-initiator-esx>`_

    .. warning::

        Applications that use SCSI persistent group reservations (PGR) and
        SCSI 2 based reservations are not supported when exporting a RBD image
        through more than one iSCSI gateway.

.. toctree::
  :maxdepth: 1
  :hidden:

  Linux <iscsi-initiator-linux>
  Microsoft Windows <iscsi-initiator-win>
  VMware ESX <iscsi-initiator-esx>
