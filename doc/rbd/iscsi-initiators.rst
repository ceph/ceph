.. _configuring-the-iscsi-initiators:

--------------------------------
Configuring the iSCSI Initiators
--------------------------------

.. include:: iscsi-deprecation.inc.rst

- :doc:`iscsi-initiator-linux`

- :doc:`iscsi-initiator-win`

- :doc:`iscsi-initiator-esx`

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
