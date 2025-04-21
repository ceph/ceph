MDS Autoscaler Module
=====================

The MDS Autoscaler Module monitors file systems to ensure sufficient MDS
daemons are available. It works by adjusting the placement specification for
the orchestrator backend of the MDS service. To enable, use:

.. sh:

   ceph mgr module enable mds_autoscaler

The module will monitor the following file system settings to inform
placement count adjustments:

- ``max_mds`` file system setting
- ``standby_count_wanted`` file system setting

The Ceph monitor daemons are still responsible for promoting or stopping MDS
according to these settings. The ``mds_autoscaler`` simply adjusts the
number of MDS which are spawned by the orchestrator.

.. note: There is no CLI or module configurations as of now. Enable or disable
   the module to turn on or off.
