================
Host Maintenance
================

All hosts that support Ceph daemons need to support maintenance activity, whether the host
is physical or virtual. This means that management workflows should provide
a simple and consistent way to support this operational requirement. This document defines
the maintenance strategy that could be implemented in cephadm and mgr/cephadm.


High Level Design
=================
Placing a host into maintenance, adopts the following workflow;

#. confirm that the removal of the host does not impact data availability (the following
   steps will assume it is safe to proceed)

   * ``orch host ok-to-stop <host>`` would be used here

#. if the host has osd daemons, apply noout to the host subtree to prevent data migration
   from triggering during the planned maintenance slot.
#. Stop the ceph target (all daemons stop)
#. Disable the ceph target on that host, to prevent a reboot from automatically starting
   ceph services again)


Exiting Maintenance, is basically the reverse of the above sequence

Admin Interaction
=================
The ceph orch command will be extended to support maintenance. 

.. code-block::

    ceph orch host enter-maintenance <host> [ --check ]
    ceph orch host exit-maintenance <host> 

.. note:: In addition, the host's status should be updated to reflect whether it
   is in maintenance or not. 

The 'check' Option 
__________________
The orch host ok-to-stop command focuses on ceph daemons (mon, osd, mds), which
provides the first check. However, a ceph cluster also uses other types of daemons
for monitoring, management and non-native protocol support which means the 
logic will need to consider service impact too. The 'check' option provides 
this additional layer to alert the user of service impact to *secondary*
daemons.

The list below shows some of these additional daemons.

* mgr (not included in ok-to-stop checks) 
* prometheus, grafana, alertmanager
* rgw
* haproxy
* iscsi gateways
* ganesha gateways

By using the --check option first, the Admin can choose whether to proceed. This 
workflow is obviously optional for the CLI user, but could be integrated into the
UI workflow to help less experienced administrators manage the cluster.

By adopting this two-phase approach, a UI based workflow would look something
like this.

#. User selects a host to place into maintenance
 
   * orchestrator checks for data **and** service impact
#. If potential impact is shown, the next steps depend on the impact type
   
   * **data availability** : maintenance is denied, informing the user of the issue
   * **service availability** : user is provided a list of affected services and 
     asked to confirm


Components Impacted
===================
Implementing this capability will require changes to the following; 

* cephadm

  * Add maintenance subcommand with the following 'verbs'; enter, exit, check

* mgr/cephadm

  * add methods to CephadmOrchestrator for enter/exit and check
  * data gathering would be skipped for hosts in a maintenance state

* mgr/orchestrator

  * add CLI commands to OrchestratorCli which expose the enter/exit and check interaction


Ideas for Future Work
=====================
#. When a host is placed into maintenance, the time of the event could be persisted. This
   would allow the orchestrator layer to establish a maintenance window for the task and 
   alert if the maintenance window has been exceeded.
#. The maintenance process could support plugins to allow other integration tasks to be
   initiated as part of the transition to and from maintenance. This plugin capability could
   support actions like; 
   
   * alert suppression to 3rd party monitoring framework(s)
   * service level reporting, to record outage windows
