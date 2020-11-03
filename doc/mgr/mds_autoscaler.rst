MDS Autoscaler Module
=====================

The MDS Autoscaler Module monitors ``fsmap`` update notifications from the mgr
daemon and takes action to spawn or kill MDS daemons for a file-system as per
changes to the:

- ``max_mds`` config value
- ``standby_count_wanted`` config value
- standby promotions to active MDS state in case of active MDS rank death

Bumping up the ``max_mds`` config option value causes a standby mds to be promoted
to hold an active rank. This leads to a drop in standby mds count. The MDS
Autoscaler module detects this deficit and the orchestrator module is notified
about the required MDS count. The orchestrator back-end then takes necessary
measures to spawn standby MDSs.

Dropping the ``max_mds`` config option causes the orchestrator back-end to kill
standby mds to achieve the new reduced count. Preferably standby mds are chosen
to be killed when the ``max_mds`` count is dropped.

An increment and decrement of the ``standby_count_wanted`` config option value
has a similar effect on the total MDS count. The orchestrator is notified about
the change and necessary action to spawn or kill standby MDSs is taken.

A death of an active MDS rank also causes promotion of a standby mds to occupy
the required active rank. The MDS Autoscaler notices the change in the standby
mds count and a message is passed to the orchestrator to maintain the necessary
MDS count.

NOTE: There is no CLI associated with the MDS Autoscaler Module.
