OSD Support Module
==================
The OSD Support module holds osd specific functionality that
is needed by different components like the orchestrators.

In current scope:

* osd draining

Enabling
--------
When an orchestrator is used this should be enabled as a dependency.
(*currently only valid for the cephadm orchestrator*)

The *osd_support* module is manually enabled with::

  ceph mgr module enable osd_support

Commands
--------

Draining
########

This mode is for draining OSDs gracefully. `Draining` in this context means gracefully emptying out OSDs by setting their
weight to zero. An OSD is considered to be drained when no PGs are left.

::

  ceph osd drain $osd_id

Takes a $osd_id and schedules it for draining. Since that process can take
quite some time, the operation will be executed in the background. To query the status
of the operation you can use:

::

  ceph osd drain status

This gives you the status of all running operations in this format::

  [{'osd_id': 0, 'pgs': 1234}, ..]

If you wish to stop an OSD from being drained::

  ceph osd drain stop [$osd_id]

Stops all **scheduled** osd drain operations (not the operations that have been started already)
if no $osd_ids are given. If $osd_ids are present it only operates on them.
To stop and reset the weight of already started operations we need to save the initial weight
(see 'Ideas for improvement')


Ideas for improvement
----------------------
- add health checks set_health_checks
- use objects to represent OSDs
  - allows timestamps, trending information etc
- save osd drain state (at least the osd_ids in the mon store)
  - resume after a mgr crash
  - save the initial weight of a osd i.e. (set to initial weight on abort)
