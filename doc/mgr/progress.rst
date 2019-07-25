Progress Module
===============

The progress module is primary used for informing users about the recovery progress
of PGs (Placement Groups) that are affected by events such as OSDs being marked in/out,
pg_autoscaler trying to match the target PG number, balancer trying to perform 
upmap/crush-compat optimization and etc. 

Ongoing events and its progress can be seen in ``ceph -s`` and a more detailed version
of this is outputted to the file mgr.x.log.

Commands
--------
::

        ceph progress

Shows the summary of all the ongoing/completed events and its duration
in chronological order.
::

        ceph progress json

Shows the summary of ongoing/completed events in json format
::
        ceph progress clear

The clear command isn't usually needed - it's to enable
the admin to "kick" this module if it seems to have done
something wrong (e.g. we have a bug causing a progress event that never finishes)
