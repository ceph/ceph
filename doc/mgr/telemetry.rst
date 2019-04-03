.. _telemetry:

Telemetry Module
================

The telemetry module sends anonymous data about the cluster back to the Ceph
developers to help understand how Ceph is used and what problems users may
be experiencing.

Reported telemetry includes:

  * capacity of the cluster 
  * number of monitors, managers, OSDs, MDSs, radosgws, or other daemons
  * software version currently being used
  * number and types of RADOS pools and CephFS file systems
  * information about daemon crashes, including

    - type of daemon
    - version of the daemon
    - operating system (OS distribution, kernel version)
    - stack trace identifying where in the Ceph code the crash occurred

The data being reported does *not* contain any sensitive
data like pool names, object names, object contents, or hostnames.

It contains counters and statistics on how the cluster has been
deployed, the version of Ceph, the distribution of the hosts and other
parameters which help the project to gain a better understanding of
the way Ceph is used.

Data is sent over HTTPS to *telemetry.ceph.com*.

Sample report
-------------

You can look at what data is reported at any time with the command::

  ceph mgr module enable telemetry
  ceph telemetry show

If you have any concerns about privacy with regard to the information included in
this report, please contact the Ceph developers.

Enabling
--------

The *telemetry* module is enabled with::

  ceph mgr module enable telemetry
  ceph telemetry on

Telemetry can be disabled with::

  ceph telemetry off

Interval
--------

The module compiles and sends a new report every 72 hours by default.
You can adjust this interval with::

  ceph config set mgr mgr/telemetry/interval 24    # report every day

Contact and Description
-----------------------

A contact and description can be added to the report.  This is completely optional.::

  ceph config set mgr mgr/telemetry/contact 'John Doe <john.doe@example.com>'
  ceph config set mgr mgr/telemetry/description 'My first Ceph cluster'

Show report
-----------

The report is sent in JSON format, and can be printed::

  ceph telemetry show

So you can inspect the content if you have privacy concerns.
