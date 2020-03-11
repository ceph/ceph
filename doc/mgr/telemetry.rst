Telemetry plugin
================
The telemetry plugin sends anonymous data about the cluster, in which it is running, back to the Ceph project.

The data being sent back to the project does not contain any sensitive data like pool names, object names, object contents or hostnames.

It contains counters and statistics on how the cluster has been deployed, the version of Ceph, the distribition of the hosts and other parameters which help the project to gain a better understanding of the way Ceph is used.

Data is sent over HTTPS to *telemetry.ceph.com*

Enabling
--------

The *telemetry* module is enabled with::

  ceph mgr module enable telemetry

To allow the telemetry module to start sharing data::

  ceph telemetry on

Please note: Telemetry data is licensed under the Community Data License Agreement - Sharing - Version 1.0 (https://cdla.io/sharing-1-0/). Hence, telemetry module can start sharing data only after you add 'sharing-1-0' to the 'ceph telemetry on' command.

Telemetry can be disabled at any time with::

  ceph telemetry off

Interval
--------
The module compiles and sends a new report every 24 hours by default.

Contact and Description
-----------------------
A contact and description can be added to the report, this is optional.

  ceph telemetry config-set contact 'John Doe <john.doe@example.com>'
  ceph telemetry config-set description 'My first Ceph cluster'

Show report
-----------
The report is sent in JSON format, and can be printed::

  ceph telemetry show

So you can inspect the content if you have privacy concerns.
