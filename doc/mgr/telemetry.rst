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


Interval
--------
The module compiles and sends a new report every 72 hours by default.

Sending telemetry through a proxy
---------------------------------

If the cluster cannot directly connect to the configured telemetry
endpoint (default *telemetry.ceph.com*), you can configure a HTTP/HTTPS
proxy server with::

  ceph config-key set mgr/telemetry/proxy https://10.0.0.1:8080

You can also include a *user:pass* if needed::

  ceph config-key set mgr/telemetry/proxy https://ceph:telemetry@10.0.0.1:8080


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
