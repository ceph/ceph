.. _telemetry:

Telemetry Module
================

The telemetry module sends anonymous data about the cluster back to the Ceph
developers to help understand how Ceph is used and what problems users may
be experiencing.

Channels
--------

The telemetry report is broken down into several "channels," each with
a different type of information.  Assuming telemetry has been enabled,
individual channels can be turned on and off.  (If telemetry is off,
the per-channel setting has no effect.)

* **basic** (default: on): Basic information about the cluster

    - capacity of the cluster
    - number of monitors, managers, OSDs, MDSs, radosgws, or other daemons
    - software version currently being used
    - number and types of RADOS pools and CephFS file systems
    - names of configuration options that have been changed from their
      default (but *not* their values)

* **crash** (default: on): Information about daemon crashes, including

    - type of daemon
    - version of the daemon
    - operating system (OS distribution, kernel version)
    - stack trace identifying where in the Ceph code the crash occurred

* **ident** (default: on): User-provided identifying information about
  the cluster

    - cluster description
    - contact email address

The data being reported does *not* contain any sensitive
data like pool names, object names, object contents, or hostnames.

It contains counters and statistics on how the cluster has been
deployed, the version of Ceph, the distribition of the hosts and other
parameters which help the project to gain a better understanding of
the way Ceph is used.

Data is sent over HTTPS to *telemetry.ceph.com*.

Enabling the module
-------------------

The module must first be enabled.  Note that even if the module is
enabled, telemetry is still "off" by default, so simply enabling the
module will *NOT* result in any data being shared.::

  ceph mgr module enable telemetry

Sample report
-------------

You can look at what data is reported at any time with the command::

  ceph telemetry show

If you have any concerns about privacy with regard to the information included in
this report, please contact the Ceph developers.

Channels
--------

Individual channels can be enabled or disabled with::

  ceph config set mgr mgr/telemetry/channel_ident false
  ceph config set mgr mgr/telemetry/channel_basic false
  ceph config set mgr mgr/telemetry/channel_crash false
  ceph telemetry show

Enabling Telemetry
------------------

To allow the *telemetry* module to start sharing data,::

  ceph telemetry on

Telemetry can be disabled at any time with::

  ceph telemetry off

Interval
--------

The module compiles and sends a new report every 24 hours by default.
You can adjust this interval with::

  ceph config set mgr mgr/telemetry/interval 72    # report every three days

Contact and Description
-----------------------

A contact and description can be added to the report.  This is
completely optional, and disabled by default.::

  ceph config set mgr mgr/telemetry/contact 'John Doe <john.doe@example.com>'
  ceph config set mgr mgr/telemetry/description 'My first Ceph cluster'
  ceph config set mgr mgr/telemetry/channel_ident true

