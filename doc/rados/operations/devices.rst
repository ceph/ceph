
.. _devices:

Device Management
=================

Ceph tracks which hardware storage devices (e.g., HDDs, SSDs) are consumed by
which daemons, and collects health metrics about those devices in order to
provide tools to predict and/or automatically respond to hardware failure.

Device tracking
---------------

You can query which storage devices are in use with::

  ceph device ls

You can also list devices by daemon or by host::

  ceph device ls-by-daemon <daemon>
  ceph device ls-by-host <host>

For any individual device, you can query information about its
location and how it is being consumed with::

  ceph device info <devid>


Enabling monitoring
-------------------

Ceph can also monitor health metrics associated with your device.  For
example, SATA hard disks implement a standard called SMART that
provides a wide range of internal metrics about the device's usage and
health, like the number of hours powered on, number of power cycles,
or unrecoverable read errors.  Other device types like SAS and NVMe
implement a similar set of metrics (via slightly different standards).
All of these can be collected by Ceph via the ``smartctl`` tool.

You can enable or disable health monitoring with::

  ceph device monitoring on

or::

  ceph device monitoring off


Scraping
--------

If monitoring is enabled, metrics will automatically be scraped at regular intervals.  That interval can be configured with::

  ceph config set mgr mgr/devicehealth/scrape_frequency <seconds>

The default is to scrape once every 24 hours.

You can manually trigger a scrape of all devices with::

  ceph device scrape-health-metrics

A single device can be scraped with::

  ceph device scrape-health-metrics <device-id>

Or a single daemon's devices can be scraped with::

  ceph device scrape-daemon-health-metrics <who>

The stored health metrics for a device can be retrieved (optionally
for a specific timestamp) with::

  ceph device get-health-metrics <devid> [sample-timestamp]

Failure prediction
------------------

TBD

Health alerts
-------------

The ``mgr/devicehealth/warn_threshold`` controls how soon an expected
device failure must be before we generate a health warning.

The stored life expectancy of all devices can be checked, and any
appropriate health alerts generated, with::

  ceph device check-health

Automatic Mitigation
--------------------

If the ``mgr/devicehealth/self_heal`` option is enabled (it is by
default), then for devices that are expected to fail soon the module
will automatically migrate data away from them by marking the devices
"out".

The ``mgr/devicehealth/mark_out_threshold`` controls how soon an
expected device failure must be before we automatically mark an osd
"out".
