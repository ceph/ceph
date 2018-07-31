Devicehealth plugin
===================

The *devicehealth* plugin includes code to manage physical devices
that back Ceph daemons (e.g., OSDs).  This includes scraping health
metrics (e.g., SMART) and responding to health metrics by migrating
data away from failing devices.

Enabling
--------

The *devicehealth* module is enabled with::

  ceph mgr module enable devicehealth

(It is enabled by default.)

Scraping
--------

Health metrics can be scraped from all devices with::

  ceph device scrape-health-metrics

A single device can be scraped with::

  ceph device scrape-health-metrics <device-id>

Or a single daemon's devices can be scraped with::

  ceph device scrape-daemon-health-metrics <who>


Health monitoring
-----------------

By default, the devicehealth module wakes up periodically and checks
the health of all devices in the system.  This will raise health
alerts if devices are expected to fail soon.  This can be disabled by
turning off the ``mgr/devicehealth/enable_monitoring`` option.

The ``mgr/devicehealth/warn_threshold`` controls how soon an expected
device failure must be before we generate a health warning.

If the ``mgr/devicehealth/self_heal`` option is enabled (it is by
default), then for devices that are expected to fail soon the module
will automatically migrate data away from them by marking the devices
"out".

The ``mgr/devicehealth/mark_out_threshold`` controls how soon an
expected device failure must be before we automatically mark an osd
"out".
