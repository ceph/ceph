.. _devices:

Device Management
=================

Device management allows Ceph to address hardware failure. Ceph tracks hardware
storage devices (HDDs, SSDs) to see which devices are managed by which daemons.
Ceph also collects health metrics about these devices. By doing so, Ceph can
provide tools that predict hardware failure and can automatically respond to
hardware failure.

Device tracking
---------------

To see a list of the storage devices that are in use, run the following
command:

.. prompt:: bash $

   ceph device ls

Alternatively, to list devices by daemon or by host, run a command of one of
the following forms:

.. prompt:: bash $

   ceph device ls-by-daemon <daemon>
   ceph device ls-by-host <host>

To see information about the location of a specific device and about how the
device is being consumed, run a command of the following form:

.. prompt:: bash $

   ceph device info <devid>

Identifying physical devices
----------------------------

To make the replacement of failed disks easier and less error-prone, you can
(in some cases) "blink" the drive's LEDs on hardware enclosures by running a
command of the following form:

.. prompt:: bash $

   ceph device light on|off <devid> [ident|fault] [--force]

.. note:: Using this command to blink the lights might not work. Whether it
   works will depend upon such factors as your kernel revision, your SES
   firmware, or the setup of your HBA.

The ``<devid>`` parameter is the device identification. To retrieve this
information, run the following command:

.. prompt:: bash $

   ceph device ls

The ``[ident|fault]`` parameter determines which kind of light will blink.  By
default, the ``ident`` light is used.

.. note:: This command works only if the Cephadm or the Rook
   :ref:`orchestrator <orchestrator-cli-module>` module is enabled.  To see
   which orchestrator module is enabled, run the following command:

   .. prompt:: bash $

      ceph orch status

The command that makes the drive's LEDs blink is ``lsmcli``. To customize this
command, configure it via a Jinja2 template by running commands of the
following forms::

   ceph config-key set mgr/cephadm/blink_device_light_cmd "<template>"
   ceph config-key set mgr/cephadm/<host>/blink_device_light_cmd "lsmcli local-disk-{{ ident_fault }}-led-{{'on' if on else 'off'}} --path '{{ path or dev }}'"

The following arguments can be used to customize the Jinja2 template:

* ``on``: A Boolean value.
* ``ident_fault``: A string that contains ``ident`` or ``fault``.
* ``dev``: The kernel name of the device on its host, for example ``sda``.
* ``path``: The device's link under ``/dev/disk/by-path``, if the Manager has
  recorded one. Otherwise this is an empty string, and the default template,
  ``{{ path or dev }}``, uses ``dev`` instead.

.. _enabling-monitoring:

Enabling monitoring
-------------------

Ceph can also monitor the health metrics associated with your device. For
example, SATA drives implement a standard called SMART that provides a wide
range of internal metrics about the device's usage and health (for example: the
number of hours powered on, the number of power cycles, the number of
unrecoverable read errors). Other device types such as SAS and NVMe present a
similar set of metrics (via slightly different standards).  All of these
metrics can be collected by Ceph via the ``smartctl`` tool.

You can enable or disable health monitoring by running one of the following
commands:

.. prompt:: bash $

   ceph device monitoring on
   ceph device monitoring off

Scraping
--------

If monitoring is enabled, device metrics will be scraped automatically at
regular intervals. To configure that interval, run a command of the following
form:

.. prompt:: bash $

   ceph config set mgr mgr/devicehealth/scrape_frequency <seconds>

By default, device metrics are scraped once every 24 hours.

To manually scrape all devices, run the following command:
   
.. prompt:: bash $

   ceph device scrape-health-metrics

To scrape a single device, run a command of the following form:

.. prompt:: bash $

   ceph device scrape-health-metrics <device-id>

To scrape a single daemon's devices, run a command of the following form:

.. prompt:: bash $

   ceph device scrape-daemon-health-metrics <who>

To retrieve the stored health metrics for a device (optionally for a specific
timestamp),  run a command of the following form:

.. prompt:: bash $

   ceph device get-health-metrics <devid> [sample-timestamp]

Failure prediction
------------------

Ceph can predict drive life expectancy and device failures by analyzing the
health metrics that it collects. The prediction modes are as follows:

* *none*: disable device failure prediction. This is the default.
* *local*: use a pre-trained prediction model from the ``diskprediction_local``
  Manager module.

The *local* mode works only when the ``diskprediction_local`` module is
enabled. To use it, run the following commands:

.. prompt:: bash $

   ceph mgr module enable diskprediction_local
   ceph config set global device_failure_prediction_mode local

In the *local* mode, failure prediction runs periodically in the background.
The predictor runs once a day, and it needs at least six sets of health
metrics for a device (six days at the default scrape interval) before it can
estimate that device's life expectancy. For this reason, life expectancy
values might be populated only after a significant amount of time has passed.
See :ref:`diskprediction`.

The life expectancy of all devices is displayed in the output of the following
command:

.. prompt:: bash $

   ceph device ls

To see the metadata of a specific device, run a command of the following form:

.. prompt:: bash $

   ceph device info <devid>

To print the local predictor's estimate for a single device (``>6w``,
``>=2w and <=6w``, ``<2w``, or ``unknown``), run a command of the following
form. It works only in the *local* mode, and it does not change the stored
life expectancy that ``ceph device ls`` shows:

.. prompt:: bash $

   ceph device predict-life-expectancy <devid>

In addition to Ceph's internal device failure prediction, you might have an
external source of information about device failures. To inform Ceph of a
specific device's life expectancy, run a command of the following form:

.. prompt:: bash $

   ceph device set-life-expectancy <devid> <from> [<to>]

Life expectancies are expressed as a time interval. This means that the
uncertainty of the life expectancy can be expressed in the form of a range of
time, and perhaps a wide range of time. The interval's end can be left
unspecified, but then Ceph neither raises ``DEVICE_HEALTH`` for the device nor
marks its OSDs ``out``.

Health alerts
-------------

The ``mgr/devicehealth/warn_threshold`` option (default 12 weeks) controls the
``DEVICE_HEALTH`` health check. The Manager raises it when the end of a
device's life expectancy interval is closer than this threshold.

To check the stored life expectancy of all devices now, raise any
appropriate health alerts, and (when ``mgr/devicehealth/self_heal`` is
enabled) mark ``out`` the OSDs on devices that are expected to fail within
``mgr/devicehealth/mark_out_threshold``, run the following command:

.. prompt:: bash $

   ceph device check-health

Automatic Migration
-------------------

The ``mgr/devicehealth/self_heal`` option (enabled by default) automatically
migrates data away from devices that are expected to fail soon. If this option
is enabled, the module marks the OSDs that use such devices ``out`` and sets
their primary affinity to 0, so that automatic migration will occur.

.. note:: The :confval:`mon_osd_min_in_ratio` option prevents this process
   from cascading to total failure. The module does not mark an OSD ``out``
   if that would bring the ratio of ``in`` OSDs below
   :confval:`mon_osd_min_in_ratio`. Instead, the cluster raises the
   ``DEVICE_HEALTH_TOOMANY`` health check. For instructions on what to do in
   this situation, see
   :ref:`DEVICE_HEALTH_TOOMANY<rados_health_checks_device_health_toomany>`.

The ``mgr/devicehealth/mark_out_threshold`` option (default 4 weeks) controls
automatic migration. When ``mgr/devicehealth/self_heal`` is enabled, the
Manager marks ``out`` the OSDs on any device whose life expectancy interval
ends closer than this threshold.
