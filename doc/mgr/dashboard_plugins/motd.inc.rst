.. _dashboard-motd:

Message of the day (MOTD)
^^^^^^^^^^^^^^^^^^^^^^^^^

Displays a configured "message of the day" (MOTD) at the top of the Ceph
Dashboard.

The importance of an MOTD can be configured by its severity, which is
``info``, ``warning`` or ``danger``. The MOTD can expire after a given time,
this means it will not be displayed in the UI anymore. Use the following
syntax to specify the expiration time: ``Ns|m|h|d|w``, where ``N`` is an
integer followed by seconds, minutes, hours, days and weeks. If the MOTD
should expire after 2 hours, use ``2h`` or ``5w`` for 5 weeks. Use ``0`` to
configure a MOTD that does not expire.

To configure a MOTD, run the following command:

.. prompt:: bash #

   ceph dashboard motd set <severity:info|warning|danger> <expires> <message>

To show the configured MOTD:

.. prompt:: bash #

   ceph dashboard motd get

To clear the configured MOTD run:

.. prompt:: bash #

   ceph dashboard motd clear

A MOTD with a severity of ``info`` or ``warning`` can be closed by the user.
A closed ``info`` MOTD stays hidden until the browser's local storage is
cleared, or until the severity or the message changes. A closed ``warning``
MOTD is shown again in a new browser session, or as soon as its severity or
message changes.
