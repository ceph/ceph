.. _dashboard-motd:

Message of the day (MOTD)
^^^^^^^^^^^^^^^^^^^^^^^^^

Displays a configured `message of the day` at the top of the Ceph Dashboard.

The importance of a MOTD can be configured via its severity, which is
`info`, `warning` or `danger`. The MOTD can expire after a given time,
this means it will not be displayed in the UI anymore. Use the following
syntax to specify the expiration time: `Ns|m|h|d|w` for seconds, minutes,
hours, days and weeks. If the MOTD should expire after 2 hours, use `2h`
or `5w` for 5 weeks. Use `0` to configure a MOTD that does not expire.

To configure a MOTD, run the following command::

  $ dashboard motd set <severity:info|warning|danger> <expires> <message>

To show the configured MOTD::

  $ dashboard motd get

To clear the configured MOTD run::

  $ ceph dashboard motd clear
