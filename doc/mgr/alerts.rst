Alerts module
=============

The alerts module can send simple alert messages about cluster health
via e-mail or by opening, updating, and resolving incidents in PagerDuty.

:note: This module is *not* intended to be a robust monitoring
       solution. The fact that it is run as part of the Ceph cluster
       itself is fundamentally limiting in that a failure of the
       ``ceph-mgr`` daemon prevents alerts from being sent. This module
       can, however, be useful for standalone clusters that exist in
       environments where other monitoring infrastructure does not
       exist.

Enabling
--------

Enable the ``alerts`` module by running the following command:

.. prompt:: bash #

   ceph mgr module enable alerts

Configuration
-------------

All of the following config options must be set when configuring SMTP.  When
setting ``mgr/alerts/smtp_destination``, specify multiple email addresses by
separating them with commas.

.. prompt:: bash #

   ceph config set mgr mgr/alerts/smtp_host <smtp-server>
   ceph config set mgr mgr/alerts/smtp_destination <email-address-to-send-to>
   ceph config set mgr mgr/alerts/smtp_sender <from-email-address>

The alerts module uses SSL and port 465 by default. These settings can be changed by running commands of the following forms:

.. prompt:: bash #

   ceph config set mgr mgr/alerts/smtp_ssl false   # if not SSL
   ceph config set mgr mgr/alerts/smtp_port <port-number>  # if not 465

To authenticate to the SMTP server, you must set the user and password:

.. prompt:: bash #

   ceph config set mgr mgr/alerts/smtp_user <username>
   ceph config set mgr mgr/alerts/smtp_password <password>

By default, the name in the ``From:`` line is simply ``Ceph``.  To change this
default (that is, to identify which cluster this is), run a command of the
following form:

.. prompt:: bash #

   ceph config set mgr mgr/alerts/smtp_from_name 'Ceph Cluster Foo'

By default, the alert module checks the cluster health once per minute and
sends a message if there a change to the cluster's health status. Change the
frequency of the alert module's cluster health checks by running a command of the following form:

.. prompt:: bash #

   ceph config set mgr mgr/alerts/interval <interval>   # e.g., "5m" for 5 minutes

PagerDuty
---------

The alerts module can also open, update, and resolve incidents in PagerDuty
using the `Events API v2
<https://developer.pagerduty.com/docs/events-api-v2/overview/>`_. Configure a
PagerDuty service with an *Events API v2* integration and copy its
*Integration Key* (also called the *routing key*).

.. prompt:: bash #

   ceph config set mgr mgr/alerts/pagerduty_routing_key <integration-key>

Each Ceph health check becomes a separate PagerDuty incident (deduplicated by
health check code). New checks trigger new incidents, checks whose count
increases update the existing incident, and cleared checks resolve the
incident. ``HEALTH_ERR`` maps to PagerDuty severity ``critical``,
``HEALTH_WARN`` maps to ``warning``.

By default the cluster's ``fsid`` is used as the PagerDuty event ``source``
field (and as a prefix on the dedup key so that multiple clusters sharing an
integration key don't collide). To override:

.. prompt:: bash #

   ceph config set mgr mgr/alerts/pagerduty_source <source-name>
   ceph config set mgr mgr/alerts/pagerduty_component <component-name>  # default: ceph

If your environment requires an alternate endpoint (for example, an on-prem
PagerDuty EU endpoint or a proxy), override the URL:

.. prompt:: bash #

   ceph config set mgr mgr/alerts/pagerduty_url https://events.eu.pagerduty.com/v2/enqueue

Each PagerDuty API request uses a 10 second timeout by default. Adjust with:

.. prompt:: bash #

   ceph config set mgr mgr/alerts/pagerduty_timeout 30

Commands
--------

To force an alert to be sent immediately, run the following command:

.. prompt:: bash #

   ceph alerts send
