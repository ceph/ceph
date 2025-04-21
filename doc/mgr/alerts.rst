Alerts module
=============

The alerts module can send simple alert messages about cluster health
via e-mail.  In the future, it will support other notification methods
as well.

:note: This module is *not* intended to be a robust monitoring
       solution.  The fact that it is run as part of the Ceph cluster
       itself is fundamentally limiting in that a failure of the
       ceph-mgr daemon prevents alerts from being sent.  This module
       can, however, be useful for standalone clusters that exist in
       environments where existing monitoring infrastructure does not
       exist.

Enabling
--------

The *alerts* module is enabled with:

.. prompt:: bash #

   ceph mgr module enable alerts

Configuration
-------------

To configure SMTP, all of the following config options must be set
(When setting ``mgr/alerts/smtp_destination``, you can use commas to separate multiple):

.. prompt:: bash #

   ceph config set mgr mgr/alerts/smtp_host *<smtp-server>*
   ceph config set mgr mgr/alerts/smtp_destination *<email-address-to-send-to>*
   ceph config set mgr mgr/alerts/smtp_sender *<from-email-address>*

By default, the module will use SSL and port 465.  To change that:

.. prompt:: bash #

   ceph config set mgr mgr/alerts/smtp_ssl false   # if not SSL
   ceph config set mgr mgr/alerts/smtp_port *<port-number>*  # if not 465

To authenticate to the SMTP server, you must set the user and password:

.. prompt:: bash #

   ceph config set mgr mgr/alerts/smtp_user *<username>*
   ceph config set mgr mgr/alerts/smtp_password *<password>*

By default, the name in the ``From:`` line is simply ``Ceph``.  To
change that (e.g., to identify which cluster this is):

.. prompt:: bash #

   ceph config set mgr mgr/alerts/smtp_from_name 'Ceph Cluster Foo'

By default, the module will check the cluster health once per minute
and, if there is a change, send a message.  To change that
frequency:

.. prompt:: bash #

   ceph config set mgr mgr/alerts/interval *<interval>*   # e.g., "5m" for 5 minutes

Commands
--------

To force an alert to be send immediately:

.. prompt:: bash #

   ceph alerts send
