dashboard plugin
================

Dashboard plugin visualizes the statistics of the cluster using a web server
hosted by ``ceph-mgr``.

Enabling
--------

The *dashboard* module is enabled with::

  ceph mgr module enable dashboard

Configuration
-------------

Like most web applications, dashboard binds to a host name and port.
By default, the ``ceph-mgr`` daemon hosting the dashboard (i.e., the
currently active manager) will bind to port 7000 and any available
IPv4 or IPv6 address on the host.

Since each ``ceph-mgr`` hosts its own instance of dashboard, it may
also be necessary to configure them separately. The hostname and port
can be changed via the configuration key facility::

  ceph config-key set mgr/dashboard/$name/server_addr $IP
  ceph config-key set mgr/dashboard/$name/server_port $PORT

where ``$name`` is the ID of the ceph-mgr who is hosting this
dashboard web app.

These settings can also be configured cluster-wide and not manager
specific.  For example,::

  ceph config-key set mgr/dashboard/server_addr $IP
  ceph config-key set mgr/dashboard/server_port $PORT

If the port is not configured, the web app will bind to port ``7000``.
If the address it not configured, the web app will bind to ``::``,
which corresponds to all available IPv4 and IPv6 addresses.

Reverse proxies
---------------

If you are accessing the dashboard via a reverse proxy configuration,
you may wish to service it under a URL prefix.  To get the dashboard
to use hyperlinks that include your prefix, you can set the
``url_prefix`` setting:

::

  ceph config-key set mgr/dashboard/url_prefix $PREFIX

so you can access the dashboard at ``http://$IP:$PORT/$PREFIX/``.

