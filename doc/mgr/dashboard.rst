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

You can configure a prefix for all URLs::

  ceph config-key set mgr/dashboard/url_prefix $PREFIX

so you can access the dashboard at ``http://$IP:$PORT/$PREFIX/``.


Load balancer
-------------

Please note that the dashboard will *only* start on the manager which
is active at that moment. Query the Ceph cluster status to see which
manager is active (e.g., ``ceph mgr dump``).  In order to make the
dashboard available via a consistent URL regardless of which manager
daemon is currently active, you may want to set up a load balancer
front-end to direct traffic to whichever manager endpoint is
available. If you use a reverse http proxy that forwards a subpath to
the dashboard, you need to configure ``url_prefix`` (see above).
