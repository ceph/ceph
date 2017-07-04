dashboard plugin
================

Dashboard plugin visualizes the statistics of the cluster using a web server
hosted by ``ceph-mgr``. Like most web applications, dashboard binds to a host
name and port. Since each ``ceph-mgr`` hosts its own instance of dashboard, we
need to configure them separately. The hostname and port are stored using the
configuration key facility. So we can configure them like::

  ceph config-key put mgr/dashboard/$name/server_addr $IP
  ceph config-key put mgr/dashboard/$name/server_port $PORT

where ``$name`` is the ID of the ceph-mgr who is hosting this dashboard web app.

These settings can also be configured cluster-wide and not manager specific, eg:

  ceph config-key put mgr/dashboard/server_addr $IP
  ceph config-key put mgr/dashboard/server_port $PORT

If the port is not configured, the web app will bind to port ``7000``.

Setting the IP to ``::`` makes the dashboard bind to all available IPv4 and IPv6
addresses.

In addition, make sure that the *dashboard* module is enabled with::

  ceph mgr module enable dashboard

Please note that the dashboard will *only* start on the manager which is active
at that moment. Query the Ceph cluster status to see which manager is active.
