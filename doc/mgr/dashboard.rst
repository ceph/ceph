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
If they are not configured, the web app will be bound to ``127.0.0.1:7000``.

