dashboard plugin
================

The dashboard plugin is a web application that visualizes information and
statistics about the Ceph cluster using a web server hosted by ``ceph-mgr``.

The dashboard currently provides insight into the following aspects of your
cluster:

* **Overall cluster health**: Displays the overall cluster status, storage
  utilization (e.g. number of objects, raw capacity, usage per pool), a list of
  pools and their status and usage statistics, access to the cluster log file.
* **Hosts**: Provides a list of all hosts associated to the cluster, which
  services are running and which version of Ceph is installed.
* **Performance counters**: Displays detailed service-specific statistics for
  each running service.
* **Monitors**: Lists all MONs, their quorum status, open sessions.
* **Configuration Reference**: Lists all available configuration options,
  their description and default values.
* **OSDs**: Lists all OSDs, their status and usage statistics as well as
  detailed information like attributes (OSD map), metadata, performance counters
  and usage histograms for read/write operations.
* **iSCSI**: Lists all hosts that run the TCMU runner service, displaying all
  images and their performance characteristics (read/write ops, traffic).
* **RBD**: Lists all RBD images and their properties (size, objects, features)
  in a given pool.
* **RBD mirroring**: Lists all active sync daemons and their status, pools and
  RBD images including their synchronization state.
* **CephFS**: Lists all active filesystem clients and associated pools,
  including their usage statistics.
* **Object Gateway**: Lists all active object gateways and their performance
  counters.

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

In order to be able to log in, you need to define a username and password, which
will be stored in the MON's configuration database::

  ceph dashboard set-login-credentials <username> <password>

The password will be stored in the configuration database in encrypted form
using ``bcrypt``. This is a global setting that applies to all dashboard instances.

You can now access the dashboard using your (JavaScript-enabled) web browser, by
pointing it to the selected TCP port and any of the host names or IP addresses
where a manager instance runs on, e.g. ``http://<$IP>:<$PORT>/``.

You should then be greeted by the dashboard login page, requesting your
previously defined username and password. Select the **Keep me logged in**
checkbox if you want to skip the username/password request when accessing the
dashboard in the future.

Reverse proxies
---------------

If you are accessing the dashboard via a reverse proxy configuration,
you may wish to service it under a URL prefix.  To get the dashboard
to use hyperlinks that include your prefix, you can set the
``url_prefix`` setting:

::

  ceph config-key set mgr/dashboard/url_prefix $PREFIX

so you can access the dashboard at ``http://$IP:$PORT/$PREFIX/``.