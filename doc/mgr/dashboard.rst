Dashboard Plugin
================

Overview
--------

The original Ceph manager dashboard that was shipped with Ceph "Luminous"
started out as a simple read-only view into various run-time information and
performance data of a Ceph cluster. It used a very simple architecture to
achieve the original goal.

However, there was a growing demand for adding more web-based management
capabilities, to make it easier to administer Ceph for users that prefer a WebUI
over using the command line.

This new dashboard module is a replacement of the previous one and an ongoing
project to add a native web based monitoring and administration application to
Ceph Manager.

The architecture and functionality of this module are derived from and inspired
by the `openATTIC Ceph management and monitoring tool
<https://openattic.org/>`_. The development is actively driven by the team
behind openATTIC at SUSE.

The intention is to reuse as much of the existing openATTIC functionality as
possible, while adapting it to the different environment. The Dashboard module's
backend code uses the CherryPy framework and a custom REST API implementation
instead of Django and the Django REST Framework.

The WebUI implementation is based on Angular/TypeScript, merging both
functionality from the original dashboard as well as adding new functionality
originally developed for the standalone version of openATTIC.

The dashboard plugin is implemented as a web application that visualizes
information and statistics about the Ceph cluster using a web server hosted by
``ceph-mgr``.

The dashboard currently provides the following features to monitor and manage
various aspects of your Ceph cluster:

* **Username/password protection**: The Dashboard can only be accessed by
  providing a configurable username and password.
* **Overall cluster health**: Displays the overall cluster status, storage
  utilization (e.g. number of objects, raw capacity, usage per pool), a list of
  pools and their status and usage statistics.
* **Cluster logs**: Display the latest updates to the cluster's event and audit
  log files.
* **Hosts**: Provides a list of all hosts associated to the cluster, which
  services are running and which version of Ceph is installed.
* **Performance counters**: Displays detailed service-specific statistics for
  each running service.
* **Monitors**: Lists all MONs, their quorum status, open sessions.
* **Configuration Reference**: Lists all available configuration options,
  their description and default values.
* **Pools**: List all Ceph pools and their details (e.g. applications, placement
  groups, replication size, EC profile, CRUSH ruleset, etc.)
* **OSDs**: Lists all OSDs, their status and usage statistics as well as
  detailed information like attributes (OSD map), metadata, performance counters
  and usage histograms for read/write operations.
* **iSCSI**: Lists all hosts that run the TCMU runner service, displaying all
  images and their performance characteristics (read/write ops, traffic).
* **RBD**: Lists all RBD images and their properties (size, objects, features).
  Create, modify and delete RBD images. Create, delete and rollback snapshots of
  selected images, protect/unprotect these snapshots against modification.
* **RBD mirroring**: Lists all active sync daemons and their status, pools and
  RBD images including their synchronization state.
* **CephFS**: Lists all active filesystem clients and associated pools,
  including their usage statistics.
* **Object Gateway**: Lists all active object gateways and their performance
  counters, object gateway users and their details (e.g. quotas) as well as
  all buckets and their details (e.g. owner, quotas). 

Enabling
--------

The *dashboard* module is enabled with::

  $ ceph mgr module enable dashboard

This can be automated (e.g. during deployment) by adding the following to
``ceph.conf``::

  [mon]
          mgr initial modules = dashboard

Note that ``mgr initial modules`` takes a space-separated list of modules, so
if you wanted to include other modules in addition to dashboard, just make it
a list like so::

          mgr initial modules = balancer dashboard

Configuration
-------------

Host name and port
^^^^^^^^^^^^^^^^^^

Like most web applications, dashboard binds to a host name and port.
By default, the ``ceph-mgr`` daemon hosting the dashboard (i.e., the
currently active manager) will bind to port 7000 and any available
IPv4 or IPv6 address on the host.

Since each ``ceph-mgr`` hosts its own instance of dashboard, it may
also be necessary to configure them separately. The hostname and port
can be changed via the configuration key facility::

  $ ceph config-key set mgr/dashboard/$name/server_addr $IP
  $ ceph config-key set mgr/dashboard/$name/server_port $PORT

where ``$name`` is the ID of the ceph-mgr who is hosting this
dashboard web app.

These settings can also be configured cluster-wide and not manager
specific.  For example,::

  $ ceph config-key set mgr/dashboard/server_addr $IP
  $ ceph config-key set mgr/dashboard/server_port $PORT

If the port is not configured, the web app will bind to port ``7000``.
If the address it not configured, the web app will bind to ``::``,
which corresponds to all available IPv4 and IPv6 addresses.

If in doubt which URL to use to access the dashboard, the ``ceph mgr services``
command will show the endpoints currently configured (look for the "dashboard"
key).

Username and password
^^^^^^^^^^^^^^^^^^^^^

In order to be able to log in, you need to define a username and password, which
will be stored in the MON's configuration database::

  $ ceph dashboard set-login-credentials <username> <password>

The password will be stored in the configuration database in encrypted form
using ``bcrypt``. This is a global setting that applies to all dashboard instances.

Enabling the Object Gateway management frontend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To use the Object Gateway management functionality of the dashboard, you will
need to provide the login credentials of a user with the ``system`` flag
enabled.

If you do not have a user which shall be used for providing those credentials,
you will also need to create one::

  $ radosgw-admin user create --uid=<user id> --display-name=<display-name> \
      --system

Take note of the keys ``access_key`` and ``secret_key`` in the output of this
command.

The credentials of an existing user can also be obtained by using
`radosgw-admin`::

  $ radosgw-admin user info --uid=<user id>

Finally, provide the credentials to the dashboard module::

  $ ceph dashboard set-rgw-api-access-key <access_key>
  $ ceph dashboard set-rgw-api-secret-key <secret_key>

This is all you have to do to get the Object Gateway management functionality
working. The host and port of the Object Gateway are determined automatically.

If multiple zones are used, it will automatically determine the host within the
master zone group and master zone. This should be sufficient for most setups,
but in some circumstances you might want to set the host and port manually::

  $ ceph dashboard set-rgw-api-host <host>
  $ ceph dashboard set-rgw-api-port <port>

In addition to the settings mentioned so far, the following settings do also
exist and you may find yourself in the situation that you have to use them::

  $ ceph dashboard set-rgw-api-scheme <scheme>  # http or https
  $ ceph dashboard set-rgw-api-admin-resource <admin-resource>
  $ ceph dashboard set-rgw-api-user-id <user-id>

Accessing the dashboard
^^^^^^^^^^^^^^^^^^^^^^^

You can now access the dashboard using your (JavaScript-enabled) web browser, by
pointing it to any of the host names or IP addresses and the selected TCP port
where a manager instance is running: e.g., ``http://<$IP>:<$PORT>/``.

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
