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
possible, while adapting it to the different environment. While openATTIC is
based on Django and the Django REST Framework, the dashboard module's backend
code uses the CherryPy framework and a custom REST API implementation.

The WebUI implementation is based on Angular/TypeScript, merging both
functionality from the original dashboard as well as adding new functionality
originally developed for the standalone version of openATTIC.

The dashboard plugin is implemented as a web application that visualizes
information and statistics about the Ceph cluster using a web server hosted by
``ceph-mgr``.

The dashboard currently provides the following features to monitor and manage
various aspects of your Ceph cluster:

* **Username/password protection**: The dashboard can only be accessed by
  providing a configurable username and password.
* **SSL/TLS support**: All HTTP communication between the web browser and the
  dashboard is secured via SSL. A self-signed certificate can be created with
  a built-in command, but it's also possible to import custom certificates
  signed and issued by a CA.
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
  Create, copy, modify and delete RBD images. Create, delete and rollback
  snapshots of selected images, protect/unprotect these snapshots against
  modification. Copy or clone snapshots, flatten cloned images.
* **RBD mirroring**: Lists all active sync daemons and their status, pools and
  RBD images including their synchronization state.
* **CephFS**: Lists all active filesystem clients and associated pools,
  including their usage statistics.
* **Object Gateway**: Lists all active object gateways and their performance
  counters. Display and manage (add/edit/delete) object gateway users and their
  details (e.g. quotas) as well as the users' buckets and their details (e.g.
  owner, quotas). 

Enabling
--------

Within a running Ceph cluster, the *dashboard* manager module is enabled with::

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

SSL/TLS Support
^^^^^^^^^^^^^^^

All HTTP connections to the dashboard are secured with SSL/TLS. 

To get the dashboard up and running quickly, you can generate and install a
self-signed certificate using the following built-in command::

  $ ceph dashboard create-self-signed-cert

Note that most web browsers will complain about such self-signed certificates
and require explicit confirmation before establishing a secure connection to the
dashboard.

To properly secure a deployment and to remove the certificate warning, a
certificate that is issued by a certificate authority (CA) should be used.

For example, a key pair can be generated with a command similar to::

  $ openssl req -new -nodes -x509 \
    -subj "/O=IT/CN=ceph-mgr-dashboard" -days 3650 \
    -keyout dashboard.key -out dashboard.crt -extensions v3_ca

The ``dashboard.crt`` file should then be signed by a CA. Once that is done, you
can enable it for all Ceph manager instances by running the following commands::

  $ ceph config-key set mgr mgr/dashboard/crt -i dashboard.crt
  $ ceph config-key set mgr mgr/dashboard/key -i dashboard.key

If different certificates are desired for each manager instance for some reason,
the name of the instance can be included as follows (where ``$name`` is the name
of the ``ceph-mgr`` instance, usually the hostname)::

  $ ceph config-key set mgr/dashboard/$name/crt -i dashboard.crt
  $ ceph config-key set mgr/dashboard/$name/key -i dashboard.key

.. note::

  You need to restart the Ceph manager processes manually after changing the SSL
  certificate and key. This can be accomplished by either running ``ceph mgr
  fail mgr`` or by disabling and re-enabling the dashboard module (which also
  triggers the manager to respawn itself)::

    $ ceph mgr module disable dashboard
    $ ceph mgr module enable dashboard

Host name and port
^^^^^^^^^^^^^^^^^^

Like most web applications, dashboard binds to a TCP/IP address and TCP port.

By default, the ``ceph-mgr`` daemon hosting the dashboard (i.e., the currently
active manager) will bind to TCP port 8443. If no specific address has been
configured, the web app will bind to ``::``, which corresponds to all available
IPv4 and IPv6 addresses.

These defaults can be changed via the configuration key facility on a
cluster-wide level (so they apply to all manager instances) as follows::

  $ ceph config-key set mgr/dashboard/server_addr $IP
  $ ceph config-key set mgr/dashboard/server_port $PORT

Since each ``ceph-mgr`` hosts its own instance of dashboard, it may also be
necessary to configure them separately. The IP address and port for a specific
manager instance can be changed with the following commands::

  $ ceph config-key mgr/dashboard/$name/server_addr $IP
  $ ceph config-key mgr/dashboard/$name/server_port $PORT

Replace ``$name`` with the ID of the ceph-mgr instance hosting the dashboard web
app.

.. note::

  The command ``ceph mgr services`` will show you all endpoints that are
  currently configured. Look for the "dashboard" key to obtain the URL for
  accessing the dashboard.

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

  $ radosgw-admin user create --uid=<user_id> --display-name=<display_name> \
      --system

Take note of the keys ``access_key`` and ``secret_key`` in the output of this
command.

The credentials of an existing user can also be obtained by using
`radosgw-admin`::

  $ radosgw-admin user info --uid=<user_id>

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
  $ ceph dashboard set-rgw-api-admin-resource <admin_resource>
  $ ceph dashboard set-rgw-api-user-id <user_id>

Enabling the Embedding of Grafana Dashboards
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Grafana and Prometheus are likely going to be bundled and installed by some
orchestration tools along Ceph in the near future, but currently, you will have
to install and configure both manually. After you have installed Prometheus and
Grafana on your preferred hosts, proceed with the following steps.

#. Enable the Ceph Exporter which comes as Ceph Manager module by running::

    $ ceph mgr module enable prometheus

    More details can be found on the `documentation
    <http://docs.ceph.com/docs/master/mgr/prometheus/>`_ of the prometheus
    module.

#. Add the corresponding scrape configuration to Prometheus. This may look
   like::

        global:
          scrape_interval: 5s

        scrape_configs:
          - job_name: 'prometheus'
            static_configs:
              - targets: ['localhost:9090']
          - job_name: 'ceph'
            static_configs:
              - targets: ['localhost:9283']
          - job_name: 'node-exporter'
            static_configs:
              - targets: ['localhost:9100']

#. Add Prometheus as data source to Grafana

#. Install the `vonage-status-panel` plugin using::

        grafana-cli plugins install vonage-status-panel

#. Add the Dashboards to Grafana by importing them

#. Configure Grafana in `/etc/grafana/grafana.ini` to adapt the URLs to the
   Ceph Dashboard properly::

        root_url = http://localhost:3000/api/grafana/proxy

After you have set up Grafana and Prometheus, you will need to configure the
connection information that the Ceph Manager Dashboard will use to access Grafana.
This includes setting the authentication method to be used, the corresponding login
credentials as well as the URL at which the Grafana instance can be reached.

The URL and TCP port can be set by using the following command::

  $ ceph dashboard set-grafana-api-url <url>  # default: 'http://localhost:3000'

You need to tell the dashboard which authentication method should be
used::

  $ ceph dashboard set-grafana-api-auth-method <method>  # default: ''

Possible values are either 'password' or 'token'.

To authenticate via username and password, you will need to set the following
values::

  $ ceph dashboard set-grafana-api-username <username>  # default: 'admin'
  $ ceph dashboard set-grafana-api-password <password>  # default: 'admin'

To use token based authentication, you will ned to set the token by issuing::

  $ ceph dashboard set-grafana-api-token <token>  # default: ''

Accessing the dashboard
^^^^^^^^^^^^^^^^^^^^^^^

You can now access the dashboard using your (JavaScript-enabled) web browser, by
pointing it to any of the host names or IP addresses and the selected TCP port
where a manager instance is running: e.g., ``httpS://<$IP>:<$PORT>/``.

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
