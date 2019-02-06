.. _mgr-dashboard:

Ceph Manager Dashboard
======================

Overview
--------

The original Ceph Manager Dashboard that was shipped with Ceph Luminous started
out as a simple read-only view into various run-time information and performance
data of a Ceph cluster. It used a very simple architecture to achieve the
original goal. However, there was a growing demand for adding more web-based
management capabilities, to make it easier to administer Ceph for users that
prefer a WebUI over using the command line.

The new :term:`Ceph Manager Dashboard` plugin is a replacement of the previous
one and adds a built-in web based monitoring and administration application to
the Ceph Manager. The architecture and functionality of this new plugin is
derived from and inspired by the `openATTIC Ceph management and monitoring tool
<https://openattic.org/>`_. The development is actively driven by the team
behind openATTIC at `SUSE <https://www.suse.com/>`_, with a lot of support from
the Ceph community and other companies like Red Hat.


The dashboard plugin's backend code uses the CherryPy framework and a custom
REST API implementation. The WebUI implementation is based on
Angular/TypeScript, merging both functionality from the original dashboard as
well as adding new functionality originally developed for the standalone version
of openATTIC. The Ceph Manager Dashboard plugin is implemented as a web
application that visualizes information and statistics about the Ceph cluster
using a web server hosted by ``ceph-mgr``.

The dashboard currently provides the following features to monitor and manage
various aspects of your Ceph cluster:

* **Multi-User and Role Management**: The dashboard supports multiple user
  accounts with different permissions (roles). The user accounts and roles
  can be modified on both the command line and via the WebUI.
  See :ref:`dashboard-user-role-management` for details.
* **Single Sign-On (SSO)**: the dashboard supports authentication
  via an external identity provider using the SAML 2.0 protocol. See
  :ref:`dashboard-sso-support` for details.
* **SSL/TLS support**: All HTTP communication between the web browser and the
  dashboard is secured via SSL. A self-signed certificate can be created with
  a built-in command, but it's also possible to import custom certificates
  signed and issued by a CA. See :ref:`dashboard-ssl-tls-support` for details.
* **Auditing**: the dashboard backend can be configured to log all PUT, POST
  and DELETE API requests in the Ceph audit log. See :ref:`dashboard-auditing`
  for instructions on how to enable this feature.
* **Internationalization (I18N)**: use the dashboard in different languages.
* **Overall cluster health**: Displays overall cluster status, performance
  and capacity metrics.
* **Cluster logs**: Display the latest updates to the cluster's event and audit
  log files.
* **Hosts**: Provides a list of all hosts associated to the cluster, which
  services are running and which version of Ceph is installed.
* **Performance counters**: Displays detailed service-specific statistics for
  each running service.
* **Monitors**: Lists all MONs, their quorum status, open sessions.
* **Configuration Editor**: View all available configuration options,
  their description, type and default values and edit the current values.
* **Pools**: List all Ceph pools and their details (e.g. applications, placement
  groups, replication size, EC profile, CRUSH ruleset, etc.)
* **OSDs**: Lists all OSDs, their status and usage statistics as well as
  detailed information like attributes (OSD map), metadata, performance counters
  and usage histograms for read/write operations. Mark OSDs as up/down/out,
  perform scrub operations. Select between different recovery profiles to adjust
  the level of backfilling activity.
* **iSCSI**: Lists all hosts that run the TCMU runner service, displaying all
  images and their performance characteristics (read/write ops, traffic).
* **RBD**: Lists all RBD images and their properties (size, objects, features).
  Create, copy, modify and delete RBD images. Create, delete and rollback
  snapshots of selected images, protect/unprotect these snapshots against
  modification. Copy or clone snapshots, flatten cloned images.
* **RBD mirroring**: Enable and configure RBD mirroring to a remote Ceph server.
  Lists all active sync daemons and their status, pools and RBD images including
  their synchronization state.
* **CephFS**: Lists all active filesystem clients and associated pools,
  including their usage statistics.
* **Object Gateway**: Lists all active object gateways and their performance
  counters. Display and manage (add/edit/delete) object gateway users and their
  details (e.g. quotas) as well as the users' buckets and their details (e.g.
  owner, quotas).
* **NFS**: Manage NFS exports of CephFS filesystems and RGW S3 buckets via NFS Ganesha.```

Enabling
--------

Within a running Ceph cluster, the Ceph Manager Dashboard is enabled with::

  $ ceph mgr module enable dashboard

Configuration
-------------

.. _dashboard-ssl-tls-support:

SSL/TLS Support
^^^^^^^^^^^^^^^

All HTTP connections to the dashboard are secured with SSL/TLS by default.

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

  $ ceph config-key set mgr/dashboard/crt -i dashboard.crt
  $ ceph config-key set mgr/dashboard/key -i dashboard.key

If different certificates are desired for each manager instance for some reason,
the name of the instance can be included as follows (where ``$name`` is the name
of the ``ceph-mgr`` instance, usually the hostname)::

  $ ceph config-key set mgr/dashboard/$name/crt -i dashboard.crt
  $ ceph config-key set mgr/dashboard/$name/key -i dashboard.key

SSL can also be disabled by setting this configuration value::

  $ ceph config set mgr mgr/dashboard/ssl false

This might be useful if the dashboard will be running behind a proxy which does
not support SSL for its upstream servers or other situations where SSL is not
wanted or required.

.. warning::

  Use caution when disabling SSL as usernames and passwords will be sent to the
  dashboard unencrypted.


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
active manager) will bind to TCP port 8443 or 8080 when SSL is disabled.

If no specific address has been configured, the web app will bind to ``::``,
which corresponds to all available IPv4 and IPv6 addresses.

These defaults can be changed via the configuration key facility on a
cluster-wide level (so they apply to all manager instances) as follows::

  $ ceph config set mgr mgr/dashboard/server_addr $IP
  $ ceph config set mgr mgr/dashboard/server_port $PORT

Since each ``ceph-mgr`` hosts its own instance of dashboard, it may also be
necessary to configure them separately. The IP address and port for a specific
manager instance can be changed with the following commands::

  $ ceph config set mgr mgr/dashboard/$name/server_addr $IP
  $ ceph config set mgr mgr/dashboard/$name/server_port $PORT

Replace ``$name`` with the ID of the ceph-mgr instance hosting the dashboard web
app.

.. note::

  The command ``ceph mgr services`` will show you all endpoints that are
  currently configured. Look for the ``dashboard`` key to obtain the URL for
  accessing the dashboard.

Username and password
^^^^^^^^^^^^^^^^^^^^^

In order to be able to log in, you need to create a user account and associate
it with at least one role. We provide a set of predefined *system roles* that
you can use. For more details please refer to the `User and Role Management`_
section.

To create a user with the administrator role you can use the following
commands::

  $ ceph dashboard ac-user-create <username> <password> administrator


Enabling the Object Gateway management frontend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

Finally, provide the credentials to the dashboard::

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

If you are using a self-signed certificate in your Object Gateway setup, then
you should disable certificate verification in the dashboard to avoid refused
connections, e.g. caused by certificates signed by unknown CA or not matching
the host name::

  $ ceph dashboard set-rgw-api-ssl-verify False

If the Object Gateway takes too long to process requests and the dashboard runs
into timeouts, then you can set the timeout value to your needs::

  $ ceph dashboard set-rest-requests-timeout <seconds>

The default value is 45 seconds.

Enabling iSCSI Management
^^^^^^^^^^^^^^^^^^^^^^^^^

The Ceph Manager Dashboard can manage iSCSI targets using the REST API provided
by the `rbd-target-api` service of the `ceph-iscsi <https://github.com/ceph/ceph-iscsi>`_
project. Please make sure that it's installed and enabled on the iSCSI gateways.

The available iSCSI gateways must be defined using the following commands::

    $ ceph dashboard iscsi-gateway-list
    $ ceph dashboard iscsi-gateway-add <gateway_name> <scheme>://<username>:<password>@<host>[:port]
    $ ceph dashboard iscsi-gateway-rm <gateway_name>

Enabling the Embedding of Grafana Dashboards
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Grafana and Prometheus are likely going to be bundled and installed by some
orchestration tools along Ceph in the near future, but currently, you will have
to install and configure both manually. After you have installed Prometheus and
Grafana on your preferred hosts, proceed with the following steps.

#. Enable the Ceph Exporter which comes as Ceph Manager module by running::

    $ ceph mgr module enable prometheus

More details can be found on the `documentation <http://docs.ceph.com/docs/master/
mgr/prometheus/>`_ of the prometheus module.

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

#. Install the `vonage-status-panel and grafana-piechart-panel` plugins using::

        grafana-cli plugins install vonage-status-panel
        grafana-cli plugins install grafana-piechart-panel

#. Add the Dashboards to Grafana:

   Dashboards can be added to Grafana by importing dashboard jsons.
   Following command can be used for downloading json files::

	wget https://raw.githubusercontent.com/ceph/ceph/master/monitoring/grafana/dashboards/<Dashboard-name>.json

   You can find all the dashboard jsons `here <https://github.com/ceph/ceph/tree/
   master/monitoring/grafana/dashboards>`_ .

   For Example, for ceph-cluster overview you can use::

        wget https://raw.githubusercontent.com/ceph/ceph/master/monitoring/grafana/dashboards/ceph-cluster.json

#. Configure Grafana in `/etc/grafana/grafana.ini` to adapt anonymous mode::

        [auth.anonymous]
        enabled = true
        org_name = Main Org.
        org_role = Viewer

After you have set up Grafana and Prometheus, you will need to configure the
connection information that the Ceph Manager Dashboard will use to access Grafana.

You need to tell the dashboard on which url Grafana instance is running/deployed::

  $ ceph dashboard set-grafana-api-url <grafana-server-url>  # default: ''

The format of url is : `<protocol>:<IP-address>:<port>`
You can directly access Grafana Instance as well to monitor your cluster.

.. _dashboard-sso-support:

Enabling Single Sign-On (SSO)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Ceph Manager Dashboard supports external authentication of users via the
`SAML 2.0 <https://en.wikipedia.org/wiki/SAML_2.0>`_ protocol. You need to create
the user accounts and associate them with the desired roles first, as authorization
is still performed by the Dashboard. However, the authentication process can be
performed by an existing Identity Provider (IdP).

.. note::
  Ceph Dashboard SSO support relies on onelogin's
  `python-saml <https://pypi.org/project/python-saml/>`_ library.
  Please ensure that this library is installed on your system, either by using
  your distribution's package management or via Python's `pip` installer.

To configure SSO on Ceph Dashboard, you should use the following command::

  $ ceph dashboard sso setup saml2 <ceph_dashboard_base_url> <idp_metadata> {<idp_username_attribute>} {<idp_entity_id>} {<sp_x_509_cert>} {<sp_private_key>}

Parameters:

* **<ceph_dashboard_base_url>**: Base URL where Ceph Dashboard is accessible (e.g., `https://cephdashboard.local`)
* **<idp_metadata>**: URL, file path or content of the IdP metadata XML (e.g., `https://myidp/metadata`)
* **<idp_username_attribute>** *(optional)*: Attribute that should be used to get the username from the authentication response. Defaults to `uid`.
* **<idp_entity_id>** *(optional)*: Use this when more than one entity id exists on the IdP metadata.
* **<sp_x_509_cert> / <sp_private_key>** *(optional)*: File path or content of the certificate that should be used by Ceph Dashboard (Service Provider) for signing and encryption.

.. note::
  The issuer value of SAML requests will follow this pattern:  **<ceph_dashboard_base_url>**/auth/saml2/metadata

To display the current SAML 2.0 configuration, use the following command::

  $ ceph dashboard sso show saml2

.. note::
  For more information about `onelogin_settings`, please check the `onelogin documentation <https://github.com/onelogin/python-saml>`_.

To disable SSO::

  $ ceph dashboard sso disable

To check if SSO is enabled::

  $ ceph dashboard sso status

To enable SSO::

  $ ceph dashboard sso enable saml2

Enabling Prometheus alerting
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using Prometheus for monitoring, you have to define `alerting rules
<https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules>`_.
To manage them you need to use the `Alertmanager
<https://prometheus.io/docs/alerting/alertmanager>`_.
If you are not using the Alertmanager yet, please `install it
<https://github.com/prometheus/alertmanager#install>`_ as it's mandatory in
order to receive and manage alerts from Prometheus.

The Alertmanager capabilities can be consumed by the dashboard in three different
ways:

#. Use the notification receiver of the dashboard.

#. Use the Prometheus Alertmanager API.

#. Use both sources simultaneously.

All three methods are going to notify you about alerts. You won't be notified
twice if you use both sources.

#. Use the notification receiver of the dashboard:

   This allows you to get notifications as `configured
   <https://prometheus.io/docs/alerting/configuration/>`_ from the Alertmanager.
   You will get notified inside the dashboard once a notification is send out,
   but you are not able to manage alerts.

   Add the dashboard receiver and the new route to your Alertmanager configuration.
   This should look like::

     route:
       receiver: 'ceph-dashboard'
     ...
     receivers:
       - name: 'ceph-dashboard'
         webhook_configs:
         - url: '<url-to-dashboard>/api/prometheus_receiver'


   Please make sure that the Alertmanager considers your SSL certificate in terms
   of the dashboard as valid. For more information about the correct
   configuration checkout the `<http_config> documentation
   <https://prometheus.io/docs/alerting/configuration/#%3Chttp_config%3E>`_.

#. Use the API of the Prometheus Alertmanager

   This allows you to manage alerts. You will see all alerts, the Alertmanager
   currently knows of, in the alerts listing. It can be found in the *Cluster*
   submenu as *Alerts*. The alerts can be sorted by name, job, severity,
   state and start time. Unfortunately it's not possible to know when an alert
   was sent out through a notification by the Alertmanager based on your
   configuration, that's why the dashboard will notify the user on any visible
   change to an alert and will notify the changed alert.

   Currently it's not yet possible to silence an alert and expire an silenced
   alert, but this is work in progress and will be added in a future release.

   To use it, specify the host and port of the Alertmanager server::

     $ ceph dashboard set-alertmanager-api-host <alertmanager-host:port>  # default: ''

   For example::

     $ ceph dashboard set-alertmanager-api-host 'http://localhost:9093'


#. Use both methods

   The different behaviors of both methods are configured in a way that they
   should not disturb each other through annoying duplicated notifications
   popping up.

Accessing the dashboard
^^^^^^^^^^^^^^^^^^^^^^^

You can now access the dashboard using your (JavaScript-enabled) web browser, by
pointing it to any of the host names or IP addresses and the selected TCP port
where a manager instance is running: e.g., ``httpS://<$IP>:<$PORT>/``.

You should then be greeted by the dashboard login page, requesting your
previously defined username and password. Select the **Keep me logged in**
checkbox if you want to skip the username/password request when accessing the
dashboard in the future.

.. _dashboard-user-role-management:

User and Role Management
------------------------

User Accounts
^^^^^^^^^^^^^

Ceph Dashboard supports managing multiple user accounts. Each user account
consists of a username, a password (stored in encrypted form using ``bcrypt``),
an optional name, and an optional email address.

User accounts are stored in MON's configuration database, and are globally
shared across all ceph-mgr instances.

We provide a set of CLI commands to manage user accounts:

- *Show User(s)*::

  $ ceph dashboard ac-user-show [<username>]

- *Create User*::

  $ ceph dashboard ac-user-create <username> [<password>] [<rolename>] [<name>] [<email>]

- *Delete User*::

  $ ceph dashboard ac-user-delete <username>

- *Change Password*::

  $ ceph dashboard ac-user-set-password <username> <password>

- *Modify User (name, and email)*::

  $ ceph dashboard ac-user-set-info <username> <name> <email>


User Roles and Permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^

User accounts are also associated with a set of roles that define which
dashboard functionality can be accessed by the user.

The Dashboard functionality/modules are grouped within a *security scope*.
Security scopes are predefined and static. The current available security
scopes are:

- **hosts**: includes all features related to the ``Hosts`` menu
  entry.
- **config-opt**: includes all features related to management of Ceph
  configuration options.
- **pool**: includes all features related to pool management.
- **osd**: includes all features related to OSD management.
- **monitor**: includes all features related to Monitor management.
- **rbd-image**: includes all features related to RBD image
  management.
- **rbd-mirroring**: includes all features related to RBD-Mirroring
  management.
- **iscsi**: includes all features related to iSCSI management.
- **rgw**: includes all features related to Rados Gateway management.
- **cephfs**: includes all features related to CephFS management.
- **manager**: include all features related to Ceph Manager
  management.
- **log**: include all features related to Ceph logs management.
- **grafana**: include all features related to Grafana proxy.
- **prometheus**: include all features related to Prometheus alert management.
- **dashboard-settings**: allows to change dashboard settings.

A *role* specifies a set of mappings between a *security scope* and a set of
*permissions*. There are four types of permissions:

- **read**
- **create**
- **update**
- **delete**

See below for an example of a role specification based on a Python dictionary::

  # example of a role
  {
    'role': 'my_new_role',
    'description': 'My new role',
    'scopes_permissions': {
      'pool': ['read', 'create'],
      'rbd-image': ['read', 'create', 'update', 'delete']
    }
  }

The above role dictates that a user has *read* and *create* permissions for
features related to pool management, and has full permissions for
features related to RBD image management.

The Dashboard already provides a set of predefined roles that we call
*system roles*, and can be used right away in a fresh Ceph Dashboard
installation.

The list of system roles are:

- **administrator**: provides full permissions for all security scopes.
- **read-only**: provides *read* permission for all security scopes except
  the dashboard settings.
- **block-manager**: provides full permissions for *rbd-image*,
  *rbd-mirroring*, and *iscsi* scopes.
- **rgw-manager**: provides full permissions for the *rgw* scope
- **cluster-manager**: provides full permissions for the *hosts*, *osd*,
  *monitor*, *manager*, and *config-opt* scopes.
- **pool-manager**: provides full permissions for the *pool* scope.
- **cephfs-manager**: provides full permissions for the *cephfs* scope.

The list of currently available roles can be retrieved by the following
command::

  $ ceph dashboard ac-role-show [<rolename>]

It is also possible to create new roles using CLI commands. The available
commands to manage roles are the following:

- *Create Role*::

  $ ceph dashboard ac-role-create <rolename> [<description>]

- *Delete Role*::

  $ ceph dashboard ac-role-delete <rolename>

- *Add Scope Permissions to Role*::

  $ ceph dashboard ac-role-add-scope-perms <rolename> <scopename> <permission> [<permission>...]

- *Delete Scope Permission from Role*::

  $ ceph dashboard ac-role-del-perms <rolename> <scopename>

To associate roles to users, the following CLI commands are available:

- *Set User Roles*::

  $ ceph dashboard ac-user-set-roles <username> <rolename> [<rolename>...]

- *Add Roles To User*::

  $ ceph dashboard ac-user-add-roles <username> <rolename> [<rolename>...]

- *Delete Roles from User*::

  $ ceph dashboard ac-user-del-roles <username> <rolename> [<rolename>...]


Example of user and custom role creation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In this section we show a full example of the commands that need to be used
in order to create a user account, that should be able to manage RBD images,
view and create Ceph pools, and have read-only access to any other scopes.

1. *Create the user*::

   $ ceph dashboard ac-user-create bob mypassword

2. *Create role and specify scope permissions*::

   $ ceph dashboard ac-role-create rbd/pool-manager
   $ ceph dashboard ac-role-add-scope-perms rbd/pool-manager rbd-image read create update delete
   $ ceph dashboard ac-role-add-scope-perms rbd/pool-manager pool read create

3. *Associate roles to user*::

   $ ceph dashboard ac-user-set-roles bob rbd/pool-manager read-only


Reverse proxies
---------------

If you are accessing the dashboard via a reverse proxy configuration,
you may wish to service it under a URL prefix.  To get the dashboard
to use hyperlinks that include your prefix, you can set the
``url_prefix`` setting:

::

  ceph config set mgr mgr/dashboard/url_prefix $PREFIX

so you can access the dashboard at ``http://$IP:$PORT/$PREFIX/``.

.. _dashboard-auditing:

Auditing
--------

The REST API is capable of logging PUT, POST and DELETE requests to the Ceph
audit log. This feature is disabled by default, but can be enabled with the
following command::

  $ ceph dashboard set-audit-api-enabled <true|false>

If enabled, the following parameters are logged per each request:

* from - The origin of the request, e.g. https://[::1]:44410
* path - The REST API path, e.g. /api/auth
* method - e.g. PUT, POST or DELETE
* user - The name of the user, otherwise 'None'

The logging of the request payload (the arguments and their values) is enabled
by default. Execute the following command to disable this behaviour::

  $ ceph dashboard set-audit-api-log-payload <true|false>

A log entry may look like this::

  2018-10-22 15:27:01.302514 mgr.x [INF] [DASHBOARD] from='https://[::ffff:127.0.0.1]:37022' path='/api/rgw/user/klaus' method='PUT' user='admin' params='{"max_buckets": "1000", "display_name": "Klaus Mustermann", "uid": "klaus", "suspended": "0", "email": "klaus.mustermann@ceph.com"}'


NFS-Ganesha Management
----------------------

Ceph Dashboard can manage `NFS Ganesha <http://nfs-ganesha.github.io/>`_ exports that use
CephFS or RadosGW as their backstore.

To enable this feature in Ceph Dashboard there are some assumptions that need
to be met regarding the way NFS-Ganesha services are configured.

The dashboard manages NFS-Ganesha config files stored in RADOS objects on the Ceph Cluster.
NFS-Ganesha must store part of their configuration in the Ceph cluster.

These configuration files must follow some conventions.
conventions.
Each export block must be stored in its own RADOS object named
``export-<id>``, where ``<id>`` must match the ``Export_ID`` attribute of the
export configuration. Then, for each NFS-Ganesha service daemon there should
exist a RADOS object named ``conf-<daemon_id>``, where ``<daemon_id>`` is an
arbitrary string that should uniquely identify the daemon instance (e.g., the
hostname where the daemon is running).
Each ``conf-<daemon_id>`` object contains the RADOS URLs to the exports that
the NFS-Ganesha daemon should serve. These URLs are of the form::

  %url rados://<pool_name>[/<namespace>]/export-<id>

Both the ``conf-<daemon_id>`` and ``export-<id>`` objects must be stored in the
same RADOS pool/namespace.


Configuring NFS-Ganesha in the dashboard
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable the management of NFS-Ganesha exports in Ceph Dashboard, we only
need to tell the Dashboard, in which RADOS pool and namespace the
configuration objects are stored. Then, Ceph Dashboard can access the objects
by following the naming convention described above.

The Dashboard command to configure the NFS-Ganesha configuration objects
location is::

  $ ceph dashboard set-ganesha-clusters-rados-pool-namespace <pool_name>[/<namespace>]

After running the above command, Ceph Dashboard is able to find the NFS-Ganesha
configuration objects and we can start manage the exports through the Web UI.


Support for multiple NFS-Ganesha clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ceph Dashboard also supports the management of NFS-Ganesha exports belonging
to different NFS-Ganesha clusters. An NFS-Ganesha cluster is a group of
NFS-Ganesha service daemons sharing the same exports. Different NFS-Ganesha
clusters are independent and don't share the exports configuration between each
other.

Each NFS-Ganesha cluster should store its configuration objects in a
different RADOS pool/namespace to isolate the configuration from each other.

To specify the locations of the configuration of each NFS-Ganesha cluster we
can use the same command as above but with a different value pattern::

  $ ceph dashboard set-ganesha-clusters-rados-pool-namespace <cluster_id>:<pool_name>[/<namespace>](,<cluster_id>:<pool_name>[/<namespace>])*

The ``<cluster_id>`` is an arbitrary string that should uniquely identify the
NFS-Ganesha cluster.

When configuring the Ceph Dashboard with multiple NFS-Ganesha clusters, the
Web UI will automatically allow to choose to which cluster an export belongs.


Plug-ins
--------

Dashboard Plug-ins allow to extend the functionality of the dashboard in a modular
and loosely coupled approach.

.. include:: dashboard_plugins/feature_toggles.inc.rst
