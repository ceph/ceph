.. _mgr-cephadm-monitoring:

Monitoring Services
===================

Ceph Dashboard uses `Prometheus <https://prometheus.io/>`_, `Grafana
<https://grafana.com/>`_, and related tools to store and visualize detailed
metrics on cluster utilization and performance.  Ceph users have three options:

#. Have cephadm deploy and configure these services.  This is the default
   when bootstrapping a new cluster unless the ``--skip-monitoring-stack``
   option is used.
#. Deploy and configure these services manually.  This is recommended for users
   with existing prometheus services in their environment (and in cases where
   Ceph is running in Kubernetes with Rook).
#. Skip the monitoring stack completely.  Some Ceph dashboard graphs will
   not be available.

The monitoring stack consists of `Prometheus <https://prometheus.io/>`_,
Prometheus exporters (:ref:`mgr-prometheus`, `Node exporter
<https://prometheus.io/docs/guides/node-exporter/>`_), `Prometheus Alert
Manager <https://prometheus.io/docs/alerting/alertmanager/>`_ and `Grafana
<https://grafana.com/>`_.

.. note::

  Prometheus' security model presumes that untrusted users have access to the
  Prometheus HTTP endpoint and logs. Untrusted users have access to all the
  (meta)data Prometheus collects that is contained in the database, plus a
  variety of operational and debugging information.

  However, Prometheus' HTTP API is limited to read-only operations.
  Configurations can *not* be changed using the API and secrets are not
  exposed. Moreover, Prometheus has some built-in measures to mitigate the
  impact of denial of service attacks.

  Please see `Prometheus' Security model
  <https://prometheus.io/docs/operating/security/>` for more detailed
  information.

Deploying monitoring with cephadm
---------------------------------

The default behavior of ``cephadm`` is to deploy a basic monitoring stack.  It
is however possible that you have a Ceph cluster without a monitoring stack,
and you would like to add a monitoring stack to it. (Here are some ways that
you might have come to have a Ceph cluster without a monitoring stack: You
might have passed the ``--skip-monitoring stack`` option to ``cephadm`` during
the installation of the cluster, or you might have converted an existing
cluster (which had no monitoring stack) to cephadm management.)

To set up monitoring on a Ceph cluster that has no monitoring, follow the
steps below:

#. Deploy a node-exporter service on every node of the cluster.  The node-exporter provides host-level metrics like CPU and memory utilization:

   .. prompt:: bash #

     ceph orch apply node-exporter

#. Deploy alertmanager:

   .. prompt:: bash #

     ceph orch apply alertmanager

#. Deploy Prometheus. A single Prometheus instance is sufficient, but
   for high availability (HA) you might want to deploy two:

   .. prompt:: bash #

     ceph orch apply prometheus

   or

   .. prompt:: bash #

     ceph orch apply prometheus --placement 'count:2'

#. Deploy grafana:

   .. prompt:: bash #

     ceph orch apply grafana

Enabling security for the monitoring stack
----------------------------------------------

By default, in a cephadm-managed cluster, the monitoring components are set up and configured without enabling security measures.
While this suffices for certain deployments, others with strict security needs may find it necessary to protect the
monitoring stack against unauthorized access. In such cases, cephadm relies on a specific configuration parameter,
`mgr/cephadm/secure_monitoring_stack`, which toggles the security settings for all monitoring components. To activate security
measures, set this option to ``true`` with a command of the following form:

   .. prompt:: bash #

     ceph config set mgr mgr/cephadm/secure_monitoring_stack true

This change will trigger a sequence of reconfigurations across all monitoring daemons, typically requiring
few minutes until all components are fully operational. The updated secure configuration includes the following modifications:

#. Prometheus: basic authentication is required to access the web portal and TLS is enabled for secure communication.
#. Alertmanager: basic authentication is required to access the web portal and TLS is enabled for secure communication.
#. Node Exporter: TLS is enabled for secure communication.
#. Grafana: TLS is enabled and authentication is requiered to access the datasource information.

In this secure setup, users will need to setup authentication
(username/password) for both Prometheus and Alertmanager. By default the
username and password are set to ``admin``/``admin``. The user can change these
value with the commands ``ceph orch prometheus set-credentials`` and ``ceph
orch alertmanager set-credentials`` respectively. These commands offer the
flexibility to input the username/password either as parameters or via a JSON
file, which enhances security. Additionally, Cephadm provides the commands
`orch prometheus get-credentials` and `orch alertmanager get-credentials` to
retrieve the current credentials.

.. _cephadm-monitoring-centralized-logs:

Centralized Logging in Ceph
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ceph now provides centralized logging with Loki & Promtail. Centralized Log Management (CLM) consolidates all log data and pushes it to a central repository, 
with an accessible and easy-to-use interface. Centralized logging is designed to make your life easier. 
Some of the advantages are:

#. **Linear event timeline**: it is easier to troubleshoot issues analyzing a single chain of events than thousands of different logs from a hundred nodes.
#. **Real-time live log monitoring**: it is impractical to follow logs from thousands of different sources.
#. **Flexible retention policies**: with per-daemon logs, log rotation is usually set to a short interval (1-2 weeks) to save disk usage.
#. **Increased security & backup**: logs can contain sensitive information and expose usage patterns. Additionally, centralized logging allows for HA, etc.

Centralized Logging in Ceph is implemented using two new services - ``loki`` & ``promtail``.

Loki: It is basically a log aggregation system and is used to query logs. It can be configured as a datasource in Grafana. 

Promtail: It acts as an agent that gathers logs from the system and makes them available to Loki.

These two services are not deployed by default in a Ceph cluster. To enable the centralized logging you can follow the steps mentioned here :ref:`centralized-logging`.

.. _cephadm-monitoring-networks-ports:

Networks and Ports
~~~~~~~~~~~~~~~~~~

All monitoring services can have the network and port they bind to configured with a yaml service specification. By default
cephadm will use ``https`` protocol when configuring Grafana daemons unless the user explicitly sets the protocol to ``http``.

example spec file:

.. code-block:: yaml

    service_type: grafana
    service_name: grafana
    placement:
      count: 1
    networks:
    - 192.169.142.0/24
    spec:
      port: 4200
      protocol: http

.. _cephadm_monitoring-images:

Using custom images
~~~~~~~~~~~~~~~~~~~

It is possible to install or upgrade monitoring components based on other
images.  To do so, the name of the image to be used needs to be stored in the
configuration first.  The following configuration options are available.

- ``container_image_prometheus``
- ``container_image_grafana``
- ``container_image_alertmanager``
- ``container_image_node_exporter``
- ``container_image_loki``
- ``container_image_promtail``
- ``container_image_haproxy``
- ``container_image_keepalived``
- ``container_image_snmp_gateway``
- ``container_image_elasticsearch``
- ``container_image_jaeger_agent``
- ``container_image_jaeger_collector``
- ``container_image_jaeger_query``

Custom images can be set with the ``ceph config`` command

.. code-block:: bash

     ceph config set mgr mgr/cephadm/<option_name> <value>

For example

.. code-block:: bash

     ceph config set mgr mgr/cephadm/container_image_prometheus prom/prometheus:v1.4.1

If there were already running monitoring stack daemon(s) of the type whose
image you've changed, you must redeploy the daemon(s) in order to have them
actually use the new image.

For example, if you had changed the prometheus image

.. prompt:: bash #

     ceph orch redeploy prometheus


.. note::

     By setting a custom image, the default value will be overridden (but not
     overwritten).  The default value changes when updates become available.
     By setting a custom image, you will not be able to update the component
     you have set the custom image for automatically.  You will need to
     manually update the configuration (image name and tag) to be able to
     install updates.

     If you choose to go with the recommendations instead, you can reset the
     custom image you have set before.  After that, the default value will be
     used again.  Use ``ceph config rm`` to reset the configuration option

     .. code-block:: bash

          ceph config rm mgr mgr/cephadm/<option_name>

     For example

     .. code-block:: bash

          ceph config rm mgr mgr/cephadm/container_image_prometheus

See also :ref:`cephadm-airgap`.

.. _cephadm-overwrite-jinja2-templates:

Using custom configuration files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By overriding cephadm templates, it is possible to completely customize the
configuration files for monitoring services.

Internally, cephadm already uses `Jinja2
<https://jinja.palletsprojects.com/en/2.11.x/>`_ templates to generate the
configuration files for all monitoring components. Starting from version 17.2.3,
cephadm supports Prometheus http service discovery, and uses this endpoint for the
definition and management of the embedded Prometheus service. The endpoint listens on
``https://<mgr-ip>:8765/sd/`` (the port is
configurable through the variable ``service_discovery_port``) and returns scrape target
information in `http_sd_config format
<https://prometheus.io/docs/prometheus/latest/configuration/configuration/#http_sd_config/>`_

Customers with external monitoring stack can use `ceph-mgr` service discovery endpoint
to get scraping configuration. Root certificate of the server can be obtained by the
following command:

   .. prompt:: bash #

     ceph orch sd dump cert

The configuration of Prometheus, Grafana, or Alertmanager may be customized by storing
a Jinja2 template for each service. This template will be evaluated every time a service
of that kind is deployed or reconfigured. That way, the custom configuration is preserved
and automatically applied on future deployments of these services.

.. note::

  The configuration of the custom template is also preserved when the default
  configuration of cephadm changes. If the updated configuration is to be used,
  the custom template needs to be migrated *manually* after each upgrade of Ceph.

Option names
""""""""""""

The following templates for files that will be generated by cephadm can be
overridden. These are the names to be used when storing with ``ceph config-key
set``:

- ``services/alertmanager/alertmanager.yml``
- ``services/grafana/ceph-dashboard.yml``
- ``services/grafana/grafana.ini``
- ``services/prometheus/prometheus.yml``
- ``services/prometheus/alerting/custom_alerts.yml``
- ``services/loki.yml``
- ``services/promtail.yml``

You can look up the file templates that are currently used by cephadm in
``src/pybind/mgr/cephadm/templates``:

- ``services/alertmanager/alertmanager.yml.j2``
- ``services/grafana/ceph-dashboard.yml.j2``
- ``services/grafana/grafana.ini.j2``
- ``services/prometheus/prometheus.yml.j2``
- ``services/loki.yml.j2``
- ``services/promtail.yml.j2``

Usage
"""""

The following command applies a single line value:

.. code-block:: bash

  ceph config-key set mgr/cephadm/<option_name> <value>

To set contents of files as template use the ``-i`` argument:

.. code-block:: bash

  ceph config-key set mgr/cephadm/<option_name> -i $PWD/<filename>

.. note::

  When using files as input to ``config-key`` an absolute path to the file must
  be used.


Then the configuration file for the service needs to be recreated.
This is done using `reconfig`. For more details see the following example.

Example
"""""""

.. code-block:: bash

  # set the contents of ./prometheus.yml.j2 as template
  ceph config-key set mgr/cephadm/services/prometheus/prometheus.yml \
    -i $PWD/prometheus.yml.j2

  # reconfig the prometheus service
  ceph orch reconfig prometheus

.. code-block:: bash

  # set additional custom alerting rules for Prometheus
  ceph config-key set mgr/cephadm/services/prometheus/alerting/custom_alerts.yml \
    -i $PWD/custom_alerts.yml

  # Note that custom alerting rules are not parsed by Jinja and hence escaping
  # will not be an issue.

Deploying monitoring without cephadm
------------------------------------

If you have an existing prometheus monitoring infrastructure, or would like
to manage it yourself, you need to configure it to integrate with your Ceph
cluster.

* Enable the prometheus module in the ceph-mgr daemon

  .. code-block:: bash

     ceph mgr module enable prometheus

  By default, ceph-mgr presents prometheus metrics on port 9283 on each host
  running a ceph-mgr daemon.  Configure prometheus to scrape these.

To make this integration easier, cephadm provides a service discovery endpoint at
``https://<mgr-ip>:8765/sd/``. This endpoint can be used by an external
Prometheus server to retrieve target information for a specific service. Information returned
by this endpoint uses the format specified by the Prometheus `http_sd_config option
<https://prometheus.io/docs/prometheus/latest/configuration/configuration/#http_sd_config/>`_

Here's an example prometheus job definition that uses the cephadm service discovery endpoint

  .. code-block:: bash

     - job_name: 'ceph-exporter'  
       http_sd_configs:  
       - url: http://<mgr-ip>:8765/sd/prometheus/sd-config?service=ceph-exporter


* To enable the dashboard's prometheus-based alerting, see :ref:`dashboard-alerting`.

* To enable dashboard integration with Grafana, see :ref:`dashboard-grafana`.

Disabling monitoring
--------------------

To disable monitoring and remove the software that supports it, run the following commands:

.. code-block:: console

  $ ceph orch rm grafana
  $ ceph orch rm prometheus --force   # this will delete metrics data collected so far
  $ ceph orch rm node-exporter
  $ ceph orch rm alertmanager
  $ ceph mgr module disable prometheus

See also :ref:`orch-rm`.

Setting up RBD-Image monitoring
-------------------------------

Due to performance reasons, monitoring of RBD images is disabled by default. For more information please see
:ref:`prometheus-rbd-io-statistics`. If disabled, the overview and details dashboards will stay empty in Grafana
and the metrics will not be visible in Prometheus.

Setting up Prometheus
-----------------------

Setting Prometheus Retention Size and Time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cephadm can configure Prometheus TSDB retention by specifying ``retention_time``
and ``retention_size`` values in the Prometheus service spec.
The retention time value defaults to 15 days (15d). Users can set a different value/unit where
supported units are: 'y', 'w', 'd', 'h', 'm' and 's'. The retention size value defaults
to 0 (disabled). Supported units in this case are: 'B', 'KB', 'MB', 'GB', 'TB', 'PB' and 'EB'.

In the following example spec we set the retention time to 1 year and the size to 1GB.

.. code-block:: yaml

    service_type: prometheus
    placement:
      count: 1
    spec:
      retention_time: "1y"
      retention_size: "1GB"

.. note::

  If you already had Prometheus daemon(s) deployed before and are updating an
  existent spec as opposed to doing a fresh Prometheus deployment, you must also
  tell cephadm to redeploy the Prometheus daemon(s) to put this change into effect.
  This can be done with a ``ceph orch redeploy prometheus`` command.

Enabling Web Admin API
~~~~~~~~~~~~~~~~~~~~~~

Cephadm supports enabling the web admin API by specifying the ``enable_admin_api``
field as ``true`` in the Prometheus service spec.

.. code-block:: yaml

    service_type: prometheus
    placement:
      count: 1
    spec:
      enable_admin_api: true

Like with retention time and size, this requires running ``ceph orch redeploy prometheus``
for the setting to be picked up.

.. note::

  There are security concerns with enabling the admin API where anyone with
  access to the HTTP endpoint may run arbitrary SQL against the Prometheus database.
  Please see https://prometheus.io/docs/operating/security/#prometheus for more information

Setting up Grafana
------------------

Manually setting the Grafana URL
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cephadm automatically configures Prometheus, Grafana, and Alertmanager in
all cases except one.

In a some setups, the Dashboard user's browser might not be able to access the
Grafana URL that is configured in Ceph Dashboard. This can happen when the
cluster and the accessing user are in different DNS zones.

If this is the case, you can use a configuration option for Ceph Dashboard
to set the URL that the user's browser will use to access Grafana. This
value will never be altered by cephadm. To set this configuration option,
issue the following command:

   .. prompt:: bash $

     ceph dashboard set-grafana-frontend-api-url <grafana-server-api>

It might take a minute or two for services to be deployed. After the
services have been deployed, you should see something like this when you issue the command ``ceph orch ls``:

.. code-block:: console

  $ ceph orch ls
  NAME           RUNNING  REFRESHED  IMAGE NAME                                      IMAGE ID        SPEC
  alertmanager       1/1  6s ago     docker.io/prom/alertmanager:latest              0881eb8f169f  present
  crash              2/2  6s ago     docker.io/ceph/daemon-base:latest-master-devel  mix           present
  grafana            1/1  0s ago     docker.io/pcuzner/ceph-grafana-el8:latest       f77afcf0bcf6   absent
  node-exporter      2/2  6s ago     docker.io/prom/node-exporter:latest             e5a616e4b9cf  present
  prometheus         1/1  6s ago     docker.io/prom/prometheus:latest                e935122ab143  present

Configuring SSL/TLS for Grafana
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``cephadm`` deploys Grafana using the certificate defined in the ceph
key/value store. If no certificate is specified, ``cephadm`` generates a
self-signed certificate during the deployment of the Grafana service. Each
certificate is specific for the host it was generated on.

A custom certificate can be configured using the following commands:

.. prompt:: bash #

  ceph config-key set mgr/cephadm/{hostname}/grafana_key -i $PWD/key.pem
  ceph config-key set mgr/cephadm/{hostname}/grafana_crt -i $PWD/certificate.pem

Where `hostname` is the hostname for the host where grafana service is deployed.

If you have already deployed Grafana, run ``reconfig`` on the service to
update its configuration:

.. prompt:: bash #

  ceph orch reconfig grafana

The ``reconfig`` command also sets the proper URL for Ceph Dashboard.

Setting the initial admin password
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, Grafana will not create an initial
admin user. In order to create the admin user, please create a file
``grafana.yaml`` with this content:

.. code-block:: yaml

  service_type: grafana
  spec:
    initial_admin_password: mypassword

Then apply this specification:

.. code-block:: bash

  ceph orch apply -i grafana.yaml
  ceph orch redeploy grafana

Grafana will now create an admin user called ``admin`` with the
given password.

Turning off anonymous access
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, cephadm allows anonymous users (users who have not provided any
login information) limited, viewer only access to the grafana dashboard. In
order to set up grafana to only allow viewing from logged in users, you can
set ``anonymous_access: False`` in your grafana spec.

.. code-block:: yaml

  service_type: grafana
  placement:
    hosts:
    - host1
  spec:
    anonymous_access: False
    initial_admin_password: "mypassword"

Since deploying grafana with anonymous access set to false without an initial
admin password set would make the dashboard inaccessible, cephadm requires
setting the ``initial_admin_password`` when ``anonymous_access`` is set to false.


Setting up Alertmanager
-----------------------

Adding Alertmanager webhooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To add new webhooks to the Alertmanager configuration, add additional
webhook urls like so:

.. code-block:: yaml

    service_type: alertmanager
    spec:
      user_data:
        default_webhook_urls:
        - "https://foo"
        - "https://bar"

Where ``default_webhook_urls`` is a list of additional URLs that are
added to the default receivers' ``<webhook_configs>`` configuration.

Run ``reconfig`` on the service to update its configuration:

.. prompt:: bash #

  ceph orch reconfig alertmanager

Turn on Certificate Validation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are using certificates for alertmanager and want to make sure
these certs are verified, you should set the "secure" option to
true in your alertmanager spec (this defaults to false).

.. code-block:: yaml

    service_type: alertmanager
    spec:
      secure: true

If you already had alertmanager daemons running before applying the spec
you must reconfigure them to update their configuration

.. prompt:: bash #

  ceph orch reconfig alertmanager

Further Reading
---------------

* :ref:`mgr-prometheus`
