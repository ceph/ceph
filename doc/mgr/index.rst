

===================
Ceph Manager Daemon
===================

The :term:`Ceph Manager` daemon (ceph-mgr) runs alongside monitor daemons,
to provide additional monitoring and interfaces to external monitoring
and management systems.

Since the 12.x (*luminous*) Ceph release, the ceph-mgr daemon is required for
normal operations.  The ceph-mgr daemon is an optional component in
the 11.x (*kraken*) Ceph release.

By default, the manager daemon requires no additional configuration, beyond
ensuring it is running.  If there is no mgr daemon running, you will
see a health warning to that effect, and some of the other information
in the output of `ceph status` will be missing or stale until a mgr is started.

Use your normal deployment tools, such as ceph-ansible or ceph-deploy, to
set up ceph-mgr daemons on each of your mon nodes.  It is not mandatory
to place mgr daemons on the same nodes as mons, but it is almost always
sensible.

.. toctree::
    :maxdepth: 1

    Installation and Configuration <administrator>
    Writing plugins <plugins>
    Balancer plugin <balancer>
    Dashboard plugin <dashboard>
    DiskPrediction plugin <diskprediction>
    Local pool plugin <localpool>
    RESTful plugin <restful>
    Zabbix plugin <zabbix>
    Prometheus plugin <prometheus>
    Influx plugin <influx>
    Hello plugin <hello>
    Telegraf plugin <telegraf>
    Telemetry plugin <telemetry>
    Iostat plugin <iostat>
