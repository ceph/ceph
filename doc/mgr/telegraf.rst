===============
Telegraf Module 
===============
The Telegraf module collects and sends statistics series to a Telegraf agent.

The Telegraf agent can buffer, aggregate, parse and process the data before
sending it to an output which can be InfluxDB, ElasticSearch and many more.

Currently the only way to send statistics to Telegraf from this module is to
use the socket listener. The module can send statistics over UDP, TCP or
a UNIX socket.

The Telegraf module was introduced in the 13.x *Mimic* release.

--------
Enabling 
--------

To enable the module, use the following command:

::

    ceph mgr module enable telegraf

If you wish to subsequently disable the module, you can use the corresponding
*disable* command:

::

    ceph mgr module disable telegraf

-------------
Configuration 
-------------

For the telegraf module to send statistics to a Telegraf agent it is
required to configure the address to send the statistics to.

Set configuration values using the following command:

::

    ceph telegraf config-set <key> <value>


The most important settings are ``address`` and ``interval``.

For example, a typical configuration might look like this:

::

    ceph telegraf config-set address udp://:8094
    ceph telegraf config-set interval 10
    
The default values for these configuration keys are:

- address: unixgram:///tmp/telegraf.sock
- interval: 15

----------------
Socket Listener
----------------
The module only supports sending data to Telegraf through the socket listener
of the Telegraf module using the Influx data format.

A typical Telegraf configuration might be:


    [[inputs.socket_listener]]
    # service_address = "tcp://:8094"
    # service_address = "tcp://127.0.0.1:http"
    # service_address = "tcp4://:8094"
    # service_address = "tcp6://:8094"
    # service_address = "tcp6://[2001:db8::1]:8094"
    service_address = "udp://:8094"
    # service_address = "udp4://:8094"
    # service_address = "udp6://:8094"
    # service_address = "unix:///tmp/telegraf.sock"
    # service_address = "unixgram:///tmp/telegraf.sock"
    data_format = "influx"

In this case the `address` configuration option for the module would need to be set
to:

  udp://:8094


Refer to the Telegraf documentation for more configuration options.
