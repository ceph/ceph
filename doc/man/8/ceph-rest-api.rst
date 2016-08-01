:orphan:

=====================================================
 ceph-rest-api -- ceph RESTlike administration server
=====================================================

.. program:: ceph-rest-api

Synopsis
========

| **ceph-rest-api** [ -c *conffile* ] [--cluster *clustername* ] [ -n *name* ] [-i *id* ]


Description
===========

**ceph-rest-api** is a WSGI application that can run as a
standalone web service or run under a web server that supports
WSGI.  It provides much of the functionality of the **ceph**
command-line tool through an HTTP-accessible interface.

Options
=======

.. option:: -c/--conf conffile

    names the ceph.conf file to use for configuration.  If -c is not
    specified, the default depends on the state of the --cluster option
    (default 'ceph'; see below).  The configuration file is searched
    for in this order:

    * $CEPH_CONF
    * /etc/ceph/${cluster}.conf
    * ~/.ceph/${cluster}.conf
    * ${cluster}.conf (in the current directory)
  
    so you can also pass this option in the environment as CEPH_CONF.

.. option:: --cluster clustername

    set *clustername* for use in the $cluster metavariable, for
    locating the ceph.conf file.  The default is 'ceph'.

.. option:: -n/--name name

    specifies the client 'name', which is used to find the
    client-specific configuration options in the config file, and
    also is the name used for authentication when connecting
    to the cluster (the entity name appearing in ceph auth list output,
    for example).  The default is 'client.restapi'. 

.. option:: -i/--id id

   specifies the client 'id', which will form the clientname
   as 'client.<id>' if clientname is not set.  If -n/-name is
   set, that takes precedence.

   Also, global Ceph options are supported.
 

Configuration parameters
========================

Supported configuration parameters include:

* **keyring** the keyring file holding the key for 'clientname'
* **public addr** ip:port to listen on (default 0.0.0.0:5000)
* **log file** (usual Ceph default)
* **restapi base url** the base URL to answer requests on (default /api/v0.1)
* **restapi log level** critical, error, warning, info, debug (default warning)

Configuration parameters are searched in the standard order:
first in the section named '<clientname>', then 'client', then 'global'.

<clientname> is either supplied by -n/--name, "client.<id>" where
<id> is supplied by -i/--id, or 'client.restapi' if neither option
is present.

A single-threaded server will run on **public addr** if the ceph-rest-api
executed directly; otherwise, configuration is specified by the enclosing
WSGI web server.

Commands
========

Commands are submitted with HTTP GET requests (for commands that
primarily return data) or PUT (for commands that affect cluster state).
HEAD and OPTIONS are also supported.  Standard HTTP status codes
are returned.

For commands that return bulk data, the request can include
Accept: application/json or Accept: application/xml to select the
desired structured output, or you may use a .json or .xml addition
to the requested PATH.  Parameters are supplied as query parameters
in the request; for parameters that take more than one value, repeat
the key=val construct.  For instance, to remove OSDs 2 and 3,
send a PUT request to ``osd/rm?ids=2&ids=3``.

Discovery
=========

Human-readable discovery of supported commands and parameters, along
with a small description of each command, is provided when the requested
path is incomplete/partially matching.  Requesting / will redirect to
the value of  **restapi base url**, and that path will give a full list
of all known commands.
For example, requesting ``api/vX.X/mon`` will return the list of API calls for
monitors - ``api/vX.X/osd`` will return the list of API calls for OSD and so on.

The command set is very similar to the commands
supported by the **ceph** tool.  One notable exception is that the
``ceph pg <pgid> <command>`` style of commands is supported here
as ``tell/<pgid>/command?args``.

Deployment as WSGI application
==============================

When deploying as WSGI application (say, with Apache/mod_wsgi,
or nginx/uwsgi, or gunicorn, etc.), use the ``ceph_rest_api.py`` module
(``ceph-rest-api`` is a thin layer around this module).  The standalone web
server is of course not used, so address/port configuration is done in
the WSGI server.  Use a python .wsgi module or the equivalent to call
``app = generate_app(conf, cluster, clientname, clientid, args)`` where:

* conf is as -c/--conf above
* cluster is as --cluster above
* clientname, -n/--name
* clientid, -i/--id, and
* args are any other generic Ceph arguments

When app is returned, it will have attributes 'ceph_addr' and 'ceph_port'
set to what the address and port are in the Ceph configuration;
those may be used for the server, or ignored.

Any errors reading configuration or connecting to the cluster cause an
exception to be raised; see your WSGI server documentation for how to
see those messages in case of problem.

Availability
============

**ceph-rest-api** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to the Ceph documentation at
http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8)
