=====================================================
 ceph-rest-api -- ceph RESTlike administration server
=====================================================

.. program:: ceph-rest-api

Synopsis
========

| **ceph-rest-api** [ -c *conffile* ] [ -n *name* ... ]


Description
===========

**ceph-rest-api** is a WSGI application that can run as a
standalone web service or run under a web server that supports
WSGI.  It provides much of the functionality of the **ceph**
command-line tool through an HTTP-accessible interface.

Options
=======

.. option:: -c/--conf *conffile*

    names the ceph.conf file to use for configuration.  If -c is not
    specified, the default depends on the state of the --cluster option
    (default 'ceph'; see below).  The configuration file is searched
    for in this order:

    * $CEPH_CONF
    * /etc/ceph/${cluster}.conf
    * ~/.ceph/${cluster}.conf
    * ${cluster}.conf (in the current directory)
  
    so you can also pass this option in the environment as CEPH_CONF.

.. option:: --cluster *clustername*

    set *clustername* for use in the $cluster metavariable, for
    locating the ceph.conf file.  The default is 'ceph'.
    You can also pass this option in the environment as
    CEPH_CLUSTER_NAME.

.. option:: -n/--name *name*

    specifies the client 'name', which is used to find the
    client-specific configuration options in the config file, and
    also is the name used for authentication when connecting
    to the cluster (the entity name appearing in ceph auth list output,
    for example).  The default is 'client.restapi'.  You can also
    pass this option in the environment as CEPH_NAME.
 

Configuration parameters
========================

Supported configuration parameters include:

* **restapi client name** the 'clientname' used for auth and ceph.conf
* **restapi keyring** the keyring file holding the key for 'clientname'
* **restapi public addr** ip:port to listen on (default 0.0.0.0:5000)
* **restapi base url** the base URL to answer requests on (default /api/v0.1)
* **restapi log level** critical, error, warning, info, debug
* **restapi log file** (default /var/local/ceph/<clientname>.log)

A server will run on **restapi public addr** if the ceph-rest-api
executed directly; otherwise, configuration is specified by the
enclosing WSGI web server.

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
of all known commands.  The command set is very similar to the commands
supported by the **ceph** tool.

Deployment as WSGI application
==============================

When deploying as WSGI application (say, with Apache/mod_wsgi,
or nginx/uwsgi, or gunicorn, etc.), use the ``ceph_rest_api.py`` module
(``ceph-rest-api`` is a thin layer around this module).  The standalone web
server is of course not used, so address/port configuration is done in
the WSGI server.  Also, configuration switches are not passed; rather,
environment variables are used:

* CEPH_CONF holds -c/--conf
* CEPH_CLUSTER_NAME holds --cluster
* CEPH_NAME holds -n/--name

Any errors reading configuration or connecting to the cluster cause
ImportError to be raised with a descriptive message on import; see
your WSGI server documentation for how to see those messages in case
of problem.


Availability
============

**ceph-rest-api** is part of the Ceph distributed file system. Please refer to the Ceph documentation at
http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8)
