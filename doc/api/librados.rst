==============
 Librados (C)
==============

.. highlight:: c

`Librados` provides low-level access to the RADOS service. For an
overview of RADOS, see :doc:`/architecture`.


Example: connecting and writing an object
=========================================

To use `Librados`, you instantiate a :c:type:`rados_t` variable (a cluster handle) and
call :c:func:`rados_create()` with a pointer to it::

	int err;
	rados_t cluster;

	err = rados_create(&cluster, NULL);
	if (err < 0) {
		fprintf(stderr, "%s: cannot create a cluster handle: %s\n", argv[0], strerror(-err));
		exit(1);
	}

Then you configure your :c:type:`rados_t` to connect to your cluster,
either by setting individual values (:c:func:`rados_conf_set()`),
using a configuration file (:c:func:`rados_conf_read_file()`), using
command line options (:c:func:`rados_conf_parse_argv`), or an
environment variable (:c:func:`rados_conf_parse_env()`)::

	err = rados_conf_read_file(cluster, "/path/to/myceph.conf");
	if (err < 0) {
		fprintf(stderr, "%s: cannot read config file: %s\n", argv[0], strerror(-err));
		exit(1);
	}

Once the cluster handle is configured, you can connect to the cluster with :c:func:`rados_connect()`::

	err = rados_connect(cluster);
	if (err < 0) {
		fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-err));
		exit(1);
	}

Then you open an "IO context", a :c:type:`rados_ioctx_t`, with :c:func:`rados_ioctx_create()`::

	rados_ioctx_t io;
	char *poolname = "mypool";

	err = rados_ioctx_create(cluster, poolname, &io);
	if (err < 0) {
		fprintf(stderr, "%s: cannot open rados pool %s: %s\n", argv[0], poolname, strerror(-err));
		rados_shutdown(cluster);
		exit(1);
	}

Note that the pool you try to access must exist.

Then you can use the RADOS data manipulation functions, for example
write into an object called ``greeting`` with
:c:func:`rados_write_full()`::

	err = rados_write_full(io, "greeting", "hello", 5);
	if (err < 0) {
		fprintf(stderr, "%s: cannot write pool %s: %s\n", argv[0], poolname, strerror(-err));
		rados_ioctx_destroy(io);
		rados_shutdown(cluster);
		exit(1);
	}

In the end, you'll want to close your IO context and connection to RADOS with :c:func:`rados_ioctx_destroy()` and :c:func:`rados_shutdown()`::

	rados_ioctx_destroy(io);
	rados_shutdown(cluster);





API calls
=========

 .. doxygenfile:: rados/librados.h
