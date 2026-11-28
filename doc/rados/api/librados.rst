==============
 Librados (C)
==============

.. highlight:: c

`librados` provides low-level access to the RADOS service. For an
overview of RADOS, see :doc:`../../architecture/index`.


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

Then you open an "I/O context", a :c:type:`rados_ioctx_t`, with :c:func:`rados_ioctx_create()`::

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

In the end, you will want to close your I/O context and connection to RADOS with :c:func:`rados_ioctx_destroy()` and :c:func:`rados_shutdown()`::

	rados_ioctx_destroy(io);
	rados_shutdown(cluster);


Asynchronous I/O
================

When doing lots of I/O, you often don't need to wait for one operation
to complete before starting the next one. `Librados` provides
asynchronous versions of several operations:

* :c:func:`rados_aio_write`
* :c:func:`rados_aio_append`
* :c:func:`rados_aio_write_full`
* :c:func:`rados_aio_read`

For each operation, you must first create a
:c:type:`rados_completion_t` that represents what to do when the
operation is safe or complete by calling
:c:func:`rados_aio_create_completion`. If you don't need anything
special to happen, you can pass NULL::

	rados_completion_t comp;
	err = rados_aio_create_completion(NULL, NULL, NULL, &comp);
	if (err < 0) {
		fprintf(stderr, "%s: could not create aio completion: %s\n", argv[0], strerror(-err));
		rados_ioctx_destroy(io);
		rados_shutdown(cluster);
		exit(1);
	}

Now you can call any of the aio operations, and wait for it to
be in memory or on disk on all replicas::

	err = rados_aio_write(io, "foo", comp, "bar", 3, 0);
	if (err < 0) {
		fprintf(stderr, "%s: could not schedule aio write: %s\n", argv[0], strerror(-err));
		rados_aio_release(comp);
		rados_ioctx_destroy(io);
		rados_shutdown(cluster);
		exit(1);
	}
	rados_aio_wait_for_complete(comp); // in memory
	rados_aio_wait_for_safe(comp); // on disk

Finally, we need to free the memory used by the completion with :c:func:`rados_aio_release`::

	rados_aio_release(comp);

You can use the callbacks to tell your application when writes are
durable, or when read buffers are full. For example, if you wanted to
measure the latency of each operation when appending to several
objects, you could schedule several writes and store the ack and
commit time in the corresponding callback, then wait for all of them
to complete using :c:func:`rados_aio_flush` before analyzing the
latencies::

	typedef struct {
		struct timeval start;
		struct timeval ack_end;
		struct timeval commit_end;
	} req_duration;

	void ack_callback(rados_completion_t comp, void *arg) {
		req_duration *dur = (req_duration *) arg;
		gettimeofday(&dur->ack_end, NULL);
	}

	void commit_callback(rados_completion_t comp, void *arg) {
		req_duration *dur = (req_duration *) arg;
		gettimeofday(&dur->commit_end, NULL);
	}

	int output_append_latency(rados_ioctx_t io, const char *data, size_t len, size_t num_writes) {
		req_duration times[num_writes];
		rados_completion_t comps[num_writes];
		for (size_t i = 0; i < num_writes; ++i) {
			gettimeofday(&times[i].start, NULL);
			int err = rados_aio_create_completion((void*) &times[i], ack_callback, commit_callback, &comps[i]);
			if (err < 0) {
				fprintf(stderr, "Error creating rados completion: %s\n", strerror(-err));
				return err;
			}
			char obj_name[100];
			snprintf(obj_name, sizeof(obj_name), "foo%ld", (unsigned long)i);
			err = rados_aio_append(io, obj_name, comps[i], data, len);
			if (err < 0) {
				fprintf(stderr, "Error from rados_aio_append: %s", strerror(-err));
				return err;
			}
		}
		// wait until all requests finish *and* the callbacks complete
		rados_aio_flush(io);
		// the latencies can now be analyzed
		printf("Request # | Ack latency (s) | Commit latency (s)\n");
		for (size_t i = 0; i < num_writes; ++i) {
			// don't forget to free the completions
			rados_aio_release(comps[i]);
			struct timeval ack_lat, commit_lat;
			timersub(&times[i].ack_end, &times[i].start, &ack_lat);
			timersub(&times[i].commit_end, &times[i].start, &commit_lat);
			printf("%9ld | %8ld.%06ld | %10ld.%06ld\n", (unsigned long) i, ack_lat.tv_sec, ack_lat.tv_usec, commit_lat.tv_sec, commit_lat.tv_usec);
		}
		return 0;
	}

Note that all the :c:type:`rados_completion_t` must be freed with :c:func:`rados_aio_release` to avoid leaking memory.


Thread safety
=============

Several threads can share one :c:type:`rados_ioctx_t`. They can call the
synchronous and asynchronous read, write, and object operation functions on the
same io context at the same time. The exceptions are:

- The io context's settings: the snapshot to read from
  (:c:func:`rados_ioctx_snap_set_read`), the snapshot context for writes
  (:c:func:`rados_ioctx_selfmanaged_snap_set_write_ctx`), the object locator
  key (:c:func:`rados_ioctx_locator_set_key`), the namespace
  (:c:func:`rados_ioctx_set_namespace`), and the flag that
  :c:func:`rados_set_pool_full_try` and :c:func:`rados_set_osdmap_full_try`
  set. Every operation reads these settings without a lock, so an operation
  that starts while another thread changes one can get a value that was never
  set, such as a namespace made from parts of the old and new names. Change
  them only while no other thread is using the io context, or give each thread
  its own io context.
- :c:func:`rados_get_last_version` returns the version from the most recent
  synchronous operation on the io context, whichever thread ran it. When
  threads share an io context, use asynchronous operations and get the version
  of each one with :c:func:`rados_aio_get_version`.
- A list context (:c:type:`rados_list_ctx_t`), a :c:type:`rados_read_op_t`,
  and a :c:type:`rados_write_op_t` can each be used by only one thread at a
  time.
- Call :c:func:`rados_ioctx_destroy` only after every thread has finished with
  the io context and every asynchronous operation on it has completed.


API calls
=========

 .. autodoxygenfile:: rados_types.h
 .. autodoxygenfile:: librados.h
