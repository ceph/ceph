Design of the libcephfs proxy
=============================

Description of the problem
--------------------------

When an application connects to a Ceph volume through the *libcephfs.so*
library, a cache is created locally inside the process. The *libcephfs.so*
implementation already deals with memory usage of the cache and adjusts it so
that it doesn't consume all the available memory. However, if multiple
processes connect to CephFS through different instances of the library, each
one of them will keep a private cache. In this case memory management is not
effective because, even configuring memory limits, the number of libcephfs
instances that can be created is unbounded and they can't work in a coordinated
way to correctly control ressource usage. Due to this, it's relatively easy to
consume all memory when all processes are using data cache intensively. This
causes the OOM killer to terminate those processes.

Proposed solution
-----------------

High level approach
^^^^^^^^^^^^^^^^^^^

The main idea is to create a *libcephfs_proxy.so* library that will provide the
same API as the original *libcephfs.so*, but won't cache any data. This library
can be used by any application currently using *libcephfs.so* in a transparent
way (i.e. no code modification required) just by linking against
*libcephfs_proxy.so* instead of *libcephfs.so*, or even using *LD_PRELOAD*.

A new *libcephfsd* daemon will also be created. This daemon will link against
the real *libcephfs.so* library and will listen for incoming connections on a
UNIX socket.

When an application starts and initiates CephFS requests through the
*libcephfs_proxy.so* library, it will connect to the *libcephfsd* daemon
through the UNIX socket and it will forward all CephFS requests to it. The
daemon will use the real *libcephfs.so* to execute those requests and the
answers will be returned back to the application, potentially caching data in
the *libcephfsd* process itself. All this will happen transparently, without
any knowledge from the application.

The daemon will share low level *libcephfs.so* mounts between different
applications to avoid creating an instance for each application, which would
have the same effect on memory as linking each application directly to the
*libcephfs.so* library. This will be done only if the configuration defined by
the applications is identical. Otherwise new independent instances will still
be created.

Some *libcephfs.so* functions will need to be implemented in an special way
inside the *libcephfsd* daemon to hide the differences caused by sharing the
same mount instance with more than one client (for example chdir/getcwd cannot
rely directly on the ``ceph_chdir()``/``ceph_getcwd()`` of *libcephfs.so*).

Initially, only the subset of the low-level interface functions of
*libcephfs.so* that are used by the Samba's VFS CephFS module will be provided.

Design of common components
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Network protocol
""""""""""""""""

Since the connection through the UNIX socket is to another process that runs on
the same machine and the data we need to pass is quite simple, we'll avoid all
the overhead of generic XDR encoding/decoding and RPC transmissions by using a
very simple serialization implemented in the code itself. For the future we may
consider using cap'n proto (https://capnproto.org), which claims to have zero
overhead for encoding and decoding, and would provide an easy way to support
backward compatibility if the network protocol needs to be modified in the
future.

Design of the *libcephfs_proxy.so* library
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This library will basically connect to the UNIX socket where the *libcephfsd*
daemon is listening, wait for requests coming from the application, serialize
all function arguments and send them to the daemon. Once the daemon responds it
will deserialize the answer and return the result to the application.

Local caching
"""""""""""""

While the main purpose of this library is to avoid independent caches on each
process, some preliminary testing has shown a big performance drop for
workloads based on metadata operations and/or small files when all requests go
through the proxy daemon. To minimize this, metadata caching should be
implemented. Metadata cache is much smaller than data cache and will provide a
good trade-off between memory usage and performance.

To implement caching in a safe way, it's required to correctly invalidate data
before it becomes stale. Currently libcephfs.so provides invalidation
notifications that can be used to implement this, but its semantics are not
fully understood yet, so the cache in the libcephfs_proxy.so library will be
designed and implemented in a future version.


Design of the *libcephfsd* daemon
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The daemon will be a regular process that will centralize libcephfs requests
coming from other processes on the same machine.

Process maintenance
"""""""""""""""""""

Since the process will work as a standalone daemon, a simple systemd unit file
will be provided to manage it as a regular system service. Most probably this
will be integrated inside cephadm in the future.

In case the *libcephfsd* daemon crashes, we'll rely on systemd to restart it.


Special functions
^^^^^^^^^^^^^^^^^

Some functions will need to be handled in a special way inside the *libcephfsd*
daemon to provide correct functionality since forwarding them directly to
*libcephfs.so* could return incorrect results due to the sharing of low-level
mounts.

**Sharing of underlying struct ceph_mount_info**

The main purpose of the proxy is to avoid creating a new mount for each process
when they are accessing the same data. To be able to provide this we need to
"virtualize" the mount points and let the application believe it's using its
own mount when, in fact, it could be using a shared mount.

The daemon will track the Ceph account used to connect to the volume, the
configuration file and any specific configuration changes done before mounting
the volume. Only if all settings are exactly the same as another already
mounted instance, then the mount will be shared. The daemon won't understand
CephFS settings nor any potential dependencies between settings. For this
reason, a very strict comparison will be done: the configuration file needs to
be identical and any other changes made afterwards need to be set to the exact
same value and in the same order so that two configurations can be considered
identical.

The check to determine whether two configurations are identical or not will be
done just before mounting the volume (i.e. ``ceph_mount()``). This means that
during the configuration phase, we may have many simultaneous mounts allocated
but not yet mounted. However only one of them will become a real mount. The
others will remain unmounted and will be eventually destroyed once users
unmount and release them.

The following functions will be affected:

* **ceph_create**

  This one will allocate a new ceph_mount_info structure, and the provided id
  will be recorded for future comparison of potentially matching mounts.

* **ceph_release**

  This one will release an unmounted ceph_mount_info structure. Unmounted
  structures won't be shared with anyone else.

* **ceph_conf_read_file**

  This one will read the configuration file, compute a checksum and make a
  copy. The copy will make sure that there are no changes in the configuration
  file since the time the checksum was computed, and the checksum will be
  recorded for future comparison of potentially matching mounts.

* **ceph_conf_get**

  This one will obtain the requested setting, recording it for future
  comparison of potentially matching mounts.

  Even though this may seem unnecessary, since the daemon is considering the
  configuration as a black box, it could be possible to have some dynamic
  setting that could return different values depending on external factors, so
  the daemon also requires that any requested setting returns the same value to
  consider two configurations identical.

* **ceph_conf_set**

  This one will record the modified value for future comparison of potentially
  matching mounts.

  In normal circumstances, some settings may be set even after having mounted
  the volume. The proxy won't allow that to avoid potential interferences with
  other clients sharing the same mount.

* **ceph_init**

  This one will be a no-op. Calling this function triggers the allocation of
  several resources and starts some threads. This is just a waste of resources
  if this *ceph_mount_info* structure is not finally mounted because it matches
  with an already existing mount.

  Only if at the time of mount (i.e. ``ceph_mount()``) there's no match with
  already existing mounts, then the mount will be initialized and mounted at
  the same time.

* **ceph_select_filesystem**

  This one will record the selected file system for future comparison of
  potentially matching mounts.

* **ceph_mount**

  This one will try to find an active mount that matches with all the
  configurations defined for this *ceph_mount_info* structure. If none is
  found, it will be mounted. Otherwise, the already existing mount will be
  shared with this client.

  The unmounted *ceph_mount_info* structures will be kept around associated
  with the mounted one.

  All "real" mounts will be made against the absolute root of the volume
  (i.e. "/") to make sure they can be shared with other clients later,
  regardless of whether they use the same mount point or not. This means that
  just after mounting, the daemon will need to resolve and store the root inode
  of the "virtual" mount point.

  The CWD (Current Working Directory) will also be initialized to the same
  inode.

* **ceph_unmount**

  This one will detach the client from the mounted *ceph_mount_info* structure
  and reattach it to one of the associated unmounted structures. If this was
  the last user of the mount, it's finally unmounted instead.

  After calling this function, the client continues using a private
  *ceph_mount_info* structure that is used exclusively by itself, so other
  configuration changes and operations can be done safely.

**Confine accesses to the intended mount point**

Since the effective mount point may not match the real mount point, some
functions could be able to return inodes outside of the effective mount point
if not handled with care. To avoid it and provide the result that the user
application expects, we will need to simulate some of them inside the
*libcephfsd* daemon.

There are three special cases to consider:

1. Handling of paths starting with "/"
2. Handling of paths containing ".." (i.e. parent directory)
3. Handling of paths containing symbolic links

When these special paths are found, they need to be handled in a special way to
make sure that the returned inodes are what the client expects.

The following functions will be affected:

* **ceph_ll_lookup**

  Lookup accepts ".." as the name to resolve. If the parent directory is the
  root of the "virtual" mount point (which may not be the same as the real
  mount point), we'll need to return the inode corresponding to the "virtual"
  mount point stored at the time of mount, instead of the real parent.

* **ceph_ll_lookup_root**

  This one needs to return the root inode stored at the time of mount.

* **ceph_ll_walk**

  This one will be completely reimplemented inside the daemon to be able to
  correctly parse each path component and symbolic link, and handle "/" and
  ".." in the correct way.

* **ceph_chdir**

  This one will resolve the passed path and store it along the corresponding
  inode inside the current "virtual" mount. The real ``ceph_chdir()`` won't be
  called.

* **ceph_getcwd**

  This one will just return the path stored in the "virtual" mount from
  previous ``ceph_chdir()`` calls.

**Handle AT_FDCWD**

Any function that receives a file descriptor could also receive the special
*AT_FDCWD* value. These functions need to check for that value and use the
"virtual" CWD instead.

Testing
-------

The proxy should be transparent to any application already using
*libcephfs.so*. This also applies to testing scripts and applications. So any
existing test against the regular *libcephfs.so* library can also be used to
test the proxy.
