.. index:: mkcephfs

====================================
 Installing Ceph using ``mkcephfs``
====================================

.. note:: ``mkcephfs`` is meant as a quick bootstrapping tool. It does
   not handle more complex operations, such as upgrades. For
   production clusters, you will want to use the :ref:`Chef cookbooks
   <install-chef>`.

Pick a host that has the Ceph software installed -- it does not have
to be a part of your cluster, but it does need to have *matching
versions* of the ``mkcephfs`` command and other Ceph tools
installed. This will be your `admin host`.


Installing the packages
=======================


.. _install-debs:

Debian/Ubuntu
-------------

We regularly build Debian and Ubuntu packages for the `amd64` and
`i386` architectures, for the following distributions:

- ``sid`` (Debian unstable)
- ``wheezy`` (Debian 7.0)
- ``squeeze`` (Debian 6.0)
- ``precise`` (Ubuntu 12.04)
- ``oneiric`` (Ubuntu 11.10)
- ``natty`` (Ubuntu 11.04)
- ``maverick`` (Ubuntu 10.10)

.. todo:: http://ceph.newdream.net/debian/dists/ also has ``lucid``
   (Ubuntu 10.04), should that be removed?

Whenever we say *DISTRO* below, replace that with the codename of your
operating system.

Run these commands on all nodes::

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/release.asc \
	| sudo apt-key add -

	sudo tee /etc/apt/sources.list.d/ceph.list <<EOF
	deb http://ceph.newdream.net/debian/ DISTRO main
	deb-src http://ceph.newdream.net/debian/ DISTRO main
	EOF

	sudo apt-get update
	sudo apt-get install ceph


.. todo:: For older distributions, you may need to make sure your apt-get may read .bz2 compressed files. This works for Debian Lenny 5.0.3: ``apt-get install bzip2``

.. todo:: Ponder packages; ceph.deb currently pulls in gceph (ceph.deb
   Recommends: ceph-client-tools ceph-fuse libcephfs1 librados2 librbd1
   btrfs-tools gceph) (other interesting: ceph-client-tools ceph-fuse
   libcephfs-dev librados-dev librbd-dev obsync python-ceph radosgw)


Red Hat / CentOS / Fedora
-------------------------

.. topic:: Status as of 2011-09

   We do not currently provide prebuilt RPMs, but we do provide a spec
   file that should work. The following will guide you to compiling it
   yourself.

To ensure you have the right build-dependencies, run::

	yum install rpm-build rpmdevtools git fuse-devel libtool \
	  libtool-ltdl-devel boost-devel libedit-devel openssl-devel \
	  gcc-c++ nss-devel libatomic_ops-devel make

To setup an RPM compilation environment, run::

	rpmdev-setuptree

To fetch the Ceph source tarball, run::

	wget -P ~/rpmbuild/SOURCES/ http://ceph.newdream.net/download/ceph-0.35.tar.gz

To build it, run::

	rpmbuild -tb ~/rpmbuild/SOURCES/ceph-0.35.tar.gz

Finally, install the RPMs::

	rpm -i rpmbuild/RPMS/x86_64/ceph-*.rpm



.. todo:: Other operating system support.


Creating a ``ceph.conf`` file
=============================

On the `admin host`, create a file with a name like
``mycluster.conf``.

Here's a template for a 3-node cluster, where all three machines run a
:ref:`monitor <monitor>` and an :ref:`object store <rados>`, and the
first one runs the :ref:`Ceph filesystem daemon <cephfs>`. Replace the
hostnames and IP addresses with your own, and add/remove hosts as
appropriate. All hostnames *must* be short form (no domain).

.. literalinclude:: mycluster.conf
   :language: ini

Note how the ``host`` variables dictate what node runs what
services. See :doc:`/config` for more information.

It is **very important** that you only run a single monitor on each node. If
you attempt to run more than one monitor on a node, you reduce your reliability
and the procedures in this documentation will not behave reliably.

.. todo:: More specific link for host= convention.

.. todo:: Point to cluster design docs, once they are ready.

.. todo:: At this point, either use 1 or 3 mons, point to :doc:`/ops/manage/grow/mon`


Running ``mkcephfs``
====================

Verify that you can manage the nodes from the host you intend to run
``mkcephfs`` on:

- Make sure you can SSH_ from the `admin host` into all the nodes
  using the short hostnames (``myserver`` not
  ``myserver.mydept.example.com``), with no user specified
  [#ssh_config]_.
- Make sure you can SSH_ from the `admin host` into all the nodes
  as ``root`` using the short hostnames.
- Make sure you can run ``sudo`` without passphrase prompts on all
  nodes [#sudo]_.

.. _SSH: http://openssh.org/

If you are not using :ref:`Btrfs <btrfs>`, enable :ref:`extended
attributes <xattr>`.

On each node, make sure the directory ``/srv/osd.N`` (with the
appropriate ``N``) exists, and the right filesystem is mounted. If you
are not using a separate filesystem for the file store, just run
``sudo mkdir /srv/osd.N`` (with the right ``N``).

Then, using the right path to the ``mycluster.conf`` file you prepared
earlier, run::

	mkcephfs -a -c mycluster.conf -k mycluster.keyring

This will place an `admin key` into ``mycluster.keyring``. This will
be used to manage the cluster. Treat it like a ``root`` password to
your filesystem.

.. todo:: Link to explanation of `admin key`.

That should SSH into all the nodes, and set up Ceph for you.

It does **not** copy the configuration, or start the services. Let's
do that::

	ssh myserver01 sudo tee /etc/ceph/ceph.conf <mycluster.conf
	ssh myserver02 sudo tee /etc/ceph/ceph.conf <mycluster.conf
	ssh myserver03 sudo tee /etc/ceph/ceph.conf <mycluster.conf
	...

	ssh myserver01 sudo /etc/init.d/ceph start
	ssh myserver02 sudo /etc/init.d/ceph start
	ssh myserver03 sudo /etc/init.d/ceph start
	...

After a little while, the cluster should come up and reach a healthy
state. We can check that::

	ceph -k mycluster.keyring -c mycluster.conf health
	2011-09-06 12:33:51.561012 mon <- [health]
	2011-09-06 12:33:51.562164 mon2 -> 'HEALTH_OK' (0)

.. todo:: Document "healthy"

.. todo:: Improve output.



.. rubric:: Footnotes

.. [#ssh_config] Something like this in your ``~/.ssh_config`` may
   help -- unfortunately you need an entry per node::

	Host myserverNN
	     Hostname myserverNN.dept.example.com
	     User ubuntu

.. [#sudo] The relevant ``sudoers`` syntax looks like this::

	%admin ALL=(ALL) NOPASSWD: ALL
