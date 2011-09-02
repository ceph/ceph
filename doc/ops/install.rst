===========================
 Installing a Ceph cluster
===========================

For development and really early stage testing, see :doc:`/dev/index`.

For installing the latest development builds, see
:doc:`/ops/autobuilt`.

Installing any complex distributed software can be a lot of work. We
support two automated ways of installing Ceph: using Chef_, or with
the ``mkcephfs`` shell script.

.. _Chef: http://wiki.opscode.com/display/chef

.. topic:: Status as of 2011-09

  This section hides a lot of the tedious underlying details. If you
  need to, or wish to, roll your own deployment automation, or are
  doing it manually, you'll have to dig into a lot more intricate
  details.  We are working on simplifying the installation, as that
  also simplifies our Chef cookbooks.


Installing Ceph using Chef
==========================

(Try saying that fast 10 times.)

.. topic:: Status as of 2011-09

  While we have Chef cookbooks in use internally, they are not yet
  ready to handle unsupervised installation of a full cluster. Stay
  tuned for updates.

.. todo:: write me


Installing Ceph using ``mkcephfs``
==================================

Installing the packages
-----------------------


.. _install-debs:

Debian/Ubuntu
~~~~~~~~~~~~~

We regularly build Debian and Ubuntu packages for the `amd64` and
`i386` architectures, for the following distributions:

- ``sid`` (Debian unstable)
- ``squeeze`` (Debian 6.0)
- ``lenny`` (Debian 5.0)
- ``oneiric`` (Ubuntu 11.11)
- ``natty`` (Ubuntu 11.04)
- ``maverick`` (Ubuntu 10.10)

.. todo:: http://ceph.newdream.net/debian/dists/ also has ``lucid``
   (Ubuntu 10.04), should that be removed?

Whenever we say *DISTRO* below, replace that with the codename of your
operating system.

Run these commands on all nodes::

	wget -q -O- https://raw.github.com/NewDreamNetwork/ceph/master/keys/release.asc \
	| sudo apt-key add -

	sudo tee /etc/apt/sources.list.d/ceph.list <<EOF
	deb http://ceph.newdream.net/debian/ DISTRO main
	deb-src http://ceph.newdream.net/debian/ DISTRO main
	EOF

	sudo apt-get update
	sudo apt-get install ceph









.. todo:: For older distributions, you may need to make sure your apt-get may read .bz2 compressed files. This works for Debian Lenny 5.0.3:

	$ apt-get install bzip2

.. todo:: ponder packages

	Package: ceph
	Recommends: ceph-client-tools, ceph-fuse, libceph1, librados2, librbd1, btrfs-tools, gceph

	Package: ceph-client-tools
	Package: ceph-fuse
	Package: libceph-dev
	Package: librados-dev
	Package: librbd-dev
	Package: obsync
	Package: python-ceph
	Package: radosgw


.. todo:: Other operating system support.


.. todo:: write me

Basically, everything somebody needs to go through to build a new
cluster when not cheating via vstart or teuthology, but without
mentioning all the design tradeoffs and options like journaling
locations or filesystems

At this point, either use 1 or 3 mons, point to :doc:`grow/mon`
