=============================
 Autobuilt unstable packages
=============================

We automatically build Debian and Ubuntu packages for any branches or
tags that appear in the |ceph.git|_. We build packages for the `amd64`
and `i386` architectures (`arch list`_), for the following
distributions (`distro list`_):

- ``natty`` (Ubuntu 11.04)
- ``squeeze`` (Debian 6.0)

.. |ceph.git| replace::
   ``ceph.git`` repository
.. _`ceph.git`: https://github.com/NewDreamNetwork/ceph

.. _`arch list`: http://ceph.newdream.net/debian-snapshot-amd64/master/dists/natty/main/
.. _`distro list`: http://ceph.newdream.net/debian-snapshot-amd64/master/dists/

The current status of autobuilt packages can be found at
http://ceph.newdream.net/gitbuilder-deb-amd64/ .

If you wish to use these packages, you need to modify the
:ref:`earlier instructions <install-debs>` as follows:

.. warning:: The following commands make your computer trust any code
   that makes it into ``ceph.git``, including work in progress
   branches and versions of code with possible security issues (that
   were fixed afterwards). Use at your own risk!

Whenever we say *DISTRO* below, replace it with the codename of your
operating system.

Whenever we say *BRANCH* below, replace it with the version of the
code you want to run, e.g. ``master``, ``stable`` or ``v0.34`` (`branch list`_ [#broken-links]_).

.. _`branch list`: http://ceph.newdream.net/debian-snapshot-amd64/

Run these commands on all nodes::

	wget -q -O- https://raw.github.com/NewDreamNetwork/ceph/master/keys/autobuild.asc \
	| sudo apt-key add -

	sudo tee /etc/apt/sources.list.d/ceph.list <<EOF
	deb http://ceph.newdream.net/debian-snapshot-amd64/BRANCH/ DISTRO main
	deb-src http://ceph.newdream.net/debian-snapshot-amd64/BRANCH/ DISTRO main
	EOF

	sudo apt-get update
	sudo apt-get install ceph

From here on, you can follow the usual set up instructions in
:doc:`/ops/install`.



.. rubric:: Footnotes

.. [#broken-links] Technical issues with how that part of the URL
   space is HTTP reverse proxied means that the links in the generated
   directory listings are broken. Please don't click on the links,
   instead edit the URL bar manually, for now.

   .. todo:: Fix the gitbuilder reverse proxy to not break relative URLs.
