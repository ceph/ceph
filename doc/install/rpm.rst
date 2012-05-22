=========================
 Installing RPM Packages
=========================

We do not yet build RPM packages for Ceph releases.  You can build them yourself from
the source tree by running::

        rpmbuild

See `Ceph Source Code <../../source>`_ for details. Once you have an RPM, you can 
install it with::

	rpm -i ceph-*.rpm

