===================
Build Ceph Packages
===================

You can create installation packages from the latest code using ``dpkg-buildpackage`` for Debian/Ubuntu or ``rpmbuild`` for the RPM Package Manager.

Debian
------

To create ``.deb`` packages, ensure that you have installed the `build prerequisites <build_prerequisites>`_ and install ``debhelper``. The requirements for 
``debhelper`` include:

- ``debhelper``

