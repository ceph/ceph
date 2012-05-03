=============
 Quick Start
=============
Ceph is intended for large-scale deployments, but you may install Ceph on a 
single host. Quick start is intended for Debian/Ubuntu Linux distributions. 

1. Login to your host.
2. Make a directory for Ceph packages. *e.g.,* ``$ mkdir ceph``
3. `Get Ceph packages <../../install/download_packages>`_ and add them to your 
   APT configuration file.
4. Update and Install Ceph packages. 
   See `Downloading Debian/Ubuntu Packages <../../install/download_packages>`_ 
   and `Installing Packages <../../install/installing_packages>`_ for details.
5. Add a ``ceph.conf`` file. 
   See `Ceph Configuration Files <../../config-cluster/ceph_conf>`_ for details.
6. Run Ceph. 
   See `Deploying Ceph with mkcephfs <../../config_cluster/deploying_ceph_with_mkcephfs>`_
