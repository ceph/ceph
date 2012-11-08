=========================
 Using Hadoop with CephFS
=========================

Hadoop Configuration
--------------------

This section describes the Hadoop configuration options used to control Ceph.
These options are intended to be set in the Hadoop configuration file
`conf/core-site.xml`.

+--------------------+--------------------------+----------------------------+
|Property            |Value                     |Notes                       |
|                    |                          |                            |
+====================+==========================+============================+
|fs.default.name     |Ceph URI                  |ceph:///                    |
|                    |                          |                            |
|                    |                          |                            |
+--------------------+--------------------------+----------------------------+
|fs.ceph.conf.file   |Local path to ceph.conf   |/etc/ceph/ceph.conf         |
|                    |                          |                            |
|                    |                          |                            |
|                    |                          |                            |
+--------------------+--------------------------+----------------------------+
|fs.ceph.conf.options|Comma separated list of   |opt1=val1,opt2=val2         |
|                    |key/value pairs           |                            |
|                    |                          |                            |
|                    |                          |                            |
+--------------------+--------------------------+----------------------------+
