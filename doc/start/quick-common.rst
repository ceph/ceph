.. ditaa:: 
           /------------------\         /----------------\
           |    admin-node    |         |      node1     |
           |                  +-------->+ cCCC           |
           |    ceph–deploy   |         |    mon.node1   |
           \---------+--------/         \----------------/
                     |
                     |                  /----------------\
                     |                  |      node2     |
                     +----------------->+ cCCC           |
                     |                  |     osd.0      |
                     |                  \----------------/
                     |
                     |                  /----------------\
                     |                  |      node3     |
                     +----------------->| cCCC           |
                                        |     osd.1      |
                                        \----------------/

For best results, create a directory on your admin node node for maintaining the
configuration that ``ceph-deploy`` generates for your cluster. ::

	mkdir my-cluster
	cd my-cluster

.. tip:: The ``ceph-deploy`` utility will output files to the 
   current directory. Ensure you are in this directory when executing
   ``ceph-deploy``.

.. tip:: ``ceph-deploy`` is talking to the
   local admin host (``admin-node``). This host must be reachable by
   its hostname. If necessary, modify ``/etc/hosts`` to add the name
   of the admin host.
