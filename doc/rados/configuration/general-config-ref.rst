==========================
 General Config Reference
==========================



``fsid``

:Description: The filesystem ID. One per cluster.
:Type: UUID
:Required: No. 
:Default: N/A. Generated if not specified.


``admin socket``

:Description: The socket for executing administrative commands irrespective 
              of whether Ceph monitors have established a quorum.

:Type: String
:Required: No
:Default: ``/var/run/ceph/$cluster-$name.asok`` 


``pid file``

:Description: Each running Ceph daemon has a running 
              process identifier (PID) file.

:Type: String
:Required: No
:Default: N/A. The default path is ``/var/run/$cluster/$name.pid``. The PID file is generated upon start-up. 


``chdir``

:Description: The directory Ceph daemons change to once they are 
              up and running. Default ``/`` directory recommended.

:Type: String
:Required: No
:Default: ``/``


``max open files``

:Description: If set, when the Ceph service starts, Ceph sets the 
              ``max open fds`` at the OS level (i.e., the max # of file 
              descriptors). It helps prevents OSDs from running out of 
              file descriptors.

:Type: 64-bit Integer
:Required: No
:Default: ``0``

``fatal signal handlers``

:Description: If set, we will install signal handlers for SEGV, ABRT, BUS, ILL,
              FPE, XCPU, XFSZ, SYS signals to generate a useful log message

:Type: Boolean
:Default: ``true``
