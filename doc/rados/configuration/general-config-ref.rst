==========================
 General Config Reference
==========================

``auth supported``

.. deprecated:: 0.51

:Description: Indicates the type of authentication used. Currently ``cephx`` 
              only. If not specified, it defaults to ``none``.

:Type: String
:Required: No
:Default: ``none``

    
``auth cluster required``

.. versionadded:: 0.51

:Description: Enables authentication for the cluster. 
              Valid setting is ``cephx``.

:Type: String
:Required: No
:Default: ``none``

    
``auth service required``

.. versionadded:: 0.51

:Description: Enables authentication for the service. 
              Valid setting is ``cephx``.

:Type: String
:Required: No
:Default: ``none``



``auth client required``

.. versionadded:: 0.51

:Description: Enables authentication for the client. 
              Valid setting is ``cephx``.

:Type: String
:Required: No
:Default: ``none``


``keyring``

:Description: The path to the cluster's keyring file. 
:Type: String
:Required: No
:Default: ``/etc/ceph/$cluster.$name.keyring,/etc/ceph/$cluster.keyring,/etc/ceph/keyring,/etc/ceph/keyring.bin``


``fsid``

:Description: The filesystem ID. One per cluster.
:Type: UUID
:Required: No. 
:Default: N/A. Generated if not specified.


``mon host``

:Description: A list of ``{hostname}:{port}`` entries that clients can use to 
              connect to a Ceph monitor. If not set, Ceph searches ``[mon.*]`` 
              sections. 

:Type: String
:Required: No
:Default: N/A


``host``

:Description: The hostname. Use this setting for specific daemon instances 
              (e.g., ``[osd.0]``).

:Type: String
:Required: Yes, for daemon instances.
:Default: ``localhost``

.. tip:: Do not use ``localhost``. To get your host name, execute 
         ``hostname -s`` on your command line and use the name of your host 
         (to the first period, not the fully-qualified domain name).

.. important:: You should not specify any value for ``host`` when using a third
               party deployment system that retrieves the host name for you.


``public network``

:Description: The IP address and netmask of the public (front-side) network 
              (e.g., ``10.20.30.40/24``). Set in ``[global]``. You may specify
              comma-delimited subnets.

:Type: ``{ip-address}/{netmask} [, {ip-address}/{netmask}]``
:Required: No
:Default: N/A


``public addr``

:Description: The IP address for the public (front-side) network. 
              Set for each daemon.

:Type: IP Address
:Required: No
:Default: N/A


``cluster network``

:Description: The IP address and netmask of the cluster (back-side) network 
              (e.g., ``10.20.30.41/24``).  Set in ``[global]``. You may specify
              comma-delimited subnets.

:Type: ``{ip-address}/{netmask} [, {ip-address}/{netmask}]``
:Required: No
:Default: N/A


``cluster addr``

:Description: The IP address for the cluster (back-side) network. 
              Set for each daemon.

:Type: Address
:Required: No
:Default: N/A


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
