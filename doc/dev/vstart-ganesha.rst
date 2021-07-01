Configuring NFS Ganesha to export CephFS with vstart
====================================================

1) Using ``cephadm``

    .. code:: bash

        $ MDS=1 MON=1 OSD=3 NFS=1 ../src/vstart.sh -n -d --cephadm

    This will deploy a single NFS Ganesha daemon using ``vstart.sh``, where
    the daemon will listen on the default NFS Ganesha port.

2) Using test orchestrator

    .. code:: bash

       $ MDS=1 MON=1 OSD=3 NFS=1 ../src/vstart.sh -n -d

    Environment variable ``NFS`` is the number of NFS Ganesha daemons to be
    deployed, each listening on a random port.

    .. note:: NFS Ganesha packages must be pre-installed for this to work.

