roles:
- - mon.a
  - mds.a
  - osd.0
  - osd.1
- - mon.b
  - mon.c
  - osd.2
  - osd.3
- - client.0
tasks:
- install:
     branch: bobtail
- ceph:
    conf:
      osd:
        filestore flush min: 0
- workunit:
     clients:
        all:
           - suites/fsx.sh
- install.upgrade:
     all:
        branch: master
- ceph.restart: [mds.a, osd.0, osd.1, osd.2, osd.3, mon.a, mon.b, mon.c]
- workunit:
     clients:
        all:
           - suites/ffsb.sh
