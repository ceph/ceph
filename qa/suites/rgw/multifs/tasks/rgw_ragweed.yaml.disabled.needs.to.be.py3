# ragweed uses nosetests, so stick to py2 for now
os_type: ubuntu

tasks:
- install:
- ceph:
- rgw: [client.0]
- ragweed:
    client.0:
      default-branch: ceph-master
      rgw_server: client.0
      stages: prepare
- ragweed:
    client.0:
      default-branch: ceph-master
      rgw_server: client.0
      stages: check
overrides:
  ceph:
    conf:
      client:
        rgw lc debug interval: 10
