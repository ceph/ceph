# 1. 查看FS客户端连接
```
$ ceph daemon mds.0 session ls
[
    {
        "id": 5122511,
        "num_leases": 0,
        "num_caps": 655,
        "state": "open",
        "replay_requests": 0,
        "completed_requests": 1,
        "reconnecting": false,
        "inst": "client.9762766 10.9.1.2:0\/4202511977",
        "client_metadata": {
            "ceph_sha1": "b1e0532418e4631af01acbc0cedd426f1905f4af",
            "ceph_version": "ceph version 0.94.10 (b1e0532418e4631af01acbc0cedd426f1905f4af)",
            "entity_id": "log_cephfs",
            "hostname": "ac_xxx-client00.gz01",
            "mount_point": "\/mnt\/log"
        }
    }
]

$ ceph daemon mds.0 session ls  | grep "hostname\|inst"
        "inst": "client.9762766 10.9.1.2:0\/4202511977",
            "hostname": "ac_xxx-client00.gz01",
```
**说明：**
 - id：client唯一id
 - num_caps：client获取的caps
 - inst：client端的ip和端口链接信息
 - ceph_version：client端的ceph-fuse版本，若使用kernel client，则为kernel_version
 - hostname：client端的主机名
 - mount_point：client在主机上对应的mount point
 - pid：client端ceph-fuse进程的pid

# 2. 查看RBD块存储客户端连接
```
# 查看存储池下面所有镜像
$ rbd ls test_pool
test_image
test_image_test
 
# 查看镜像挂载的客户端
$ rbd status test_pool/test_image
Watchers:
    watcher=10.9.1.2:0/671061150 client.8012074 cookie=140171290999856 
```

# 3. 查看RGW客户端连接
```
# 查看日志格式
$  tail /var/log/ceph/ceph-client.rgw.op-xxx-ceph00.log
2018-07-23 19:17:32.546214 7f8779fcb700  1 civetweb: 0x7f87a80266e0: 10.9.1.2 - - [23/Jul/2018:19:17:32 +0800] "HEAD /epp-xxx/2018-07-23/VjFfMTAwMDAwMV9NQUxPSEI1TVAyMEJMSkZNMkg4TUVKSzJSNg/03%3A50%3A25.824Z/308357/SCfn3qka.png.lz4 HTTP/1.1" 200 0 - Boto/2.49.0 Python/3.6.3 Linux/3.10.0-514.16.1.el7.x86_64
2018-07-23 19:17:32.548567 7f8779fcb700  1 ====== starting new request req=0x7f8779fc5710 =====
2018-07-23 19:17:32.561416 7f8779fcb700  1 ====== req done req=0x7f8779fc5710 op status=0 http_status=200 ======
2018-07-23 19:17:32.561462 7f8779fcb700  1 civetweb: 0x7f87a80266e0: 10.9.1.2 - - [23/Jul/2018:19:17:32 +0800] "GET /epp-xxx/2018-07-23/VjFfMTAwMDAwMV9NQUxPSEI1TVAyMEJMSkZNMkg4TUVKSzJSNg/03%3A50%3A25.824Z/308357/SCfn3qka.png.lz4 HTTP/1.1" 200 0 - Boto/2.49.0 Python/3.6.3 Linux/3.10.0-514.16.1.el7.x86_64 
 
 
# 统计访问的客户端
cat /var/log/ceph/ceph-client.rgw.op-xxx-ceph00.log | grep civetweb | awk -F ' ' '{print $7}' | sort | uniq  -c
   2297 10.9.1.2
```
