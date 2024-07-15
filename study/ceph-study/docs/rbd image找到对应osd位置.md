# 1. 查找image指纹信息
```
$ rbd info test_pool/test_image
rbd image 'test_image':
    size 102400 MB in 25600 objects
    order 22 (4096 kB objects)
    block_name_prefix: rbd_data.12c074b0dc51  #指纹就是12c074b0dc51
    format: 2
    features: layering, exclusive-lock, object-map, fast-diff, deep-flatten
    flags:
    create_timestamp: Sat Mar 24 22:46:35 2018
```

# 2. 根据指纹找到这个image的object
```
rados -p test_pool ls | grep 12c074b0dc51
rbd_data.12c074b0dc51.00000000000000bd
rbd_data.12c074b0dc51.0000000000000060
```

# 3. 根据object 查找对应的osd位置
```
$ ceph osd map test_pool rbd_data.12c074b0dc51.0000000000000092
osdmap e403 pool 'test_pool' (1) object 'rbd_data.12c074b0dc51.0000000000000092' -> pg 1.10eddf7f (1.17f) -> up ([17,1,4], p17) acting ([17,1,4], p17)
```
