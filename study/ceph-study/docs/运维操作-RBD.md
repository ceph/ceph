# 1.说明
## 1.1介绍
RBD全称RADOS block device，是Ceph对外提供的块设备服务。

# 2. 常用操作
## 2.1 查看pool里所有镜像
```
$ rbd ls rbd
03aa248c-0de5-45e5-9c2b-5fe26b230275
97ee910f-8427-4b58-9b6a-4ed3df7049d0
a-0124fc5c-02e2-40b8-a05f-101da2f7d9a8
a-289320f4-ae8e-49e6-89f7-deec439a3831
a-44750fdc-6202-4b45-a780-8eea75ce59e4
a-4f950dfc-49ac-456a-ad35-0ba32b8434fe
a-628571e1-2410-4b6b-a77e-cee0f75b205d
a-baad3cd0-ea33-4e83-9a7a-6c8f2771cdda
```

## 2.2 查看pool里镜像的信息
```
$ rbd info -p rbd --image test-ui-2-2d0cffe7-31ab-4170-b2df-35bbaf46c0ed
rbd image 'test-ui-2-2d0cffe7-31ab-4170-b2df-35bbaf46c0ed':
    size 1024 MB in 256 objects
    order 22 (4096 kB objects)
    block_name_prefix: rbd_data.1c2e01d173297
    format: 2
    features: layering
    flags:
    create_timestamp: Tue Nov 21 13:44:41 2017
```

## 2.3 为pool创建镜像
```
$ rbd create -p rbd --size 1000 lihang
 
$ rbd -p rbd info lihang
rbd image 'lihang':
    size 1000 MB in 250 objects
    order 22 (4096 kB objects)
    block_name_prefix: rbd_data.1eaf374b0dc51
    format: 2
    features: layering, exclusive-lock, object-map, fast-diff, deep-flatten
    flags:
    create_timestamp: Thu Nov 23 19:32:34 2017
```

## 2.4 删除pool里镜像
```
$ rbd rm -p rbd lihang
Removing image: 100% complete...done.
```

## 2.5 调整pool里镜像的尺寸
```
$ rbd resize -p rbd --size 20000 lihang
Resizing image: 100% complete...done.
```

## 2.6 为pool里镜像创建快照
```
$ rbd snap create rbd/lihang@test
 
$ rbd snap ls -p rbd lihang
SNAPID NAME     SIZE TIMESTAMP
    37 test 20000 MB Thu Nov 23 19:36:27 2017
```

# 3. 参数梳理
## 3.1 参数介绍
