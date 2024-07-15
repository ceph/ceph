# 1.说明
## 1.1介绍
MDS全称Ceph Metadata Server，是CephFS服务依赖的元数据服务。



# 2. 常用操作
## 2.1 查看mds的状态
```
$ ceph mds stat
test_fs-1/1/1 up test1_fs-1/1/1 up  {[test1_fs:0]=ceph-xx-osd03.gz01=up:active,[test_fs:0]=ceph-xx-osd00=up:active}
```

## 2.2 查看mds的映射信息
```
$ ceph mds dump
dumped fsmap epoch 50
fs_name	test_fs
epoch	50
flags	4
created	2017-09-05 10:06:56.343105
modified	2017-09-05 10:06:56.343105
tableserver	0
root	0
session_timeout	60
session_autoclose	300
max_file_size	1099511627776
last_failure	0
last_failure_osd_epoch	4787
compat	compat={},rocompat={},incompat={1=base v0.20,2=client writeable ranges,3=default file layouts on dirs,4=dir inode in separate object,5=mds uses versioned encoding,6=dirfrag is stored in omap,8=file layout v2}
max_mds	1
in	0
up	{0=104262}
failed
damaged
stopped
data_pools	[2]
metadata_pool	3
inline_data	disabled
balancer
standby_count_wanted	1
104262:	100.0.0.34:6800/1897776151 'ceph-xx-osd00' mds.0.37 up:active seq 151200
```

## 2.3 删除mds节点
```
$ ceph mds rm 0 mds.ceph-xx-osd00
```

## 2.4 增加数据存储池
```
$ ceph mds add_data_pool <pool>
```

## 2.5 关闭mds集群
```
$ ceph mds cluster_down
marked fsmap DOWN
```

## 2.6 启动mds集群
```
$ ceph mds cluster_up
unmarked fsmap DOWN
```

## 2.7  可删除兼容功能
```
$ ceph mds compat rm_compat <int[0-]>
```

## 2.8 可删除不兼容的功能
```
$ ceph mds compat rm_incompat <int[0-]>
```

## 2.9 查看兼容性选项
```
$ ceph mds compat show
```

## 2.10 删除数据存储池
```
$ ceph mds remove_data_pool <pool>
```

## 2.11 停止指定mds
```
$ ceph mds stop <node1>
```

## 2.12 向某个mds发送命令
```
$ ceph mds tell <node> <args> [<args>...]
```

 

## 2.13 添加mds机器
```
#添加一个机器 new_host 到现有mds集群中 
su - ceph  -c "ceph-deploy --ceph-conf /etc/ceph/ceph.conf  mds create $new_host"
```

## 2.14 查看客户端session
```
ceph daemon mds.ceph-xx-mds01.gz01 session ls
```
