# 1.说明
## 1.1介绍
PG全称Placement Grouops，是一个逻辑的概念，一个PG包含多个OSD。引入PG这一层其实是为了更好的分配数据和定位数据。

# 2. 常用操作
## 2.1 查看pg组映射信息
```
$ ceph pg dump
```

## 2.2 查看一个PG的map
```
$ ceph pg map 1.2f6
osdmap e7768 pg 1.2f6 (1.2f6) -> up [6,14,25] acting [6,14,25]  
#其中[6,14,25]代表存储在osd.6、osd.14、osd.25节点，osd.6代表主副本存储的位置
```

## 2.3 查看PG状态
```
$ ceph pg stat
5416 pgs: 5416 active+clean; 471 GB data, 1915 GB used, 154 TB / 156 TB avail
```

## 2.4 查看pg详细信息
```
$ ceph pg 1.2f6 query
```

## 2.5 查看pg中stuck状态
```
$ ceph pg dump_stuck unclean
ok
 
$ ceph pg dump_stuck inactive
ok
 
$ ceph pg dump_stuck stale
ok
```

## 2.6 显示集群所有pg统计
```
$ ceph pg dump --format plain
```

## 2.7 恢复一个丢失的pg
```
$ ceph pg {pg-id} mark_unfound_lost revert
```

## 2.8 显示非正常状态的pg
```
$ ceph pg dump_stuck inactive|unclean|stale
```

# 3. 参数梳理
## 3.1 参数介绍




