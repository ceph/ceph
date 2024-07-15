## 1. 故障现场
- 通过监控发现集群状态是HEALTH_ERR状态， 并且发现mds0: Metadata damage detected。 顾名思义，猜测应该是元信息损坏导致的。
![image.png](https://upload-images.jianshu.io/upload_images/2099201-30abf6d47b9cfadf.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 2. 分析damage是啥原因导致
![image.png](https://upload-images.jianshu.io/upload_images/2099201-86a82c56e96cb3cb.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**大概意思是：** 
- 从元数据存储池读取时，遇到了元数据损坏或丢失的情况。这条消息表明损坏之处已经被妥善隔离了，以使 MDS 继续运作，如此一来，若有客户端访问损坏的子树就返回 IO 错误。关于损坏的细节信息可用 damage ls 管理套接字命令获取。只要一遇到受损元数据，此消息就会立即出现。

## 3. 查看damage ls 
- 通过指令查询到damage ls 显示的信息，可以发现里面有个ino编号。
![image.png](https://upload-images.jianshu.io/upload_images/2099201-ffb6abd72a8b3181.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


## 4. 通过转换拿到十六进制ino
- 通过ino:1099519182934  ->  ino: 10000734856
![image.png](https://upload-images.jianshu.io/upload_images/2099201-25595295f80e9e4c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 5. 检查是否属于目录(10000734856)
- 通过指令查找发现该ino确定是目录
![image.png](https://upload-images.jianshu.io/upload_images/2099201-0090e8f112742e33.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 6. 确定目录名
![image.png](https://upload-images.jianshu.io/upload_images/2099201-1069c5b1987217ff.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 7. 该目录下面的所有文件
![image.png](https://upload-images.jianshu.io/upload_images/2099201-88ee36a3547395a6.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 8. 查看fs挂载的目录是否匹配
```
ceph fs ls -f json-pretty
```

## 9. 修复这个目录元信息
```
ceph --admin-daemon /var/run/ceph/ceph-mds.00.asok  scrub_path /dir repair
```
![image.png](https://upload-images.jianshu.io/upload_images/2099201-15d4a5a2d776dabb.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 10. 跟踪代码
**参考文件：**
- [https://github.com/ceph/ceph/blob/5cdf9c3380098f5d2b1d988ab623c74baad55ee3/src/mds/MDSRank.cc#L2245](https://github.com/ceph/ceph/blob/5cdf9c3380098f5d2b1d988ab623c74baad55ee3/src/mds/MDSRank.cc#L2245)
- [https://github.com/ceph/ceph/blob/5cdf9c3380098f5d2b1d988ab623c74baad55ee3/src/mds/MDCache.cc#L12197](https://github.com/ceph/ceph/blob/5cdf9c3380098f5d2b1d988ab623c74baad55ee3/src/mds/MDCache.cc#L12197)

![image.png](https://upload-images.jianshu.io/upload_images/2099201-6376697d8248b70f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
![image.png](https://upload-images.jianshu.io/upload_images/2099201-61580ab6b2892b1c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


## 11. 总结
### 11.1 问题过程回顾
- 集群ERR
- 发现mds0: Metadata damage detected
- 查看damage ino
- 根据ino定位跟踪目录
- 根据目录名知道业务存储的数据
- 修复问题

## 12. 修复方案
### 12.1方案一：删除ino对应的目录（生产环境实战演练过)
1.业务方备份迁移数据
2.查看damage ls 
![image.png](https://upload-images.jianshu.io/upload_images/2099201-6f6a0cf39d94a7d9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
3.检查该ino确实没有对应的目录
![image.png](https://upload-images.jianshu.io/upload_images/2099201-b605084d103a66ea.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
4.删除damage rm信息
![image.png](https://upload-images.jianshu.io/upload_images/2099201-a2fe4d4e1a53501f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

5.检查集群状态(集群状态从ERR恢复到WARN)
![image.png](https://upload-images.jianshu.io/upload_images/2099201-e0d245d85e657396.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

### 12.2 方案二：修复该目录元信息
1.通过指令修复目录
```
ceph --admin-daemon /var/run/ceph/ceph-mds.ceph-newpublic-osd02.py.asok scrub_path /dir/xxx repair
```


