## 一、简介
目前官方提供了ceph-immmutable-object-cache守护进程负责将内容缓存到本地缓存目录上。为了获得更好的性能，建议使用SSD作为底层存储介质。

## 二、IO流程
![image.png](https://upload-images.jianshu.io/upload_images/2099201-a655d7c52caa016a.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

1. ceph-immmutable-object-cache守护进程启动进行初始化，并且时监听本地域套接字，并等待来自librbd客户端的连接。

2. 客户端librbd通过域套接字来连接缓存守护进程， 并且向缓存守护进程进行注册。

3. 客户端librbd读取时请求到缓存守护进程进行查找。如果未查找到，守护进程会直接读取RADOS对象，然后写入到本地缓存目录.否则，找到更新LRU移动到头部。

4. 如果返回告诉客户端未缓存， 则客户端librbd直接从rados中获取信息。(下次librbd则直接从本地获取)

![image.png](https://upload-images.jianshu.io/upload_images/2099201-7e49d98cace7ba83.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 三、策略
1.  MAP信息维护filename和Entry信息。
![image.png](https://upload-images.jianshu.io/upload_images/2099201-543614b432b42aeb.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

2.  LRU链表维护Entry信息，保证容量达到阈值剔除缓存。
![image.png](https://upload-images.jianshu.io/upload_images/2099201-aa1f8a52be314bc1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

3.  Entry信息，维护最大容量、容量水位、最大ops。
![image.png](https://upload-images.jianshu.io/upload_images/2099201-acdd7969d3dc9bce.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 四、 存储格式
filename: pool_nspace + ":" + std::to_string(pool_id) + ":" +  std::to_string(snap_id) + ":" + oid

cache_file_dir:  ceph_crc32c(0, file_name, length)  % 100



key:  m_cache_root_dir + cache_file_dir + cache_file_name

val：object_name





