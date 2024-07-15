# 1.bucket index背景简介
bucket index是整个RGW里面一个非常关键的数据结构，用于存储bucket的索引数据，默认情况下单个bucket的index全部存储在一个shard文件（shard数量为0，主要以OMAP-keys方式存储在leveldb中），随着单个bucket内的Object数量增加，整个shard文件的体积也在不断增长，当shard文件体积过大就会引发各种问题。

# 2. 问题及故障
## 2.1 故障现象描述
1. Flapping OSD's when RGW buckets have millions of objects
2. ● Possible causes
3. ○ The first issue here is when RGW buckets have millions of objects their
4. bucket index shard RADOS objects become very large with high
5. number OMAP keys stored in leveldb. Then operations like deep-scrub,
6. bucket index listing etc takes a lot of time to complete and this triggers
7. OSD's to flap. If sharding is not used this issue become worse because
8. then only one RADOS index objects will be holding all the OMAP keys.

RGW的index数据以omap形式存储在OSD所在节点的leveldb中，当单个bucket存储的Object数量高达百万数量级的时候，
deep-scrub和bucket list一类的操作将极大的消耗磁盘资源，导致对应OSD出现异常，
如果不对bucket的index进行shard切片操作(shard切片实现了将单个bucket index的LevelDB实例水平切分到多个OSD上)，数据量大了以后很容易出事。 

1. ○ The second issue is when you have good amount of DELETEs it causes
2. loads of stale data in OMAP and this triggers leveldb compaction all the
3. time which is single threaded and non optimal with this kind of workload
4. and causes osd_op_threads to suicide because it is always compacting
5. hence OSD’s starts flapping.

RGW在处理大量DELETE请求的时候，会导致底层LevelDB频繁进行数据库compaction(数据压缩，对磁盘性能损耗很大)操作，而且刚好整个compaction在LevelDB中又是单线程处理，很容易到达osdopthreads超时上限而导致OSD自杀。

**常见的问题有:**
 1. 对index pool进行scrub或deep-scrub的时候，如果shard对应的Object过大，会极大消耗底层存储设备性能，造成io请求超时。
 2. 底层deep-scrub的时候耗时过长，会出现request blocked，导致大量http请求超时而出现50x错误，从而影响到整个RGW服务的可用性。
 3. 当坏盘或者osd故障需要恢复数据的时候，恢复一个大体积的shard文件将耗尽存储节点性能，甚至可能因为OSD响应超时而导致整个集群出现雪崩。

## 2.2 根因跟踪
当bucket index所在的OSD omap过大的时候，一旦出现异常导致OSD进程崩溃，这个时候就需要进行现场"救火"，用最快的速度恢复OSD服务。
先确定对应OSD的OMAP大小，这个过大会导致OSD启动的时候消耗大量时间和资源去加载levelDB数据，导致OSD无法启动（超时自杀）。
特别是这一类OSD启动需要占用非常大的内存消耗，一定要注意预留好内存。（物理内存40G左右，不行用swap顶上）
![image.png](https://upload-images.jianshu.io/upload_images/2099201-1d0af9180a9fc1e9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

# 3. 临时解决方案
## 3.1 关闭集群scrub, deep-scrub提升集群稳定性
```shell
$ ceph osd set noscrub
$ ceph osd set nodeep-scrub
```
## 3.2 调高timeout参数，减少OSD自杀的概率
```shell
osd_op_thread_timeout = 90 #default is 15
osd_op_thread_suicide_timeout = 2000 #default is 150
If filestore op threads are hitting timeout
filestore_op_thread_timeout = 180 #default is 60
filestore_op_thread_suicide_timeout = 2000 #default is 180
Same can be done for recovery thread also.
osd_recovery_thread_timeout = 120 #default is 30
osd_recovery_thread_suicide_timeout = 2000
```

## 3.2 手工压缩OMAP
在可以停OSD的情况下，可以对OSD进行compact操作，推荐在ceph 0.94.6以上版本，低于这个版本有bug。 [https://github.com/ceph/ceph/pull/7645/files](https://github.com/ceph/ceph/pull/7645/files)

 1. ○ The third temporary step could be taken if OSD's have very large OMAP
 2. directories you can verify it with command: du -sh /var/lib/ceph/osd/ceph-$id/current/omap, then do manual leveldb compaction for OSD's.
 3. ■ ceph tell osd.$id compact or
 4. ■ ceph daemon osd.$id compact or
 5. ■ Add leveldb_compact_on_mount = true in [osd.$id] or [osd] section
 6. and restart the OSD.
 7. ■ This makes sure that it compacts the leveldb and then bring the
 8. OSD back up/in which really helps.

```shell
#开启noout操作
$ ceph osd set noout
  
#停OSD服务
$ systemctl stop ceph-osd@<osd-id>
  
#在ceph.conf中对应的[osd.id]加上下面配置
leveldb_compact_on_mount = true
  
#启动osd服务
$ systemctl start ceph-osd@<osd-id>
  
  
#使用ceph -s命令观察结果，最好同时使用tailf命令去观察对应的OSD日志.等所有pg处于active+clean之后再继续下面的操作
$ ceph -s
#确认compact完成以后的omap大小:
$ du -sh /var/lib/ceph/osd/ceph-$id/current/omap
  
#删除osd中临时添加的leveldb_compact_on_mount配置
  
#取消noout操作(视情况而定，建议线上还是保留noout):
$ ceph osd unset noout
```

# 4. 永久解决方案
## 4.1 提前规划好bucket shard
 - index pool一定要上SSD，这个是本文优化的前提，没硬件支撑后面这些操作都是白搭。
 - 合理设置bucket 的shard 数量
shard的数量并不是越多越好，过多的shard会导致部分类似list bucket的操作消耗大量底层存储IO，导致部分请求耗时过长。
shard的数量还要考虑到你OSD的故障隔离域和副本数设置。比如你设置index pool的size为2，并且有2个机柜，共24个OSD节点，理想情况下每个shard的2个副本都应该分布在2个机柜里面，比如当你shard设置为8的时候，总共有8*2=16个shard文件需要存储，那么这16个shard要做到均分到2个机柜。同时如果你shard超过24个，这很明显也是不合适的。
 - 控制好单个bucket index shard的平均体积，目前推荐单个shard存储的Object信息条目在10-15W左右，过多则需要对相应的bucket做单独reshard操作（注意这个是高危操作，谨慎使用）。比如你预计单个bucket最多存储100W个Object，那么100W/8＝12.5W，设置shard数为8是比较合理的。shard文件中每条omapkey记录大概占用200 byte的容量，那么150000*200/1024/1024 ≈ 28.61 MB，也就是说要控制单个shard文件的体积在28MB以内。

 - 业务层面控制好每个bucket的Object上限，按每个shard文件平均10-15W Object为宜。

### 4.1.1 配置Bucket Index Sharding

To enable and configure bucket index sharding on all new buckets, use:  [redhat-bucket_sharding](https://access.redhat.com/documentation/en-us/red_hat_ceph_storage/1.3/html/object_gateway_guide_for_red_hat_enterprise_linux/administration_cli#bucket_sharding)
*   the `rgw_override_bucket_index_max_shards` setting for simple configurations,
*   the `bucket_index_max_shards` setting for federated configurations

**Simple configurations：**
```
#1. 修改配置文件设置相应的参数。 Note that maximum number of shards is 7877.
[global]
rgw_override_bucket_index_max_shards = 10
#2. 重启rgw服务，让其生效
systemctl restart ceph-radosgw.target
 
#3. 查看bucket shard数
rados -p default.rgw.buckets.index ls | wc -l
1000
```
**Federated configurations**
In federated configurations, each zone can have a different index_pool setting to manage failover. To configure a consistent shard count for zones in one region, set the bucket_index_max_shards setting in the configuration for that region. To do so:

```
#1. Extract the region configuration to the region.json file:
$ radosgw-admin region get > region.json
 
#2. In the region.json file, set the bucket_index_max_shards setting for each named zone.
 
#3. Reset the region:
$ radosgw-admin region set < region.json
 
#4. Update the region map:
$ radosgw-admin regionmap update --name <name>
 
#5. Replace <name> with the name of the Ceph Object Gateway user, for example:
$ radosgw-admin regionmap update --name client.rgw.ceph-client
```

**上传文件Demo:**
```python
#_*_coding:utf-8_*_
#yum install python-boto
import boto
import boto.s3.connection
#pip install filechunkio
from filechunkio import  FileChunkIO
import math
import  threading
import os
import Queue
class Chunk(object):
    num = 0
    offset = 0
    len = 0
    def __init__(self,n,o,l):
        self.num=n
        self.offset=o
        self.length=l
class CONNECTION(object):
    def __init__(self,access_key,secret_key,ip,port,is_secure=False,chrunksize=8<<20): #chunksize最小8M否则上传过程会报错
        self.conn=boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host=ip,port=port,
        is_secure=is_secure,
        calling_format=boto.s3.connection.OrdinaryCallingFormat()
        )
        self.chrunksize=chrunksize
        self.port=port
    #查询
    def list_all(self):
        all_buckets=self.conn.get_all_buckets()
        for bucket in all_buckets:
            print u'容器名: %s' %(bucket.name)
            for key in bucket.list():
                print ' '*5,"%-20s%-20s%-20s%-40s%-20s" %(key.mode,key.owner.id,key.size,key.last_modified.split('.')[0],key.name)
    def list_single(self,bucket_name):
        try:
            single_bucket = self.conn.get_bucket(bucket_name)
        except Exception as e:
            print 'bucket %s is not exist' %bucket_name
            return
        print u'容器名: %s' % (single_bucket.name)
        for key in single_bucket.list():
            print ' ' * 5, "%-20s%-20s%-20s%-40s%-20s" % (key.mode, key.owner.id, key.size, key.last_modified.split('.')[0], key.name)
    #普通小文件下载：文件大小<=8M
    def dowload_file(self,filepath,key_name,bucket_name):
        all_bucket_name_list = [i.name for i in self.conn.get_all_buckets()]
        if bucket_name not in all_bucket_name_list:
            print 'Bucket %s is not exist,please try again' % (bucket_name)
            return
        else:
            bucket = self.conn.get_bucket(bucket_name)
        all_key_name_list = [i.name for i in bucket.get_all_keys()]
        if key_name not in all_key_name_list:
            print 'File %s is not exist,please try again' % (key_name)
            return
        else:
            key = bucket.get_key(key_name)
        if not os.path.exists(os.path.dirname(filepath)):
            print 'Filepath %s is not exists, sure to create and try again' % (filepath)
            return
        if os.path.exists(filepath):
            while True:
                d_tag = raw_input('File %s already exists, sure you want to cover (Y/N)?' % (key_name)).strip()
                if d_tag not in ['Y', 'N'] or len(d_tag) == 0:
                    continue
                elif d_tag == 'Y':
                    os.remove(filepath)
                    break
                elif d_tag == 'N':
                    return
        os.mknod(filepath)
        try:
            key.get_contents_to_filename(filepath)
        except Exception:
            pass
    # 普通小文件上传：文件大小<=8M
    def upload_file(self,filepath,key_name,bucket_name):
        try:
            bucket = self.conn.get_bucket(bucket_name)
        except Exception as e:
            print 'bucket %s is not exist' % bucket_name
            tag = raw_input('Do you want to create the bucket %s: (Y/N)?' % bucket_name).strip()
            while tag not in ['Y', 'N']:
                tag = raw_input('Please input (Y/N)').strip()
            if tag == 'N':
                return
            elif tag == 'Y':
                self.conn.create_bucket(bucket_name)
                bucket = self.conn.get_bucket(bucket_name)
        all_key_name_list = [i.name for i in bucket.get_all_keys()]
        if key_name in all_key_name_list:
            while True:
                f_tag = raw_input(u'File already exists, sure you want to cover (Y/N)?: ').strip()
                if f_tag not in ['Y', 'N'] or len(f_tag) == 0:
                    continue
                elif f_tag == 'Y':
                    break
                elif f_tag == 'N':
                    return
        key=bucket.new_key(key_name)
        if not os.path.exists(filepath):
            print 'File %s does not exist, please make sure you want to upload file path and try again' %(key_name)
            return
        try:
            f=file(filepath,'rb')
            data=f.read()
            key.set_contents_from_string(data)
        except Exception:
            pass
    def delete_file(self,key_name,bucket_name):
        all_bucket_name_list = [i.name for i in self.conn.get_all_buckets()]
        if bucket_name not in all_bucket_name_list:
            print 'Bucket %s is not exist,please try again' % (bucket_name)
            return
        else:
            bucket = self.conn.get_bucket(bucket_name)
        all_key_name_list = [i.name for i in bucket.get_all_keys()]
        if key_name not in all_key_name_list:
            print 'File %s is not exist,please try again' % (key_name)
            return
        else:
            key = bucket.get_key(key_name)
        try:
            bucket.delete_key(key.name)
        except Exception:
            pass
    def delete_bucket(self,bucket_name):
        all_bucket_name_list = [i.name for i in self.conn.get_all_buckets()]
        if bucket_name not in all_bucket_name_list:
            print 'Bucket %s is not exist,please try again' % (bucket_name)
            return
        else:
            bucket = self.conn.get_bucket(bucket_name)
        try:
            self.conn.delete_bucket(bucket.name)
        except Exception:
            pass
 
    #队列生成
    def init_queue(self,filesize,chunksize):   #8<<20 :8*2**20
        chunkcnt=int(math.ceil(filesize*1.0/chunksize))
        q=Queue.Queue(maxsize=chunkcnt)
        for i in range(0,chunkcnt):
            offset=chunksize*i
            length=min(chunksize,filesize-offset)
            c=Chunk(i+1,offset,length)
            q.put(c)
        return q
    #分片上传object
    def upload_trunk(self,filepath,mp,q,id):
        while not q.empty():
            chunk=q.get()
            fp=FileChunkIO(filepath,'r',offset=chunk.offset,bytes=chunk.length)
            mp.upload_part_from_file(fp,part_num=chunk.num)
            fp.close()
            q.task_done()
    #文件大小获取---->S3分片上传对象生成----->初始队列生成(--------------->文件切，生成切分对象)
    def upload_file_multipart(self,filepath,key_name,bucket_name,threadcnt=8):
        filesize=os.stat(filepath).st_size
        try:
            bucket=self.conn.get_bucket(bucket_name)
        except Exception as e:
            print 'bucket %s is not exist' % bucket_name
            tag=raw_input('Do you want to create the bucket %s: (Y/N)?' %bucket_name).strip()
            while tag not in ['Y','N']:
                tag=raw_input('Please input (Y/N)').strip()
            if tag == 'N':
                return
            elif tag == 'Y':
                self.conn.create_bucket(bucket_name)
                bucket = self.conn.get_bucket(bucket_name)
        all_key_name_list=[i.name for i in bucket.get_all_keys()]
        if key_name  in all_key_name_list:
            while True:
                f_tag=raw_input(u'File already exists, sure you want to cover (Y/N)?: ').strip()
                if f_tag not in ['Y','N'] or len(f_tag) == 0:
                    continue
                elif f_tag == 'Y':
                    break
                elif f_tag == 'N':
                    return
        mp=bucket.initiate_multipart_upload(key_name)
        q=self.init_queue(filesize,self.chrunksize)
        for i in range(0,threadcnt):
            t=threading.Thread(target=self.upload_trunk,args=(filepath,mp,q,i))
            t.setDaemon(True)
            t.start()
        q.join()
        mp.complete_upload()
    #文件分片下载
    def download_chrunk(self,filepath,key_name,bucket_name,q,id):
        while not q.empty():
            chrunk=q.get()
            offset=chrunk.offset
            length=chrunk.length
            bucket=self.conn.get_bucket(bucket_name)
            resp=bucket.connection.make_request('GET',bucket_name,key_name,headers={'Range':"bytes=%d-%d" %(offset,offset+length)})
            data=resp.read(length)
            fp=FileChunkIO(filepath,'r+',offset=chrunk.offset,bytes=chrunk.length)
            fp.write(data)
            fp.close()
            q.task_done()
    def download_file_multipart(self,filepath,key_name,bucket_name,threadcnt=8):
        all_bucket_name_list=[i.name for i in self.conn.get_all_buckets()]
        if bucket_name not in all_bucket_name_list:
            print 'Bucket %s is not exist,please try again' %(bucket_name)
            return
        else:
            bucket=self.conn.get_bucket(bucket_name)
        all_key_name_list = [i.name for i in bucket.get_all_keys()]
        if key_name not in all_key_name_list:
            print 'File %s is not exist,please try again' %(key_name)
            return
        else:
            key=bucket.get_key(key_name)
        if not os.path.exists(os.path.dirname(filepath)):
            print 'Filepath %s is not exists, sure to create and try again' % (filepath)
            return
        if os.path.exists(filepath):
            while True:
                d_tag = raw_input('File %s already exists, sure you want to cover (Y/N)?' % (key_name)).strip()
                if d_tag not in ['Y', 'N'] or len(d_tag) == 0:
                    continue
                elif d_tag == 'Y':
                    os.remove(filepath)
                    break
                elif d_tag == 'N':
                    return
        os.mknod(filepath)
        filesize=key.size
        q=self.init_queue(filesize,self.chrunksize)
        for i in range(0,threadcnt):
            t=threading.Thread(target=self.download_chrunk,args=(filepath,key_name,bucket_name,q,i))
            t.setDaemon(True)
            t.start()
        q.join()
    def generate_object_download_urls(self,key_name,bucket_name,valid_time=0):
        all_bucket_name_list = [i.name for i in self.conn.get_all_buckets()]
        if bucket_name not in all_bucket_name_list:
            print 'Bucket %s is not exist,please try again' % (bucket_name)
            return
        else:
            bucket = self.conn.get_bucket(bucket_name)
        all_key_name_list = [i.name for i in bucket.get_all_keys()]
        if key_name not in all_key_name_list:
            print 'File %s is not exist,please try again' % (key_name)
            return
        else:
            key = bucket.get_key(key_name)
        try:
            key.set_canned_acl('public-read')
            download_url = key.generate_url(valid_time, query_auth=False, force_http=True)
            if self.port != 80:
                x1=download_url.split('/')[0:3]
                x2=download_url.split('/')[3:]
                s1=u'/'.join(x1)
                s2=u'/'.join(x2)
                s3=':%s/' %(str(self.port))
                download_url=s1+s3+s2
                print download_url
        except Exception:
            pass
if __name__ == '__main__':
    #约定：
    #1:filepath指本地文件的路径(上传路径or下载路径),指的是绝对路径
    #2:bucket_name相当于文件在对象存储中的目录名或者索引名
    #3:key_name相当于文件在对象存储中对应的文件名或文件索引
    access_key = "FYT71CYU3UQKVMC8YYVY"
    secret_key = "rVEASbWAytjVLv1G8Ta8060lY3yrcdPTsEL0rfwr"
    ip='127.0.0.1'
    port=7480
    conn=CONNECTION(access_key,secret_key,ip,port)
    #查看所有bucket以及其包含的文件
    #conn.list_all()
    #简单上传,用于文件大小<=8M
    #conn.upload_file('/etc/passwd','passwd','test_bucket01')
    conn.upload_file('/tmp/test.log','test1','test_bucket12')
    #查看单一bucket下所包含的文件信息
    conn.list_single('test_bucket12')
 
    #简单下载,用于文件大小<=8M
    # conn.dowload_file('/lhf_test/test01','passwd','test_bucket01')
    # conn.list_single('test_bucket01')
    #删除文件
    # conn.delete_file('passwd','test_bucket01')
    # conn.list_single('test_bucket01')
    #
    #删除bucket
    # conn.delete_bucket('test_bucket01')
    # conn.list_all()
    #切片上传(多线程),用于文件大小>8M,8M可修改，但不能小于8M,否则会报错切片太小
    # conn.upload_file_multipart('/etc/passwd','passwd_multi_upload','test_bucket01')
    # conn.list_single('test_bucket01')
    # 切片下载(多线程),用于文件大小>8M,8M可修改，但不能小于8M，否则会报错切片太小
    # conn.download_file_multipart('/lhf_test/passwd_multi_dowload','passwd_multi_upload','test_bucket01')
    #生成下载url
    #conn.generate_object_download_urls('passwd_multi_upload','test_bucket01')
    #conn.list_all()
```

## 4.2 对bucket做reshard操作

To reshard the bucket index pool: [redhat-bucket_sharding](https://access.redhat.com/documentation/en-us/red_hat_ceph_storage/1.3/html/object_gateway_guide_for_red_hat_enterprise_linux/administration_cli#bucket_sharding)

```
#注意下面的操作一定要确保对应的bucket相关的操作都已经全部停止，之后使用下面命令备份bucket的index
$ radosgw-admin bi list --bucket=<bucket_name> > <bucket_name>.list.backup
 
#通过下面的命令恢复数据
$ radosgw-admin bi put --bucket=<bucket_name> < <bucket_name>.list.backup
  
#查看bucket的index id
$ radosgw-admin bucket stats --bucket=bucket-maillist
{
    "bucket": "bucket-maillist",
    "pool": "default.rgw.buckets.data",
    "index_pool": "default.rgw.buckets.index",
    "id": "0a6967a5-2c76-427a-99c6-8a788ca25034.54133.1", #注意这个id
    "marker": "0a6967a5-2c76-427a-99c6-8a788ca25034.54133.1",
    "owner": "user",
    "ver": "0#1,1#1",
    "master_ver": "0#0,1#0",
    "mtime": "2017-08-23 13:42:59.007081",
    "max_marker": "0#,1#",
    "usage": {},
    "bucket_quota": {
        "enabled": false,
        "max_size_kb": -1,
        "max_objects": -1
    }
}
  
  
#Reshard对应bucket的index操作如下:
#使用命令将"bucket-maillist"的shard调整为4，注意命令会输出osd和new两个bucket的instance id
  
$ radosgw-admin bucket reshard --bucket="bucket-maillist" --num-shards=4
*** NOTICE: operation will not remove old bucket index objects ***
***         these will need to be removed manually             ***
old bucket instance id: 0a6967a5-2c76-427a-99c6-8a788ca25034.54133.1
new bucket instance id: 0a6967a5-2c76-427a-99c6-8a788ca25034.54147.1
total entries: 3
  
  
#之后使用下面的命令删除旧的instance id
  
$ radosgw-admin bi purge --bucket="bucket-maillist" --bucket-id=0a6967a5-2c76-427a-99c6-8a788ca25034.54133.1
  
#查看最终结果
$ radosgw-admin bucket stats --bucket=bucket-maillist
{
    "bucket": "bucket-maillist",
    "pool": "default.rgw.buckets.data",
    "index_pool": "default.rgw.buckets.index",
    "id": "0a6967a5-2c76-427a-99c6-8a788ca25034.54147.1", #id已经变更
    "marker": "0a6967a5-2c76-427a-99c6-8a788ca25034.54133.1",
    "owner": "user",
    "ver": "0#2,1#1,2#1,3#2",
    "master_ver": "0#0,1#0,2#0,3#0",
    "mtime": "2017-08-23 14:02:19.961205",
    "max_marker": "0#,1#,2#,3#",
    "usage": {
        "rgw.main": {
            "size_kb": 50,
            "size_kb_actual": 60,
            "num_objects": 3
        }
    },
    "bucket_quota": {
        "enabled": false,
        "max_size_kb": -1,
        "max_objects": -1
    }
}
```
