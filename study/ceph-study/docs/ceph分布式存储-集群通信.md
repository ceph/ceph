# 1. Ceph通信框架
## 1.1 Ceph通信框架种类介绍
**网络通信框架三种不同的实现方式：**
 - Simple线程模式
    特点：每一个网络链接，都会创建两个线程，一个用于接收，一个用于发送。
    缺点：大量的链接会产生大量的线程，会消耗CPU资源，影响性能。
 - Async事件的I/O多路复用模式
    特点：这种是目前网络通信中广泛采用的方式。k版默认已经使用Asnyc了。
 - XIO方式使用了开源的网络通信库accelio来实现
    特点：这种方式需要依赖第三方的库accelio稳定性，目前处于试验阶段。

## 1.2 Ceph通信框架设计模式
**设计模式(Subscribe/Publish)：**
订阅发布模式又名观察者模式，它意图是“定义对象间的一种一对多的依赖关系，
当一个对象的状态发生改变时，所有依赖于它的对象都得到通知并被自动更新”。

## 1.3 Ceph通信框架流程图
![ceph_message.png](https://upload-images.jianshu.io/upload_images/2099201-8662667e6a06e931.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**步骤：**
 - Accepter监听peer的请求, 调用 SimpleMessenger::add_accept_pipe() 创建新的 Pipe 到 SimpleMessenger::pipes 来处理该请求。
 - Pipe用于消息的读取和发送。该类主要有两个组件，Pipe::Reader，Pipe::Writer用来处理消息读取和发送。
 - Messenger作为消息的发布者, 各个 Dispatcher 子类作为消息的订阅者, Messenger 收到消息之后，  通过 Pipe 读取消息，然后转给 Dispatcher 处理。
 - Dispatcher是订阅者的基类，具体的订阅后端继承该类,初始化的时候通过 Messenger::add_dispatcher_tail/head 注册到 Messenger::dispatchers. 收到消息后，通知该类处理。
 - DispatchQueue该类用来缓存收到的消息, 然后唤醒 DispatchQueue::dispatch_thread 线程找到后端的 Dispatch 处理消息。

![ceph_message_2.png](https://upload-images.jianshu.io/upload_images/2099201-f7e6ef5c9d3fe38f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


## 1.4 Ceph通信框架类图
![ceph_message_3.png](https://upload-images.jianshu.io/upload_images/2099201-a7d2248cb9963f1d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 1.5 Ceph通信数据格式
通信协议格式需要双方约定数据格式。

**消息的内容主要分为三部分：**
 - header              //消息头，类型消息的信封
 - user data          //需要发送的实际数据
   - payload     //操作保存元数据
   - middle      //预留字段
   - data          //读写数据
 - footer             //消息的结束标记
```
class Message : public RefCountedObject {
protected:
  ceph_msg_header  header;      // 消息头
  ceph_msg_footer  footer;		// 消息尾
  bufferlist       payload;  // "front" unaligned blob
  bufferlist       middle;   // "middle" unaligned blob
  bufferlist       data;     // data payload (page-alignment will be preserved where possible)

  /* recv_stamp is set when the Messenger starts reading the
   * Message off the wire */
  utime_t recv_stamp;		//开始接收数据的时间戳
  /* dispatch_stamp is set when the Messenger starts calling dispatch() on
   * its endpoints */
  utime_t dispatch_stamp;	//dispatch 的时间戳
  /* throttle_stamp is the point at which we got throttle */
  utime_t throttle_stamp;	//获取throttle 的slot的时间戳
  /* time at which message was fully read */
  utime_t recv_complete_stamp;	//接收完成的时间戳

  ConnectionRef connection;		//网络连接

  uint32_t magic = 0;			//消息的魔术字

  bi::list_member_hook<> dispatch_q;	//boost::intrusive 成员字段
};

struct ceph_msg_header {
	__le64 seq;       // 当前session内 消息的唯一 序号
	__le64 tid;       // 消息的全局唯一的 id
	__le16 type;      // 消息类型
	__le16 priority;  // 优先级
	__le16 version;   // 版本号

	__le32 front_len; // payload 的长度
	__le32 middle_len;// middle 的长度
	__le32 data_len;  // data 的 长度
	__le16 data_off;  // 对象的数据偏移量


	struct ceph_entity_name src; //消息源

	/* oldest code we think can decode this.  unknown if zero. */
	__le16 compat_version;
	__le16 reserved;
	__le32 crc;       /* header crc32c */
} __attribute__ ((packed));

struct ceph_msg_footer {
	__le32 front_crc, middle_crc, data_crc; //crc校验码
	__le64  sig; //消息的64位signature
	__u8 flags; //结束标志
} __attribute__ ((packed));
```
