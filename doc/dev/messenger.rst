============================
 Messenger notes
============================

Messenger is the Ceph network layer implementation. Currently Ceph supports
three messenger type "simple", "async" and "xio". The latter two are both
experiment features and shouldn't use them in production environment.

ceph_perf_msgr
==============

ceph_perf_msgr is used to do benchmark for messenger module only and can help
to find the bottleneck or time consuming within messenger moduleIt just like
"iperf", we need to start server-side program firstly:

# ./ceph_perf_msgr_server 172.16.30.181:10001 0

The first argument is ip:port pair which is telling the destination address the
client need to specified. The second argument tells the "think time" when
dispatching messages. After Giant, CEPH_OSD_OP message which is the actual client
read/write io request is fast dispatched without queueing to Dispatcher, in order
to achieve better performance. So CEPH_OSD_OP message will be processed inline,
"think time" is used by mock this "inline process" process.

# ./ceph_perf_msgr_client 172.16.30.181:10001 1 32 10000 10 4096 

The first argument is specified the server ip:port, and the second argument is
used to specify client threads. The third argument specify the concurrency(the
max inflight messages for each client thread), the fourth argument specify the
io numbers will be issued to server per client thread. The fifth argument is
used to indicate the "think time" for client thread when receiving messages,
this is also used to mock the client fast dispatch process. The last argument
specify the message data length to issue.
