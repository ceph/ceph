#include "include/types.h"

#include "OSD.h"
#include "FakeStore.h"
#include "OSDCluster.h"

#include "mds/MDS.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "msg/HostMonitor.h"

#include "messages/MGenericMessage.h"
#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDGetClusterAck.h"

#include "common/Logger.h"
#include "common/LogType.h"
#include "common/Mutex.h"

#include "OSD/ThreadPool.h"

#include <queue>

#include <iostream>
#include <cassert>
#include <errno.h>
#include <sys/stat.h>

void main(int argc, char *argv) {
  ThreadPool t(10);

}

ThreadPool::Threadpool(int howmany) {
  num_ops = 0;
  num_threads = 0;

  int status;

  num_threads = howmany;

  for(int i = 0; i < howmany; i++) {
    status = pthread_create(thread[i], NULL, do_ops, (void *)&i);
  }
}

ThreadPool::~Threadpool() {
  queue_lock.Lock();
  for(int i = num_ops; i > 0; i--) 
    get_op();

  for(int i = 0; i < num_threads; i++) {
    put_op((MOSDOp *)NULL);
  }

  for(int i = 0; i < num_threads; i++) {
    cout << "Waiting for thread " << i << " to die";
    pthread_join(threads[i]);
  }

  queue_lock.Unlock();
}

void do_ops(void *whoami) {
  MOSDOp *op;

  cout << "Thread " << (int)i << " ready for action\n";
  while(1) {
    op = get_op();

    if(op == NULL) {
      cout << "Thread " << (int)i << " dying";
      pthread_exit(0);
    }
      
    OSD.do_op(op);
  }
}

MOSDOp *get_op() {
  MOSDOp *op;
  queue_lock.Lock();
  op = op_queue.pop();
  num_ops--;
  queue_lock.Unlock();
}

void put_op(MOSDOp *op) {
  queue_lock.Lock();
  opqueue.push(op);
  num_ops++;
  queue_lock.Unlock();
}

