#define MAX_THREADS 1000

class ThreadPool {
  queue<MOSDOp *> op_queue;
  Mutex queue_lock;
  pthread_t thread[MAX_THREADS];
  int num_ops;
  int num_threads;

  ThreadPool::Threadpool(int howmany);

  ThreadPool::~Threadpool();

  void put_op(MOSDOp *op);

  void do_ops(void *whoami);

  MOSDOp *get_op();
}
