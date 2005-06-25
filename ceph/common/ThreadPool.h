#ifndef THREADPOOL
#define THREADPOOL

#include <queue>
#include <pthread.h>
#include <common/Mutex.h>
#include <common/Cond.h>
#include<common/Semaphore.h>

using namespace std;
 
#define MAX_THREADS 1000

template <class U, class T>
class ThreadPool {

 private:
  queue<T *> q;
  Mutex q_lock;
  Semaphore q_sem;
  pthread_t *thread;
  int num_ops;
  int num_threads;
  void (*func)(U*,T*);
  U *u;

  static void *foo(void *arg)
  {
    ThreadPool *t = (ThreadPool *)arg;
    t->do_ops(arg);
  }

  void * do_ops(void *nothing)
  {
    T* op;

    cout << "Thread "<< pthread_self() << " ready for action\n";
    while(1) {
      q_sem.Get();
      op = get_op();

      if(op == NULL) {
	cout << "Thread exiting\n";
	pthread_exit(0);
      }
      cout << "Thread "<< pthread_self() << " calling the function\n";
      func(u, op);
    }
  }


  T* get_op()
  {
    T* op;

    q_lock.Lock();
    op = q.front();
    q.pop();
    num_ops--;
    q_lock.Unlock();

    return op;
  }

 public:

  ThreadPool(int howmany, void (*f)(U*,T*), U *obj)
  {
    int status;

    u = obj;
    num_ops = 0;
    func = f;
    num_threads = howmany;
    thread = new pthread_t[num_threads];

    for(int i = 0; i < howmany; i++) {
      status = pthread_create(&thread[i], NULL, (void*(*)(void *))&ThreadPool::foo, this);
    }
  }

  ~ThreadPool()
  {
    for(int i = 0; i < num_threads; i++) {
      cout << "Killing thread " << i << "\n";
      pthread_cancel(thread[i]);
    }
    delete thread;
  }

  void put_op(T* op)
  {
    q_lock.Lock();
    q.push(op);
    num_ops++;
    q_sem.Put();
    q_lock.Unlock();
  }

};
#endif
