#ifndef THREADPOOL
#define THREADPOOL

#include <queue>
#include <pthread.h>
#include <common/Mutex.h>
#include <common/Cond.h>
#include <common/Semaphore.h>


// debug output
#include "config.h"
#define tpdout(x) if (x <= g_conf.debug) cout << myname << " "
#define DBLVL 10


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
  string myname;

  static void *foo(void *arg)
  {
    ThreadPool *t = (ThreadPool *)arg;
    t->do_ops(arg);
	return 0;
  }

  void * do_ops(void *nothing)
  {
    T* op;
	
    tpdout(DBLVL) << "Thread "<< pthread_self() << " ready for action\n";
    while(1) {
      q_sem.Get();
      op = get_op();
	  
      if(op == NULL) {
		tpdout(DBLVL) << "Thread exiting\n";
		//pthread_exit(0);
		return 0;   // like this, i think!
      }
      //tpdout(DBLVL) << "Thread "<< pthread_self() << " calling the function\n";
      func(u, op);
    }
	return 0;
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

  ThreadPool(char *myname, int howmany, void (*f)(U*,T*), U *obj)
  {
    int status;

	this->myname = myname;
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
	// put null ops to make threads exit cleanly
    for(int i = 0; i < num_threads; i++) 
	  put_op(0);

	// wait for them to die
    for(int i = 0; i < num_threads; i++) {
      tpdout(DBLVL) << "Joining thread " << i << "\n";
	  void *rval = 0;  // we don't actually care
      pthread_join(thread[i], &rval);
    }
    delete[] thread;
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
