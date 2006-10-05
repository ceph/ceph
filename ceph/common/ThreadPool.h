// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef THREADPOOL
#define THREADPOOL

#include <list>
using namespace std;


#include <pthread.h>
#include <common/Mutex.h>
#include <common/Cond.h>
#include <common/Semaphore.h>


// debug output
#include "config.h"
#define tpdout(x) if (x <= g_conf.debug) cout << myname 
#define DBLVL 15


using namespace std;
 
#define MAX_THREADS 1000

template <class U, class T>
class ThreadPool {

 private:
  list<T> q;
  Mutex q_lock;
  Semaphore q_sem;

  int num_ops;
  int num_threads;
  vector<pthread_t> thread;

  U u;
  void (*func)(U,T);
  void (*prefunc)(U,T);
  string myname;

  static void *foo(void *arg)
  {
    ThreadPool *t = (ThreadPool *)arg;
    t->do_ops(arg);
    return 0;
  }

  void *do_ops(void *nothing)
  {
    tpdout(DBLVL) << ".do_ops thread " << pthread_self() << " starting" << endl;
    while (1) {
      q_sem.Get();
      if (q.empty()) break;

      T op = get_op();
      tpdout(DBLVL) << ".func thread "<< pthread_self() << " on " << op << endl;
      func(u, op);
    }
    tpdout(DBLVL) << ".do_ops thread " << pthread_self() << " exiting" << endl;
    return 0;
  }


  T get_op()
  {
    T op;
    q_lock.Lock();
    {
      op = q.front();
      q.pop_front();
      num_ops--;
      
      if (prefunc && op) {
        tpdout(DBLVL) << ".prefunc thread "<< pthread_self() << " on " << op << endl;
        prefunc(u, op);
      }
    }
    q_lock.Unlock();

    return op;
  }

 public:

  ThreadPool(char *myname, int howmany, void (*f)(U,T), U obj, void (*pf)(U,T) = 0) :
    num_ops(0), num_threads(howmany), 
    thread(num_threads),
    u(obj),
    func(f), prefunc(pf), 
    myname(myname) {
    tpdout(DBLVL) << ".cons num_threads " << num_threads << endl;
    
    // start threads
    int status;
    for(int i = 0; i < howmany; i++) {
      status = pthread_create(&thread[i], NULL, (void*(*)(void *))&ThreadPool::foo, this);
      assert(status == 0);
    }
  }
  
  ~ThreadPool() {
    // bump sem to make threads exit cleanly
    for(int i = 0; i < num_threads; i++) 
      q_sem.Put();
    
    // wait for them to die
    for(int i = 0; i < num_threads; i++) {
      tpdout(DBLVL) << ".des joining thread " << thread[i] << endl;
      void *rval = 0;  // we don't actually care
      pthread_join(thread[i], &rval);
    }
  }
  
  void put_op(T op) {
    tpdout(DBLVL) << ".put_op " << op << endl;
    q_lock.Lock();
    q.push_back(op);
    num_ops++;
    q_sem.Put();
    q_lock.Unlock();
  }

};
#endif
