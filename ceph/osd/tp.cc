
#include <iostream>
#include <string>

using namespace std;

#include "common/Mutex.h"
#include "osd/ThreadPool.h"
// #include <thread.h>

class Op {
  int i;

public:
 
  Op(int i)
  {
    this->i = i;
  }

  int get()
  {
    return i;
  }
};

void foo(Op *o)
{
  cout << "Thread "<< pthread_self() << ": " << o->get() << "\n";
  usleep(1);
  
  //  sched_yield();
}

int main(int argc, char *argv)
{
  ThreadPool<Op> *t = new ThreadPool<Op>(10, foo);

  for(int i = 0; i < 100; i++) {
    Op *o = new Op(i); 
    t->put_op(o);
  }

  sleep(1);

  delete(t);

  return 0;
}

