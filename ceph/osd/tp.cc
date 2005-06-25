
#include <iostream>
#include <string>

using namespace std;

#include "common/Mutex.h"
#include "common/ThreadPool.h"
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

void foop(class TP *t, class Op *o);

class TP {
public:

  void foo(Op *o)
  {
    cout << "Thread "<< pthread_self() << ": " << o->get() << "\n";
    usleep(1);
    
    //  sched_yield();
  }

  int main(int argc, char *argv)
  {
    ThreadPool<TP,Op> *t = new ThreadPool<TP,Op>(10, (void (*)(TP*, Op*))foop, this);
    
    for(int i = 0; i < 100; i++) {
      Op *o = new Op(i); 
      t->put_op(o);
    }
    
    sleep(1);
    
    delete(t);
    
    return 0;
  }
};

void foop(class TP *t, class Op *o) {
  t->foo(o);
}

int main(int argc, char *argv) {
  TP t;

  t.main(argc,argv);
}

