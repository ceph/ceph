
#include <iostream>
#include <string>
#include <stdlib.h>

using namespace std;

#include "common/Mutex.h"
#include "osd/ThreadPool.h"

class Op {
  int i;

public:
 
  Op(int i) {
    this->i = i;
  }

  int get() {
    return i;
  }
};

void foo(Op *o) {
  cout << "Thread "<< pthread_self() << ": " << o->get() << "\n";
  usleep(1);
}

int main(int argc, char *argv) {
  ThreadPool<Op> *t = new ThreadPool<Op>(10, foo);

  sleep(1);

  for(int i = 0; i < 100; i++) {
    Op *o = new Op(i); 
    t->put_op(o);
  }

  sleep(1);
  delete(t);

  return 0;
}

