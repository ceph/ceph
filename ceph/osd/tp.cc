// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */


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

