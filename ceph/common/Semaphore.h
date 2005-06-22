#ifndef _Sem_Posix_
#define _Sem_Posix_

#include <cassert>

class Semaphore
{
  Mutex m;
  Cond c;
  int count;

  public:

  Semaphore()
  {
    count = 0;
  }

  void Put()  { 
    m.Lock();
    count++;
    c.Signal();
    m.Unlock();
  }

  void Get() 
  { 
    m.Lock();
    while(count <= 0) {
      C.Wait(m);
    }
    count--;
    m.Unlock();
  }
};

#endif // !_Mutex_Posix_
