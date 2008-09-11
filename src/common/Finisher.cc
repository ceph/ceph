
#include "config.h"
#include "Finisher.h"

void Finisher::start()
{
  finisher_thread.create();
}

void Finisher::stop()
{
  finisher_lock.Lock();
  finisher_stop = true;
  finisher_cond.Signal();
  finisher_lock.Unlock();
  finisher_thread.join();
}

void *Finisher::finisher_thread_entry()
{
  finisher_lock.Lock();
  //dout_generic(10) << "finisher_thread start" << dendl;

  while (!finisher_stop) {
    while (!finisher_queue.empty()) {
      vector<Context*> ls;
      ls.swap(finisher_queue);

      finisher_lock.Unlock();

      finish_contexts(ls, 0);

      finisher_lock.Lock();
    }
    if (finisher_stop) break;
    
    //dout_generic(30) << "finisher_thread sleeping" << dendl;
    finisher_cond.Wait(finisher_lock);
  }

  //dout_generic(10) << "finisher_thread start" << dendl;
  finisher_lock.Unlock();
  return 0;
}

