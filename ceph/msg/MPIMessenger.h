#ifndef __MPIMESSENGER_H
#define __MPIMESSENGER_H

#include "Messenger.h"
#include "Dispatcher.h"

class MPIMessenger : public Messenger {
 protected:
  int whoami;

  class Logger *logger;
  
 public:
  MPIMessenger(long me);
  ~MPIMessenger();

  virtual int init(Dispatcher *dis);
  virtual int shutdown();
  virtual int send_message(Message *m, long dest, int port=0, int fromport=0);
  virtual int wait_message(time_t seconds);

  virtual int loop();
};

extern int mpimessenger_world();
extern int mpimessenger_init(int& argc, char**& argv);
extern int mpimessenger_loop();
extern int mpimessenger_shutdown();

#endif
