
#ifndef __FAKEMESSENGER_H
#define __FAKEMESSENGER_H

#include "Messenger.h"
#include "Dispatcher.h"

#include <list>
#include <map>

class FakeMessenger : public Messenger {
 protected:
  int whoami;

  class Logger *logger;

 public:
  FakeMessenger(long me);
  ~FakeMessenger();

  virtual int shutdown();

  // msg interface
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0);
  
  // use CheesySerializer for now!
  virtual Message* sendrecv(Message *m, msg_addr_t dest, int port=0) { assert(0); };
};

int fakemessenger_do_loop();
void fakemessenger_startthread();
void fakemessenger_stopthread();



#endif
