
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

  virtual int init(Dispatcher *dis);
  virtual int shutdown();
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0);
  virtual int wait_message(time_t seconds);

  virtual int loop();

};


int fakemessenger_do_loop();

#endif
