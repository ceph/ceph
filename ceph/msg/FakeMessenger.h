
#ifndef __FAKEMESSENGER_H
#define __FAKEMESSENGER_H

#include "Messenger.h"

#include <list>
#include <map>

class FakeMessenger : public Messenger {
 protected:
  int whoami;
  
 public:
  FakeMessenger();
  
  virtual int init(MDS *m);
  virtual int shutdown();
  virtual bool send_message(Message *m, int dest);
  virtual int wait_message(time_t seconds);

  virtual int loop();


};


int fakemessenger_do_loop();

#endif
