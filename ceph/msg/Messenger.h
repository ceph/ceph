
#ifndef __MESSENGER_H
#define __MESSENGER_H

#include <list>
using namespace std;

#include "Message.h"


class Messenger {
 protected:
  list<Message*> incoming;

 public:
  Messenger() {}
  ~Messenger() {}

  // ...
  virtual int init(int whoami) = 0;
  virtual int shutdown() = 0;
  virtual bool send_message(Message *m) = 0;
  virtual int wait_message(time_t seconds) = 0;
  
  virtual int loop() {
	while (1) {
	  if (wait_message(0) < 0)
		break;
	  Message *m = get_message();
	  if (!m) continue;
	  dispatch_message(m);
	}
	return 0;
  }


  // queue
  Message *get_message() {
	if (incoming.size() > 0) {
	  Message *m = incoming.front();
	  incoming.pop_front();
	  return m;
	}
	return NULL;
  }

  bool queue_incoming(Message *m) {
	incoming.push_back(m);
	return true;
  }
  int num_incoming() {
	return incoming.size();
  }



  // asdf
  void dispatch_message(Message *m);

};

#endif
