
#ifndef __MESSENGER_H
#define __MESSENGER_H

#include <list>
using namespace std;

#include "Message.h"


class Messenger {
 protected:
  list<Message*> incoming;
  list<Message*> outgoing;

 public:
  Messenger() {}
  ~Messenger() {}

  // ...
  bool send_message(Message *m);


  int poll() {
	while (1) {
	  if (wait_message(0) < 0)
		continue;
	  Message *m = get_message();
	  if (!m) continue;
	  dispatch_message(m);
	}
  }


  // queue
  int wait_message(time_t seconds) {
	//	return 0;   // got one!
	//return -1;  // timeout
  }

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



  // asdf
  void dispatch_message(Message *m);

};

#endif
