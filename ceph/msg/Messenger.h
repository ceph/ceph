
#ifndef __MESSENGER_H
#define __MESSENGER_H

#include <list>
using namespace std;

#include "Message.h"
#include "Dispatcher.h"

class MDS;

class Messenger {
 protected:
  Dispatcher     *dispatcher;
  list<Message*> incoming;
  MDS            *mymds;

 public:
  Messenger() {
  }
  virtual ~Messenger() {
	remove_dispatcher();
  }
  
  void set_dispatcher(Dispatcher *d) {
	dispatcher = d;
  }
  Dispatcher *remove_dispatcher() {
	Dispatcher *t = dispatcher;
	dispatcher = 0;
	return t;
  }

  // ...
  virtual int init(Dispatcher *d) = 0;
  virtual int shutdown() = 0;
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;
  virtual int wait_message(time_t seconds) = 0;
  virtual void done() {}
  
  virtual int loop() {
	while (1) {
	  if (wait_message(0) < 0)
		break;
	  Message *m = get_message();
	  if (!m) continue;
	  dispatcher->dispatch(m);
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


  void dispatch(Message *m) {
	dispatcher->dispatch(m);
  }




};


extern Message *decode_message(crope& rope);



#endif
