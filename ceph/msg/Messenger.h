
#ifndef __MESSENGER_H
#define __MESSENGER_H

#include <list>
using namespace std;

#include "Message.h"
#include "Dispatcher.h"

class MDS;
class Timer;

class Messenger {
 protected:
  Dispatcher     *dispatcher;     // i deliver incoming messages here.
  list<Message*> incoming;        // incoming queue

 public:
  Messenger() : dispatcher(0) { }
  
  // administrative
  void set_dispatcher(Dispatcher *d) { dispatcher = d; }

  virtual int shutdown() = 0;

  // -- message interface
  // send message
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;

  // make a procedure call
  virtual Message* sendrecv(Message *m, msg_addr_t dest, int port=0) = 0;

  // wait (block) for a message, or timeout.  Don't return message yet.
  //virtual int wait_message(time_t seconds) = 0;

  // wait (block) for a request from anyone
  //virtual Message *recv() = 0

  
  // events
  virtual void trigger_timer(Timer *t) = 0;


  // -- incoming queue --
  // (that nothing uses)
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
