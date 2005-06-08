
#ifndef __FAKEMESSENGER_H
#define __FAKEMESSENGER_H

#include "Messenger.h"
#include "Dispatcher.h"

#include <list>
#include <map>

class Timer;

class FakeMessenger : public Messenger {
 protected:
  int whoami;

  class Logger *logger;

  list<Message*>       incoming;        // incoming queue

 public:
  FakeMessenger(long me);
  ~FakeMessenger();

  virtual int shutdown();

  // msg interface
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0);
  
  // use CheesySerializer for now!
  virtual Message* sendrecv(Message *m, msg_addr_t dest, int port=0) { assert(0); };

  // events
  virtual void trigger_timer(Timer *t);


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

};

int fakemessenger_do_loop();
int fakemessenger_do_loop_2();
void fakemessenger_startthread();
void fakemessenger_stopthread();
void fakemessenger_wait();

#endif
