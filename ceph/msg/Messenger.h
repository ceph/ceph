
#ifndef __MESSENGER_H
#define __MESSENGER_H

#include <list>
#include <vector>
using namespace std;

#include "Message.h"
#include "Dispatcher.h"

class MDS;
class Timer;

class Messenger {
 private:
  Dispatcher          *dispatcher;

 public:
  Messenger() : dispatcher(0) { }
  
  virtual int shutdown() = 0;
  
  // dispatching incoming messages
  void set_dispatcher(Dispatcher *d) { dispatcher = d; }
  Dispatcher *get_dispatcher() { return dispatcher; }
  virtual void dispatch(Message *m) {
	assert(dispatcher);
	dispatcher->dispatch(m);
  }

  // send message
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;

  // make a procedure call
  virtual Message* sendrecv(Message *m, msg_addr_t dest, int port=0) = 0;

  // events
  virtual void trigger_timer(Timer *t) = 0;

};


extern Message *decode_message(msg_envelope_t &env, bufferlist& bl);



#endif
