#ifndef __CHEESY_MESSENGER_H
#define __CHEESY_MESSENGER_H

#include "Dispatcher.h"
#include "Message.h"

#include "SerialMessenger.h"

#include "Semaphore.h"
#include "Mutex.h"

#include <map>
using namespace std;

class CheesySerializer : public SerialMessenger {
 protected:
  long last_tid;
  
  // my state, whatever
  Messenger *messenger;        // this is how i communicate
  Dispatcher *dispatcher;      // this is where i send unsolicited messages
  
  Mutex                  lock;      // protect call_sem, call_reply
  map<long, Semaphore*>  call_sem;
  map<long, Message*>    call_reply;
  
 public:
  CheesySerializer(Messenger *msg, Dispatcher *me) {
	this->messenger = msg;
	this->dispatcher = me;
	last_tid = 1;
  }

  // incoming messages
  void dispatch(Message *m);

  // outgoing messages
  void send(Message *m, msg_addr_t dest, 
			int port=0, int fromport=0);          // doesn't block
  Message *sendrecv(Message *m, msg_addr_t dest, 
					int port=0, int fromport=0);  // blocks for matching reply
};

#endif
