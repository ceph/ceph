#ifndef __CHEESY_MESSENGER_H
#define __CHEESY_MESSENGER_H

#include "Dispatcher.h"
#include "Message.h"

#include "Messenger.h"

#include "common/Cond.h"
#include "common/Mutex.h"

#include <assert.h>

#include <map>
using namespace std;

class CheesySerializer : public Messenger,
						 public Dispatcher {
 protected:
  long last_pcid;
  
  Messenger *messenger;        // this is how i communicate
  
  Mutex                  lock;      // protect call_sem, call_reply
  map<long, Cond*>       call_cond;
  map<long, Message*>    call_reply;
  
 public:
  CheesySerializer(Messenger *msg) {
	messenger = msg;
	messenger->set_dispatcher(this);
	last_pcid = 1;
  }
  CheesySerializer() {
	if (messenger) 
	  delete messenger;
  }

  int shutdown();

  // incoming messages
  void dispatch(Message *m);

  // outgoing messages
  int  send_message(Message *m, msg_addr_t dest, 
					int port=0, int fromport=0);     // doesn't block
  Message *sendrecv(Message *m, msg_addr_t dest, 
					int port=0);                     // blocks for matching reply

  void trigger_timer(class Timer *t) {
	messenger->trigger_timer(t);
  }
};

#endif
