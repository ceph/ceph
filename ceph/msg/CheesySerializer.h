
#include "SerialMessenger.h"
#include "Messenger.h"
#include "Dispatcher.h"

#include "Semaphore.h"


class CheesySerializer : public SerialMessenger {
  // my state, whatever
  Messenger *messenger;        // this is how i communicate
  Dispatcher *dispatcher;      // this is where i send unsolicited messages
  
  bool waiting_for_reply;
  Message *reply;
  Semaphore waiter;

 public:
  CheesySerializer(Messenger *msg, Dispatcher *me) {
	this->messenger = msg;
	this->dispatcher = me;
	waiting_for_reply = false;
	reply = 0;
  }

  // i receive my messages here
  void dispatch(Message *m);

  // my stuff
  void start();  // start my thread
  void message_thread();
  
  void send(Message *m, msg_addr_t dest, int port=0, int fromport=0);          // doesn't block
  Message *sendrecv(Message *m, msg_addr_t dest, int port=0, int fromport=0);  // blocks for matching reply
};

