#ifndef __DISPATCHER_H
#define __DISPATCHER_H

#include "Message.h"

class Messenger;

class Dispatcher {
 private:
  Messenger *dis_messenger;
  int        dis_port;

 public:
  // how i receive messages
  virtual void dispatch(Message *m) = 0;

  // messenger uses this to tell me how to send messages
  void set_messenger_port(Messenger *m, int port) {
	dis_messenger = m;
	dis_port = port;
  }

  // this is how i send messages
  int send_message(Message *m, msg_addr_t dest, int dest_port);
};

#endif
