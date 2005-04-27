#ifndef __SERIAL_MESSENGER_H
#define __SERIAL_MESSENGER_H

#include "Dispatcher.h"
#include "Message.h"

class SerialMessenger : public Dispatcher {
 public:
  virtual void dispatch(Message *m) = 0;      // i receive my messages here
  virtual void send(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;          // doesn't block
  virtual Message *sendrecv(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;  // blocks for matching reply
};

#endif
