
#ifndef __MPING_H
#define __MPING_H

#include "../include/Message.h"

class MPing : public Message {
 public:
  int ttl;
  MPing(int n) : Message(MSG_PING) {
	ttl = n;
  }
};

#endif
