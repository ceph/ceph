
#ifndef __MPING_H
#define __MPING_H

#include "msg/Message.h"

class MPing : public Message {
 public:
  int ttl;
  MPing(int n) : Message(MSG_PING) {
	ttl = n;
  }
  MPing() {}

  virtual int decode_payload(crope s) {
	ttl = *((int*)s.c_str());
  }
  virtual crope get_payload() {
	return crope((char*)&ttl, sizeof(int));
  }
  virtual char *get_type_name() { return "ping"; }
};

#endif
