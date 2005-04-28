
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

  virtual void decode_payload(crope& s) {
	s.copy(0, sizeof(ttl), (char*)&ttl);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&ttl, sizeof(ttl));
  }

  virtual char *get_type_name() { return "ping"; }
};

#endif
