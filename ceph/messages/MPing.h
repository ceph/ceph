
#ifndef __MPING_H
#define __MPING_H

#include "../include/Message.h"

class MPing : public Message {
 public:
  int ttl;
  MPing(int n) {
	ttl = n;
	dest_port = MSG_SUBSYS_SERVER;
	type = MSG_PING;
  }
};

#endif
