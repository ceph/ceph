
#ifndef __MPING_H
#define __MPING_H

#include "../include/Message.h"

class MPing : public Message {
 public:
  int num;
  MPing(int n) {
	num = n;
	subsys = MSG_SUBSYS_SERVER;
	type = MSG_PING;
  }
};

#endif
