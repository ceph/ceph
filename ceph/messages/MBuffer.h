#ifndef __MBUFFER_H
#define __MBUFFER_H

#include "../include/Message.h"

class MBuffer : public Message {
 public:
  char *buf;
  size_t len;

  MBuffer(int type, char *b, size_t l) :
	Message(type) {
	buf = b;
	len = l;
  }
  ~MBuffer() {
	//if (buf) { delete buf; buf = 0; }
  }
};

#endif
