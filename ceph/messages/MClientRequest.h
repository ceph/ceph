#ifndef __MCLIENTREQUEST_H
#define __MCLIENTREQUEST_H

#include "../include/Message.h"

class MClientRequest : public Message {
 public:
  long tid;
  int op;

  inodeno_t ino;
  string path;

  MClientRequest(long tid, int op) : Message(MSG_CLIENT_REQUEST) {
	this->tid = tid;
	this->op = op;
  }
};

#endif
