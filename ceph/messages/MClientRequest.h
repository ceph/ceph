#ifndef __MCLIENTREQUEST_H
#define __MCLIENTREQUEST_H

#include "include/Message.h"

class MClientRequest : public Message {
 public:
  long tid;
  int op;
  int client;

  inodeno_t ino;
  string path;

  MClientRequest(long tid, int op, int client) : Message(MSG_CLIENT_REQUEST) {
	this->tid = tid;
	this->op = op;
	this->client = client;
  }
};

#endif
