#ifndef __MCLIENTREPLY_H
#define __MCLIENTREPLY_H

#include "../include/Message.h"

class MClientReply : public Message {
 public:
  long tid;
  int op;

  // reply data
  

  MClientReply(MClientRequest *req) : Message(MSG_CLIENT_REQUEST) {
	this->tid = req->tid;
	this->op = req->mop;
  }
};

#endif
