#ifndef __MINODESYNCSTART_H
#define __MINODESYNCSTART_H

#include "include/Message.h"

class MInodeSyncStart : public Message {
 public:
  inodeno_t ino;
  int authority;

  MInodeSyncStart(inodeno_t ino, int auth) :
	Message(MSG_MDS_INODESYNCSTART) {
	this->ino = ino;
	this->authority = auth;
  }
};

#endif
