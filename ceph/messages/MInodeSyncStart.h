#ifndef __MINODESYNCSTART_H
#define __MINODESYNCSTART_H

#include "include/Message.h"

class MInodeSyncStart : public Message {
 public:
  inodeno_t ino;

  MInodeSyncStart(inodeno_t ino, int asker) :
	Message(MSG_MDS_INODESYNCSTART) {
	this->ino = ino;
	this->asker = asker;
  }
};

#endif
