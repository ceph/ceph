#ifndef __MINODESYNCACK_H
#define __MINODESYNCACK_H

#include "include/Message.h"

class MInodeSyncAck : public Message {
 public:
  inodeno_t ino;

  MInodeSyncAck(inodeno_t ino) :
	Message(MSG_MDS_INODESYNCACK) {
	this->ino = ino;
  }
};

#endif
