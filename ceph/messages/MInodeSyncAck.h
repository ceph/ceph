#ifndef __MINODESYNCACK_H
#define __MINODESYNCACK_H

#include "include/Message.h"

class MInodeSyncAck : public Message {
 public:
  inodeno_t ino;
  bool haveit;

  MInodeSyncAck(inodeno_t ino, bool haveit = true) :
	Message(MSG_MDS_INODESYNCACK) {
	this->ino = ino;
	this->haveit = haveit;
  }
};

#endif
