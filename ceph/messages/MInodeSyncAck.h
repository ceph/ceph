#ifndef __MINODESYNCACK_H
#define __MINODESYNCACK_H

#include "include/Message.h"

class MInodeSyncAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }

  MInodeSyncAck() {}
  MInodeSyncAck(inodeno_t ino) :
	Message(MSG_MDS_INODESYNCACK) {
	this->ino = ino;
  }
  virtual char *get_type_name() { return "ISyAck"; }

  virtual int decode_payload(crope s) {
	s.copy(0,sizeof(inodeno_t), (char*)&ino);
  }
  virtual crope get_payload() {
	return crope((char*)&ino, sizeof(ino));
  }
};

#endif
