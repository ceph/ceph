#ifndef __MINODESYNCACK_H
#define __MINODESYNCACK_H

#include "include/Message.h"

class MInodeSyncAck : public Message {
  inodeno_t ino;
  bool have;

 public:
  inodeno_t get_ino() { return ino; }
  bool did_have() { return have; }

  MInodeSyncAck() {}
  MInodeSyncAck(inodeno_t ino, bool have=true) :
	Message(MSG_MDS_INODESYNCACK) {
	this->ino = ino;
	this->have = have;
  }
  virtual char *get_type_name() { return "ISyAck"; }

  virtual int decode_payload(crope s) {
	s.copy(0,sizeof(inodeno_t), (char*)&ino);
	s.copy(sizeof(inodeno_t), sizeof(have), (char*)&have);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	s.append((char*)&have, sizeof(have));
  }
};

#endif
