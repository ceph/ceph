#ifndef __MINODELOCKACK_H
#define __MINODELOCKACK_H

#include "msg/Message.h"

class MInodeLockAck : public Message {
  inodeno_t ino;
  bool have;

 public:
  inodeno_t get_ino() { return ino; }
  bool did_have() { return have; }

  MInodeLockAck() {}
  MInodeLockAck(inodeno_t ino, bool have=true) :
	Message(MSG_MDS_INODELOCKACK) {
	this->ino = ino;
	this->have = have;
  }
  virtual char *get_type_name() { return "ILoAck"; }

  virtual int decode_payload(crope s) {
	s.copy(0,sizeof(inodeno_t), (char*)&ino);
	s.copy(sizeof(inodeno_t), sizeof(have), (char*)&have);
	return 0;
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	s.append((char*)&have, sizeof(have));
	return s;
  }
};

#endif
