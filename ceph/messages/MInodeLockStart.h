#ifndef __MINODELOCKSTART_H
#define __MINODELOCKSTART_H

#include "msg/Message.h"

class MInodeLockStart : public Message {
  inodeno_t ino;
  int asker;

 public:
  inodeno_t get_ino() { return ino; }
  int get_asker() { return asker; }

  MInodeLockStart() {}
  MInodeLockStart(inodeno_t ino, int asker) :
	Message(MSG_MDS_INODELOCKSTART) {
	this->ino = ino;
	this->asker = asker;
  }
  virtual char *get_type_name() { return "ILoSt"; }

  virtual int decode_payload(crope s) {
	s.copy(0,sizeof(inodeno_t), (char*)&ino);
	s.copy(sizeof(inodeno_t), sizeof(int), (char*)&asker);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(inodeno_t));
	s.append((char*)&asker, sizeof(int));
	return s;
  }

};

#endif
