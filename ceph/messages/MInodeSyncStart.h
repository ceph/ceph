#ifndef __MINODESYNCSTART_H
#define __MINODESYNCSTART_H

#include "msg/Message.h"

class MInodeSyncStart : public Message {
  inodeno_t ino;
  int asker;

 public:
  inodeno_t get_ino() { return ino; }
  int get_asker() { return asker; }

  MInodeSyncStart() {}
  MInodeSyncStart(inodeno_t ino, int asker) :
	Message(MSG_MDS_INODESYNCSTART) {
	this->ino = ino;
	this->asker = asker;
  }
  virtual char *get_type_name() { return "ISySt"; }

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
