#ifndef __MINODESYNCRECALL_H
#define __MINODESYNCRECALL_H

#include "include/Message.h"

class MInodeSyncRecall : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }

  MInodeSyncRecall() {}
  MInodeSyncRecall(inodeno_t ino) :
	Message(MSG_MDS_INODESYNCRECALL) {
	this->ino = ino;
  }
  virtual char *get_type_name() { return "ISyRec"; }

  virtual int decode_payload(crope s) {
	s.copy(0,sizeof(inodeno_t), (char*)&ino);
	return 0;
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	return s;
  }
};

#endif
