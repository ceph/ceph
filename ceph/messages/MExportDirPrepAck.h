#ifndef __MEXPORTDIRPREPACK_H
#define __MEXPORTDIRPREPACK_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirPrepAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }
  
  MExportDirPrepAck() {}
  MExportDirPrepAck(inodeno_t ino) :
	Message(MSG_MDS_EXPORTDIRPREPACK) {
	this->ino = ino;
  }
  
  virtual char *get_type_name() { return "ExPAck"; }

  virtual int decode_payload(crope s) {
	s.copy(0,sizeof(ino),(char*)&ino);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	return s;
  }
};

#endif
