#ifndef __MEXPORTDIRACK_H
#define __MEXPORTDIRACK_H

#include "MExportDir.h"

class MExportDirAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }
  
  MExportDirAck() {}
  MExportDirAck(MExportDir *req) :
	Message(MSG_MDS_EXPORTDIRACK) {
	ino = req->get_ino();
  }  
  virtual char *get_type_name() { return "ExAck"; }
  
  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
	return 0;
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	return s;
  }

};

#endif
