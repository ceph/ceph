#ifndef __MEXPORTDIRFINISH_H
#define __MEXPORTDIRFINISH_H

#include "MExportDir.h"

class MExportDirFinish : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }
  
  MExportDirFinish() {}
  MExportDirFinish(inodeno_t ino) :
	Message(MSG_MDS_EXPORTDIRFINISH) {
	this->ino = ino;
  }  
  virtual char *get_type_name() { return "ExFin"; }
  
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
