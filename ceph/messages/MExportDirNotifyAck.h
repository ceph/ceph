#ifndef __MEXPORTDIRNOTIFYACK_H
#define __MEXPORTDIRNOTIFYACK_H

#include "include/Message.h"
#include <string>
using namespace std;

class MExportDirNotifyAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }
  
  MExportDirNotifyAck() {}
  MExportDirNotifyAck(inodeno_t ino) :
	Message(MSG_MDS_EXPORTDIRNOTIFYACK) {
	this->ino = ino;
  }
  virtual char *get_type_name() { return "ExNotA"; }

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
