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

  virtual void decode_payload(crope& s) {
	s.copy(0, sizeof(ino), (char*)&ino);
  }
  
  virtual void encode_payload(crope& s) {
	s.append((char*)&ino, sizeof(ino));
  }
  
};

#endif
