#ifndef __MUNHASHDIRPREP_H
#define __MUNHASHDIRPREP_H

#include "msg/Message.h"

class MUnhashDirPrep : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }
  
  MUnhashDirPrep() {}
  MUnhashDirPrep(inodeno_t ino) :
	Message(MSG_MDS_UNHASHDIRPREP) {
	this->ino = ino;
  }  
  virtual char *get_type_name() { return "UHP"; }
  
  virtual void decode_payload() {
	payload.copy(0, sizeof(ino), (char*)&ino);
  }
  virtual void encode_payload() {
	payload.append((char*)&ino, sizeof(ino));
  }

};

#endif
