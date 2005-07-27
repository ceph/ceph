#ifndef __MHASHDIRACK_H
#define __MHASHDIRACK_H

#include "MHashDir.h"

class MHashDirAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }
  
  MHashDirAck() {}
  MHashDirAck(inodeno_t ino) :
	Message(MSG_MDS_HASHDIRACK) {
	this->ino = ino;
  }  
  virtual char *get_type_name() { return "HAck"; }
  
  virtual void decode_payload() {
	payload.copy(0, sizeof(ino), (char*)&ino);
  }
  virtual void encode_payload() {
	payload.append((char*)&ino, sizeof(ino));
  }

};

#endif
