#ifndef __MHASHDIRPREPACK_H
#define __MHASHDIRPREPACK_H

#include "msg/Message.h"
#include "include/types.h"

class MHashDirPrepAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }
  
  MHashDirPrepAck() {}
  MHashDirPrepAck(inodeno_t ino) :
	Message(MSG_MDS_HASHDIRPREPACK) {
	this->ino = ino;
  }
  
  virtual char *get_type_name() { return "HPAck"; }

  void decode_payload() {
	payload.copy(0, sizeof(ino), (char*)&ino);
  }
  void encode_payload() {
	payload.append((char*)&ino, sizeof(ino));
  }
};

#endif
