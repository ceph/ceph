#ifndef __MUNHASHDIRNOTIFYACK_H
#define __MUNHASHDIRNOTIFYACK_H

#include "msg/Message.h"

class MUnhashDirNotifyAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }
  
  MUnhashDirNotifyAck() {}
  MUnhashDirNotifyAck(inodeno_t ino) :
	Message(MSG_MDS_UNHASHDIRNOTIFYACK) {
	this->ino = ino;
  }  
  virtual char *get_type_name() { return "UHNa"; }
  
  virtual void decode_payload() {
	payload.copy(0, sizeof(ino), (char*)&ino);
  }
  virtual void encode_payload() {
	payload.append((char*)&ino, sizeof(ino));
  }

};

#endif
