#ifndef __MHASHREADDIR_H
#define __MHASHREADDIR_H

#include "include/types.h"
#include "msg/Message.h"

class MHashReaddir : public Message {
  inodeno_t ino;

 public:
  MHashReaddir() { }
  MHashReaddir(inodeno_t ino) :
	Message(MSG_MDS_HASHREADDIR) {
	this->ino = ino;
  }

  inodeno_t get_ino() { return ino; }

  virtual char *get_type_name() { return "Hls"; }

  virtual void decode_payload() {
	payload.copy(0, sizeof(ino), (char*)&ino);
  }
  virtual void encode_payload() {
	payload.append((char*)&ino, sizeof(ino));
  }

};

#endif
