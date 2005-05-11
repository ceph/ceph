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

  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&ino, sizeof(ino));
  }
};

#endif
