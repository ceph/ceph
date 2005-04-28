#ifndef __MEXPORTDIRWARNING_H
#define __MEXPORTDIRWARNING_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirWarning : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }

  MExportDirWarning() {}
  MExportDirWarning(inodeno_t ino) : 
	Message(MSG_MDS_EXPORTDIRWARNING) {
	this->ino = ino;
  }

  virtual char *get_type_name() { return "ExW"; }

  virtual void decode_payload(crope& s) {
	s.copy(0, sizeof(ino), (char*)&ino);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&ino, sizeof(ino));
  }
};

#endif
