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
