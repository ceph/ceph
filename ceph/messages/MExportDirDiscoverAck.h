#ifndef __MEXPORTDIRDISCOVERACK_H
#define __MEXPORTDIRDISCOVERACK_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirDiscoverAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }

  MExportDirDiscoverAck() {}
  MExportDirDiscoverAck(inodeno_t ino) : 
	Message(MSG_MDS_EXPORTDIRDISCOVERACK) {
	this->ino = ino;
  }
  virtual char *get_type_name() { return "ExDisA"; }


  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
    return sizeof(ino);
  }

  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	return s;
  }
};

#endif
