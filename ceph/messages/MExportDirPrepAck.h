#ifndef __MEXPORTDIRPREPACK_H
#define __MEXPORTDIRPREPACK_H

#include "include/Message.h"
#include "include/types.h"

class MExportDirPrepAck : public Message {
 public:
  inodeno_t ino;

  MExportDirPrepAck(inodeno_t ino) :
	Message(MSG_MDS_EXPORTDIRPREPACK) {
	this->ino = ino;
  }
  
  virtual char *get_type_name() { return "ExPAck"; }
};

#endif
