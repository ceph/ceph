#ifndef __MEXPORTDIRACK_H
#define __MEXPORTDIRACK_H

#include "MExportDir.h"

class MExportDirAck : public Message {
 public:
  inodeno_t ino;

  MExportDirAck(MExportDir *req) :
	Message(MSG_MDS_EXPORTDIRACK) {
	ino = req->ino;
  }  
  virtual char *get_type_name() { return "expack"; }
  
};

#endif
