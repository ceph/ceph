#ifndef __MEXPORTDIRDISCOVERACK_H
#define __MEXPORTDIRDISCOVERACK_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirDiscoverAck : public Message {
  inodeno_t ino;
  bool success;

 public:
  inodeno_t get_ino() { return ino; }
  bool is_success() { return success; }

  MExportDirDiscoverAck() {}
  MExportDirDiscoverAck(inodeno_t ino, bool success=true) : 
	Message(MSG_MDS_EXPORTDIRDISCOVERACK) {
	this->ino = ino;
	this->success = false;
  }
  virtual char *get_type_name() { return "ExDisA"; }


  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	s.copy(off, sizeof(success), (char*)&success);
	off += sizeof(success);
  }

  virtual void get_payload(crope& s) {
	s.append((char*)&ino, sizeof(ino));
	s.append((char*)&success, sizeof(success));
  }
};

#endif
