#ifndef __MDIRSYNCRELEASE_H
#define __MDIRSYNCRELEASE_H

#include "include/Message.h"

class MDirSyncRelease : public Message {
  inodeno_t ino;
  unsigned nitems;
  // time stamp?  FIXME

 public:
  inodeno_t get_ino() { return ino; }
  size_t& get_nitems() { return nitems; }
  
  MDirSyncRelease() {}
  MDirSyncRelease(CDir *dir) :
	Message(MSG_MDS_DIRSYNCRELEASE) {
	this->ino = dir->ino();
	this->nitems = dir->get_auth_size();
  }
  virtual char *get_type_name() { return "DSyFin"; }

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
	s.copy(sizeof(ino), sizeof(nitems), (char*)&nitems);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	s.append((char*)&nitems, sizeof(nitems));
	return s;
  }
  
};

#endif
