#ifndef __MINODESYNCRELEASE_H
#define __MINODESYNCRELEASE_H

#include "include/Message.h"

class MInodeSyncRelease : public Message {
  inode_t inode;

 public:
  inodeno_t get_ino() { return inode.ino; }
  inode_t& get_inode() { return inode; }

  MInodeSyncRelease() {}
  MInodeSyncRelease(CInode *in) :
	Message(MSG_MDS_INODESYNCRELEASE) {
	this->inode = in->get_inode();
  }
  virtual char *get_type_name() { return "ISyFin"; }

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(inode), (char*)&inode);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&inode, sizeof(inode));
	return s;
  }
  
};

#endif
