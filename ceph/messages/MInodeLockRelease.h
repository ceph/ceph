#ifndef __MINODELOCKRELEASE_H
#define __MINODELOCKRELEASE_H

#include "include/Message.h"

class MInodeLockRelease : public Message {
  inode_t inode;

 public:
  inodeno_t get_ino() { return inode.ino; }
  inode_t& get_inode() { return inode; }

  MInodeLockRelease() {}
  MInodeLockRelease(CInode *in) :
	Message(MSG_MDS_INODELOCKRELEASE) {
	this->inode = in->get_inode();
  }
  virtual char *get_type_name() { return "ILoFin"; }

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
