#ifndef __MINODESYNCFINISH_H
#define __MINODESYNCFINISH_H

#include "include/Message.h"

class MInodeSyncRelease : public Message {
 public:
  inodeno_t ino;
  inode_t inode;

  MInodeSyncFinish(inodeno_t ino,
				   inode_t& inode) :
	Message(MSG_MDS_INODESYNCRELEASE) {
	this->ino = ino;
	this->inode = inode;
  }
};

#endif
