#ifndef __MINODESYNCRELEASE_H
#define __MINODESYNCRELEASE_H

#include "include/Message.h"

class MInodeSyncRelease : public Message {
 public:
  inodeno_t ino;
  inode_t inode;

  MInodeSyncRelease(inodeno_t ino,
					inode_t& inode) :
	Message(MSG_MDS_INODESYNCRELEASE) {
	this->ino = ino;
	this->inode = inode;
  }
};

#endif
