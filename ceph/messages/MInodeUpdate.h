#ifndef __MINODEUPDATE_H
#define __MINODEUPDATE_H

#include "include/Message.h"

#include <set>
using namespace std;

class MInodeUpdate : public Message {
 public:
  inode_t  inode;
  set<int> cached_by;

  MInodeUpdate(inode_t& inode, set<int>cached_by) :
	Message(MSG_MDS_INODEUPDATE) {
	this->inode = inode;
	this->cached_by = cached_by;
  }
};

#endif
