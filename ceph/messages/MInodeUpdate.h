#ifndef __MINODEUPDATE_H
#define __MINODEUPDATE_H

#include "include/Message.h"

#include <set>
using namespace std;

class MInodeUpdate : public Message {
 public:
  inode_t  inode;
  set<int> cached_by;
  int      dir_auth;

  MInodeUpdate(inode_t& inode, set<int>cached_by, int dir_auth) :
	Message(MSG_MDS_INODEUPDATE) {
	this->inode = inode;
	this->cached_by = cached_by;
	this->dir_auth = dir_auth;
  }
  virtual char *get_type_name() { return "iup"; }
};

#endif
