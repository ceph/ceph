#ifndef __MDIRUPDATE_H
#define __MDIRUPDATE_H

#include "include/Message.h"

class MDirUpdate : public Message {
 public:
  inodeno_t ino;
  int dir_auth;
  int dir_rep;
  set<int> dir_rep_by;

  MDirUpdate(inodeno_t ino,
			 int dir_auth,
			 int dir_rep,
			 set<int>& dir_rep_by) :
	Message(MSG_MDS_DIRUPDATE) {
	this->ino = ino;
	this->dir_auth = dir_auth;
	this->dir_rep = dir_rep;
	this->dir_rep_by = dir_rep_by;
  }
};

#endif
