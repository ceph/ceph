#ifndef __MINODEUPDATE_H
#define __MINODEUPDATE_H

#include "include/Message.h"

#include <set>
using namespace std;

typedef struct {
  inode_t  inode;
  int      dir_auth;
  int ncached_by;
} MInodeUpdate_st;

class MInodeUpdate : public Message {
  MInodeUpdate_st st;
  set<int> cached_by;

 public:
  inode_t& get_inode() { return st.inode; }
  set<int>& get_cached_by() { return cached_by; }
  int get_dir_auth() { return st.dir_auth; }
  
  MInodeUpdate() {}
  MInodeUpdate(inode_t& inode, set<int>cached_by, int dir_auth) :
	Message(MSG_MDS_INODEUPDATE) {
	this->st.inode = inode;
	this->st.dir_auth = dir_auth;
	this->cached_by = cached_by;
  }
  virtual char *get_type_name() { return "iup"; }

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(st), (char*)&st);
	for (int i=0; i<st.ncached_by; i++) {
	  int j;
	  s.copy(sizeof(st) + i*sizeof(int), sizeof(int), (char*)&j);
	  cached_by.insert(j);
	}
  }
  virtual crope get_payload() {
	crope s;
	st.ncached_by = cached_by.size();
	s.append((char*)&st, sizeof(st));
	for (set<int>::iterator it = cached_by.begin();
		 it != cached_by.end();
		 it++) {
	  int j = *it;
	  s.append((char*)&j, sizeof(int));
	}
	return s;
  }
	  
};

#endif
