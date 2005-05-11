#ifndef __MDIRUPDATE_H
#define __MDIRUPDATE_H

#include "msg/Message.h"

typedef struct {
  inodeno_t ino;
  int dir_rep;
} MDirUpdate_st;

class MDirUpdate : public Message {
  MDirUpdate_st st;
  set<int> dir_rep_by;

 public:
  inodeno_t get_ino() { return st.ino; }
  int get_dir_rep() { return st.dir_rep; }
  set<int>& get_dir_rep_by() { return dir_rep_by; } 

  MDirUpdate() {}
  MDirUpdate(inodeno_t ino,
			 int dir_rep,
			 set<int>& dir_rep_by) :
	Message(MSG_MDS_DIRUPDATE) {
	this->st.ino = ino;
	this->st.dir_rep = dir_rep;
	this->dir_rep_by = dir_rep_by;
  }
  virtual char *get_type_name() { return "dup"; }

  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(st), (char*)&st);
	off += sizeof(st);
	_unrope(dir_rep_by, s, off);
  }

  virtual void encode_payload(crope& r) {
	r.append((char*)&st, sizeof(st));
	_rope(dir_rep_by, r);
  }
};

#endif
