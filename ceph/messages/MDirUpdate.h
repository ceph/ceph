#ifndef __MDIRUPDATE_H
#define __MDIRUPDATE_H

#include "include/Message.h"

typedef struct {
  inodeno_t ino;
  int dir_rep;
  int ndir_rep_by;
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

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(st), (char*)&st);
	for (int i=0; i<st.ndir_rep_by; i++) {
	  int k;
	  s.copy(sizeof(st) + i*sizeof(int), sizeof(int), (char*)&k);
	  dir_rep_by.insert(k);
	}
	return 0;
  }

  virtual crope get_payload() {
	crope r;
	st.ndir_rep_by = dir_rep_by.size();
	r.append((char*)&st, sizeof(st));
	for (set<int>::iterator it = dir_rep_by.begin();
		 it != dir_rep_by.end();
		 it++) {
	  int i = *it;
	  r.append((char*)&i, sizeof(int));
	}
	return r;
  }
};

#endif
