#ifndef __MEXPORTDIRNOTIFY_H
#define __MEXPORTDIRNOTIFY_H

#include "include/Message.h"
#include <string>
using namespace std;

class MExportDirNotify : public Message {
  int       new_auth;
  int       old_auth;
  inodeno_t ino;
  
  list<inodeno_t> subdirs;

 public:
  inodeno_t get_ino() { return ino; }
  int get_new_auth() { return new_auth; }
  int get_old_auth() { return old_auth; }
  list<inodeno_t>::iterator subdirs_begin() { return subdirs.begin(); }
  list<inodeno_t>::iterator subdirs_end() { return subdirs.end(); }
  int num_subdirs() { return subdirs.size(); }

  MExportDirNotify() {}
  MExportDirNotify(inodeno_t ino, int old_auth, int new_auth) :
	Message(MSG_MDS_EXPORTDIRNOTIFY) {
	this->ino = ino;
	this->old_auth = old_auth;
	this->new_auth = new_auth;
  }
  virtual char *get_type_name() { return "ExNot"; }
  
  void copy_subdirs(list<inodeno_t>& s) {
	this->subdirs = s;
  }

  virtual int decode_payload(crope s) {
	int off = 0;
	s.copy(off, sizeof(int), (char*)&new_auth);
	off += sizeof(int);
	s.copy(off, sizeof(int), (char*)&old_auth);
	off += sizeof(int);
	s.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);

	// subdirs
	while (off < s.length()) {
	  inodeno_t i;
	  s.copy(off, sizeof(i), (char*)&i);
	  subdirs.push_back(i);
	  off += sizeof(inodeno_t);
	}
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&new_auth, sizeof(int));
	s.append((char*)&old_auth, sizeof(int));
	s.append((char*)&ino, sizeof(ino));

	// subdirs
	for (list<inodeno_t>::iterator it = subdirs.begin();
		 it != subdirs.end();
		 it++) {
	  inodeno_t ino = *it;
	  s.append((char*)&ino, sizeof(ino));
	}
	return s;
  }
};

#endif
