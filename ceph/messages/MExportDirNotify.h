#ifndef __MEXPORTDIRNOTIFY_H
#define __MEXPORTDIRNOTIFY_H

#include "include/Message.h"
#include <string>
using namespace std;

class MExportDirNotify : public Message {
  int       new_auth;
  string    path;

 public:
  string& get_path() { return path; }
  int get_new_auth() { return new_auth; }

  MExportDirNotify() {}
  MExportDirNotify(string& path, int new_auth) :
	Message(MSG_MDS_EXPORTDIRNOTIFY) {
	this->path = path;
	this->new_auth = new_auth;
  }
  virtual char *get_type_name() { return "exnot"; }

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(int), (char*)&new_auth);
	path = s.c_str() + sizeof(int);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&new_auth, sizeof(int));
	s.append(path.c_str());
	s.append((char)0);
	return s;
  }
};

#endif
