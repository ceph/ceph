#ifndef __MHASHDIR_H
#define __MHASHDIR_H

#include "include/Message.h"

#include <ext/rope>
using namespace std;

class MHashDir : public Message {
  string path;
  
 public:  
  crope  dir_rope;

  MHashDir() {}
  MHashDir(string& path) : 
	Message(MSG_MDS_HASHDIR) {
	this->path = path;
  }
  virtual char *get_type_name() { return "Ha"; }

  string& get_path() { return path; }
  crope& get_state() { return dir_rope; }
  
  virtual int decode_payload(crope s) {
	path = s.c_str();
	dir_rope = s.substr(path.length() + 1, s.length() - path.length() - 1);
	return 0;
  }
  virtual crope get_payload() {
	crope s;
	s.append(path.c_str(), path.length() + 1);
	s.append(dir_rope);
	return s;
  }

};

#endif
