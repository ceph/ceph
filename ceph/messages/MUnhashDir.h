#ifndef __MUNHASHDIR_H
#define __MUNHASHDIR_H

#include "msg/Message.h"

#include <ext/rope>
using namespace std;

class MUnhashDir : public Message {
  string path;
  
 public:  
  MUnhashDir() {}
  MUnhashDir(string& path) : 
	Message(MSG_MDS_UNHASHDIR) {
	this->path = path;
  }
  virtual char *get_type_name() { return "UHa"; }

  string& get_path() { return path; }
  
  virtual int decode_payload(crope s) {
	path = s.c_str();
	return 0;
  }
  virtual crope get_payload() {
	crope s;
	s.append(path.c_str(), path.length()+1);
	return s;
  }

};

#endif
