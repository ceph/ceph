#ifndef __MUNHASHDIRACK_H
#define __MUNHASHDIRACK_H

#include "msg/Message.h"

#include <ext/rope>
using namespace std;

class MUnhashDirAck : public Message {
  inodeno_t ino;
  
 public:  
  crope  dir_rope;

  MUnhashDirAck() {}
  MUnhashDirAck(inodeno_t ino) : 
	Message(MSG_MDS_UNHASHDIRACK) {
	this->ino = ino;
  }
  virtual char *get_type_name() { return "UHaAck"; }

  inodeno_t get_ino() { return ino; }
  crope& get_state() { return dir_rope; }
  
  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
	dir_rope = s.substr(sizeof(ino), s.length() - sizeof(ino));
	return 0;
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	s.append(dir_rope);
	return s;
  }

};

#endif
