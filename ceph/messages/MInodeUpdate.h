#ifndef __MINODEUPDATE_H
#define __MINODEUPDATE_H

#include "include/Message.h"

#include <set>
using namespace std;

class MInodeUpdate : public Message {
  int nonce;
  crope inode_basic_state;

 public:
  inodeno_t get_ino() { 
	inodeno_t ino;
	inode_basic_state.copy(0, sizeof(inodeno_t), (char*)&ino);
	return ino;
  }
  int get_nonce() { return nonce; }
  
  MInodeUpdate() {}
  MInodeUpdate(CInode *in, int nonce) :
	Message(MSG_MDS_INODEUPDATE) {
	inode_basic_state = in->encode_basic_state();
	this->nonce = nonce;
  }
  virtual char *get_type_name() { return "Iup"; }

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(int), (char*)&nonce);
	inode_basic_state = s.substr(sizeof(int), s.length()-sizeof(int));
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&nonce, sizeof(int));
	s.append(inode_basic_state);
	return s;
  }
	  
};

#endif
