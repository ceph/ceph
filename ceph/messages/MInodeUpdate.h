#ifndef __MINODEUPDATE_H
#define __MINODEUPDATE_H

#include "include/Message.h"

#include <set>
using namespace std;

class MInodeUpdate : public Message {
  crope inode_basic_state;

 public:
  inodeno_t get_ino() { 
	inodeno_t ino = inode_basic_state.copy(0, sizeof(inodeno_t), (char*)&ino);
	return ino;
  }
  
  MInodeUpdate() {}
  MInodeUpdate(CInode *in) :
	Message(MSG_MDS_INODEUPDATE) {
	inode_basic_state = in->encode_basic_state();
  }
  virtual char *get_type_name() { return "Iup"; }

  virtual int decode_payload(crope s) {
	inode_basic_state = s;
  }
  virtual crope get_payload() {
	return inode_basic_state;
  }
	  
};

#endif
