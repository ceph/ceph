#ifndef __MINODEEXPIRE_H
#define __MINODEEXPIRE_H

class MInodeExpire : public Message {
 public:
  inodeno_t ino;

  MInodeExpire(inodeno_t ino) :
	Message(MSG_MDS_INODEEXPIRE) {
	this->ino = ino;
  }
  virtual char *get_type_name() { return "inex";}
};

#endif
