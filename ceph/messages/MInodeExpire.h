#ifndef __MINODEEXPIRE_H
#define __MINODEEXPIRE_H

class MInodeExpire : public Message {
 public:
  inodeno_t ino;
  int hops;
  int from;

  MInodeExpire(inodeno_t ino, int from) :
	Message(MSG_MDS_INODEEXPIRE) {
	this->ino = ino;
	this->from = from;
	hops = 0;
  }
  virtual char *get_type_name() { return "inex";}
};

#endif
