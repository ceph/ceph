#ifndef __MINODEEXPIRE_H
#define __MINODEEXPIRE_H

class MInodeExpire : public Message {
 public:
  inodeno_t ino;
  int hops;
  int from;
  bool soft;    // i got an update for something i don't have; ignore at will

  MInodeExpire(inodeno_t ino, int from, bool soft=false) :
	Message(MSG_MDS_INODEEXPIRE) {
	this->ino = ino;
	this->from = from;
	this->soft = soft;
	hops = 0;
  }
  virtual char *get_type_name() { return "inex";}
};

#endif
