#ifndef __MINODEUNLINK_H
#define __MINODEUNLINK_H

class MInodeUnlink : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }

  MInodeUnlink() {}
  MInodeUnlink(inodeno_t ino) :
	Message(MSG_MDS_INODEUNLINK) {
	this->ino = ino;
  }
  virtual char *get_type_name() { return "Irm";}
  
  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino,sizeof(ino));
	return s;
  }
};

#endif
