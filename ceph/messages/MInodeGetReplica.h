#ifndef __MINODEGETREPLICA_H
#define __MINODEGETREPLICA_H

class MInodeGetReplica : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }

  MInodeGetReplica() {}
  MInodeGetReplica(inodeno_t ino) :
	Message(MSG_MDS_INODEGETREPLICA) {
	this->ino = ino;
  }
  virtual char *get_type_name() { return "GIno";}
  
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
