#ifndef __MINODEUNLINKACK_H
#define __MINODEUNLINKACK_H

class MInodeUnlinkAck : public Message {
  inodeno_t ino;
  bool haveit;

 public:
  inodeno_t get_ino() { return ino; }

  MInodeUnlinkAck() {}
  MInodeUnlinkAck(inodeno_t ino, bool haveit = true) :
	Message(MSG_MDS_INODEUNLINKACK) {
	this->ino = ino;
	this->haveit = haveit;
  }
  virtual char *get_type_name() { return "Irmack";}
  
  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
	s.copy(sizeof(ino), sizeof(haveit), (char*)&haveit);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	s.append((char*)&haveit, sizeof(haveit));
	return s;
  }
};

#endif
