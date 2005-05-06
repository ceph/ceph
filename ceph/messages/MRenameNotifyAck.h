#ifndef __MRENAMENOTIFYACK_H
#define __MRENAMENOTIFYACK_H

class MRenameNotifyAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }

  MRenameNotifyAck() {}
  MRenameNotifyAck(inodeno_t ino) :
	Message(MSG_MDS_RENAMENOTIFYACK) {
	this->ino = ino;
  }
  virtual char *get_type_name() { return "RnotA";}
  
  virtual void decode_payload(crope& s) {
	int off = 0;
	s.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&ino,sizeof(ino));
  }
};

#endif
