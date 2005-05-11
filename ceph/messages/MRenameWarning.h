#ifndef __MRENAMEWARNING_H
#define __MRENAMEWARNING_H

class MRenameWarning : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }

  MRenameWarning() {}
  MRenameWarning(inodeno_t ino) :
	Message(MSG_MDS_RENAMEWARNING) {
	this->ino = ino;
  }
  virtual char *get_type_name() { return "RnW";}
  
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&ino,sizeof(ino));
  }
};

#endif
