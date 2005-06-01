#ifndef __MINODEUNLINKACK_H
#define __MINODEUNLINKACK_H

typedef struct {
  inodeno_t ino;
} MInodeUnlinkAck_st;

class MInodeUnlinkAck : public Message {
  MInodeUnlinkAck_st st;

 public:
  inodeno_t get_ino() { return st.ino; }

  MInodeUnlinkAck() {}
  MInodeUnlinkAck(inodeno_t ino) :
	Message(MSG_MDS_INODEUNLINKACK) {
	st.ino = ino;
  }
  virtual char *get_type_name() { return "InUlA";}
  
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(st), (char*)&st);
	off += sizeof(st);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&st,sizeof(st));
  }
};

#endif
