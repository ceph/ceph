#ifndef __MINODEWRITERCLOSED_H
#define __MINODEWRITERCLOSED_H

typedef struct {
  inodeno_t ino;
  int from;
} MInodeWriterClosed_st;

class MInodeWriterClosed : public Message {
  MInodeWriterClosed_st st;

 public:
  inodeno_t get_ino() { return st.ino; }
  int get_from() { return st.from; }

  MInodeWriterClosed() {}
  MInodeWriterClosed(inodeno_t ino, int from) :
	Message(MSG_MDS_INODEWRITERCLOSED) {
	st.ino = ino;
	st.from = from;
  }
  virtual char *get_type_name() { return "IWrCl";}
  
  virtual void decode_payload(crope& s) {
	s.copy(0, sizeof(st), (char*)&st);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&st,sizeof(st));
  }
};

#endif
