#ifndef __MDIREXPIRE_H
#define __MDIREXPIRE_H

typedef struct {
  inodeno_t ino;
  int nonce;
  int from;
} MDirExpire_st;

class MDirExpire : public Message {
  MDirExpire_st st;

 public:
  inodeno_t get_ino() { return st.ino; }
  int get_from() { return st.from; }
  int get_nonce() { return st.nonce; }

  MDirExpire() {}
  MDirExpire(inodeno_t ino, int from, int nonce) :
	Message(MSG_MDS_DIREXPIRE) {
	st.ino = ino;
	st.from = from;
	st.nonce = nonce;
  }
  virtual char *get_type_name() { return "DirEx";}
  
  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(st), (char*)&st);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&st,sizeof(st));
	return s;
  }
};

#endif
