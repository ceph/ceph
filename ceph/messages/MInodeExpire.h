#ifndef __MINODEEXPIRE_H
#define __MINODEEXPIRE_H

typedef struct {
  inodeno_t ino;
  int hops;
  int from;
  bool soft;    // i got an update for something i don't have; ignore at will
} MInodeExpire_st;

class MInodeExpire : public Message {
  MInodeExpire_st st;

 public:
  inodeno_t get_ino() { return st.ino; }
  int get_hops() { return st.hops; }
  int get_from() { return st.from; }
  bool is_soft() { return st.soft; }
  void add_hop() { st.hops++; }

  MInodeExpire() {}
  MInodeExpire(inodeno_t ino, int from, bool soft=false) :
	Message(MSG_MDS_INODEEXPIRE) {
	st.ino = ino;
	st.from = from;
	st.soft = soft;
	st.hops = 0;
  }
  virtual char *get_type_name() { return "InEx";}

  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(st), (char*)&st);
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&st,sizeof(st));
  }
};

#endif
