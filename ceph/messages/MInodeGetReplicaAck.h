#ifndef __MINODEGETREPLICAACK_H
#define __MINODEGETREPLICAACK_H

class MInodeGetReplicaAck : public Message {
  inodeno_t ino;
  int nonce;
  //crope state;
  
 public:
  inodeno_t get_ino() { return ino; }
  int get_nonce() { return nonce; }
  //crope& get_state() { return state; }

  MInodeGetReplicaAck() {}
  MInodeGetReplicaAck(inodeno_t ino, int nonce ) :
	Message(MSG_MDS_INODEGETREPLICA) {
	this->ino = ino;
	this->nonce = nonce;
	//this->state = state;
  }
  virtual char *get_type_name() { return "GInoA";}
  
  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
	s.copy(sizeof(ino), sizeof(int), (char*)&nonce);
	//state = s.substr(sizeof(ino), s.length() - sizeof(ino));
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	s.append((char*)&nonce, sizeof(nonce));
	//s.append(state);
	return s;
  }
};

#endif
