#ifndef __MINODEGETREPLICAACK_H
#define __MINODEGETREPLICAACK_H

class MInodeGetReplicaAck : public Message {
  inodeno_t ino;
  //crope state;
  
 public:
  inodeno_t get_ino() { return ino; }
  //crope& get_state() { return state; }

  MInodeGetReplicaAck() {}
  MInodeGetReplicaAck(inodeno_t ino
				   //, crope& state
					  ) :
	Message(MSG_MDS_INODEGETREPLICA) {
	this->ino = ino;
	//this->state = state;
  }
  virtual char *get_type_name() { return "GInoA";}
  
  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
	//state = s.substr(sizeof(ino), s.length() - sizeof(ino));
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino,sizeof(ino));
	//s.append(state);
	return s;
  }
};

#endif
