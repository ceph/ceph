#ifndef __MINODESYNCACK_H
#define __MINODESYNCACK_H

#include "msg/Message.h"

class MInodeSyncAck : public Message {
  inodeno_t ino;
  bool have;
  bool wantback;

 public:
  inodeno_t get_ino() { return ino; }
  bool did_have() { return have; }
  bool replica_wantsback() { return wantback; }

  MInodeSyncAck() {}
  MInodeSyncAck(inodeno_t ino, bool have=true, bool wantback=false) :
	Message(MSG_MDS_INODESYNCACK) {
	this->ino = ino;
	this->have = have;
	this->wantback = wantback;
  }
  virtual char *get_type_name() { return "ISyAck"; }

  virtual int decode_payload(crope s) {
	s.copy(0,sizeof(inodeno_t), (char*)&ino);
	s.copy(sizeof(inodeno_t), sizeof(have), (char*)&have);
	s.copy(sizeof(inodeno_t)+sizeof(have), sizeof(wantback), (char*)&wantback);
	return 0;
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	s.append((char*)&have, sizeof(have));
	s.append((char*)&wantback, sizeof(wantback));
	return s;
  }
};

#endif
