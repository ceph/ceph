#ifndef __MDIRSYNCSTART_H
#define __MDIRSYNCSTART_H

#include "msg/Message.h"

class MDirSyncStart : public Message {
  inodeno_t ino;
  int asker;

 public:
  inodeno_t get_ino() { return ino; }
  int get_asker() { return asker; }

  MDirSyncStart() {}
  MDirSyncStart(inodeno_t ino, int asker) :
	Message(MSG_MDS_DIRSYNCSTART) {
	this->ino = ino;
	this->asker = asker;
  }
  virtual char *get_type_name() { return "DSySt"; }

  virtual void decode_payload(crope& s) {
	s.copy(0,sizeof(inodeno_t), (char*)&ino);
	s.copy(sizeof(inodeno_t), sizeof(int), (char*)&asker);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&ino, sizeof(inodeno_t));
	s.append((char*)&asker, sizeof(int));
  }

};

#endif
