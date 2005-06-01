#ifndef __MCLIENTMOUNT_H
#define __MCLIENTMOUNT_H

#include "msg/Message.h"

class MClientMount : public Message {
  long pcid;
  int mkfs;

 public:
  MClientMount() : Message(MSG_CLIENT_MOUNT) { 
	mkfs = 0;
  }

  void set_mkfs(int m) { mkfs = m; }
  int get_mkfs() { return mkfs; }

  void set_pcid(long pcid) { this->pcid = pcid; }
  long get_pcid() { return pcid; }

  char *get_type_name() { return "Cmnt"; }

  virtual void decode_payload(crope& s, int& off) {  
	s.copy(off, sizeof(pcid), (char*)&pcid);
	off += sizeof(pcid);
	s.copy(off, sizeof(mkfs), (char*)&mkfs);
	off += sizeof(mkfs);
  }
  virtual void encode_payload(crope& s) {  
	s.append((char*)&pcid, sizeof(pcid));
	s.append((char*)&mkfs, sizeof(mkfs));
  }
};

#endif
