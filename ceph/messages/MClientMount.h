#ifndef __MCLIENTMOUNT_H
#define __MCLIENTMOUNT_H

#include "msg/Message.h"

class MClientMount : public Message {
  long pcid;

 public:
  MClientMount() : Message(MSG_CLIENT_MOUNT) { }

  void set_pcid(long pcid) { this->pcid = pcid; }
  long get_pcid() { return pcid; }

  char *get_type_name() { return "Cmnt"; }

  virtual void decode_payload(crope& s, int& off) {  
	s.copy(off, sizeof(pcid), (char*)&pcid);
	off += sizeof(pcid);
  }
  virtual void encode_payload(crope& s) {  
	s.append((char*)&pcid, sizeof(pcid));
  }
};

#endif
