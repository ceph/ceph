#ifndef __MGENERICMESSAGE_H
#define __MGENERICMESSAGE_H

#include "msg/Message.h"

class MGenericMessage : public Message {
  char tname[100];
  long pcid;

 public:
  MGenericMessage(int t) : Message(t) { 
	sprintf(tname, "%d", type);
  }

  void set_pcid(long pcid) { this->pcid = pcid; }
  long get_pcid() { return pcid; }

  char *get_type_name() { return tname; }

  virtual void decode_payload(crope& s, int& off) {  
	s.copy(off, sizeof(pcid), (char*)&pcid);
	off += sizeof(pcid);
  }
  virtual void encode_payload(crope& s) {  
	s.append((char*)&pcid, sizeof(pcid));
  }
};

#endif
