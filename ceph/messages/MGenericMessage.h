#ifndef __MGENERICMESSAGE_H
#define __MGENERICMESSAGE_H

#include "msg/Message.h"

class MGenericMessage : public Message {
  char tname[100];
  long pcid;

 public:
  MGenericMessage(int t) : Message(t), pcid(0) { 
	sprintf(tname, "generic%d", get_type());
  }

  void set_pcid(long pcid) { this->pcid = pcid; }
  long get_pcid() { return pcid; }

  char *get_type_name() { return tname; }

  virtual void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(pcid), (char*)&pcid);
	off += sizeof(pcid);
  }
  virtual void encode_payload() {
	payload.append((char*)&pcid, sizeof(pcid));
  }
};

#endif
