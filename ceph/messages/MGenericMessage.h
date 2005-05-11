#ifndef __MGENERICMESSAGE_H
#define __MGENERICMESSAGE_H

#include "msg/Message.h"

class MGenericMessage : public Message {
  char tname[100];
 public:
  MGenericMessage(int t) : Message(t) { 
	sprintf(tname, "%d", type);
  }

  char *get_type_name() { return tname; }

  virtual void decode_payload(crope& s, int& off) {  }
  virtual void encode_payload(crope& s) {  }
};

#endif
