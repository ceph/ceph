#ifndef __MNSLOOKUP_H
#define __MNSLOOKUP_H

#include "msg/Message.h"

class MNSLookup : public Message {
  msg_addr_t entity;

 public:
  MNSLookup() {}
  MNSLookup(msg_addr_t e) :
	Message(MSG_NS_LOOKUP) {
	entity = e;
  }
  
  char *get_type_name() { return "NSLook"; }

  msg_addr_t get_entity() { return entity; }

  void encode_payload() {
	payload.append((char*)&entity, sizeof(entity));
  }
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(entity), (char*)&entity);
	off += sizeof(entity);
  }
};


#endif

