#ifndef __MNSREGISTERACK_H
#define __MNSREGISTERACK_H

#include "msg/Message.h"
#include "msg/TCPMessenger.h"

class MNSRegisterAck : public Message {
  msg_addr_t entity;
  long tid;

 public:
  MNSRegisterAck() {}
  MNSRegisterAck(long t, msg_addr_t e) : 
	Message(MSG_NS_REGISTERACK) { 
	entity = e;
	tid = t;
  }
  
  char *get_type_name() { return "NSRegA"; }

  msg_addr_t get_entity() { return entity; }
  long get_tid() { return tid; }

  void encode_payload() {
	payload.append((char*)&entity, sizeof(entity));
	payload.append((char*)&tid, sizeof(tid));
  }
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(entity), (char*)&entity);
	off += sizeof(entity);
	payload.copy(off, sizeof(tid), (char*)&tid);
	off += sizeof(tid);
  }
};


#endif

