#ifndef __PAXOSSERVICEMESSAGE_H
#define __PAXOSSERVICEMESSAGE_H

#include "msg/Message.h"

class PaxosServiceMessage : public Message {
 public:
  version_t version;

  PaxosServiceMessage() : Message(MSG_PAXOS), version(0) { }
  PaxosServiceMessage(int type, version_t v) : Message(type), version(v) { }

  void paxos_encode() {
    ::encode(version, payload);
  }

  void paxos_decode( bufferlist::iterator& p ) {
    ::decode(version, p);
  }

  void encode_payload() {
    assert(0);
    paxos_encode();
  }

  void decode_payload() {
    assert(0);
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
  }
  
  const char *get_type_name() { return "PaxosServiceMessage"; }
};

#endif
