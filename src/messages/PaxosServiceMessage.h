#ifndef __PAXOSSERVICEMESSAGE_H
#define __PAXOSSERVICEMESSAGE_H

#include "msg/Message.h"

#define VERSION_T 0

class PaxosServiceMessage : public Message {
 protected:
  version_t version;

 public:
  virtual ~PaxosServiceMessage() { }
  PaxosServiceMessage() : Message(MSG_PAXOS), version(0) { }
  PaxosServiceMessage( int type, version_t v) : Message(type), version(v) { }

  void paxos_encode() {
    ::encode(version, payload);
  }

  void paxos_decode( bufferlist::iterator& p ) {
    ::decode(version, p);
  }

  void encode_payload() {
    paxos_encode();
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
  }
  
  const char *get_type_name() { return "PaxosServiceMessage"; }
};

#endif
