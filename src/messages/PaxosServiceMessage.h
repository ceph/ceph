#ifndef __PAXOSSERVICEMESSAGE_H
#define __PAXOSSERVICEMESSAGE_H

#include "msg/Message.h"

class PaxosServiceMessage : public Message {
 public:
  version_t version;
  __s16 session_mon;
  __u64 session_mon_tid;
  
  PaxosServiceMessage() : Message(MSG_PAXOS), version(0), session_mon(-1) { }
  PaxosServiceMessage(int type, version_t v) : Message(type), version(v), session_mon(-1) { }

  void paxos_encode() {
    ::encode(version, payload);
    ::encode(session_mon, payload);
    ::encode(session_mon_tid, payload);
  }

  void paxos_decode( bufferlist::iterator& p ) {
    ::decode(version, p);
    ::decode(session_mon, p);
    ::decode(session_mon_tid, p);
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
