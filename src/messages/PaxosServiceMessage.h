#ifndef __PAXOSSERVICEMESSAGE_H
#define __PAXOSSERVICEMESSAGE_H

#include "msg/Message.h"
#include "mon/Session.h"

class PaxosServiceMessage : public Message {
 public:
  version_t version;
  __s16 session_mon;
  __u64 session_mon_tid;
  
 PaxosServiceMessage() : Message(MSG_PAXOS),
			 version(0), session_mon(-1), session_mon_tid(0) { }
  PaxosServiceMessage(int type, version_t v) : Message(type),
					       version(v), session_mon(-1), session_mon_tid(0) { }
 protected:
  ~PaxosServiceMessage() {}

 public:
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

  /** 
   * These messages are only used by the monitors and clients,
   * and the client doesn't care, so we're creating a monitor-specific
   * function here. Note that this function explicitly exists to bypass
   * the normal ref-counting, so don't expect the returned pointer to be
   * very long-lived -- it will still only last as long as the Session would
   * normally.
   */
  MonSession *get_session() {
    MonSession *session = (MonSession *)get_connection()->get_priv();
    if (session)
      session->put();
    return session;
  }
  
  const char *get_type_name() { return "PaxosServiceMessage"; }
};

#endif
