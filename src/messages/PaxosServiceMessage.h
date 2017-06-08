#ifndef CEPH_PAXOSSERVICEMESSAGE_H
#define CEPH_PAXOSSERVICEMESSAGE_H

#include "msg/Message.h"
#include "mon/Session.h"

class PaxosServiceMessage : public Message {
 public:
  version_t version;
  __s16 deprecated_session_mon;
  uint64_t deprecated_session_mon_tid;

  // track which epoch the leader received a forwarded request in, so we can
  // discard forwarded requests appropriately on election boundaries.
  epoch_t rx_election_epoch;
  
  PaxosServiceMessage()
    : Message(MSG_PAXOS),
      version(0), deprecated_session_mon(-1), deprecated_session_mon_tid(0),
      rx_election_epoch(0) { }
  PaxosServiceMessage(int type, version_t v, int enc_version=1, int compat_enc_version=0)
    : Message(type, enc_version, compat_enc_version),
      version(v), deprecated_session_mon(-1), deprecated_session_mon_tid(0),
      rx_election_epoch(0)  { }
 protected:
  ~PaxosServiceMessage() {}

 public:
  void paxos_encode() {
    ::encode(version, payload);
    ::encode(deprecated_session_mon, payload);
    ::encode(deprecated_session_mon_tid, payload);
  }

  void paxos_decode( bufferlist::iterator& p ) {
    ::decode(version, p);
    ::decode(deprecated_session_mon, p);
    ::decode(deprecated_session_mon_tid, p);
  }

  void encode_payload(uint64_t features) {
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
  
  const char *get_type_name() const { return "PaxosServiceMessage"; }
};

#endif
