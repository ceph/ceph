#ifndef CEPH_PAXOSSERVICEMESSAGE_H
#define CEPH_PAXOSSERVICEMESSAGE_H

#include "msg/Message.h"
#include "mon/Session.h"

class PaxosServiceMessage : public MessageSubType<PaxosServiceMessage> {
public:
  version_t version;
  __s16 deprecated_session_mon;
  uint64_t deprecated_session_mon_tid;

  // track which epoch the leader received a forwarded request in, so we can
  // discard forwarded requests appropriately on election boundaries.
  epoch_t rx_election_epoch;
  
  PaxosServiceMessage()
    : MessageSubType(MSG_PAXOS),
      version(0), deprecated_session_mon(-1), deprecated_session_mon_tid(0),
      rx_election_epoch(0) { }
  PaxosServiceMessage(int type, version_t v, int enc_version=1, int compat_enc_version=0)
    : MessageSubType(type, enc_version, compat_enc_version),
      version(v), deprecated_session_mon(-1), deprecated_session_mon_tid(0),
      rx_election_epoch(0)  { }
 protected:
  virtual ~PaxosServiceMessage() override {}

 public:
  void paxos_encode() {
    using ceph::encode;
    encode(version, payload);
    encode(deprecated_session_mon, payload);
    encode(deprecated_session_mon_tid, payload);
  }

  void paxos_decode(bufferlist::const_iterator& p ) {
    decode(version, p);
    decode(deprecated_session_mon, p);
    decode(deprecated_session_mon_tid, p);
  }

  void encode_payload(uint64_t features) override {
    ceph_abort();
    paxos_encode();
  }

  void decode_payload() override {
    ceph_abort();
    auto p = payload.cbegin();
    paxos_decode(p);
  }

  std::string_view get_type_name() const override { return "PaxosServiceMessage"; }
};

#endif
