#pragma once

class MOSDBeacon : public PaxosServiceMessage {
public:
  MOSDBeacon(epoch_t e = 0)
    : PaxosServiceMessage(MSG_OSD_BEACON, e)
    {}
  void encode_payload(uint64_t features) override {
    paxos_encode();
  }
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
  }
  const char *get_type_name() const override { return "osd_beacon"; }
  void print(ostream &out) const {
    out << get_type_name();
  }
};
