// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MOSDFULL_H
#define CEPH_MOSDFULL_H

#include "messages/PaxosServiceMessage.h"
#include "osd/OSDMap.h"

// tell the mon to update the full/nearfull bits.  note that in the
// future this message could be generalized to other state bits, but
// for now name it for its sole application.

class MOSDFull : public MessageInstance<MOSDFull, PaxosServiceMessage> {
public:
  friend factory;

  epoch_t map_epoch = 0;
  uint32_t state = 0;

private:
  ~MOSDFull() {}

public:
  MOSDFull(epoch_t e, unsigned s)
    : MessageInstance(MSG_OSD_FULL, e), map_epoch(e), state(s) { }
  MOSDFull()
    : MessageInstance(MSG_OSD_FULL, 0) {}

public:
  void encode_payload(uint64_t features) {
    using ceph::encode;
    paxos_encode();
    encode(map_epoch, payload);
    encode(state, payload);
  }
  void decode_payload() {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(map_epoch, p);
    decode(state, p);
  }

  std::string_view get_type_name() const { return "osd_full"; }
  void print(ostream &out) const {
    set<string> states;
    OSDMap::calc_state_set(state, states);
    out << "osd_full(e" << map_epoch << " " << states << " v" << version << ")";
  }

};

#endif
