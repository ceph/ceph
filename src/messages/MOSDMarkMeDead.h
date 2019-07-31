// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "messages/PaxosServiceMessage.h"

class MOSDMarkMeDead : public PaxosServiceMessage {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

 public:
  uuid_d fsid;
  int32_t target_osd;
  epoch_t epoch = 0;

  MOSDMarkMeDead()
    : PaxosServiceMessage{MSG_OSD_MARK_ME_DEAD, 0,
			  HEAD_VERSION, COMPAT_VERSION} { }
  MOSDMarkMeDead(const uuid_d &fs, int osd,
		 epoch_t e)
    : PaxosServiceMessage{MSG_OSD_MARK_ME_DEAD, e,
			  HEAD_VERSION, COMPAT_VERSION},
      fsid(fs), target_osd(osd),
      epoch(e) {}
 private:
  ~MOSDMarkMeDead() override {}

public:
  epoch_t get_epoch() const { return epoch; }

  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    decode(target_osd, p);
    decode(epoch, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    encode(fsid, payload);
    encode(target_osd, payload, features);
    encode(epoch, payload);
  }

  std::string_view get_type_name() const override { return "MOSDMarkMeDead"; }
  void print(ostream& out) const override {
    out << "MOSDMarkMeDead("
	<< "osd." << target_osd
	<< ", epoch " << epoch
	<< ", fsid=" << fsid
	<< ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
