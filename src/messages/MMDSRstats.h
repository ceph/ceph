// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MMDSRSTATS_H
#define CEPH_MMDSRSTATS_H

#include "messages/MMDSOp.h"

class MMDSRstats : public MMDSOp {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  std::string_view get_type_name() const override { return "mds_rstats"; }
  void print(ostream& o) const override {
    o << "mds_rstats(e" << epoch << " f " << flushed_to << " a " << advance_to << ")";
  }

  unsigned get_epoch() const { return epoch; }
  utime_t get_flushed_to() const { return flushed_to; }
  utime_t get_advance_to() const { return advance_to; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(flushed_to, payload);
    encode(advance_to, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(flushed_to, p);
    decode(advance_to, p);
  }

protected:
  MMDSRstats() : MMDSOp(MSG_MDS_RSTATS, HEAD_VERSION, COMPAT_VERSION) {}
  MMDSRstats(unsigned e, const utime_t& f, const utime_t& a) :
    MMDSOp(MSG_MDS_RSTATS, HEAD_VERSION, COMPAT_VERSION),
    epoch(e), flushed_to(f), advance_to(a) {}
  ~MMDSRstats() override {}

private:
  unsigned epoch;
  utime_t flushed_to;
  utime_t advance_to;

  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
