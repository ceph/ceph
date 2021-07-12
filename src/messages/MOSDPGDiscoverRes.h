// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MOSDPGDiscoverRes_H
#define CEPH_MOSDPGDiscoverRes_H

#include "msg/Message.h"

class MOSDPGDiscoverRes: public Message {

  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

  epoch_t epoch = 0;
  /// query_epoch is the epoch of the query being responded to, or
  /// the current epoch if this is not being sent in response to a
  /// query. This allows the recipient to disregard responses to old
  /// queries.
  epoch_t query_epoch = 0;

public:
  spg_t pgid;
  shard_id_t to;
  shard_id_t from;
  vector<hobject_t> found;

  epoch_t get_epoch() const { return epoch; }
  epoch_t get_query_epoch() const { return query_epoch; }

  MOSDPGDiscoverRes() : Message(MSG_OSD_PG_DISC_RES, HEAD_VERSION, COMPAT_VERSION) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
  MOSDPGDiscoverRes(spg_t pgid, shard_id_t to, shard_id_t from,
                version_t mv, vector<hobject_t>& f, epoch_t query_epoch)
    : Message(MSG_OSD_PG_DISC_RES, HEAD_VERSION, COMPAT_VERSION),
      epoch(mv), query_epoch(query_epoch),
      pgid(pgid), to(to), from(from),
      found(f)  {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }

private:
  ~MOSDPGDiscoverRes() override {}

public:
  std::string_view get_type_name() const override { return "PGdiscres"; }
  void print(ostream& out) const override {
    // NOTE: log is not const, but operator<< doesn't touch fields
    // swapped out by OSD code.
    out << "pg_disc_res(pgid " << pgid << " epoch " << epoch
        << " found " << found
        << " query_epoch " << query_epoch << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid, payload);
    encode(epoch, payload);
    encode(found, payload);
    encode(query_epoch, payload);
    encode(to, payload);
    encode(from, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(pgid, p);
    decode(epoch, p);
    decode(found, p);
    decode(query_epoch, p);
    decode(to, p);
    decode(from, p);
  }
};

 #endif
