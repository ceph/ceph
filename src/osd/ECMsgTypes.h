// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef ECBMSGTYPES_H
#define ECBMSGTYPES_H

#include <fmt/format.h>

#include "osd_types.h"
#include "include/buffer.h"
#include "os/ObjectStore.h"
#include "boost/tuple/tuple.hpp"

struct ECSubWrite {
  pg_shard_t from;
  ceph_tid_t tid;
  osd_reqid_t reqid;
  hobject_t soid;
  pg_stat_t stats;
  ObjectStore::Transaction t;
  eversion_t at_version;
  eversion_t trim_to;
  eversion_t roll_forward_to;
  std::vector<pg_log_entry_t> log_entries;
  std::set<hobject_t> temp_added;
  std::set<hobject_t> temp_removed;
  std::optional<pg_hit_set_history_t> updated_hit_set_history;
  bool backfill_or_async_recovery = false;
  ECSubWrite() : tid(0) {}
  ECSubWrite(
    pg_shard_t from,
    ceph_tid_t tid,
    osd_reqid_t reqid,
    hobject_t soid,
    const pg_stat_t &stats,
    const ObjectStore::Transaction &t,
    eversion_t at_version,
    eversion_t trim_to,
    eversion_t roll_forward_to,
    std::vector<pg_log_entry_t> log_entries,
    std::optional<pg_hit_set_history_t> updated_hit_set_history,
    const std::set<hobject_t> &temp_added,
    const std::set<hobject_t> &temp_removed,
    bool backfill_or_async_recovery)
    : from(from), tid(tid), reqid(reqid),
      soid(soid), stats(stats), t(t),
      at_version(at_version),
      trim_to(trim_to), roll_forward_to(roll_forward_to),
      log_entries(log_entries),
      temp_added(temp_added),
      temp_removed(temp_removed),
      updated_hit_set_history(updated_hit_set_history),
      backfill_or_async_recovery(backfill_or_async_recovery)
    {}
  void claim(ECSubWrite &other) {
    from = other.from;
    tid = other.tid;
    reqid = other.reqid;
    soid = other.soid;
    stats = other.stats;
    t.swap(other.t);
    at_version = other.at_version;
    trim_to = other.trim_to;
    roll_forward_to = other.roll_forward_to;
    log_entries.swap(other.log_entries);
    temp_added.swap(other.temp_added);
    temp_removed.swap(other.temp_removed);
    updated_hit_set_history = other.updated_hit_set_history;
    backfill_or_async_recovery = other.backfill_or_async_recovery;
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ECSubWrite*>& o);
private:
  // no outside copying -- slow
  ECSubWrite(ECSubWrite& other);
  const ECSubWrite& operator=(const ECSubWrite& other);
};
WRITE_CLASS_ENCODER(ECSubWrite)

struct ECSubWriteReply {
  pg_shard_t from;
  ceph_tid_t tid;
  eversion_t last_complete;
  bool committed;
  bool applied;
  ECSubWriteReply() : tid(0), committed(false), applied(false) {}
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ECSubWriteReply*>& o);
};
WRITE_CLASS_ENCODER(ECSubWriteReply)

struct ECSubRead {
  pg_shard_t from;
  ceph_tid_t tid;
  std::map<hobject_t, std::list<boost::tuple<uint64_t, uint64_t, uint32_t> >> to_read;
  std::set<hobject_t> attrs_to_read;
  std::map<hobject_t, std::vector<std::pair<int, int>>> subchunks;
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ECSubRead*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(ECSubRead)

struct ECSubReadReply {
  pg_shard_t from;
  ceph_tid_t tid;
  std::map<hobject_t, std::list<std::pair<uint64_t, ceph::buffer::list> >> buffers_read;
  std::map<hobject_t, std::map<std::string, ceph::buffer::list, std::less<>>> attrs_read;
  std::map<hobject_t, int> errors;
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ECSubReadReply*>& o);
};
WRITE_CLASS_ENCODER(ECSubReadReply)

std::ostream &operator<<(
  std::ostream &lhs, const ECSubWrite &rhs);
std::ostream &operator<<(
  std::ostream &lhs, const ECSubWriteReply &rhs);
std::ostream &operator<<(
  std::ostream &lhs, const ECSubRead &rhs);
std::ostream &operator<<(
  std::ostream &lhs, const ECSubReadReply &rhs);

template <> struct fmt::formatter<ECSubWrite> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ECSubWriteReply> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ECSubRead> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ECSubReadReply> : fmt::ostream_formatter {};

#endif
