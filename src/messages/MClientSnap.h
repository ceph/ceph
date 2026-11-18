// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MCLIENTSNAP_H
#define CEPH_MCLIENTSNAP_H

#include <algorithm>
#include <cstdint>
#include <vector>

#include "include/ceph_fs_encoder.h"
#include "include/fs_types.h"
#include "msg/Message.h"

class MClientSnap final : public SafeMessage {
public:
  ceph_mds_snap_head head;
  ceph::buffer::list bl;
  
  // (for split only)
  std::vector<inodeno_t> split_inos;
  std::vector<inodeno_t> split_realms;

  // Kernel clients reject an msgr2 front segment longer than
  // CEPH_MSG_MAX_FRONT_LEN (include/linux/ceph/libceph.h). A snaprealm
  // split lists every capped inode that moved; one MClientSnap for a
  // large directory (quota, snapshot, rename) overflows that limit and
  // the client drops the session.
  // https://tracker.ceph.com/issues/76530
  //
  // The front is ceph_mds_snap_head, then raw le64 inode numbers, then
  // raw le64 child-realm inode numbers, then the snap trace. Counts live
  // in the head, so there is no per-list length prefix.
  static constexpr size_t MAX_FRONT_LEN = 16 * 1024 * 1024;

  struct chunk_t {
    size_t ino_off = 0;
    size_t ino_count = 0;
    size_t realm_off = 0;
    size_t realm_count = 0;
  };

  enum class split_plan_t {
    SINGLE,        // one message stays within MAX_FRONT_LEN
    CHUNKED,       // chunks cover every entry, each within the limit
    UNSPLITTABLE,  // the trace itself does not fit; cannot chunk
  };

  static size_t front_len(size_t n_inos, size_t n_realms,
                          size_t trace_len) {
    return sizeof(ceph_mds_snap_head) +
           (n_inos + n_realms) * sizeof(uint64_t) +
           trace_len;
  }

  // How many split entries (inos or child realms) fit beside this trace.
  static size_t max_split_entries(size_t trace_len,
                                  size_t limit = MAX_FRONT_LEN) {
    const size_t fixed = sizeof(ceph_mds_snap_head) + trace_len;
    if (fixed >= limit) {
      return 0;
    }
    return (limit - fixed) / sizeof(uint64_t);
  }

  static bool front_fits(size_t n_inos, size_t n_realms, size_t trace_len,
                         size_t limit = MAX_FRONT_LEN) {
    const size_t fixed = sizeof(ceph_mds_snap_head) + trace_len;
    if (fixed > limit) {
      return false;
    }
    const size_t room = limit - fixed;
    if (n_realms > room / sizeof(uint64_t)) {
      return false;
    }
    const size_t left = room - n_realms * sizeof(uint64_t);
    return n_inos <= left / sizeof(uint64_t);
  }

  // Child realms are packed before inodes. Every chunk the caller builds
  // must also carry the snap trace: the kernel treats a SPLIT whose trace
  // has no realm info as corrupt and closes the session.
  static split_plan_t plan_split(size_t n_inos, size_t n_realms,
                                 size_t trace_len,
                                 std::vector<chunk_t>& chunks,
                                 size_t limit = MAX_FRONT_LEN) {
    chunks.clear();
    if (front_fits(n_inos, n_realms, trace_len, limit)) {
      return split_plan_t::SINGLE;
    }
    const size_t max_entries = max_split_entries(trace_len, limit);
    if (max_entries == 0 || (n_inos == 0 && n_realms == 0)) {
      return split_plan_t::UNSPLITTABLE;
    }

    size_t ino_off = 0;
    size_t realm_off = 0;
    while (ino_off < n_inos || realm_off < n_realms) {
      size_t room = max_entries;
      chunk_t chunk;
      chunk.realm_off = realm_off;
      chunk.realm_count = std::min(n_realms - realm_off, room);
      room -= chunk.realm_count;
      chunk.ino_off = ino_off;
      chunk.ino_count = std::min(n_inos - ino_off, room);
      realm_off += chunk.realm_count;
      ino_off += chunk.ino_count;
      chunks.push_back(chunk);
    }
    return split_plan_t::CHUNKED;
  }

protected:
  MClientSnap(int o=0) : 
    SafeMessage{CEPH_MSG_CLIENT_SNAP} {
    memset(&head, 0, sizeof(head));
    head.op = o;
  }
  ~MClientSnap() final {}

public:  
  std::string_view get_type_name() const override { return "client_snap"; }
  void print(std::ostream& out) const override {
    out << "client_snap(" << ceph_snap_op_name(head.op);
    if (head.split)
      out << " split=" << inodeno_t(head.split);
    out << " tracelen=" << bl.length();
    out << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    head.num_split_inos = split_inos.size();
    head.num_split_realms = split_realms.size();
    head.trace_len = bl.length();
    encode(head, payload);
    ceph::encode_nohead(split_inos, payload);
    ceph::encode_nohead(split_realms, payload);
    ceph::encode_nohead(bl, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(head, p);
    ceph::decode_nohead(head.num_split_inos, split_inos, p);
    ceph::decode_nohead(head.num_split_realms, split_realms, p);
    ceph::decode_nohead(head.trace_len, bl, p);
    ceph_assert(p.end());
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
