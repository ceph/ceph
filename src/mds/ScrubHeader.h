// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef SCRUB_HEADER_H_
#define SCRUB_HEADER_H_

#include <memory>
#include <string>
#include <string_view>

#include "include/ceph_assert.h"

namespace ceph {
class Formatter;
};

/**
 * Externally input parameters for a scrub, associated with the root
 * of where we are doing a recursive scrub
 */
class ScrubHeader {
public:
  ScrubHeader() {}
  ScrubHeader(std::string_view tag_, bool tag_internal_, bool force_,
              bool recursive_, bool repair_)
    : tag(tag_), tag_internal(tag_internal_), force(force_),
      recursive(recursive_), repair(repair_) {}

  // Set after construction because it won't be known until we've
  // started resolving path and locking
  void set_origin(inodeno_t ino) { origin = ino; }

  bool get_recursive() const { return recursive; }
  bool get_repair() const { return repair; }
  bool get_force() const { return force; }
  bool is_internal_tag() const { return tag_internal; }
  inodeno_t get_origin() const { return origin; }
  const std::string& get_tag() const { return tag; }

  bool get_repaired() const { return repaired; }
  void set_repaired() { repaired = true; }

  void set_epoch_last_forwarded(unsigned epoch) { epoch_last_forwarded = epoch; }
  unsigned get_epoch_last_forwarded() const { return epoch_last_forwarded; }

  void inc_num_pending() { ++num_pending; }
  void dec_num_pending() {
    ceph_assert(num_pending > 0);
    --num_pending;
  }
  unsigned get_num_pending() const { return num_pending; }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(tag, bl);
    encode(tag_internal, bl);
    encode(origin, bl);
    encode(force, bl);
    encode(recursive, bl);
    encode(repair, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(tag, p);
    decode(tag_internal, p);
    decode(origin, p);
    decode(force, p);
    decode(recursive, p);
    decode(repair, p);
    DECODE_FINISH(p);
  }

protected:
  std::string tag;
  bool tag_internal = true;
  bool force = false;
  bool recursive = false;
  bool repair = false;
  inodeno_t origin;

  bool repaired = false;  // May be set during scrub if repairs happened
  unsigned epoch_last_forwarded = 0;
  unsigned num_pending = 0;
};
WRITE_CLASS_ENCODER(ScrubHeader)

typedef std::shared_ptr<ScrubHeader> ScrubHeaderRef;
typedef std::shared_ptr<const ScrubHeader> ScrubHeaderRefConst;

#endif // SCRUB_HEADER_H_
