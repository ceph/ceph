// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>

#include "osd_types.h"

/**
 * BackfillInterval
 *
 * Represents the objects in a range [begin, end)
 *
 * Possible states:
 * 0) Empty instance - default constructed
 * 1) Unpopulated    - BackfillInterval::begin == BackfillInterval::end
 * 2) Populated      - BackfillInterval::objects contains all existing objects
 *                     in logical range of [BackfillInterval::begin, BackfillInterval::end)
 */

struct BackfillInterval {
  // info about a backfill interval on a peer
  eversion_t version; /// version at which the scan occurred
  std::map<hobject_t,eversion_t> objects;
  hobject_t begin; /// object to start populating the interval from
  hobject_t end;   /// object to start populating the interval to
  bool populated = false;

  /// clear content
  void clear() {
    *this = BackfillInterval();
  }

  // Constructs an unpopulated instance where
  // begin==end. This is used for the
  // initalzation of peer_backfill_info.
  BackfillInterval(hobject_t begin);

  // *This ctor overload will be removed in the next commits*
  BackfillInterval(hobject_t begin,
                   hobject_t end);

  // Construct a fully populated instance
  BackfillInterval(hobject_t begin,
                   hobject_t end,
                   const std::map<hobject_t,eversion_t>&& _objects,
                   eversion_t _version);

  // Construct a fully populated instance
  BackfillInterval(hobject_t begin,
                   hobject_t end,
                   const ceph::buffer::list& data);

  BackfillInterval() = default;

  // populate the objects in the interval
  void populate(const ceph::buffer::list& data) {
    ceph_assert(objects.empty() && !populated);
    auto p = data.cbegin();
    decode_noclear(objects, p);
    populated = true;
  }

  // populate the objects in the interval and update version
  void populate(const std::map<hobject_t,eversion_t>&& _objects,
                eversion_t _version) {
    ceph_assert(objects.empty() && !populated);
    objects = std::move(_objects);
    version = _version;
    populated = true;
  }

  /// true if interval is populated
  bool is_populated() {
    return populated;
  }

  /// true if there are no objects in this interval
  bool empty() const {
    return objects.empty();
  }

  /// true if interval extends to the end of the range
  bool extends_to_end() const {
    return end.is_max();
  }

  /// removes items <= soid and adjusts begin to the first object
  void trim_to(const hobject_t &soid) {
    trim();
    while (!objects.empty() &&
           objects.begin()->first <= soid) {
      pop_front();
    }
  }

  /// Adjusts begin to the first object
  void trim() {
    if (!objects.empty())
      begin = objects.begin()->first;
    else
      begin = end;
  }

  /// drop first entry, and adjust @begin accordingly
  void pop_front() {
    ceph_assert(!objects.empty());
    objects.erase(objects.begin());
    trim();
  }

  /// dump
  void dump(ceph::Formatter *f) const {
    f->dump_stream("begin") << begin;
    f->dump_stream("end") << end;
    f->open_array_section("objects");
    for (std::map<hobject_t, eversion_t>::const_iterator i =
           objects.begin();
         i != objects.end();
         ++i) {
      f->open_object_section("object");
      f->dump_stream("object") << i->first;
      f->dump_stream("version") << i->second;
      f->close_section();
    }
    f->close_section();
  }
};

std::ostream &operator<<(std::ostream &out, const BackfillInterval &bi);

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<BackfillInterval> : fmt::ostream_formatter {};
#endif
