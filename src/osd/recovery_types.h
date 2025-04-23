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
 * 1) begin == end == hobject_t() indicates the the interval is unpopulated
 * 2) Else, objects contains all objects in [begin, end)
 *
 * ReplicaBackfillInterval
 *
 * Stores a map of hobject_t and eversion to track the version number of
 * the objects being backfilled in an interval for one specific shard
 *
 * PrimaryBackfillInterval
 *
 * Stores a multimap of hobject and pair<shard_id_t, eversion>.
 *
 * Only shards that are backfill targets will be tracked. For replicated and
 * non-optimized EC pools there is one entry per hobject_t and shard_id_t will
 * be NO_SHARD.
 *
 * For optimized EC pools partial writes mean it is possible that different
 * shards have different eversions, hence there may be multiple entries per
 * hobject_t. To conserve memory it is permitted to have an entry for NO_SHARD
 * and additional entries for the same hobject for specific shards. In this
 * case shards that are not specifically listed are expected to be at the
 * eversion for the NO_SHARD entry.
 *
 * Example: EC pool with 4+2 profile
 *
 *   test:head, <NO_SHARD, 1'23>
 *   test:head, <1,        1'20>
 *
 * Shards 0 and 2-5 are expected to be at version 1'23, shard 1 has skipped
 * recent updates and is expected to be at version 1'20
 */

template <typename T>
class BackfillInterval {
public:
  // info about a backfill interval on a peer
  eversion_t version; /// version at which the scan occurred
  hobject_t begin;
  hobject_t end;
  T objects;

  virtual ~BackfillInterval() = default;
  BackfillInterval() = default;
  BackfillInterval(const BackfillInterval&) = default;
  BackfillInterval(BackfillInterval&&) = default;
  BackfillInterval& operator=(const BackfillInterval&) = default;
  BackfillInterval& operator=(BackfillInterval&&) = default;

  /// clear content
  virtual void clear() = 0;

  /// clear objects std::list only
  void clear_objects() {
    objects.clear();
  }

  /// reinstantiate with a new start+end position and sort order
  void reset(hobject_t start) {
    clear();
    begin = end = start;
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
    if (!objects.empty()) {
      begin = objects.begin()->first;
    } else {
      begin = end;
    }
  }

  /// drop first entry, and adjust @begin accordingly
  virtual void pop_front() = 0;

  /// dump
  virtual void dump(ceph::Formatter *f) const = 0;
};

class PrimaryBackfillInterval: public BackfillInterval<std::multimap<hobject_t,
					std::pair<shard_id_t, eversion_t>>> {
public:

  /// clear content
  void clear() override {
    *this = PrimaryBackfillInterval();
  }

  /// drop first entry, and adjust @begin accordingly
  void pop_front() override {
    ceph_assert(!objects.empty());
    // Use erase(key) to erase all entries for key
    objects.erase(objects.begin()->first);
    trim();
  }

  /// dump
  void dump(ceph::Formatter *f) const override {
    f->dump_stream("begin") << begin;
    f->dump_stream("end") << end;
    f->open_array_section("objects");
    for (const auto& [hoid, shard_version] : objects) {
      const auto& [shard, version] = shard_version;
      f->open_object_section("object");
      f->dump_stream("object") << hoid;
      f->dump_stream("shard") << shard;
      f->dump_stream("version") << version;
      f->close_section();
    }
    f->close_section();
  }
};

class ReplicaBackfillInterval: public BackfillInterval<std::map<hobject_t,
								eversion_t>> {
public:
  /// clear content
  void clear() override {
    *this = ReplicaBackfillInterval();
  }

  /// drop first entry, and adjust @begin accordingly
  void pop_front() {
    ceph_assert(!objects.empty());
    objects.erase(objects.begin());
    trim();
  }

  /// dump
  void dump(ceph::Formatter *f) const override {
    f->dump_stream("begin") << begin;
    f->dump_stream("end") << end;
    f->open_array_section("objects");
    for (const auto& [hoid, version] : objects) {
      f->open_object_section("object");
      f->dump_stream("object") << hoid;
      f->dump_stream("version") << version;
      f->close_section();
    }
    f->close_section();
  }
};

template<typename T> std::ostream& operator<<(std::ostream& out,
					      const BackfillInterval<T>& bi)
{
  out << "BackfillInfo(" << bi.begin << "-" << bi.end << " ";
  if (!bi.objects.empty()) {
    out << bi.objects.size() << " objects " << bi.objects;
  }
  out << ")";
  return out;
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<PrimaryBackfillInterval> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ReplicaBackfillInterval> : fmt::ostream_formatter {};
#endif
