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
  void pop_front() {
    ceph_assert(!objects.empty());
    objects.erase(objects.begin());
    trim();
  }

  /// dump
  virtual void dump(ceph::Formatter *f) const = 0;
};

class PrimaryBackfillInterval: public BackfillInterval<std::multimap<hobject_t,std::pair<shard_id_t,eversion_t>>> {
public:
  /// clear content
  void clear() override {
    *this = PrimaryBackfillInterval();
  }

  /// dump
  void dump(ceph::Formatter *f) const override {
    f->dump_stream("begin") << begin;
    f->dump_stream("end") << end;
    f->open_array_section("objects");
    if (!objects.empty()) {
      for (const auto& [hoid,shard_version] : objects) {
	const auto& [shard,version] = shard_version;
	f->open_object_section("object");
	f->dump_stream("object") << hoid;
	f->dump_stream("shard") << shard;
	f->dump_stream("version") << version;
	f->close_section();
      }
    }
    f->close_section();
  }
};

class ReplicaBackfillInterval: public BackfillInterval<std::map<hobject_t,eversion_t>> {
public:
  /// clear content
  void clear() override {
    *this = ReplicaBackfillInterval();
  }

  /// dump
  void dump(ceph::Formatter *f) const override {
    f->dump_stream("begin") << begin;
    f->dump_stream("end") << end;
    f->open_array_section("objects");
    if (!objects.empty()) {
      for (const auto& [hoid,version] : objects) {
	f->open_object_section("object");
	f->dump_stream("object") << hoid;
	f->dump_stream("version") << version;
	f->close_section();
      }
    }
    f->close_section();
  }
};

template<typename T> std::ostream& operator<<(std::ostream& out, const BackfillInterval<T>& bi)
{
  out << "BackfillInfo(" << bi.begin << "-" << bi.end << " ";
  if (!bi.objects.empty()) {
    out << bi.objects.size() << " objects";
    out << " " << bi.objects;
  }
  out << ")";
  return out;
}

#if FMT_VERSION >= 90000
template <typename T> struct fmt::formatter<BackfillInterval<T>> : fmt::ostream_formatter {};
#endif
