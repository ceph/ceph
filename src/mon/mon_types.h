// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef CEPH_MON_TYPES_H
#define CEPH_MON_TYPES_H

#include <map>

#include "include/utime.h"
#include "include/util.h"
#include "common/Formatter.h"
#include "common/bit_str.h"
#include "include/Context.h"

#define PAXOS_MDSMAP     0
#define PAXOS_OSDMAP     1
#define PAXOS_LOG        2
#define PAXOS_MONMAP     3
#define PAXOS_AUTH       4
#define PAXOS_MGR        5
#define PAXOS_MGRSTAT    6
#define PAXOS_HEALTH     7
#define PAXOS_CONFIG     8
#define PAXOS_NUM        9

inline const char *get_paxos_name(int p) {
  switch (p) {
  case PAXOS_MDSMAP: return "mdsmap";
  case PAXOS_MONMAP: return "monmap";
  case PAXOS_OSDMAP: return "osdmap";
  case PAXOS_LOG: return "logm";
  case PAXOS_AUTH: return "auth";
  case PAXOS_MGR: return "mgr";
  case PAXOS_MGRSTAT: return "mgrstat";
  case PAXOS_HEALTH: return "health";
  case PAXOS_CONFIG: return "config";
  default: ceph_abort(); return 0;
  }
}

#define CEPH_MON_ONDISK_MAGIC "ceph mon volume v012"

extern const string CONFIG_PREFIX;

// map of entity_type -> features -> count
struct FeatureMap {
  std::map<uint32_t,std::map<uint64_t,uint64_t>> m;

  void add(uint32_t type, uint64_t features) {
    if (type == CEPH_ENTITY_TYPE_MON) {
      return;
    }
    m[type][features]++;
  }

  void add_mon(uint64_t features) {
    m[CEPH_ENTITY_TYPE_MON][features]++;
  }

  void rm(uint32_t type, uint64_t features) {
    if (type == CEPH_ENTITY_TYPE_MON) {
      return;
    }
    auto p = m.find(type);
    ceph_assert(p != m.end());
    auto q = p->second.find(features);
    ceph_assert(q != p->second.end());
    if (--q->second == 0) {
      p->second.erase(q);
      if (p->second.empty()) {
	m.erase(p);
      }
    }
  }

  FeatureMap& operator+=(const FeatureMap& o) {
    for (auto& p : o.m) {
      auto &v = m[p.first];
      for (auto& q : p.second) {
	v[q.first] += q.second;
      }
    }
    return *this;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(m, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    decode(m, p);
    DECODE_FINISH(p);
  }

  void dump(Formatter *f) const {
    for (auto& p : m) {
      f->open_array_section(ceph_entity_type_name(p.first));
      for (auto& q : p.second) {
	f->open_object_section("group");
        std::stringstream ss;
        ss << "0x" << std::hex << q.first << std::dec;
        f->dump_string("features", ss.str());
	f->dump_string("release", ceph_release_name(
			 ceph_release_from_features(q.first)));
	f->dump_unsigned("num", q.second);
	f->close_section();
      }
      f->close_section();
    }
  }
};
WRITE_CLASS_ENCODER(FeatureMap)

/**
 * leveldb store stats
 *
 * If we ever decide to support multiple backends for the monitor store,
 * we should then create an abstract class 'MonitorStoreStats' of sorts
 * and inherit it on LevelDBStoreStats.  I'm sure you'll figure something
 * out.
 */
struct LevelDBStoreStats {
  uint64_t bytes_total;
  uint64_t bytes_sst;
  uint64_t bytes_log;
  uint64_t bytes_misc;
  utime_t last_update;

  LevelDBStoreStats() :
    bytes_total(0),
    bytes_sst(0),
    bytes_log(0),
    bytes_misc(0)
  {}

  void dump(Formatter *f) const {
    ceph_assert(f != NULL);
    f->dump_int("bytes_total", bytes_total);
    f->dump_int("bytes_sst", bytes_sst);
    f->dump_int("bytes_log", bytes_log);
    f->dump_int("bytes_misc", bytes_misc);
    f->dump_stream("last_updated") << last_update;
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(bytes_total, bl);
    encode(bytes_sst, bl);
    encode(bytes_log, bl);
    encode(bytes_misc, bl);
    encode(last_update, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &p) {
    DECODE_START(1, p);
    decode(bytes_total, p);
    decode(bytes_sst, p);
    decode(bytes_log, p);
    decode(bytes_misc, p);
    decode(last_update, p);
    DECODE_FINISH(p);
  }

  static void generate_test_instances(list<LevelDBStoreStats*>& ls) {
    ls.push_back(new LevelDBStoreStats);
    ls.push_back(new LevelDBStoreStats);
    ls.back()->bytes_total = 1024*1024;
    ls.back()->bytes_sst = 512*1024;
    ls.back()->bytes_log = 256*1024;
    ls.back()->bytes_misc = 256*1024;
    ls.back()->last_update = utime_t();
  }
};
WRITE_CLASS_ENCODER(LevelDBStoreStats)

// data stats

struct DataStats {
  ceph_data_stats_t fs_stats;
  // data dir
  utime_t last_update;
  LevelDBStoreStats store_stats;

  void dump(Formatter *f) const {
    ceph_assert(f != NULL);
    f->dump_int("kb_total", (fs_stats.byte_total/1024));
    f->dump_int("kb_used", (fs_stats.byte_used/1024));
    f->dump_int("kb_avail", (fs_stats.byte_avail/1024));
    f->dump_int("avail_percent", fs_stats.avail_percent);
    f->dump_stream("last_updated") << last_update;
    f->open_object_section("store_stats");
    store_stats.dump(f);
    f->close_section();
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(3, 1, bl);
    encode(fs_stats.byte_total, bl);
    encode(fs_stats.byte_used, bl);
    encode(fs_stats.byte_avail, bl);
    encode(fs_stats.avail_percent, bl);
    encode(last_update, bl);
    encode(store_stats, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator &p) {
    DECODE_START(1, p);
    // we moved from having fields in kb to fields in byte
    if (struct_v > 2) {
      decode(fs_stats.byte_total, p);
      decode(fs_stats.byte_used, p);
      decode(fs_stats.byte_avail, p);
    } else {
      uint64_t t;
      decode(t, p);
      fs_stats.byte_total = t*1024;
      decode(t, p);
      fs_stats.byte_used = t*1024;
      decode(t, p);
      fs_stats.byte_avail = t*1024;
    }
    decode(fs_stats.avail_percent, p);
    decode(last_update, p);
    if (struct_v > 1)
      decode(store_stats, p);

    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(DataStats)

struct ScrubResult {
  map<string,uint32_t> prefix_crc;  ///< prefix -> crc
  map<string,uint64_t> prefix_keys; ///< prefix -> key count

  bool operator!=(const ScrubResult& other) {
    return prefix_crc != other.prefix_crc || prefix_keys != other.prefix_keys;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(prefix_crc, bl);
    encode(prefix_keys, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    decode(prefix_crc, p);
    decode(prefix_keys, p);
    DECODE_FINISH(p);
  }
  void dump(Formatter *f) const {
    f->open_object_section("crc");
    for (map<string,uint32_t>::const_iterator p = prefix_crc.begin(); p != prefix_crc.end(); ++p)
      f->dump_unsigned(p->first.c_str(), p->second);
    f->close_section();
    f->open_object_section("keys");
    for (map<string,uint64_t>::const_iterator p = prefix_keys.begin(); p != prefix_keys.end(); ++p)
      f->dump_unsigned(p->first.c_str(), p->second);
    f->close_section();
  }
  static void generate_test_instances(list<ScrubResult*>& ls) {
    ls.push_back(new ScrubResult);
    ls.push_back(new ScrubResult);
    ls.back()->prefix_crc["foo"] = 123;
    ls.back()->prefix_keys["bar"] = 456;
  }
};
WRITE_CLASS_ENCODER(ScrubResult)

inline ostream& operator<<(ostream& out, const ScrubResult& r) {
  return out << "ScrubResult(keys " << r.prefix_keys << " crc " << r.prefix_crc << ")";
}

/// for information like os, kernel, hostname, memory info, cpu model.
typedef map<string, string> Metadata;

namespace ceph {
  namespace features {
    namespace mon {
      /**
       * Get a feature's name based on its value.
       *
       * @param b raw feature value
       *
       * @remarks
       *    Consumers should not assume this interface will never change.
       * @remarks
       *    As the number of features increase, so may the internal representation
       *    of the raw features. When this happens, this interface will change
       *    accordingly. So should consumers of this interface.
       */
      static inline const char *get_feature_name(uint64_t b);
    }
  }
}


inline const char *ceph_mon_feature_name(uint64_t b)
{
  return ceph::features::mon::get_feature_name(b);
};

class mon_feature_t {

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  // mon-specific features
  uint64_t features;

public:

  explicit constexpr
  mon_feature_t(const uint64_t f) : features(f) { }

  mon_feature_t() :
    features(0) { }

  constexpr
  mon_feature_t(const mon_feature_t &o) :
    features(o.features) { }

  mon_feature_t& operator&=(const mon_feature_t other) {
    features &= other.features;
    return (*this);
  }

  /**
   * Obtain raw features
   *
   * @remarks
   *    Consumers should not assume this interface will never change.
   * @remarks
   *    As the number of features increase, so may the internal representation
   *    of the raw features. When this happens, this interface will change
   *    accordingly. So should consumers of this interface.
   */
  uint64_t get_raw() const {
    return features;
  }

  constexpr
  friend mon_feature_t operator&(const mon_feature_t a,
                                 const mon_feature_t b) {
    return mon_feature_t(a.features & b.features);
  }

  mon_feature_t& operator|=(const mon_feature_t other) {
    features |= other.features;
    return (*this);
  }

  constexpr
  friend mon_feature_t operator|(const mon_feature_t a,
                                 const mon_feature_t b) {
    return mon_feature_t(a.features | b.features);
  }

  constexpr
  friend mon_feature_t operator^(const mon_feature_t a,
                                 const mon_feature_t b) {
    return mon_feature_t(a.features ^ b.features);
  }

  mon_feature_t& operator^=(const mon_feature_t other) {
    features ^= other.features;
    return (*this);
  }

  bool operator==(const mon_feature_t other) const {
    return (features == other.features);
  }

  bool operator!=(const mon_feature_t other) const {
    return (features != other.features);
  }

  bool empty() const {
    return features == 0;
  }

  /**
   * Set difference of our features in respect to @p other
   *
   * Returns all the elements in our features that are not in @p other
   *
   * @returns all the features not in @p other
   */
  mon_feature_t diff(const mon_feature_t other) const {
    return mon_feature_t((features ^ other.features) & features);
  }

  /**
   * Set intersection of our features and @p other
   *
   * Returns all the elements common to both our features and the
   * features of @p other
   *
   * @returns the features common to @p other and us
   */
  mon_feature_t intersection(const mon_feature_t other) const {
    return mon_feature_t((features & other.features));
  }

  /**
   * Checks whether we have all the features in @p other
   *
   * Returns true if we have all the features in @p other
   *
   * @returns true if we contain all the features in @p other
   * @returns false if we do not contain some of the features in @p other
   */
  bool contains_all(const mon_feature_t other) const {
    mon_feature_t d = intersection(other);
    return d == other;
  }

  /**
   * Checks whether we contain any of the features in @p other.
   *
   * @returns true if we contain any of the features in @p other
   * @returns false if we don't contain any of the features in @p other
   */
  bool contains_any(const mon_feature_t other) const {
    mon_feature_t d = intersection(other);
    return !d.empty();
  }

  void set_feature(const mon_feature_t f) {
    features |= f.features;
  }

  void unset_feature(const mon_feature_t f) {
    features &= ~(f.features);
  }

  void print(ostream& out) const {
    out << "[";
    print_bit_str(features, out, ceph::features::mon::get_feature_name);
    out << "]";
  }

  void print_with_value(ostream& out) const {
    out << "[";
    print_bit_str(features, out, ceph::features::mon::get_feature_name, true);
    out << "]";
  }

  void dump(Formatter *f, const char *sec_name = NULL) const {
    f->open_array_section((sec_name ? sec_name : "features"));
    dump_bit_str(features, f, ceph::features::mon::get_feature_name);
    f->close_section();
  }

  void dump_with_value(Formatter *f, const char *sec_name = NULL) const {
    f->open_array_section((sec_name ? sec_name : "features"));
    dump_bit_str(features, f, ceph::features::mon::get_feature_name, true);
    f->close_section();
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(HEAD_VERSION, COMPAT_VERSION, bl);
    encode(features, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
    DECODE_START(COMPAT_VERSION, p);
    decode(features, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(mon_feature_t)

namespace ceph {
  namespace features {
    namespace mon {
      constexpr mon_feature_t FEATURE_KRAKEN(     (1ULL << 0));
      constexpr mon_feature_t FEATURE_LUMINOUS(   (1ULL << 1));
      constexpr mon_feature_t FEATURE_MIMIC(      (1ULL << 2));
      constexpr mon_feature_t FEATURE_OSDMAP_PRUNE (1ULL << 3);
      constexpr mon_feature_t FEATURE_NAUTILUS(    (1ULL << 4));

      constexpr mon_feature_t FEATURE_RESERVED(   (1ULL << 63));
      constexpr mon_feature_t FEATURE_NONE(       (0ULL));

      /**
       * All the features this monitor supports
       *
       * If there's a feature above, it should be OR'ed to this list.
       */
      constexpr mon_feature_t get_supported() {
        return (
	  FEATURE_KRAKEN |
	  FEATURE_LUMINOUS |
	  FEATURE_MIMIC |
          FEATURE_OSDMAP_PRUNE |
	  FEATURE_NAUTILUS |
	  FEATURE_NONE
	  );
      }
      /**
       * All the features that, once set, cannot be removed.
       *
       * Features should only be added to this list if you want to make
       * sure downgrades are not possible after a quorum supporting all
       * these features has been formed.
       *
       * Any feature in this list will be automatically set on the monmap's
       * features once all the monitors in the quorum support it.
       */
      constexpr mon_feature_t get_persistent() {
        return (
	  FEATURE_KRAKEN |
	  FEATURE_LUMINOUS |
	  FEATURE_MIMIC |
	  FEATURE_NAUTILUS |
	  FEATURE_OSDMAP_PRUNE |
	  FEATURE_NONE
	  );
      }

      constexpr mon_feature_t get_optional() {
        return (
          FEATURE_OSDMAP_PRUNE |
          FEATURE_NONE
          );
      }

      static inline mon_feature_t get_feature_by_name(const std::string &n);
    }
  }
}

static inline int infer_ceph_release_from_mon_features(mon_feature_t f)
{
  if (f.contains_all(ceph::features::mon::FEATURE_NAUTILUS)) {
    return CEPH_RELEASE_NAUTILUS;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_MIMIC)) {
    return CEPH_RELEASE_MIMIC;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_LUMINOUS)) {
    return CEPH_RELEASE_LUMINOUS;
  }
  if (f.contains_all(ceph::features::mon::FEATURE_KRAKEN)) {
    return CEPH_RELEASE_KRAKEN;
  }
  return 0;
}

static inline const char *ceph::features::mon::get_feature_name(uint64_t b) {
  mon_feature_t f(b);

  if (f == FEATURE_KRAKEN) {
    return "kraken";
  } else if (f == FEATURE_LUMINOUS) {
    return "luminous";
  } else if (f == FEATURE_MIMIC) {
    return "mimic";
  } else if (f == FEATURE_OSDMAP_PRUNE) {
    return "osdmap-prune";
  } else if (f == FEATURE_NAUTILUS) {
    return "nautilus";
  } else if (f == FEATURE_RESERVED) {
    return "reserved";
  }
  return "unknown";
}

inline mon_feature_t ceph::features::mon::get_feature_by_name(const std::string &n) {

  if (n == "kraken") {
    return FEATURE_KRAKEN;
  } else if (n == "luminous") {
    return FEATURE_LUMINOUS;
  } else if (n == "mimic") {
    return FEATURE_MIMIC;
  } else if (n == "osdmap-prune") {
    return FEATURE_OSDMAP_PRUNE;
  } else if (n == "nautilus") {
    return FEATURE_NAUTILUS;
  } else if (n == "reserved") {
    return FEATURE_RESERVED;
  }
  return FEATURE_NONE;
}

inline ostream& operator<<(ostream& out, const mon_feature_t& f) {
  out << "mon_feature_t(";
  f.print(out);
  out << ")";
  return out;
}


struct ProgressEvent {
  string message;                  ///< event description
  float progress;                  ///< [0..1]

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(message, bl);
    encode(progress, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    decode(message, p);
    decode(progress, p);
    DECODE_FINISH(p);
  }
  void dump(Formatter *f) const {
    f->dump_string("message", message);
    f->dump_float("progress", progress);
  }
};
WRITE_CLASS_ENCODER(ProgressEvent)

#endif
