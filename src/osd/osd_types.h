// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_TYPES_H
#define CEPH_OSD_TYPES_H

#include <atomic>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <variant>

#ifdef WITH_CRIMSON
#include <boost/smart_ptr/local_shared_ptr.hpp>
#endif

#include "include/mempool.h"
#include "common/fmt_common.h"

#include "msg/msg_types.h"
#include "include/common_fwd.h" // for CephContext
#include "include/compat.h"
#include "include/types.h"
#include "include/utime.h"
#include "include/CompatSet.h"
#include "common/dout.h"
#include "common/histogram.h" // for pow2_hist_t
#include "include/interval_set.h"
#include "include/inline_memory.h"
#include "common/Formatter.h"
#include "common/hobject.h"
#include "common/snap_types.h"
#include "common/ceph_mutex.h"
#include "common/strtol.h" // for ritoa()
#include "HitSet.h"
#include "librados/ListObjectImpl.h"
#include "pg_features.h"
#include "ECTypes.h"

#include "osd/osd_types_core.h"
#include "osd/osd_types_stats.h"
#include "osd/osd_types_pool.h"
#include "osd/osd_types_peering.h"
#include "osd/osd_types_op.h"
#include "osd/osd_types_object.h"
#include "osd/osd_types_log.h"
#include "osd/osd_types_client.h"
#include "osd/osd_types_superblock.h"
#include "osd/osd_types_scrub.h"

class CrushWrapper;

/**
 * pg creation info
 */
struct pg_create_t {
  epoch_t created;   // epoch pg created
  pg_t parent;       // split from parent (if != pg_t())
  __s32 split_bits;

  pg_create_t()
    : created(0), split_bits(0) {}
  pg_create_t(unsigned c, pg_t p, int s)
    : created(c), parent(p), split_bits(s) {}

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static std::list<pg_create_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(pg_create_t)

// PromoteCounter

struct PromoteCounter {
  std::atomic<unsigned long long>  attempts{0};
  std::atomic<unsigned long long>  objects{0};
  std::atomic<unsigned long long>  bytes{0};

  void attempt() {
    attempts++;
  }

  void finish(uint64_t size) {
    objects++;
    bytes += size;
  }

  void sample_and_attenuate(uint64_t *a, uint64_t *o, uint64_t *b) {
    *a = attempts;
    *o = objects;
    *b = bytes;
    attempts = *a / 2;
    objects = *o / 2;
    bytes = *b / 2;
  }
};

// prefix pgmeta_oid keys with _ so that PGLog::read_log_and_missing() can
// easily skip them
static const std::string_view infover_key = "_infover";
static const std::string_view info_key = "_info";
static const std::string_view biginfo_key = "_biginfo";
static const std::string_view epoch_key = "_epoch";
static const std::string_view fastinfo_key = "_fastinfo";

static const __u8 pg_latest_struct_v = 10;
// v10 is the new past_intervals encoding
// v9 was fastinfo_key addition
// v8 was the move to a per-pg pgmeta object
// v7 was SnapMapper addition in 86658392516d5175b2756659ef7ffaaf95b0f8ad
// (first appeared in cuttlefish).
static const __u8 pg_compat_struct_v = 10;

int prepare_info_keymap(
  CephContext* cct,
  std::map<std::string,ceph::buffer::list> *km,
  std::string *key_to_remove,
  epoch_t epoch,
  pg_info_t &info,
  pg_info_t &last_written_info,
  PastIntervals &past_intervals,
  bool dirty_big_info,
  bool dirty_epoch,
  bool try_fast_info,
  PerfCounters *logger = nullptr,
  DoutPrefixProvider *dpp = nullptr);

namespace ceph::os {
  class Transaction;
};

void create_pg_collection(
  ceph::os::Transaction& t, spg_t pgid, int bits);

void init_pg_ondisk(
  ceph::os::Transaction& t, spg_t pgid, const pg_pool_t *pool);

// filter for pg listings
class PGLSFilter {
  CephContext* cct;
protected:
  std::string xattr;
public:
  PGLSFilter();
  virtual ~PGLSFilter();
  virtual bool filter(const hobject_t &obj,
                      const ceph::buffer::list& xattr_data) const = 0;

  /**
   * Arguments passed from the RADOS client.  Implementations must
   * handle any encoding errors, and return an appropriate error code,
   * or 0 on valid input.
   */
  virtual int init(ceph::buffer::list::const_iterator &params) = 0;

  /**
   * xattr key, or empty string.  If non-empty, this xattr will be fetched
   * and the value passed into ::filter
   */
  virtual const std::string& get_xattr() const { return xattr; }

  /**
   * If true, objects without the named xattr (if xattr name is not empty)
   * will be rejected without calling ::filter
   */
  virtual bool reject_empty_xattr() const { return true; }
};

class PGLSPlainFilter : public PGLSFilter {
  std::string val;
public:
  int init(ceph::buffer::list::const_iterator &params) override;
  ~PGLSPlainFilter() override {}
  bool filter(const hobject_t& obj,
              const ceph::buffer::list& xattr_data) const override;
};

// alias name for this structure:
using missing_map_t = std::map<hobject_t,
  std::pair<std::optional<uint32_t>,
    std::optional<uint32_t>>>;

#endif
