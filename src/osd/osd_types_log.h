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

#ifndef CEPH_OSD_TYPES_LOG_H
#define CEPH_OSD_TYPES_LOG_H

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "osd/osd_types_core.h"
#include "osd/osd_types_object.h"
#include "osd/osd_types_op.h"
#include "osd/osd_types_pool.h"


class PGBackend;
class ObjectModDesc {
  bool can_local_rollback;
  bool rollback_info_completed;

  // version required to decode, reflected in encode/decode version
  __u8 max_required_version = 1;
public:
  class Visitor {
  public:
    virtual void append(uint64_t old_offset) {}
    virtual void setattrs(std::map<std::string, std::optional<ceph::buffer::list>> &attrs) {}
    virtual void ec_omap(bool clear_omap, std::optional<ceph::buffer::list> omap_header,
      std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> &omap_updates) {}
    virtual void rmobject(version_t old_version) {}
    /**
     * Used to support the unfound_lost_delete log event: if the stashed
     * version exists, we unstash it, otherwise, we do nothing.  This way
     * each replica rolls back to whatever state it had prior to the attempt
     * at mark unfound lost delete
     */
    virtual void try_rmobject(version_t old_version) {
      rmobject(old_version);
    }
    virtual void create() {}
    virtual void update_snaps(const std::set<snapid_t> &old_snaps) {}
    virtual void rollback_extents(
      const version_t gen,
      const std::vector<std::pair<uint64_t, uint64_t>> &extents,
      const uint64_t object_size,
      const std::vector<shard_id_set> &shards) {}
    virtual ~Visitor() {}
  };
  void visit(Visitor *visitor) const;
  mutable ceph::buffer::list bl;
  enum ModID {
    APPEND = 1,
    SETATTRS = 2,
    DELETE = 3,
    CREATE = 4,
    UPDATE_SNAPS = 5,
    TRY_DELETE = 6,
    ROLLBACK_EXTENTS = 7,
    EC_OMAP = 8
  };
  ObjectModDesc() : can_local_rollback(true), rollback_info_completed(false) {
    bl.reassign_to_mempool(mempool::mempool_osd_pglog);
  }
  void claim(ObjectModDesc &other) {
    bl = std::move(other.bl);
    can_local_rollback = other.can_local_rollback;
    rollback_info_completed = other.rollback_info_completed;
  }
  void claim_append(ObjectModDesc &other) {
    if (!can_local_rollback || rollback_info_completed) {
      return;
    }
    if (!other.can_local_rollback) {
      mark_unrollbackable();
      return;
    }
    bl.claim_append(other.bl);
    rollback_info_completed = other.rollback_info_completed;
  }
  void swap(ObjectModDesc &other) {
    bl.swap(other.bl);

    using std::swap;
    swap(other.can_local_rollback, can_local_rollback);
    swap(other.rollback_info_completed, rollback_info_completed);
    swap(other.max_required_version, max_required_version);
  }
  void append_id(ModID id) {
    using ceph::encode;
    uint8_t _id(id);
    encode(_id, bl);
  }
  void append(uint64_t old_size) {
    if (!can_local_rollback || rollback_info_completed) {
      return;
    }
    ENCODE_START(1, 1, bl);
    append_id(APPEND);
    encode(old_size, bl);
    ENCODE_FINISH(bl);
  }
  void setattrs(std::map<std::string, std::optional<ceph::buffer::list>> &old_attrs) {
    if (!can_local_rollback || rollback_info_completed) {
      return;
    }
    ENCODE_START(1, 1, bl);
    append_id(SETATTRS);
    encode(old_attrs, bl);
    ENCODE_FINISH(bl);
  }
  void ec_omap(bool clear_omap, std::optional<ceph::buffer::list> omap_header,
    std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> &omap_updates) {
    if(!can_local_rollback) {
      return;
    }
    ENCODE_START(1, 1, bl);
    append_id(EC_OMAP);
    encode(clear_omap, bl);
    encode(omap_header, bl);
    encode(omap_updates, bl);
    ENCODE_FINISH(bl);
  }
  bool rmobject(version_t deletion_version) {
    if (!can_local_rollback || rollback_info_completed) {
      return false;
    }
    ENCODE_START(1, 1, bl);
    append_id(DELETE);
    encode(deletion_version, bl);
    ENCODE_FINISH(bl);
    rollback_info_completed = true;
    return true;
  }
  bool try_rmobject(version_t deletion_version) {
    if (!can_local_rollback || rollback_info_completed) {
      return false;
    }
    ENCODE_START(1, 1, bl);
    append_id(TRY_DELETE);
    encode(deletion_version, bl);
    ENCODE_FINISH(bl);
    rollback_info_completed = true;
    return true;
  }
  void create() {
    if (!can_local_rollback || rollback_info_completed) {
      return;
    }
    rollback_info_completed = true;
    ENCODE_START(1, 1, bl);
    append_id(CREATE);
    ENCODE_FINISH(bl);
  }
  void update_snaps(const std::set<snapid_t> &old_snaps) {
    if (!can_local_rollback || rollback_info_completed) {
      return;
    }
    ENCODE_START(1, 1, bl);
    append_id(UPDATE_SNAPS);
    encode(old_snaps, bl);
    ENCODE_FINISH(bl);
  }
  void rollback_extents(
   const version_t gen,
   const std::vector<std::pair<uint64_t, uint64_t>> &extents,
   const uint64_t object_size,
   const std::vector<shard_id_set> &shards) {
    ceph_assert(can_local_rollback);
    ceph_assert(!rollback_info_completed);
    if (max_required_version < 2) {
      max_required_version = 2;
    }
    ENCODE_START(3, 2, bl);
    append_id(ROLLBACK_EXTENTS);
    encode(gen, bl);
    encode(extents, bl);
    encode(object_size, bl);
    encode(shards, bl);
    ENCODE_FINISH(bl);
  }

  // Version for legacy EC (can be deleted when EC*L.cc is deleted.
  void rollback_extents(
   const version_t gen,
   const std::vector<std::pair<uint64_t, uint64_t>> &extents) {
    ceph_assert(can_local_rollback);
    ceph_assert(!rollback_info_completed);
    if (max_required_version < 2) {
      max_required_version = 2;
    }
    ENCODE_START(2, 2, bl);
    append_id(ROLLBACK_EXTENTS);
    encode(gen, bl);
    encode(extents, bl);
    ENCODE_FINISH(bl);
  }

  // cannot be rolled back
  void mark_unrollbackable() {
    can_local_rollback = false;
    bl.clear();
  }
  bool can_rollback() const {
    return can_local_rollback;
  }
  bool empty() const {
    return can_local_rollback && (bl.length() == 0);
  }

  bool requires_kraken() const {
    return max_required_version >= 2;
  }

  /**
   * Create fresh copy of bl bytes to avoid keeping large buffers around
   * in the case that bl contains ptrs which point into a much larger
   * message buffer
   */
  void trim_bl() const {
    if (bl.length() > 0) {
      bl.rebuild();
    }
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static std::list<ObjectModDesc> generate_test_instances();
};
WRITE_CLASS_ENCODER(ObjectModDesc)
/**
 * pg_log_entry_t - single entry/event in pg log
 *
 */
struct pg_log_entry_t {
  enum {
    MODIFY = 1,   // some unspecified modification (but not *all* modifications)
    CLONE = 2,    // cloned object from head
    DELETE = 3,   // deleted object
    //BACKLOG = 4,  // event invented by generate_backlog [obsolete]
    LOST_REVERT = 5, // lost new version, revert to an older version.
    LOST_DELETE = 6, // lost new version, revert to no object (deleted).
    LOST_MARK = 7,   // lost new version, now EIO
    PROMOTE = 8,     // promoted object from another tier
    CLEAN = 9,       // mark an object clean
    ERROR = 10,      // write that returned an error
    REPLACE = 11,    // replace (delete + recreate) operation
  };
  static const char *get_op_name(int op) {
    switch (op) {
    case MODIFY:
      return "modify";
    case PROMOTE:
      return "promote";
    case CLONE:
      return "clone";
    case DELETE:
      return "delete";
    case LOST_REVERT:
      return "l_revert";
    case LOST_DELETE:
      return "l_delete";
    case LOST_MARK:
      return "l_mark";
    case CLEAN:
      return "clean";
    case ERROR:
      return "error";
    case REPLACE:
      return "replace";
    default:
      return "unknown";
    }
  }
  const char *get_op_name() const {
    return get_op_name(op);
  }

  // describes state for a locally-rollbackable entry
  ObjectModDesc mod_desc;
  ceph::buffer::list snaps;   // only for clone entries
  hobject_t  soid;
  osd_reqid_t reqid;  // caller+tid to uniquely identify request
  mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > extra_reqids;

  /// map extra_reqids by index to error return code (if any)
  mempool::osd_pglog::map<uint32_t, int> extra_reqid_return_codes;

  eversion_t version, prior_version, reverting_to;
  version_t user_version; // the user version for this entry
  utime_t     mtime;  // this is the _user_ mtime, mind you
  int32_t return_code; // only stored for ERRORs for dup detection

  std::vector<pg_log_op_return_item_t> op_returns;

  __s32      op;
  bool invalid_hash; // only when decoding sobject_t based entries
  bool invalid_pool; // only when decoding pool-less hobject based entries
  ObjectCleanRegions clean_regions;

  shard_id_set written_shards; // EC partial writes do not update every shard

  pg_log_entry_t()
   : user_version(0), return_code(0), op(0),
     invalid_hash(false), invalid_pool(false) {
    snaps.reassign_to_mempool(mempool::mempool_osd_pglog);
  }
  pg_log_entry_t(int _op, const hobject_t& _soid,
                const eversion_t& v, const eversion_t& pv,
                version_t uv,
                const osd_reqid_t& rid, const utime_t& mt,
                int return_code)
   : soid(_soid), reqid(rid), version(v), prior_version(pv), user_version(uv),
     mtime(mt), return_code(return_code), op(_op),
     invalid_hash(false), invalid_pool(false) {
    snaps.reassign_to_mempool(mempool::mempool_osd_pglog);
  }

  bool is_clone() const { return op == CLONE; }
  bool is_modify() const { return op == MODIFY; }
  bool is_promote() const { return op == PROMOTE; }
  bool is_clean() const { return op == CLEAN; }
  bool is_lost_revert() const { return op == LOST_REVERT; }
  bool is_lost_delete() const { return op == LOST_DELETE; }
  bool is_lost_mark() const { return op == LOST_MARK; }
  bool is_error() const { return op == ERROR; }
  bool is_replace() const { return op == REPLACE; }

  bool is_update() const {
    return
      is_clone() || is_modify() || is_promote() || is_clean() ||
      is_lost_revert() || is_lost_mark() || is_replace();
  }
  bool is_delete() const {
    return op == DELETE || op == LOST_DELETE;
  }

  bool can_rollback() const {
    return mod_desc.can_rollback();
  }

  void mark_unrollbackable() {
    mod_desc.mark_unrollbackable();
  }

  bool requires_kraken() const {
    return mod_desc.requires_kraken();
  }

  // Errors are only used for dup detection, whereas
  // the index by objects is used by recovery, copy_get,
  // and other facilities that don't expect or need to
  // be aware of error entries.
  bool object_is_indexed() const {
    return !is_error();
  }

  bool reqid_is_indexed() const {
    return reqid != osd_reqid_t() &&
      (op == MODIFY || op == DELETE || op == ERROR || op == REPLACE);
  }

  void set_op_returns(const std::vector<OSDOp>& ops) {
    op_returns.resize(ops.size());
    for (unsigned i = 0; i < ops.size(); ++i) {
      op_returns[i].rval = ops[i].rval;
      op_returns[i].bl = ops[i].outdata;
    }
  }

  std::string get_key_name() const;

  /// EC partial writes: test if a shard was written
  bool is_written_shard(const shard_id_t shard) const {
    return written_shards.empty() || written_shards.contains(shard);
  }

  void encode_with_checksum(ceph::buffer::list& bl) const;
  void decode_with_checksum(ceph::buffer::list::const_iterator& p);

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  std::string fmt_print() const;
  static std::list<pg_log_entry_t> generate_test_instances();

};
WRITE_CLASS_ENCODER(pg_log_entry_t)

std::ostream& operator<<(std::ostream& out, const pg_log_entry_t& e);

struct pg_log_dup_t {
  osd_reqid_t reqid;  // caller+tid to uniquely identify request
  eversion_t version;
  version_t user_version; // the user version for this entry
  int32_t return_code; // only stored for ERRORs for dup detection

  std::vector<pg_log_op_return_item_t> op_returns;

  pg_log_dup_t()
    : user_version(0), return_code(0)
  {}
  explicit pg_log_dup_t(const pg_log_entry_t& entry)
    : reqid(entry.reqid), version(entry.version),
      user_version(entry.user_version),
      return_code(entry.return_code),
      op_returns(entry.op_returns)
  {}
  pg_log_dup_t(const eversion_t& v, version_t uv,
	       const osd_reqid_t& rid, int return_code)
    : reqid(rid), version(v), user_version(uv),
      return_code(return_code)
  {}

  std::string get_key_name() const;
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static std::list<pg_log_dup_t> generate_test_instances();

  bool operator==(const pg_log_dup_t &rhs) const {
    return reqid == rhs.reqid &&
      version == rhs.version &&
      user_version == rhs.user_version &&
      return_code == rhs.return_code &&
      op_returns == rhs.op_returns;
  }
  bool operator!=(const pg_log_dup_t &rhs) const {
    return !(*this == rhs);
  }

  friend std::ostream& operator<<(std::ostream& out, const pg_log_dup_t& e);
};
WRITE_CLASS_ENCODER(pg_log_dup_t)

std::ostream& operator<<(std::ostream& out, const pg_log_dup_t& e);

/**
 * pg_log_t - incremental log of recent pg changes.
 *
 *  serves as a recovery queue for recent changes.
 */
struct pg_log_t {
  /*
   *   head - newest entry (update|delete)
   *   tail - entry previous to oldest (update|delete) for which we have
   *          complete negative information.  
   * i.e. we can infer pg contents for any store whose last_update >= tail.
   */
  eversion_t head;    // newest entry
  eversion_t tail;    // version prior to oldest

protected:
  // We can rollback rollback-able entries > can_rollback_to
  eversion_t can_rollback_to;

  // always <= can_rollback_to, indicates how far stashed rollback
  // data can be found
  eversion_t rollback_info_trimmed_to;

public:
  // the actual log
  mempool::osd_pglog::list<pg_log_entry_t> log;

  // entries just for dup op detection ordered oldest to newest
  mempool::osd_pglog::list<pg_log_dup_t> dups;

  pg_log_t() = default;
  pg_log_t(const eversion_t &last_update,
	   const eversion_t &log_tail,
	   const eversion_t &can_rollback_to,
	   const eversion_t &rollback_info_trimmed_to,
	   mempool::osd_pglog::list<pg_log_entry_t> &&entries,
	   mempool::osd_pglog::list<pg_log_dup_t> &&dup_entries)
    : head(last_update), tail(log_tail), can_rollback_to(can_rollback_to),
      rollback_info_trimmed_to(rollback_info_trimmed_to),
      log(std::move(entries)), dups(std::move(dup_entries)) {}
  pg_log_t(const eversion_t &last_update,
	   const eversion_t &log_tail,
	   const eversion_t &can_rollback_to,
	   const eversion_t &rollback_info_trimmed_to,
	   const std::list<pg_log_entry_t> &entries,
	   const std::list<pg_log_dup_t> &dup_entries)
    : head(last_update), tail(log_tail), can_rollback_to(can_rollback_to),
      rollback_info_trimmed_to(rollback_info_trimmed_to) {
    for (auto &&entry: entries) {
      log.push_back(entry);
    }
    for (auto &&entry: dup_entries) {
      dups.push_back(entry);
    }
  }

  void clear() {
    eversion_t z;
    rollback_info_trimmed_to = can_rollback_to = head = tail = z;
    log.clear();
    dups.clear();
  }

  eversion_t get_rollback_info_trimmed_to() const {
    return rollback_info_trimmed_to;
  }
  eversion_t get_can_rollback_to() const {
    return can_rollback_to;
  }


  pg_log_t split_out_child(pg_t child_pgid, unsigned split_bits) {
    mempool::osd_pglog::list<pg_log_entry_t> oldlog, childlog;
    oldlog.swap(log);

    eversion_t old_tail;
    unsigned mask = ~((~0)<<split_bits);
    for (auto i = oldlog.begin();
	 i != oldlog.end();
      ) {
      if ((i->soid.get_hash() & mask) == child_pgid.m_seed) {
	childlog.push_back(*i);
      } else {
	log.push_back(*i);
      }
      oldlog.erase(i++);
    }

    // osd_reqid is unique, so it doesn't matter if there are extra
    // dup entries in each pg. To avoid storing oid with the dup
    // entries, just copy the whole list.
    auto childdups(dups);

    return pg_log_t(
      head,
      tail,
      can_rollback_to,
      rollback_info_trimmed_to,
      std::move(childlog),
      std::move(childdups));
    }

  mempool::osd_pglog::list<pg_log_entry_t> rewind_from_head(eversion_t newhead, bool *dirty_log = nullptr) {
    ceph_assert(newhead >= tail);

    mempool::osd_pglog::list<pg_log_entry_t>::iterator p = log.end();
    mempool::osd_pglog::list<pg_log_entry_t> divergent;
    while (true) {
      if (p == log.begin()) {
	// yikes, the whole thing is divergent!
	using std::swap;
	swap(divergent, log);
	break;
      }
      --p;
      if (p->version.version <= newhead.version) {
	/*
	 * look at eversion.version here.  we want to avoid a situation like:
	 *  our log: 100'10 (0'0) m 10000004d3a.00000000/head by client4225.1:18529
	 *  new log: 122'10 (0'0) m 10000004d3a.00000000/head by client4225.1:18529
	 *  lower_bound = 100'9
	 * i.e, same request, different version.  If the eversion.version is > the
	 * lower_bound, we it is divergent.
	 */
	++p;
	divergent.splice(divergent.begin(), log, p, log.end());
	break;
      }
      ceph_assert(p->version > newhead);
    }
    head = newhead;

    if (can_rollback_to > newhead) {
      can_rollback_to = newhead;
      if (dirty_log) {
	*dirty_log = true;
      }
    }

    if (rollback_info_trimmed_to > newhead) {
      rollback_info_trimmed_to = newhead;
      if (dirty_log) {
	*dirty_log = true;
      }
    }

    return divergent;
  }

  void merge_from(const std::vector<pg_log_t*>& slogs, eversion_t last_update) {
    log.clear();

    // sort and merge dups
    std::multimap<eversion_t,pg_log_dup_t> sorted;
    for (auto& d : dups) {
      sorted.emplace(d.version, d);
    }
    for (auto l : slogs) {
      for (auto& d : l->dups) {
	sorted.emplace(d.version, d);
      }
    }
    dups.clear();
    for (auto& i : sorted) {
      dups.push_back(i.second);
    }

    head = last_update;
    tail = last_update;
    can_rollback_to = last_update;
    rollback_info_trimmed_to = last_update;
  }

  bool empty() const {
    return log.empty();
  }

  bool null() const {
    return head.version == 0 && head.epoch == 0;
  }

  uint64_t approx_size() const {
    return head.version - tail.version;
  }

  static void filter_log(spg_t import_pgid, const OSDMap &curmap,
    const std::string &hit_set_namespace, const pg_log_t &in,
    pg_log_t &out, pg_log_t &reject);

  /**
   * copy entries from the tail of another pg_log_t
   *
   * @param other pg_log_t to copy from
   * @param from copy entries after this version
   */
  void copy_after(CephContext* cct, const pg_log_t &other, eversion_t from);

  /**
   * copy up to N entries
   *
   * @param other source log
   * @param max max number of entries to copy
   */
  void copy_up_to(CephContext* cct, const pg_log_t &other, int max);

  std::ostream& print(std::ostream& out) const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl, int64_t pool = -1);
  void dump(ceph::Formatter *f) const;
  static std::list<pg_log_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(pg_log_t)

inline std::ostream& operator<<(std::ostream& out, const pg_log_t& log)
{
  out << "log((" << log.tail << "," << log.head << "], crt="
      << log.get_can_rollback_to() << ")";
  return out;
}


/**
 * pg_missing_t - summary of missing objects.
 *
 *  kept in memory, as a supplement to pg_log_t
 *  also used to pass missing info in messages.
 */
struct pg_missing_item {
  eversion_t need, have;
  ObjectCleanRegions clean_regions;
  enum missing_flags_t {
    FLAG_NONE = 0,
    FLAG_DELETE = 1,
  } flags;
  pg_missing_item() : flags(FLAG_NONE) {}
  explicit pg_missing_item(eversion_t n) : need(n), flags(FLAG_NONE) {}  // have no old version
  pg_missing_item(eversion_t n, eversion_t h, bool is_delete=false, bool old_style = false) :
    need(n), have(h) {
    set_delete(is_delete);
    if (old_style)
      clean_regions.mark_fully_dirty();
    if (have == eversion_t())
      clean_regions.mark_object_new();
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const {
    using ceph::encode;
    if (HAVE_FEATURE(features, SERVER_OCTOPUS)) {
      // encoding a zeroed eversion_t to differentiate between OSD_RECOVERY_DELETES、
      // SERVER_OCTOPUS and legacy unversioned encoding - a need value of 0'0 is not
      // possible. This can be replaced with the legacy encoding
      encode(eversion_t(), bl);
      encode(eversion_t(-1, -1), bl);
      encode(need, bl);
      encode(have, bl);   
      encode(static_cast<uint8_t>(flags), bl);
      encode(clean_regions, bl);
    } else {
      encode(eversion_t(), bl);
      encode(need, bl);
      encode(have, bl);
      encode(static_cast<uint8_t>(flags), bl);
    }
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    eversion_t e, l;
    decode(e, bl);
    decode(l, bl);
    if(l == eversion_t(-1, -1)) {
      // support all
      decode(need, bl);
      decode(have, bl);
      uint8_t f;
      decode(f, bl);
      flags = static_cast<missing_flags_t>(f);
      decode(clean_regions, bl);
     } else {
      // support OSD_RECOVERY_DELETES
      need = l;
      decode(have, bl);
      uint8_t f;
      decode(f, bl);
      flags = static_cast<missing_flags_t>(f); 
      clean_regions.mark_fully_dirty();
    }
  }

  void set_delete(bool is_delete) {
    flags = is_delete ? FLAG_DELETE : FLAG_NONE;
  }

  bool is_delete() const {
    return (flags & FLAG_DELETE) == FLAG_DELETE;
  }

  std::string flag_str() const {
    if (flags == FLAG_NONE) {
      return "none";
    } else {
      return "delete";
    }
  }

  void dump(ceph::Formatter *f) const {
    f->dump_stream("need") << need;
    f->dump_stream("have") << have;
    f->dump_stream("flags") << flag_str();
    f->dump_stream("clean_regions") << clean_regions;
  }
  static std::list<pg_missing_item> generate_test_instances() {
    std::list<pg_missing_item> o;
    o.emplace_back();
    o.emplace_back();
    o.back().need = eversion_t(1, 2);
    o.back().have = eversion_t(1, 1);
    o.emplace_back();
    o.back().need = eversion_t(3, 5);
    o.back().have = eversion_t(3, 4);
    o.back().clean_regions.mark_data_region_dirty(4096, 8192);
    o.back().clean_regions.mark_omap_dirty();
    o.back().flags = FLAG_DELETE;
    return o;
  }
  bool operator==(const pg_missing_item &rhs) const {
    return need == rhs.need && have == rhs.have && flags == rhs.flags;
  }
  bool operator!=(const pg_missing_item &rhs) const {
    return !(*this == rhs);
  }
};
WRITE_CLASS_ENCODER_FEATURES(pg_missing_item)
std::ostream& operator<<(std::ostream& out, const pg_missing_item &item);
#if FMT_VERSION >= 90000
template <> struct fmt::formatter<pg_missing_item> : fmt::ostream_formatter {};
#endif

class pg_missing_const_i {
public:
  virtual const std::map<hobject_t, pg_missing_item> &
    get_items() const = 0;
  virtual const std::multimap<eversion_t, hobject_t> &get_rmissing() const = 0;
  virtual bool get_may_include_deletes() const = 0;
  virtual unsigned int num_missing() const = 0;
  virtual bool have_missing() const = 0;
  virtual bool is_missing(const hobject_t& oid, pg_missing_item *out = nullptr) const = 0;
  virtual bool is_missing(const hobject_t& oid, eversion_t v) const = 0;
  virtual ~pg_missing_const_i() {}
};


template <bool Track>
class ChangeTracker {
public:
  void changed(const hobject_t &obj) {}
  template <typename F>
  void get_changed(F &&f) const {}
  void flush() {}
  bool is_clean() const {
    return true;
  }
};
template <>
class ChangeTracker<true> {
  std::set<hobject_t> _changed;
public:
  void changed(const hobject_t &obj) {
    _changed.insert(obj);
  }
  template <typename F>
  void get_changed(F &&f) const {
    for (auto const &i: _changed) {
      f(i);
    }
  }
  void flush() {
    _changed.clear();
  }
  bool is_clean() const {
    return _changed.empty();
  }
};

template <bool TrackChanges>
class pg_missing_set : public pg_missing_const_i {
  using item = pg_missing_item;
  std::map<hobject_t, item> missing;  // oid -> (need v, have v)
  /**
   * rmissing
   *
   * Reverse mapping from pg_missing_item::need -> object
   *
   * This mapping uses eversion_t as a key because although the log
   * and missing sets may not contain two entries with the same version,
   * this doesn't hold *during* the log merge process because
   * the order of divergent entries may not match the order of the
   * corresponding entries in the authoritiative log.
   *
   * See https://tracker.ceph.com/issues/74306
   */
  std::multimap<eversion_t, hobject_t> rmissing;  // v -> oid
  ChangeTracker<TrackChanges> tracker;
private:
  // Private wrapper functions for rmissing manipulation
  // These ensure rmissing can only be modified through controlled interfaces
  
  // Erase a version mapping, returns count of erased elements (0 or 1)
  size_t rmissing_erase(const eversion_t& version, const hobject_t& oid) {
    auto range = rmissing.equal_range(version);
    for (auto it = range.first; it != range.second; ++it) {
      if (it->second == oid) {
        rmissing.erase(it);
        return 1;
      }
    }
    // If we get here, the (version, oid) pair wasn't found
    return 0;
  }

  // Insert a version-to-object mapping while allowing distinct objects to
  // legitimately share the same version. The same object still may not be
  // inserted twice for that version.
  void rmissing_insert(
      const eversion_t& version,
      const hobject_t& object) {
    auto it = rmissing.lower_bound(version);

    if (it != rmissing.end() && it->first == version) {
      auto range = rmissing.equal_range(version);
      for (auto check_it = range.first; check_it != range.second; ++check_it) {
        if (check_it->second == object) {
          return; // Entry already exists.
        }
      }
    }

    rmissing.insert(it, {version, object});
  }

public:
  pg_missing_set() = default;

  template <typename missing_type>
  pg_missing_set(const missing_type &m) {
    missing = m.get_items();
    rmissing = m.get_rmissing();
    may_include_deletes = m.get_may_include_deletes();
    for (auto &&i: missing)
      tracker.changed(i.first);
  }

  bool may_include_deletes = false;

  const std::map<hobject_t, item> &get_items() const override {
    return missing;
  }
  const std::multimap<eversion_t, hobject_t> &get_rmissing() const override {
    return rmissing;
  }
  bool get_may_include_deletes() const override {
    return may_include_deletes;
  }
  unsigned int num_missing() const override {
    return missing.size();
  }
  bool have_missing() const override {
    return !missing.empty();
  }
  void merge(const pg_log_entry_t& e) {
    auto miter = missing.find(e.soid);
    if (miter != missing.end() && miter->second.have != eversion_t() && e.version > miter->second.have)
      miter->second.clean_regions.merge(e.clean_regions);
  }
  bool is_missing(const hobject_t& oid, pg_missing_item *out = nullptr) const override {
    auto iter = missing.find(oid);
    if (iter == missing.end())
      return false;
    if (out)
      *out = iter->second;
    return true;
  }
  bool is_missing(const hobject_t& oid, eversion_t v) const override {
    std::map<hobject_t, item>::const_iterator m =
      missing.find(oid);
    if (m == missing.end())
      return false;
    const item &item(m->second);
    if (item.need > v)
      return false;
    return true;
  }

  bool is_missing_any_head_or_clone_of(const hobject_t& oid) const {
    return missing.lower_bound(oid.get_object_boundary()) !=
      missing.lower_bound(oid.get_max_object_boundary());
  }

  eversion_t get_oldest_need() const {
    if (missing.empty()) {
      return eversion_t();
    }
    auto it = missing.find(rmissing.begin()->second);
    ceph_assert(it != missing.end());
    return it->second.need;
  }

  void claim(pg_missing_set&& o) {
    static_assert(!TrackChanges, "Can't use claim with TrackChanges");
    missing = std::move(o.missing);
    rmissing = std::move(o.rmissing);
  }

  /*
   * this needs to be called in log order as we extend the log.  it
   * assumes missing is accurate up through the previous log entry.
   */
  void add_next_event(const pg_log_entry_t& e, const pg_pool_t &pool, shard_id_t shard) {
    std::map<hobject_t, item>::iterator missing_it;
    missing_it = missing.find(e.soid);
    bool is_missing_divergent_item = missing_it != missing.end();
    bool skipped = false;
    if (e.prior_version == eversion_t() || e.is_clone()) {
      // new object.
      if (is_missing_divergent_item) {  // use iterator
        auto erased = rmissing_erase(missing_it->second.need, e.soid);
        ceph_assert(erased == 1);  // Should always erase exactly one entry
        // .have = nil
        missing_it->second = item(e.version, eversion_t(), e.is_delete());
        missing_it->second.clean_regions.mark_fully_dirty();
      } else if (pool.is_nonprimary_shard(shard) && !e.is_written_shard(shard)) {
	// new object, partial write and not already missing - skip
	skipped = true;
      } else {
         // create new element in missing map
         // .have = nil
        missing[e.soid] = item(e.version, eversion_t(), e.is_delete());
        missing[e.soid].clean_regions.mark_fully_dirty();
      }
    } else if (is_missing_divergent_item) {
      // already missing (prior).
      auto erased = rmissing_erase((missing_it->second).need, e.soid);
      ceph_assert(erased == 1);  // Should always erase exactly one entry
      missing_it->second.need = e.version;  // leave .have unchanged.
      missing_it->second.set_delete(e.is_delete());
      if (e.is_lost_revert())
        missing_it->second.clean_regions.mark_fully_dirty();
      else
        missing_it->second.clean_regions.merge(e.clean_regions);
    } else if (pool.is_nonprimary_shard(shard) && !e.is_written_shard(shard)) {
      // existing object, partial write and not already missing - skip
      skipped = true;
    } else {
      // not missing, we must have prior_version (if any)
      ceph_assert(!is_missing_divergent_item);
      missing[e.soid] = item(e.version, e.prior_version, e.is_delete());
      if (e.is_lost_revert())
        missing[e.soid].clean_regions.mark_fully_dirty();
      else
        missing[e.soid].clean_regions = e.clean_regions;
    }
    if (!skipped) {
      rmissing_insert(e.version, e.soid);
      tracker.changed(e.soid);
    }
  }

  void revise_need(hobject_t oid, eversion_t need, bool is_delete) {
    auto p = missing.find(oid);
    if (p != missing.end()) {
      auto erased = rmissing_erase((p->second).need, oid);
      ceph_assert(erased == 1);  // Should always erase exactly one entry
      p->second.need = need;          // do not adjust .have
      p->second.set_delete(is_delete);
      p->second.clean_regions.mark_fully_dirty();
    } else {
      missing[oid] = item(need, eversion_t(), is_delete);
      missing[oid].clean_regions.mark_fully_dirty();
    }
    rmissing_insert(need, oid);
    tracker.changed(oid);
  }

  void revise_have(hobject_t oid, eversion_t have) {
    auto p = missing.find(oid);
    if (p != missing.end()) {
      tracker.changed(oid);
      (p->second).have = have;
    }
  }

  void mark_fully_dirty(const hobject_t& oid) {
    auto p = missing.find(oid);
    if (p != missing.end()) {
      tracker.changed(oid);
      (p->second).clean_regions.mark_fully_dirty();
    }
  }

  void add(const hobject_t& oid, eversion_t need, eversion_t have,
	   bool is_delete) {
    missing[oid] = item(need, have, is_delete, true);
    rmissing_insert(need, oid);
    tracker.changed(oid);
  }

  void add(const hobject_t& oid, pg_missing_item&& item) {
    rmissing_insert(item.need, oid);
    missing.insert({oid, std::move(item)});
    tracker.changed(oid);
  }

  void rm(const hobject_t& oid, eversion_t v) {
    std::map<hobject_t, item>::iterator p = missing.find(oid);
    if (p != missing.end() && p->second.need <= v)
      rm(p);
  }

  void rm(std::map<hobject_t, item>::const_iterator m) {
    tracker.changed(m->first);
    auto erased = rmissing_erase(m->second.need, m->first);
    ceph_assert(erased == 1);  // Should always erase exactly one entry
    missing.erase(m);
  }

  void got(const hobject_t& oid, eversion_t v) {
    std::map<hobject_t, item>::iterator p = missing.find(oid);
    ceph_assert(p != missing.end());
    ceph_assert(p->second.need <= v || p->second.is_delete());
    got(p);
  }

  void got(std::map<hobject_t, item>::const_iterator m) {
    tracker.changed(m->first);
    auto erased = rmissing_erase(m->second.need, m->first);
    ceph_assert(erased == 1);  // Should always erase exactly one entry
    missing.erase(m);
  }

  void split_into(
    pg_t child_pgid,
    unsigned split_bits,
    pg_missing_set *omissing) {
    omissing->may_include_deletes = may_include_deletes;
    unsigned mask = ~((~0)<<split_bits);
    for (std::map<hobject_t, item>::iterator i = missing.begin();
	 i != missing.end();
      ) {
      if ((i->first.get_hash() & mask) == child_pgid.m_seed) {
	omissing->add(i->first, i->second.need, i->second.have,
		      i->second.is_delete());
	rm(i++);
      } else {
	++i;
      }
    }
  }

  void clear() {
    for (auto const &i: missing)
      tracker.changed(i.first);
    missing.clear();
    rmissing.clear();
  }

  void encode(ceph::buffer::list &bl, uint64_t features) const {
    ENCODE_START(5, 2, bl)
    encode(missing, bl, features);
    encode(may_include_deletes, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl, int64_t pool = -1) {
    for (auto const &i: missing)
      tracker.changed(i.first);
    DECODE_START_LEGACY_COMPAT_LEN(5, 2, 2, bl);
    decode(missing, bl);
    if (struct_v >= 4) {
      decode(may_include_deletes, bl);
    }
    DECODE_FINISH(bl);

    if (struct_v < 3) {
      // Handle hobject_t upgrade
      std::map<hobject_t, item> tmp;
      for (std::map<hobject_t, item>::iterator i =
	     missing.begin();
	   i != missing.end();
	) {
	if (!i->first.is_max() && i->first.pool == -1) {
	  hobject_t to_insert(i->first);
	  to_insert.pool = pool;
	  tmp[to_insert] = i->second;
	  missing.erase(i++);
	} else {
	  ++i;
	}
      }
      missing.insert(tmp.begin(), tmp.end());
    }

    for (std::map<hobject_t,item>::iterator it =
	   missing.begin();
	 it != missing.end();
	 ++it) {
      rmissing_insert(it->second.need, it->first);
    }
    for (auto const &i: missing)
      tracker.changed(i.first);
  }
  void dump(ceph::Formatter *f) const {
    f->open_array_section("missing");
    for (std::map<hobject_t,item>::const_iterator p =
	   missing.begin(); p != missing.end(); ++p) {
      f->open_object_section("item");
      f->dump_stream("object") << p->first;
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
    f->dump_bool("may_include_deletes", may_include_deletes);
  }
  template <typename F>
  void filter_objects(F &&f) {
    for (auto i = missing.begin(); i != missing.end();) {
      if (f(i->first)) {
	rm(i++);
      } else {
        ++i;
      }
    }
  }
  static std::list<pg_missing_set> generate_test_instances() {
    std::list<pg_missing_set> o;
    o.emplace_back();
    o.back().may_include_deletes = true;
    o.emplace_back();
    o.back().add(
      hobject_t(object_t("foo"), "foo", 123, 456, 0, ""),
      eversion_t(5, 6), eversion_t(5, 1), false);
    o.back().may_include_deletes = true;
    o.emplace_back();
    o.back().add(
      hobject_t(object_t("foo"), "foo", 123, 456, 0, ""),
      eversion_t(5, 6), eversion_t(5, 1), true);
    o.back().may_include_deletes = true;
    return o;
  }
  template <typename F>
  void get_changed(F &&f) const {
    tracker.get_changed(f);
  }
  void flush() {
    tracker.flush();
  }
  bool is_clean() const {
    return tracker.is_clean();
  }
  template <typename missing_t>
  bool debug_verify_from_init(
    const missing_t &init_missing,
    std::ostream *oss) const {
    if (!TrackChanges)
      return true;
    auto check_missing(init_missing.get_items());
    tracker.get_changed([&](const hobject_t &hoid) {
	check_missing.erase(hoid);
	if (missing.count(hoid)) {
	  check_missing.insert(*(missing.find(hoid)));
	}
      });
    bool ok = true;
    if (check_missing.size() != missing.size()) {
      if (oss) {
	*oss << "Size mismatch, check: " << check_missing.size()
	     << ", actual: " << missing.size() << "\n";
      }
      ok = false;
    }
    for (auto &i: missing) {
      if (!check_missing.count(i.first)) {
	if (oss)
	  *oss << "check_missing missing " << i.first << "\n";
	ok = false;
      } else if (check_missing[i.first] != i.second) {
	if (oss)
	  *oss << "check_missing missing item mismatch on " << i.first
	       << ", check: " << check_missing[i.first]
	       << ", actual: " << i.second << "\n";
	ok = false;
      }
    }
    if (oss && !ok) {
      *oss << "check_missing: " << check_missing << "\n";
      std::set<hobject_t> changed;
      tracker.get_changed([&](const hobject_t &hoid) { changed.insert(hoid); });
      *oss << "changed: " << changed << "\n";
    }
    return ok;
  }
};
template <bool TrackChanges>
void encode(
  const pg_missing_set<TrackChanges> &c, ceph::buffer::list &bl, uint64_t features=0) {
  ENCODE_DUMP_PRE();
  c.encode(bl, features);
  ENCODE_DUMP_POST(cl);
}
template <bool TrackChanges>
void decode(pg_missing_set<TrackChanges> &c, ceph::buffer::list::const_iterator &p) {
  c.decode(p);
}
template <bool TrackChanges>
std::ostream& operator<<(std::ostream& out, const pg_missing_set<TrackChanges> &missing)
{
  out << "missing(" << missing.num_missing()
      << " may_include_deletes = " << missing.may_include_deletes;
  //if (missing.num_lost()) out << ", " << missing.num_lost() << " lost";
  out << ")";
  return out;
}

using pg_missing_t = pg_missing_set<false>;
using pg_missing_tracker_t = pg_missing_set<true>;

#endif // CEPH_OSD_TYPES_LOG_H
