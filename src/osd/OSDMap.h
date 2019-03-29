// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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


#ifndef CEPH_OSDMAP_H
#define CEPH_OSDMAP_H

/*
 * describe properties of the OSD cluster.
 *   disks, disk groups, total # osds,
 *
 */
#include "include/types.h"
#include "osd_types.h"

//#include "include/ceph_features.h"
#include "crush/CrushWrapper.h"
#include <vector>
#include <list>
#include <set>
#include <map>
#include <memory>
#include "include/btree_map.h"

// forward declaration
class CephContext;
class CrushWrapper;
class health_check_map_t;

/*
 * we track up to two intervals during which the osd was alive and
 * healthy.  the most recent is [up_from,up_thru), where up_thru is
 * the last epoch the osd is known to have _started_.  i.e., a lower
 * bound on the actual osd death.  down_at (if it is > up_from) is an
 * upper bound on the actual osd death.
 *
 * the second is the last_clean interval [first,last].  in that case,
 * the last interval is the last epoch known to have been either
 * _finished_, or during which the osd cleanly shut down.  when
 * possible, we push this forward to the epoch the osd was eventually
 * marked down.
 *
 * the lost_at is used to allow build_prior to proceed without waiting
 * for an osd to recover.  In certain cases, progress may be blocked 
 * because an osd is down that may contain updates (i.e., a pg may have
 * gone rw during an interval).  If the osd can't be brought online, we
 * can force things to proceed knowing that we _might_ be losing some
 * acked writes.  If the osd comes back to life later, that's fine to,
 * but those writes will still be lost (the divergent objects will be
 * thrown out).
 */
struct osd_info_t {
  epoch_t last_clean_begin;  // last interval that ended with a clean osd shutdown
  epoch_t last_clean_end;
  epoch_t up_from;   // epoch osd marked up
  epoch_t up_thru;   // lower bound on actual osd death (if > up_from)
  epoch_t down_at;   // upper bound on actual osd death (if > up_from)
  epoch_t lost_at;   // last epoch we decided data was "lost"
  
  osd_info_t() : last_clean_begin(0), last_clean_end(0),
		 up_from(0), up_thru(0), down_at(0), lost_at(0) {}

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  static void generate_test_instances(std::list<osd_info_t*>& o);
};
WRITE_CLASS_ENCODER(osd_info_t)

std::ostream& operator<<(std::ostream& out, const osd_info_t& info);

struct osd_xinfo_t {
  utime_t down_stamp;      ///< timestamp when we were last marked down
  float laggy_probability; ///< encoded as __u32: 0 = definitely not laggy, 0xffffffff definitely laggy
  __u32 laggy_interval;    ///< average interval between being marked laggy and recovering
  uint64_t features;       ///< features supported by this osd we should know about
  __u32 old_weight;        ///< weight prior to being auto marked out

  osd_xinfo_t() : laggy_probability(0), laggy_interval(0),
                  features(0), old_weight(0) {}

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  static void generate_test_instances(std::list<osd_xinfo_t*>& o);
};
WRITE_CLASS_ENCODER(osd_xinfo_t)

std::ostream& operator<<(std::ostream& out, const osd_xinfo_t& xi);


struct PGTempMap {
#if 1
  ceph::buffer::list data;
  typedef btree::btree_map<pg_t,int32_t*> map_t;
  map_t map;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    uint32_t n = map.size();
    encode(n, bl);
    for (auto &p : map) {
      encode(p.first, bl);
      bl.append((char*)p.second, (*p.second + 1) * sizeof(int32_t));
    }
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    data.clear();
    map.clear();
    uint32_t n;
    decode(n, p);
    if (!n)
      return;
    auto pstart = p;
    size_t start_off = pstart.get_off();
    std::vector<std::pair<pg_t,size_t>> offsets;
    offsets.resize(n);
    for (unsigned i=0; i<n; ++i) {
      pg_t pgid;
      decode(pgid, p);
      offsets[i].first = pgid;
      offsets[i].second = p.get_off() - start_off;
      uint32_t vn;
      decode(vn, p);
      p.advance(vn * sizeof(int32_t));
    }
    size_t len = p.get_off() - start_off;
    pstart.copy(len, data);
    if (data.get_num_buffers() > 1) {
      data.rebuild();
    }
    //map.reserve(n);
    char *start = data.c_str();
    for (auto i : offsets) {
      map.insert(map.end(), std::make_pair(i.first, (int32_t*)(start + i.second)));
    }
  }
  void rebuild() {
    ceph::buffer::list bl;
    encode(bl);
    auto p = std::cbegin(bl);
    decode(p);
  }
  friend bool operator==(const PGTempMap& l, const PGTempMap& r) {
    return
      l.map.size() == r.map.size() &&
      l.data.contents_equal(r.data);
  }

  class iterator {
    map_t::const_iterator it;
    map_t::const_iterator end;
    std::pair<pg_t,std::vector<int32_t>> current;
    void init_current() {
      if (it != end) {
	current.first = it->first;
	ceph_assert(it->second);
	current.second.resize(*it->second);
	int32_t *p = it->second + 1;
	for (int n = 0; n < *it->second; ++n, ++p) {
	  current.second[n] = *p;
	}
      }
    }
  public:
    iterator(map_t::const_iterator p,
	     map_t::const_iterator e)
      : it(p), end(e) {
      init_current();
    }

    const std::pair<pg_t,std::vector<int32_t>>& operator*() const {
      return current;
    }
    const std::pair<pg_t,std::vector<int32_t>>* operator->() const {
      return &current;
    }
    friend bool operator==(const iterator& l, const iterator& r) {
      return l.it == r.it;
    }
    friend bool operator!=(const iterator& l, const iterator& r) {
      return l.it != r.it;
    }
    iterator& operator++() {
      ++it;
      if (it != end)
	init_current();
      return *this;
    }
    iterator operator++(int) {
      iterator r = *this;
      ++it;
      if (it != end)
	init_current();
      return r;
    }
  };
  iterator begin() const {
    return iterator(map.begin(), map.end());
  }
  iterator end() const {
    return iterator(map.end(), map.end());
  }
  iterator find(pg_t pgid) const {
    return iterator(map.find(pgid), map.end());
  }
  size_t size() const {
    return map.size();
  }
  size_t count(pg_t pgid) const {
    return map.count(pgid);
  }
  void erase(pg_t pgid) {
    map.erase(pgid);
  }
  void clear() {
    map.clear();
    data.clear();
  }
  void set(pg_t pgid, const mempool::osdmap::vector<int32_t>& v) {
    using ceph::encode;
    size_t need = sizeof(int32_t) * (1 + v.size());
    if (need < data.get_append_buffer_unused_tail_length()) {
      ceph::buffer::ptr z(data.get_append_buffer_unused_tail_length());
      z.zero();
      data.append(z.c_str(), z.length());
    }
    encode(v, data);
    map[pgid] = (int32_t*)(data.back().end_c_str()) - (1 + v.size());
  }
  mempool::osdmap::vector<int32_t> get(pg_t pgid) {
    mempool::osdmap::vector<int32_t> v;
    int32_t *p = map[pgid];
    size_t n = *p++;
    v.resize(n);
    for (size_t i = 0; i < n; ++i, ++p) {
      v[i] = *p;
    }
    return v;
  }
#else
  // trivial implementation
  mempool::osdmap::map<pg_t,mempool::osdmap::vector<int32_t> > pg_temp;

  void encode(ceph::buffer::list& bl) const {
    encode(pg_temp, bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    decode(pg_temp, p);
  }
  friend bool operator==(const PGTempMap& l, const PGTempMap& r) {
    return
      l.pg_temp.size() == r.pg_temp.size() &&
      l.pg_temp == r.pg_temp;
  }

  class iterator {
    mempool::osdmap::map<pg_t,mempool::osdmap::vector<int32_t> >::const_iterator it;
  public:
    iterator(mempool::osdmap::map<pg_t,
	     mempool::osdmap::vector<int32_t> >::const_iterator p)
      : it(p) {}

    std::pair<pg_t,const mempool::osdmap::vector<int32_t>&> operator*() const {
      return *it;
    }
    const std::pair<const pg_t,mempool::osdmap::vector<int32_t>>* operator->() const {
      return &*it;
    }
    friend bool operator==(const iterator& l, const iterator& r) {
      return l.it == r.it;
    }
    friend bool operator!=(const iterator& l, const iterator& r) {
      return l.it != r.it;
    }
    iterator& operator++() {
      ++it;
      return *this;
    }
    iterator operator++(int) {
      iterator r = *this;
      ++it;
      return r;
    }
  };
  iterator begin() const {
    return iterator(pg_temp.cbegin());
  }
  iterator end() const {
    return iterator(pg_temp.cend());
  }
  iterator find(pg_t pgid) const {
    return iterator(pg_temp.find(pgid));
  }
  size_t size() const {
    return pg_temp.size();
  }
  size_t count(pg_t pgid) const {
    return pg_temp.count(pgid);
  }
  void erase(pg_t pgid) {
    pg_temp.erase(pgid);
  }
  void clear() {
    pg_temp.clear();
  }
  void set(pg_t pgid, const mempool::osdmap::vector<int32_t>& v) {
    pg_temp[pgid] = v;
  }
  const mempool::osdmap::vector<int32_t>& get(pg_t pgid) {
    return pg_temp.at(pgid);
  }
#endif
  void dump(ceph::Formatter *f) const {
    for (const auto &pg : *this) {
      f->open_object_section("osds");
      f->dump_stream("pgid") << pg.first;
      f->open_array_section("osds");
      for (const auto osd : pg.second)
	f->dump_int("osd", osd);
      f->close_section();
      f->close_section();
    }
  }
};
WRITE_CLASS_ENCODER(PGTempMap)

/** OSDMap
 */
class OSDMap {
public:
  MEMPOOL_CLASS_HELPERS();

  typedef interval_set<
    snapid_t,
    mempool::osdmap::flat_map<snapid_t,snapid_t>> snap_interval_set_t;

  class Incremental {
  public:
    MEMPOOL_CLASS_HELPERS();

    /// feature bits we were encoded with.  the subsequent OSDMap
    /// encoding should match.
    uint64_t encode_features;
    uuid_d fsid;
    epoch_t epoch;   // new epoch; we are a diff from epoch-1 to epoch
    utime_t modified;
    int64_t new_pool_max; //incremented by the OSDMonitor on each pool create
    int32_t new_flags;
    int8_t new_require_osd_release = -1;

    // full (rare)
    ceph::buffer::list fullmap;  // in lieu of below.
    ceph::buffer::list crush;

    // incremental
    int32_t new_max_osd;
    mempool::osdmap::map<int64_t,pg_pool_t> new_pools;
    mempool::osdmap::map<int64_t,std::string> new_pool_names;
    mempool::osdmap::set<int64_t> old_pools;
    mempool::osdmap::map<std::string,std::map<std::string,std::string> > new_erasure_code_profiles;
    mempool::osdmap::vector<std::string> old_erasure_code_profiles;
    mempool::osdmap::map<int32_t,entity_addrvec_t> new_up_client;
    mempool::osdmap::map<int32_t,entity_addrvec_t> new_up_cluster;
    mempool::osdmap::map<int32_t,uint32_t> new_state;             // XORed onto previous state.
    mempool::osdmap::map<int32_t,uint32_t> new_weight;
    mempool::osdmap::map<pg_t,mempool::osdmap::vector<int32_t> > new_pg_temp;     // [] to remove
    mempool::osdmap::map<pg_t, int32_t> new_primary_temp;            // [-1] to remove
    mempool::osdmap::map<int32_t,uint32_t> new_primary_affinity;
    mempool::osdmap::map<int32_t,epoch_t> new_up_thru;
    mempool::osdmap::map<int32_t,std::pair<epoch_t,epoch_t> > new_last_clean_interval;
    mempool::osdmap::map<int32_t,epoch_t> new_lost;
    mempool::osdmap::map<int32_t,uuid_d> new_uuid;
    mempool::osdmap::map<int32_t,osd_xinfo_t> new_xinfo;

    mempool::osdmap::map<entity_addr_t,utime_t> new_blacklist;
    mempool::osdmap::vector<entity_addr_t> old_blacklist;
    mempool::osdmap::map<int32_t, entity_addrvec_t> new_hb_back_up;
    mempool::osdmap::map<int32_t, entity_addrvec_t> new_hb_front_up;

    mempool::osdmap::map<pg_t,mempool::osdmap::vector<int32_t>> new_pg_upmap;
    mempool::osdmap::map<pg_t,mempool::osdmap::vector<std::pair<int32_t,int32_t>>> new_pg_upmap_items;
    mempool::osdmap::set<pg_t> old_pg_upmap, old_pg_upmap_items;
    mempool::osdmap::map<int64_t, snap_interval_set_t> new_removed_snaps;
    mempool::osdmap::map<int64_t, snap_interval_set_t> new_purged_snaps;

    std::string cluster_snapshot;

    float new_nearfull_ratio = -1;
    float new_backfillfull_ratio = -1;
    float new_full_ratio = -1;

    int8_t new_require_min_compat_client = -1;

    utime_t new_last_up_change, new_last_in_change;

    mutable bool have_crc;      ///< crc values are defined
    uint32_t full_crc;  ///< crc of the resulting OSDMap
    mutable uint32_t inc_crc;   ///< crc of this incremental

    int get_net_marked_out(const OSDMap *previous) const;
    int get_net_marked_down(const OSDMap *previous) const;
    int identify_osd(uuid_d u) const;

    void encode_client_old(ceph::buffer::list& bl) const;
    void encode_classic(ceph::buffer::list& bl, uint64_t features) const;
    void encode(ceph::buffer::list& bl, uint64_t features=CEPH_FEATURES_ALL) const;
    void decode_classic(ceph::buffer::list::const_iterator &p);
    void decode(ceph::buffer::list::const_iterator &bl);
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<Incremental*>& o);

    explicit Incremental(epoch_t e=0) :
      encode_features(0),
      epoch(e), new_pool_max(-1), new_flags(-1), new_max_osd(-1),
      have_crc(false), full_crc(0), inc_crc(0) {
    }
    explicit Incremental(ceph::buffer::list &bl) {
      auto p = std::cbegin(bl);
      decode(p);
    }
    explicit Incremental(ceph::buffer::list::const_iterator &p) {
      decode(p);
    }

    pg_pool_t *get_new_pool(int64_t pool, const pg_pool_t *orig) {
      if (new_pools.count(pool) == 0)
	new_pools[pool] = *orig;
      return &new_pools[pool];
    }
    bool has_erasure_code_profile(const std::string &name) const {
      auto i = new_erasure_code_profiles.find(name);
      return i != new_erasure_code_profiles.end();
    }
    void set_erasure_code_profile(const std::string &name,
				  const std::map<std::string,std::string>& profile) {
      new_erasure_code_profiles[name] = profile;
    }
    mempool::osdmap::map<std::string,std::map<std::string,std::string>> get_erasure_code_profiles() const {
      return new_erasure_code_profiles;
    }

    /// propagate update pools' snap metadata to any of their tiers
    int propagate_snaps_to_tiers(CephContext *cct, const OSDMap &base);

    /// filter out osds with any pending state changing
    size_t get_pending_state_osds(std::vector<int> *osds) {
      ceph_assert(osds);
      osds->clear();

      for (auto &p : new_state) {
        osds->push_back(p.first);
      }

      return osds->size();
    }

    bool pending_osd_has_state(int osd, unsigned state) {
      return new_state.count(osd) && (new_state[osd] & state) != 0;
    }

    void pending_osd_state_set(int osd, unsigned state) {
      new_state[osd] |= state;
    }

    // cancel the specified pending osd state if there is any
    // return ture on success, false otherwise.
    bool pending_osd_state_clear(int osd, unsigned state) {
      if (!pending_osd_has_state(osd, state)) {
        // never has been set or already has been cancelled.
        return false;
      }

      new_state[osd] &= ~state;
      if (!new_state[osd]) {
        // all flags cleared
        new_state.erase(osd);
      }
      return true;
    }

  };
  
private:
  uuid_d fsid;
  epoch_t epoch;        // what epoch of the osd cluster descriptor is this
  utime_t created, modified; // epoch start time
  int32_t pool_max;     // the largest pool num, ever

  uint32_t flags;

  int num_osd;         // not saved; see calc_num_osds
  int num_up_osd;      // not saved; see calc_num_osds
  int num_in_osd;      // not saved; see calc_num_osds

  int32_t max_osd;
  std::vector<uint32_t> osd_state;

  utime_t last_up_change, last_in_change;

  // These features affect OSDMap[::Incremental] encoding, or the
  // encoding of some type embedded therein (CrushWrapper, something
  // from osd_types, etc.).
  static constexpr uint64_t SIGNIFICANT_FEATURES =
    CEPH_FEATUREMASK_PGID64 |
    CEPH_FEATUREMASK_PGPOOL3 |
    CEPH_FEATUREMASK_OSDENC |
    CEPH_FEATUREMASK_OSDMAP_ENC |
    CEPH_FEATUREMASK_OSD_POOLRESEND |
    CEPH_FEATUREMASK_NEW_OSDOP_ENCODING |
    CEPH_FEATUREMASK_MSG_ADDR2 |
    CEPH_FEATUREMASK_CRUSH_TUNABLES5 |
    CEPH_FEATUREMASK_CRUSH_CHOOSE_ARGS |
    CEPH_FEATUREMASK_SERVER_LUMINOUS |
    CEPH_FEATUREMASK_SERVER_MIMIC |
    CEPH_FEATUREMASK_SERVER_NAUTILUS;

  struct addrs_s {
    mempool::osdmap::vector<std::shared_ptr<entity_addrvec_t> > client_addrs;
    mempool::osdmap::vector<std::shared_ptr<entity_addrvec_t> > cluster_addrs;
    mempool::osdmap::vector<std::shared_ptr<entity_addrvec_t> > hb_back_addrs;
    mempool::osdmap::vector<std::shared_ptr<entity_addrvec_t> > hb_front_addrs;
  };
  std::shared_ptr<addrs_s> osd_addrs;

  entity_addrvec_t _blank_addrvec;

  mempool::osdmap::vector<__u32>   osd_weight;   // 16.16 fixed point, 0x10000 = "in", 0 = "out"
  mempool::osdmap::vector<osd_info_t> osd_info;
  std::shared_ptr<PGTempMap> pg_temp;  // temp pg mapping (e.g. while we rebuild)
  std::shared_ptr< mempool::osdmap::map<pg_t,int32_t > > primary_temp;  // temp primary mapping (e.g. while we rebuild)
  std::shared_ptr< mempool::osdmap::vector<__u32> > osd_primary_affinity; ///< 16.16 fixed point, 0x10000 = baseline

  // remap (post-CRUSH, pre-up)
  mempool::osdmap::map<pg_t,mempool::osdmap::vector<int32_t>> pg_upmap; ///< remap pg
  mempool::osdmap::map<pg_t,mempool::osdmap::vector<std::pair<int32_t,int32_t>>> pg_upmap_items; ///< remap osds in up set

  mempool::osdmap::map<int64_t,pg_pool_t> pools;
  mempool::osdmap::map<int64_t,std::string> pool_name;
  mempool::osdmap::map<std::string, std::map<std::string,std::string>> erasure_code_profiles;
  mempool::osdmap::map<std::string,int64_t> name_pool;

  std::shared_ptr< mempool::osdmap::vector<uuid_d> > osd_uuid;
  mempool::osdmap::vector<osd_xinfo_t> osd_xinfo;

  mempool::osdmap::unordered_map<entity_addr_t,utime_t> blacklist;

  /// queue of snaps to remove
  mempool::osdmap::map<int64_t, snap_interval_set_t> removed_snaps_queue;

  /// removed_snaps additions this epoch
  mempool::osdmap::map<int64_t, snap_interval_set_t> new_removed_snaps;

  /// removed_snaps removals this epoch
  mempool::osdmap::map<int64_t, snap_interval_set_t> new_purged_snaps;

  epoch_t cluster_snapshot_epoch;
  std::string cluster_snapshot;
  bool new_blacklist_entries;

  float full_ratio = 0, backfillfull_ratio = 0, nearfull_ratio = 0;

  /// min compat client we want to support
  uint8_t require_min_compat_client = 0;  // CEPH_RELEASE_*

public:
  /// require osds to run at least this release
  uint8_t require_osd_release = 0;    // CEPH_RELEASE_*

private:
  mutable uint64_t cached_up_osd_features;

  mutable bool crc_defined;
  mutable uint32_t crc;

  void _calc_up_osd_features();

 public:
  bool have_crc() const { return crc_defined; }
  uint32_t get_crc() const { return crc; }

  std::shared_ptr<CrushWrapper> crush;       // hierarchical map
private:
  uint32_t crush_version = 1;

  friend class OSDMonitor;

 public:
  OSDMap() : epoch(0), 
	     pool_max(0),
	     flags(0),
	     num_osd(0), num_up_osd(0), num_in_osd(0),
	     max_osd(0),
	     osd_addrs(std::make_shared<addrs_s>()),
	     pg_temp(std::make_shared<PGTempMap>()),
	     primary_temp(std::make_shared<mempool::osdmap::map<pg_t,int32_t>>()),
	     osd_uuid(std::make_shared<mempool::osdmap::vector<uuid_d>>()),
	     cluster_snapshot_epoch(0),
	     new_blacklist_entries(false),
	     cached_up_osd_features(0),
	     crc_defined(false), crc(0),
	     crush(std::make_shared<CrushWrapper>()) {
  }

private:
  OSDMap(const OSDMap& other) = default;
  OSDMap& operator=(const OSDMap& other) = default;
public:

  /// return feature mask subset that is relevant to OSDMap encoding
  static uint64_t get_significant_features(uint64_t features) {
    return SIGNIFICANT_FEATURES & features;
  }

  uint64_t get_encoding_features() const;

  void deepish_copy_from(const OSDMap& o) {
    *this = o;
    primary_temp.reset(new mempool::osdmap::map<pg_t,int32_t>(*o.primary_temp));
    pg_temp.reset(new PGTempMap(*o.pg_temp));
    osd_uuid.reset(new mempool::osdmap::vector<uuid_d>(*o.osd_uuid));

    if (o.osd_primary_affinity)
      osd_primary_affinity.reset(new mempool::osdmap::vector<__u32>(*o.osd_primary_affinity));

    // NOTE: this still references shared entity_addrvec_t's.
    osd_addrs.reset(new addrs_s(*o.osd_addrs));

    // NOTE: we do not copy crush.  note that apply_incremental will
    // allocate a new CrushWrapper, though.
  }

  // map info
  const uuid_d& get_fsid() const { return fsid; }
  void set_fsid(uuid_d& f) { fsid = f; }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  void set_epoch(epoch_t e);

  uint32_t get_crush_version() const {
    return crush_version;
  }

  /* stamps etc */
  const utime_t& get_created() const { return created; }
  const utime_t& get_modified() const { return modified; }

  bool is_blacklisted(const entity_addr_t& a) const;
  bool is_blacklisted(const entity_addrvec_t& a) const;
  void get_blacklist(std::list<std::pair<entity_addr_t,utime_t > > *bl) const;
  void get_blacklist(std::set<entity_addr_t> *bl) const;

  std::string get_cluster_snapshot() const {
    if (cluster_snapshot_epoch == epoch)
      return cluster_snapshot;
    return std::string();
  }

  float get_full_ratio() const {
    return full_ratio;
  }
  float get_backfillfull_ratio() const {
    return backfillfull_ratio;
  }
  float get_nearfull_ratio() const {
    return nearfull_ratio;
  }
  void get_full_pools(CephContext *cct,
                      std::set<int64_t> *full,
                      std::set<int64_t> *backfillfull,
                      std::set<int64_t> *nearfull) const;
  void get_full_osd_counts(std::set<int> *full, std::set<int> *backfill,
			   std::set<int> *nearfull) const;


  /***** cluster state *****/
  /* osds */
  int get_max_osd() const { return max_osd; }
  void set_max_osd(int m);

  unsigned get_num_osds() const {
    return num_osd;
  }
  unsigned get_num_up_osds() const {
    return num_up_osd;
  }
  unsigned get_num_in_osds() const {
    return num_in_osd;
  }
  /// recalculate cached values for get_num{,_up,_in}_osds
  int calc_num_osds();

  void get_all_osds(std::set<int32_t>& ls) const;
  void get_up_osds(std::set<int32_t>& ls) const;
  void get_out_osds(std::set<int32_t>& ls) const;
  unsigned get_num_pg_temp() const {
    return pg_temp->size();
  }

  int get_flags() const { return flags; }
  bool test_flag(int f) const { return flags & f; }
  void set_flag(int f) { flags |= f; }
  void clear_flag(int f) { flags &= ~f; }

  void get_flag_set(std::set<std::string> *flagset) const;

  static void calc_state_set(int state, std::set<std::string>& st);

  int get_state(int o) const {
    ceph_assert(o < max_osd);
    return osd_state[o];
  }
  int get_state(int o, std::set<std::string>& st) const {
    ceph_assert(o < max_osd);
    unsigned t = osd_state[o];
    calc_state_set(t, st);
    return osd_state[o];
  }
  void set_state(int o, unsigned s) {
    ceph_assert(o < max_osd);
    osd_state[o] = s;
  }
  void set_weight(int o, unsigned w) {
    ceph_assert(o < max_osd);
    osd_weight[o] = w;
    if (w)
      osd_state[o] |= CEPH_OSD_EXISTS;
  }
  unsigned get_weight(int o) const {
    ceph_assert(o < max_osd);
    return osd_weight[o];
  }
  float get_weightf(int o) const {
    return (float)get_weight(o) / (float)CEPH_OSD_IN;
  }
  void adjust_osd_weights(const std::map<int,double>& weights, Incremental& inc) const;

  void set_primary_affinity(int o, int w) {
    ceph_assert(o < max_osd);
    if (!osd_primary_affinity)
      osd_primary_affinity.reset(
	new mempool::osdmap::vector<__u32>(
	  max_osd, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY));
    (*osd_primary_affinity)[o] = w;
  }
  unsigned get_primary_affinity(int o) const {
    ceph_assert(o < max_osd);
    if (!osd_primary_affinity)
      return CEPH_OSD_DEFAULT_PRIMARY_AFFINITY;
    return (*osd_primary_affinity)[o];
  }
  float get_primary_affinityf(int o) const {
    return (float)get_primary_affinity(o) / (float)CEPH_OSD_MAX_PRIMARY_AFFINITY;
  }

  bool has_erasure_code_profile(const std::string &name) const {
    auto i = erasure_code_profiles.find(name);
    return i != erasure_code_profiles.end();
  }
  int get_erasure_code_profile_default(CephContext *cct,
				       std::map<std::string,std::string> &profile_map,
				       std::ostream *ss);
  void set_erasure_code_profile(const std::string &name,
				const std::map<std::string,std::string>& profile) {
    erasure_code_profiles[name] = profile;
  }
  const std::map<std::string,std::string> &get_erasure_code_profile(
    const std::string &name) const {
    static std::map<std::string,std::string> empty;
    auto i = erasure_code_profiles.find(name);
    if (i == erasure_code_profiles.end())
      return empty;
    else
      return i->second;
  }
  const mempool::osdmap::map<std::string,std::map<std::string,std::string>> &get_erasure_code_profiles() const {
    return erasure_code_profiles;
  }

  bool exists(int osd) const {
    //assert(osd >= 0);
    return osd >= 0 && osd < max_osd && (osd_state[osd] & CEPH_OSD_EXISTS);
  }

  bool is_destroyed(int osd) const {
    return exists(osd) && (osd_state[osd] & CEPH_OSD_DESTROYED);
  }

  bool is_up(int osd) const {
    return exists(osd) && (osd_state[osd] & CEPH_OSD_UP);
  }

  bool has_been_up_since(int osd, epoch_t epoch) const {
    return is_up(osd) && get_up_from(osd) <= epoch;
  }

  bool is_down(int osd) const {
    return !is_up(osd);
  }

  bool is_out(int osd) const {
    return !exists(osd) || get_weight(osd) == CEPH_OSD_OUT;
  }

  bool is_in(int osd) const {
    return !is_out(osd);
  }

  bool is_noup(int osd) const {
    return exists(osd) && (osd_state[osd] & CEPH_OSD_NOUP);
  }

  bool is_nodown(int osd) const {
    return exists(osd) && (osd_state[osd] & CEPH_OSD_NODOWN);
  }

  bool is_noin(int osd) const {
    return exists(osd) && (osd_state[osd] & CEPH_OSD_NOIN);
  }

  bool is_noout(int osd) const {
    return exists(osd) && (osd_state[osd] & CEPH_OSD_NOOUT);
  }

  void get_noup_osds(std::vector<int> *osds) const {
    ceph_assert(osds);
    osds->clear();

    for (int i = 0; i < max_osd; i++) {
      if (is_noup(i)) {
        osds->push_back(i);
      }
    }
  }

  void get_nodown_osds(std::vector<int> *osds) const {
    ceph_assert(osds);
    osds->clear();

    for (int i = 0; i < max_osd; i++) {
      if (is_nodown(i)) {
        osds->push_back(i);
      }
    }
  }

  void get_noin_osds(std::vector<int> *osds) const {
    ceph_assert(osds);
    osds->clear();

    for (int i = 0; i < max_osd; i++) {
      if (is_noin(i)) {
        osds->push_back(i);
      }
    }
  }

  void get_noout_osds(std::vector<int> *osds) const {
    ceph_assert(osds);
    osds->clear();

    for (int i = 0; i < max_osd; i++) {
      if (is_noout(i)) {
        osds->push_back(i);
      }
    }
  }

  /**
   * check if an entire crush subtree is down
   */
  bool subtree_is_down(int id, std::set<int> *down_cache) const;
  bool containing_subtree_is_down(CephContext *cct, int osd, int subtree_type, std::set<int> *down_cache) const;

  bool subtree_type_is_down(CephContext *cct, int id, int subtree_type, std::set<int> *down_in_osds, std::set<int> *up_in_osds,
                            std::set<int> *subtree_up, std::unordered_map<int, std::set<int> > *subtree_type_down) const;

  int identify_osd(const entity_addr_t& addr) const;
  int identify_osd(const uuid_d& u) const;
  int identify_osd_on_all_channels(const entity_addr_t& addr) const;

  bool have_addr(const entity_addr_t& addr) const {
    return identify_osd(addr) >= 0;
  }
  int find_osd_on_ip(const entity_addr_t& ip) const;

  const entity_addrvec_t& get_addrs(int osd) const {
    ceph_assert(exists(osd));
    return osd_addrs->client_addrs[osd] ?
      *osd_addrs->client_addrs[osd] : _blank_addrvec;
  }
  const entity_addrvec_t& get_most_recent_addrs(int osd) const {
    return get_addrs(osd);
  }
  const entity_addrvec_t &get_cluster_addrs(int osd) const {
    ceph_assert(exists(osd));
    return osd_addrs->cluster_addrs[osd] ?
      *osd_addrs->cluster_addrs[osd] : _blank_addrvec;
  }
  const entity_addrvec_t &get_hb_back_addrs(int osd) const {
    ceph_assert(exists(osd));
    return osd_addrs->hb_back_addrs[osd] ?
      *osd_addrs->hb_back_addrs[osd] : _blank_addrvec;
  }
  const entity_addrvec_t &get_hb_front_addrs(int osd) const {
    ceph_assert(exists(osd));
    return osd_addrs->hb_front_addrs[osd] ?
      *osd_addrs->hb_front_addrs[osd] : _blank_addrvec;
  }

  const uuid_d& get_uuid(int osd) const {
    ceph_assert(exists(osd));
    return (*osd_uuid)[osd];
  }

  const epoch_t& get_up_from(int osd) const {
    ceph_assert(exists(osd));
    return osd_info[osd].up_from;
  }
  const epoch_t& get_up_thru(int osd) const {
    ceph_assert(exists(osd));
    return osd_info[osd].up_thru;
  }
  const epoch_t& get_down_at(int osd) const {
    ceph_assert(exists(osd));
    return osd_info[osd].down_at;
  }
  const osd_info_t& get_info(int osd) const {
    ceph_assert(osd < max_osd);
    return osd_info[osd];
  }

  const osd_xinfo_t& get_xinfo(int osd) const {
    ceph_assert(osd < max_osd);
    return osd_xinfo[osd];
  }
  
  int get_next_up_osd_after(int n) const {
    if (get_max_osd() == 0)
      return -1;
    for (int i = n + 1; i != n; ++i) {
      if (i >= get_max_osd())
	i = 0;
      if (i == n)
	break;
      if (is_up(i))
	return i;
    }
    return -1;
  }

  int get_previous_up_osd_before(int n) const {
    if (get_max_osd() == 0)
      return -1;
    for (int i = n - 1; i != n; --i) {
      if (i < 0)
	i = get_max_osd() - 1;
      if (i == n)
	break;
      if (is_up(i))
	return i;
    }
    return -1;
  }


  void get_random_up_osds_by_subtree(int n,     // whoami
                                     std::string &subtree,
                                     int limit, // how many
                                     std::set<int> skip,
                                     std::set<int> *want) const;

  /**
   * get feature bits required by the current structure
   *
   * @param entity_type [in] what entity type we are asking about
   * @param mask [out] std::set of all possible map-related features we could std::set
   * @return feature bits used by this map
   */
  uint64_t get_features(int entity_type, uint64_t *mask) const;

  /**
   * get oldest *client* version (firefly, hammer, etc.) that can connect given
   * the feature bits required (according to get_features()).
   */
  uint8_t get_min_compat_client() const;

  /**
   * gets the required minimum *client* version that can connect to the cluster.
   */
  uint8_t get_require_min_compat_client() const;

  /**
   * get intersection of features supported by up osds
   */
  uint64_t get_up_osd_features() const;

  void maybe_remove_pg_upmaps(CephContext *cct,
                              const OSDMap& oldmap,
			      const OSDMap& nextmap,
                              Incremental *pending_inc);

  int apply_incremental(const Incremental &inc);

  /// try to re-use/reference addrs in oldmap from newmap
  static void dedup(const OSDMap *oldmap, OSDMap *newmap);

  static void clean_temps(CephContext *cct,
			  const OSDMap& oldmap,
			  const OSDMap& nextmap,
			  Incremental *pending_inc);

  // serialize, unserialize
private:
  void encode_client_old(ceph::buffer::list& bl) const;
  void encode_classic(ceph::buffer::list& bl, uint64_t features) const;
  void decode_classic(ceph::buffer::list::const_iterator& p);
  void post_decode();
public:
  void encode(ceph::buffer::list& bl, uint64_t features=CEPH_FEATURES_ALL) const;
  void decode(ceph::buffer::list& bl);
  void decode(ceph::buffer::list::const_iterator& bl);


  /****   mapping facilities   ****/
  int map_to_pg(
    int64_t pool,
    const std::string& name,
    const std::string& key,
    const std::string& nspace,
    pg_t *pg) const;
  int object_locator_to_pg(const object_t& oid, const object_locator_t& loc,
			   pg_t &pg) const;
  pg_t object_locator_to_pg(const object_t& oid,
			    const object_locator_t& loc) const {
    pg_t pg;
    int ret = object_locator_to_pg(oid, loc, pg);
    ceph_assert(ret == 0);
    return pg;
  }


  static object_locator_t file_to_object_locator(const file_layout_t& layout) {
    return object_locator_t(layout.pool_id, layout.pool_ns);
  }

  ceph_object_layout file_to_object_layout(object_t oid,
					   file_layout_t& layout) const {
    return make_object_layout(oid, layout.pool_id, layout.pool_ns);
  }

  ceph_object_layout make_object_layout(object_t oid, int pg_pool,
					std::string nspace) const;

  int get_pg_num(int pg_pool) const
  {
    const pg_pool_t *pool = get_pg_pool(pg_pool);
    ceph_assert(NULL != pool);
    return pool->get_pg_num();
  }

  bool pg_exists(pg_t pgid) const {
    const pg_pool_t *p = get_pg_pool(pgid.pool());
    return p && pgid.ps() < p->get_pg_num();
  }

  int get_pg_pool_min_size(pg_t pgid) const {
    if (!pg_exists(pgid)) {
      return -ENOENT;
    }
    const pg_pool_t *p = get_pg_pool(pgid.pool());
    ceph_assert(p);
    return p->get_min_size();
  }

  int get_pg_pool_size(pg_t pgid) const {
    if (!pg_exists(pgid)) {
      return -ENOENT;
    }
    const pg_pool_t *p = get_pg_pool(pgid.pool());
    ceph_assert(p);
    return p->get_size();
  }

  int get_pg_pool_crush_rule(pg_t pgid) const {
    if (!pg_exists(pgid)) {
      return -ENOENT;
    }
    const pg_pool_t *p = get_pg_pool(pgid.pool());
    ceph_assert(p);
    return p->get_crush_rule();
  }

private:
  /// pg -> (raw osd std::list)
  void _pg_to_raw_osds(
    const pg_pool_t& pool, pg_t pg,
    std::vector<int> *osds,
    ps_t *ppps) const;
  int _pick_primary(const std::vector<int>& osds) const;
  void _remove_nonexistent_osds(const pg_pool_t& pool, std::vector<int>& osds) const;

  void _apply_primary_affinity(ps_t seed, const pg_pool_t& pool,
			       std::vector<int> *osds, int *primary) const;

  /// apply pg_upmap[_items] mappings
  void _apply_upmap(const pg_pool_t& pi, pg_t pg, std::vector<int> *raw) const;

  /// pg -> (up osd std::list)
  void _raw_to_up_osds(const pg_pool_t& pool, const std::vector<int>& raw,
                       std::vector<int> *up) const;


  /**
   * Get the pg and primary temp, if they are specified.
   * @param temp_pg [out] Will be empty or contain the temp PG mapping on return
   * @param temp_primary [out] Will be the value in primary_temp, or a value derived
   * from the pg_temp (if specified), or -1 if you should use the calculated (up_)primary.
   */
  void _get_temp_osds(const pg_pool_t& pool, pg_t pg,
                      std::vector<int> *temp_pg, int *temp_primary) const;

  /**
   *  map to up and acting. Fills in whatever fields are non-NULL.
   */
  void _pg_to_up_acting_osds(const pg_t& pg, std::vector<int> *up, int *up_primary,
                             std::vector<int> *acting, int *acting_primary,
			     bool raw_pg_to_pg = true) const;

public:
  /***
   * This is suitable only for looking at raw CRUSH outputs. It skips
   * applying the temp and up checks and should not be used
   * by anybody for data mapping purposes.
   * raw and primary must be non-NULL
   */
  void pg_to_raw_osds(pg_t pg, std::vector<int> *raw, int *primary) const;
  void pg_to_raw_upmap(pg_t pg, std::vector<int> *raw_upmap) const;
  /// map a pg to its acting set. @return acting set size
  void pg_to_acting_osds(const pg_t& pg, std::vector<int> *acting,
                        int *acting_primary) const {
    _pg_to_up_acting_osds(pg, NULL, NULL, acting, acting_primary);
  }
  void pg_to_acting_osds(pg_t pg, std::vector<int>& acting) const {
    return pg_to_acting_osds(pg, &acting, NULL);
  }
  /**
   * This does not apply temp overrides and should not be used
   * by anybody for data mapping purposes. Specify both pointers.
   */
  void pg_to_raw_up(pg_t pg, std::vector<int> *up, int *primary) const;
  /**
   * map a pg to its acting set as well as its up set. You must use
   * the acting set for data mapping purposes, but some users will
   * also find the up set useful for things like deciding what to
   * set as pg_temp.
   * Each of these pointers must be non-NULL.
   */
  void pg_to_up_acting_osds(pg_t pg, std::vector<int> *up, int *up_primary,
                            std::vector<int> *acting, int *acting_primary) const {
    _pg_to_up_acting_osds(pg, up, up_primary, acting, acting_primary);
  }
  void pg_to_up_acting_osds(pg_t pg, std::vector<int>& up, std::vector<int>& acting) const {
    int up_primary, acting_primary;
    pg_to_up_acting_osds(pg, &up, &up_primary, &acting, &acting_primary);
  }
  bool pg_is_ec(pg_t pg) const {
    auto i = pools.find(pg.pool());
    ceph_assert(i != pools.end());
    return i->second.is_erasure();
  }
  bool get_primary_shard(const pg_t& pgid, spg_t *out) const {
    auto i = get_pools().find(pgid.pool());
    if (i == get_pools().end()) {
      return false;
    }
    if (!i->second.is_erasure()) {
      *out = spg_t(pgid);
      return true;
    }
    int primary;
    std::vector<int> acting;
    pg_to_acting_osds(pgid, &acting, &primary);
    for (uint8_t i = 0; i < acting.size(); ++i) {
      if (acting[i] == primary) {
        *out = spg_t(pgid, shard_id_t(i));
        return true;
      }
    }
    return false;
  }
  bool get_primary_shard(const pg_t& pgid, int *primary, spg_t *out) const {
    auto i = get_pools().find(pgid.pool());
    if (i == get_pools().end()) {
      return false;
    }
    std::vector<int> acting;
    pg_to_acting_osds(pgid, &acting, primary);
    if (i->second.is_erasure()) {
      for (uint8_t i = 0; i < acting.size(); ++i) {
	if (acting[i] == *primary) {
	  *out = spg_t(pgid, shard_id_t(i));
	  return true;
	}
      }
    } else {
      *out = spg_t(pgid);
      return true;
    }
    return false;
  }

  const mempool::osdmap::map<int64_t,snap_interval_set_t>&
  get_removed_snaps_queue() const {
    return removed_snaps_queue;
  }
  const mempool::osdmap::map<int64_t,snap_interval_set_t>&
  get_new_removed_snaps() const {
    return new_removed_snaps;
  }
  const mempool::osdmap::map<int64_t,snap_interval_set_t>&
  get_new_purged_snaps() const {
    return new_purged_snaps;
  }

  int64_t lookup_pg_pool_name(const std::string& name) const {
    auto p = name_pool.find(name);
    if (p == name_pool.end())
      return -ENOENT;
    return p->second;
  }

  int64_t get_pool_max() const {
    return pool_max;
  }
  const mempool::osdmap::map<int64_t,pg_pool_t>& get_pools() const {
    return pools;
  }
  mempool::osdmap::map<int64_t,pg_pool_t>& get_pools() {
    return pools;
  }
  void get_pool_ids_by_rule(int rule_id, std::set<int64_t> *pool_ids) const {
    ceph_assert(pool_ids);
    for (auto &p: pools) {
      if (p.second.get_crush_rule() == rule_id) {
        pool_ids->insert(p.first);
      }
    }
  }
  void get_pool_ids_by_osd(CephContext *cct,
                           int osd,
                           std::set<int64_t> *pool_ids) const;
  const std::string& get_pool_name(int64_t p) const {
    auto i = pool_name.find(p);
    ceph_assert(i != pool_name.end());
    return i->second;
  }
  const mempool::osdmap::map<int64_t,std::string>& get_pool_names() const {
    return pool_name;
  }
  bool have_pg_pool(int64_t p) const {
    return pools.count(p);
  }
  const pg_pool_t* get_pg_pool(int64_t p) const {
    auto i = pools.find(p);
    if (i != pools.end())
      return &i->second;
    return NULL;
  }
  unsigned get_pg_size(pg_t pg) const {
    auto p = pools.find(pg.pool());
    ceph_assert(p != pools.end());
    return p->second.get_size();
  }
  int get_pg_type(pg_t pg) const {
    auto p = pools.find(pg.pool());
    ceph_assert(p != pools.end());
    return p->second.get_type();
  }


  pg_t raw_pg_to_pg(pg_t pg) const {
    auto p = pools.find(pg.pool());
    ceph_assert(p != pools.end());
    return p->second.raw_pg_to_pg(pg);
  }

  // pg -> acting primary osd
  int get_pg_acting_primary(pg_t pg) const {
    int primary = -1;
    _pg_to_up_acting_osds(pg, nullptr, nullptr, nullptr, &primary);
    return primary;
  }

  /*
   * check whether an spg_t maps to a particular osd
   */
  bool is_up_acting_osd_shard(spg_t pg, int osd) const {
    std::vector<int> up, acting;
    _pg_to_up_acting_osds(pg.pgid, &up, NULL, &acting, NULL, false);
    if (pg.shard == shard_id_t::NO_SHARD) {
      if (calc_pg_role(osd, acting, acting.size()) >= 0 ||
	  calc_pg_role(osd, up, up.size()) >= 0)
	return true;
    } else {
      if (pg.shard < (int)acting.size() && acting[pg.shard] == osd)
	return true;
      if (pg.shard < (int)up.size() && up[pg.shard] == osd)
	return true;
    }
    return false;
  }


  /* what replica # is a given osd? 0 primary, -1 for none. */
  static int calc_pg_rank(int osd, const std::vector<int>& acting, int nrep=0);
  static int calc_pg_role(int osd, const std::vector<int>& acting, int nrep=0);
  static bool primary_changed(
    int oldprimary,
    const std::vector<int> &oldacting,
    int newprimary,
    const std::vector<int> &newacting);
  
  /* rank is -1 (stray), 0 (primary), 1,2,3,... (replica) */
  int get_pg_acting_rank(pg_t pg, int osd) const {
    std::vector<int> group;
    pg_to_acting_osds(pg, group);
    return calc_pg_rank(osd, group, group.size());
  }
  /* role is -1 (stray), 0 (primary), 1 (replica) */
  int get_pg_acting_role(const pg_t& pg, int osd) const {
    std::vector<int> group;
    pg_to_acting_osds(pg, group);
    return calc_pg_role(osd, group, group.size());
  }

  bool osd_is_valid_op_target(pg_t pg, int osd) const {
    int primary;
    std::vector<int> group;
    pg_to_acting_osds(pg, &group, &primary);
    if (osd == primary)
      return true;
    if (pg_is_ec(pg))
      return false;

    return calc_pg_role(osd, group, group.size()) >= 0;
  }

  int clean_pg_upmaps(
    CephContext *cct,
    Incremental *pending_inc) const;

  bool try_pg_upmap(
    CephContext *cct,
    pg_t pg,                       ///< pg to potentially remap
    const std::set<int>& overfull,      ///< osds we'd want to evacuate
    const std::vector<int>& underfull,  ///< osds to move to, in order of preference
    std::vector<int> *orig,
    std::vector<int> *out);             ///< resulting alternative mapping

  int calc_pg_upmaps(
    CephContext *cct,
    float max_deviation, ///< max deviation from target (value < 1.0)
    int max_iterations,  ///< max iterations to run
    const std::set<int64_t>& pools,        ///< [optional] restrict to pool
    Incremental *pending_inc
    );

  int get_osds_by_bucket_name(const std::string &name, std::set<int> *osds) const;

  bool have_pg_upmaps(pg_t pg) const {
    return pg_upmap.count(pg) ||
      pg_upmap_items.count(pg);
  }

  /*
   * handy helpers to build simple maps...
   */
  /**
   * Build an OSD map suitable for basic usage. If **num_osd** is >= 0
   * it will be initialized with the specified number of OSDs in a
   * single host. If **num_osd** is < 0 the layout of the OSD map will 
   * be built by reading the content of the configuration file.
   *
   * @param cct [in] in core ceph context 
   * @param e [in] initial epoch
   * @param fsid [in] id of the cluster
   * @param num_osd [in] number of OSDs if >= 0 or read from conf if < 0
   * @return **0** on success, negative errno on error.
   */
private:
  int build_simple_optioned(CephContext *cct, epoch_t e, uuid_d &fsid,
			    int num_osd, int pg_bits, int pgp_bits,
			    bool default_pool);
public:
  int build_simple(CephContext *cct, epoch_t e, uuid_d &fsid,
		   int num_osd) {
    return build_simple_optioned(cct, e, fsid, num_osd, 0, 0, false);
  }
  int build_simple_with_pool(CephContext *cct, epoch_t e, uuid_d &fsid,
			     int num_osd, int pg_bits, int pgp_bits) {
    return build_simple_optioned(cct, e, fsid, num_osd,
				 pg_bits, pgp_bits, true);
  }
  static int _build_crush_types(CrushWrapper& crush);
  static int build_simple_crush_map(CephContext *cct, CrushWrapper& crush,
				    int num_osd, std::ostream *ss);
  static int build_simple_crush_map_from_conf(CephContext *cct,
					      CrushWrapper& crush,
					      std::ostream *ss);
  static int build_simple_crush_rules(
    CephContext *cct, CrushWrapper& crush,
    const std::string& root,
    std::ostream *ss);

  bool crush_rule_in_use(int rule_id) const;

  int validate_crush_rules(CrushWrapper *crush, std::ostream *ss) const;

  void clear_temp() {
    pg_temp->clear();
    primary_temp->clear();
  }

private:
  void print_osd_line(int cur, std::ostream *out, ceph::Formatter *f) const;
public:
  void print(std::ostream& out) const;
  void print_pools(std::ostream& out) const;
  void print_summary(ceph::Formatter *f, std::ostream& out, const std::string& prefix, bool extra=false) const;
  void print_oneline_summary(std::ostream& out) const;

  enum {
    DUMP_IN = 1,         // only 'in' osds
    DUMP_OUT = 2,        // only 'out' osds
    DUMP_UP = 4,         // only 'up' osds
    DUMP_DOWN = 8,       // only 'down' osds
    DUMP_DESTROYED = 16, // only 'destroyed' osds
  };
  void print_tree(ceph::Formatter *f, std::ostream *out, unsigned dump_flags=0, std::string bucket="") const;

  int summarize_mapping_stats(
    OSDMap *newmap,
    const std::set<int64_t> *pools,
    std::string *out,
    ceph::Formatter *f) const;

  std::string get_flag_string() const;
  static std::string get_flag_string(unsigned flags);
  static void dump_erasure_code_profiles(
    const mempool::osdmap::map<std::string,std::map<std::string,std::string> > &profiles,
    ceph::Formatter *f);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<OSDMap*>& o);
  bool check_new_blacklist_entries() const { return new_blacklist_entries; }

  void check_health(health_check_map_t *checks) const;

  int parse_osd_id_list(const std::vector<std::string>& ls,
			std::set<int> *out,
			std::ostream *ss) const;

  float pool_raw_used_rate(int64_t poolid) const;

};
WRITE_CLASS_ENCODER_FEATURES(OSDMap)
WRITE_CLASS_ENCODER_FEATURES(OSDMap::Incremental)

typedef std::shared_ptr<const OSDMap> OSDMapRef;

inline std::ostream& operator<<(std::ostream& out, const OSDMap& m) {
  m.print_oneline_summary(out);
  return out;
}

class PGMap;

void print_osd_utilization(const OSDMap& osdmap,
                           const PGMap& pgmap,
                           std::ostream& out,
                           ceph::Formatter *f,
                           bool tree,
                           const std::string& class_name,
                           const std::string& item_name);

#endif
