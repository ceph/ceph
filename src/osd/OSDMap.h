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


#ifndef CEPH_OSDMAP_H
#define CEPH_OSDMAP_H

/*
 * describe properties of the OSD cluster.
 *   disks, disk groups, total # osds,
 *
 */
#include "common/config.h"
#include "include/types.h"
#include "osd_types.h"
#include "msg/Message.h"
#include "common/Mutex.h"
#include "common/Clock.h"

#include "include/ceph_features.h"

#include "crush/CrushWrapper.h"

#include "include/interval_set.h"

#include <vector>
#include <list>
#include <set>
#include <map>
#include <tr1/memory>
using namespace std;

#include <ext/hash_set>
using __gnu_cxx::hash_set;

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

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<osd_info_t*>& o);
};
WRITE_CLASS_ENCODER(osd_info_t)

ostream& operator<<(ostream& out, const osd_info_t& info);


struct osd_xinfo_t {
  utime_t down_stamp;      ///< timestamp when we were last marked down
  float laggy_probability; ///< encoded as __u32: 0 = definitely not laggy, 0xffffffff definitely laggy
  __u32 laggy_interval;    ///< average interval between being marked laggy and recovering

  osd_xinfo_t() : laggy_probability(0), laggy_interval(0) {}

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<osd_xinfo_t*>& o);
};
WRITE_CLASS_ENCODER(osd_xinfo_t)

ostream& operator<<(ostream& out, const osd_xinfo_t& xi);


/** OSDMap
 */
class OSDMap {

public:
  class Incremental {
  public:
    uuid_d fsid;
    epoch_t epoch;   // new epoch; we are a diff from epoch-1 to epoch
    utime_t modified;
    int64_t new_pool_max; //incremented by the OSDMonitor on each pool create
    int32_t new_flags;

    // full (rare)
    bufferlist fullmap;  // in leiu of below.
    bufferlist crush;

    // incremental
    int32_t new_max_osd;
    map<int64_t,pg_pool_t> new_pools;
    map<int64_t,string> new_pool_names;
    set<int64_t> old_pools;
    map<int32_t,entity_addr_t> new_up_client;
    map<int32_t,entity_addr_t> new_up_cluster;
    map<int32_t,uint8_t> new_state;             // XORed onto previous state.
    map<int32_t,uint32_t> new_weight;
    map<pg_t,vector<int32_t> > new_pg_temp;     // [] to remove
    map<int32_t,epoch_t> new_up_thru;
    map<int32_t,pair<epoch_t,epoch_t> > new_last_clean_interval;
    map<int32_t,epoch_t> new_lost;
    map<int32_t,uuid_d> new_uuid;
    map<int32_t,osd_xinfo_t> new_xinfo;

    map<entity_addr_t,utime_t> new_blacklist;
    vector<entity_addr_t> old_blacklist;
    map<int32_t, entity_addr_t> new_hb_back_up;
    map<int32_t, entity_addr_t> new_hb_front_up;

    string cluster_snapshot;

    int get_net_marked_out(const OSDMap *previous) const;
    int get_net_marked_down(const OSDMap *previous) const;
    int identify_osd(uuid_d u) const;

    void encode_client_old(bufferlist& bl) const;
    void encode(bufferlist& bl, uint64_t features=CEPH_FEATURES_ALL) const;
    void decode(bufferlist::iterator &p);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<Incremental*>& o);

    Incremental(epoch_t e=0) :
      epoch(e), new_pool_max(-1), new_flags(-1), new_max_osd(-1) {
      memset(&fsid, 0, sizeof(fsid));
    }
    Incremental(bufferlist &bl) {
      bufferlist::iterator p = bl.begin();
      decode(p);
    }
    Incremental(bufferlist::iterator &p) {
      decode(p);
    }
  };
  
private:
  uuid_d fsid;
  epoch_t epoch;        // what epoch of the osd cluster descriptor is this
  utime_t created, modified; // epoch start time
  int32_t pool_max;     // the largest pool num, ever

  uint32_t flags;

  int num_osd;         // not saved
  int32_t max_osd;
  vector<uint8_t> osd_state;

  struct addrs_s {
    vector<std::tr1::shared_ptr<entity_addr_t> > client_addr;
    vector<std::tr1::shared_ptr<entity_addr_t> > cluster_addr;
    vector<std::tr1::shared_ptr<entity_addr_t> > hb_back_addr;
    vector<std::tr1::shared_ptr<entity_addr_t> > hb_front_addr;
    entity_addr_t blank;
  };
  std::tr1::shared_ptr<addrs_s> osd_addrs;

  vector<__u32>   osd_weight;   // 16.16 fixed point, 0x10000 = "in", 0 = "out"
  vector<osd_info_t> osd_info;
  std::tr1::shared_ptr< map<pg_t,vector<int> > > pg_temp;  // temp pg mapping (e.g. while we rebuild)

  map<int64_t,pg_pool_t> pools;
  map<int64_t,string> pool_name;
  map<string,int64_t> name_pool;

  std::tr1::shared_ptr< vector<uuid_d> > osd_uuid;
  vector<osd_xinfo_t> osd_xinfo;

  hash_map<entity_addr_t,utime_t> blacklist;

  epoch_t cluster_snapshot_epoch;
  string cluster_snapshot;

 public:
  std::tr1::shared_ptr<CrushWrapper> crush;       // hierarchical map

  friend class OSDMonitor;
  friend class PGMonitor;
  friend class MDS;

 public:
  OSDMap() : epoch(0), 
	     pool_max(-1),
	     flags(0),
	     num_osd(0), max_osd(0),
	     osd_addrs(new addrs_s),
	     pg_temp(new map<pg_t,vector<int> >),
	     osd_uuid(new vector<uuid_d>),
	     cluster_snapshot_epoch(0),
	     crush(new CrushWrapper) {
    memset(&fsid, 0, sizeof(fsid));
  }

  // map info
  const uuid_d& get_fsid() const { return fsid; }
  void set_fsid(uuid_d& f) { fsid = f; }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  void set_epoch(epoch_t e);

  /* stamps etc */
  const utime_t& get_created() const { return created; }
  const utime_t& get_modified() const { return modified; }

  bool is_blacklisted(const entity_addr_t& a) const;

  string get_cluster_snapshot() const {
    if (cluster_snapshot_epoch == epoch)
      return cluster_snapshot;
    return string();
  }

  /***** cluster state *****/
  /* osds */
  int get_max_osd() const { return max_osd; }
  void set_max_osd(int m);

  int get_num_osds() const {
    return num_osd;
  }
  int calc_num_osds();

  void get_all_osds(set<int32_t>& ls) const;
  int get_num_up_osds() const;
  int get_num_in_osds() const;

  int get_flags() const { return flags; }
  int test_flag(int f) const { return flags & f; }
  void set_flag(int f) { flags |= f; }
  void clear_flag(int f) { flags &= ~f; }

  static void calc_state_set(int state, set<string>& st);

  int get_state(int o) const {
    assert(o < max_osd);
    return osd_state[o];
  }
  int get_state(int o, set<string>& st) const {
    assert(o < max_osd);
    unsigned t = osd_state[o];
    calc_state_set(t, st);
    return osd_state[o];
  }
  void set_state(int o, unsigned s) {
    assert(o < max_osd);
    osd_state[o] = s;
  }
  void set_weightf(int o, float w) {
    set_weight(o, (int)((float)CEPH_OSD_IN * w));
  }
  void set_weight(int o, unsigned w) {
    assert(o < max_osd);
    osd_weight[o] = w;
    if (w)
      osd_state[o] |= CEPH_OSD_EXISTS;
  }
  unsigned get_weight(int o) const {
    assert(o < max_osd);
    return osd_weight[o];
  }
  float get_weightf(int o) const {
    return (float)get_weight(o) / (float)CEPH_OSD_IN;
  }
  void adjust_osd_weights(const map<int,double>& weights, Incremental& inc) const;

  bool exists(int osd) const {
    //assert(osd >= 0);
    return osd >= 0 && osd < max_osd && (osd_state[osd] & CEPH_OSD_EXISTS);
  }

  bool is_up(int osd) const {
    return exists(osd) && (osd_state[osd] & CEPH_OSD_UP);
  }

  bool is_down(int osd) const {
    return !exists(osd) || !is_up(osd);
  }

  bool is_out(int osd) const {
    return !exists(osd) || get_weight(osd) == CEPH_OSD_OUT;
  }

  bool is_in(int osd) const {
    return exists(osd) && !is_out(osd);
  }

  /**
   * check if an entire crush subtre is down
   */
  bool subtree_is_down(int id, set<int> *down_cache) const;
  bool containing_subtree_is_down(CephContext *cct, int osd, int subtree_type, set<int> *down_cache) const;
  
  int identify_osd(const entity_addr_t& addr) const;
  int identify_osd(const uuid_d& u) const;

  bool have_addr(const entity_addr_t& addr) const {
    return identify_osd(addr) >= 0;
  }
  bool find_osd_on_ip(const entity_addr_t& ip) const;
  bool have_inst(int osd) const {
    return exists(osd) && is_up(osd); 
  }
  const entity_addr_t &get_addr(int osd) const {
    assert(exists(osd));
    return osd_addrs->client_addr[osd] ? *osd_addrs->client_addr[osd] : osd_addrs->blank;
  }
  const entity_addr_t &get_cluster_addr(int osd) const {
    assert(exists(osd));
    if (!osd_addrs->cluster_addr[osd] || *osd_addrs->cluster_addr[osd] == entity_addr_t())
      return get_addr(osd);
    return *osd_addrs->cluster_addr[osd];
  }
  const entity_addr_t &get_hb_back_addr(int osd) const {
    assert(exists(osd));
    return osd_addrs->hb_back_addr[osd] ? *osd_addrs->hb_back_addr[osd] : osd_addrs->blank;
  }
  const entity_addr_t &get_hb_front_addr(int osd) const {
    assert(exists(osd));
    return osd_addrs->hb_front_addr[osd] ? *osd_addrs->hb_front_addr[osd] : osd_addrs->blank;
  }
  entity_inst_t get_inst(int osd) const {
    assert(is_up(osd));
    return entity_inst_t(entity_name_t::OSD(osd), get_addr(osd));
  }
  entity_inst_t get_cluster_inst(int osd) const {
    assert(is_up(osd));
    return entity_inst_t(entity_name_t::OSD(osd), get_cluster_addr(osd));
  }
  entity_inst_t get_hb_back_inst(int osd) const {
    assert(is_up(osd));
    return entity_inst_t(entity_name_t::OSD(osd), get_hb_back_addr(osd));
  }
  entity_inst_t get_hb_front_inst(int osd) const {
    assert(is_up(osd));
    return entity_inst_t(entity_name_t::OSD(osd), get_hb_front_addr(osd));
  }

  const uuid_d& get_uuid(int osd) const {
    assert(exists(osd));
    return (*osd_uuid)[osd];
  }

  const epoch_t& get_up_from(int osd) const {
    assert(exists(osd));
    return osd_info[osd].up_from;
  }
  const epoch_t& get_up_thru(int osd) const {
    assert(exists(osd));
    return osd_info[osd].up_thru;
  }
  const epoch_t& get_down_at(int osd) const {
    assert(exists(osd));
    return osd_info[osd].down_at;
  }
  const osd_info_t& get_info(int osd) const {
    assert(osd < max_osd);
    return osd_info[osd];
  }

  const osd_xinfo_t& get_xinfo(int osd) const {
    assert(osd < max_osd);
    return osd_xinfo[osd];
  }
  
  int get_any_up_osd() const {
    for (int i=0; i<max_osd; i++)
      if (is_up(i))
	return i;
    return -1;
  }

  int get_next_up_osd_after(int n) const {
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

  /**
   * get feature bits required by the current structure
   *
   * @param mask [out] set of all possible map-related features we could set
   * @return feature bits used by this map
   */
  uint64_t get_features(uint64_t *mask) const;

  int apply_incremental(const Incremental &inc);

  /// try to re-use/reference addrs in oldmap from newmap
  static void dedup(const OSDMap *oldmap, OSDMap *newmap);

  // serialize, unserialize
private:
  void encode_client_old(bufferlist& bl) const;
public:
  void encode(bufferlist& bl, uint64_t features=CEPH_FEATURES_ALL) const;
  void decode(bufferlist& bl);
  void decode(bufferlist::iterator& p);


  /****   mapping facilities   ****/
  int object_locator_to_pg(const object_t& oid, const object_locator_t& loc, pg_t &pg) const;
  pg_t object_locator_to_pg(const object_t& oid, const object_locator_t& loc) const {
    pg_t pg;
    int ret = object_locator_to_pg(oid, loc, pg);
    assert(ret == 0);
    return pg;
  }

  static object_locator_t file_to_object_locator(const ceph_file_layout& layout) {
    return object_locator_t(layout.fl_pg_pool);
  }

  // oid -> pg
  ceph_object_layout file_to_object_layout(object_t oid, ceph_file_layout& layout) const {
    return make_object_layout(oid, layout.fl_pg_pool);
  }

  ceph_object_layout make_object_layout(object_t oid, int pg_pool) const;

  int get_pg_num(int pg_pool) const
  {
    const pg_pool_t *pool = get_pg_pool(pg_pool);
    return pool->get_pg_num();
  }

private:
  /// pg -> (raw osd list)
  int _pg_to_osds(const pg_pool_t& pool, pg_t pg, vector<int>& osds) const;
  void _remove_nonexistent_osds(vector<int>& osds) const;

  /// pg -> (up osd list)
  void _raw_to_up_osds(pg_t pg, vector<int>& raw, vector<int>& up) const;

  bool _raw_to_temp_osds(const pg_pool_t& pool, pg_t pg, vector<int>& raw, vector<int>& temp) const;

public:
  int pg_to_osds(pg_t pg, vector<int>& raw) const;
  int pg_to_acting_osds(pg_t pg, vector<int>& acting) const;
  void pg_to_raw_up(pg_t pg, vector<int>& up) const;
  void pg_to_up_acting_osds(pg_t pg, vector<int>& up, vector<int>& acting) const;

  int64_t lookup_pg_pool_name(const string& name) {
    if (name_pool.count(name))
      return name_pool[name];
    return -ENOENT;
  }

  int64_t const_lookup_pg_pool_name(const char *name) const {
    return const_cast<OSDMap *>(this)->lookup_pg_pool_name(name);
  }

  int64_t get_pool_max() const {
    return pool_max;
  }
  const map<int64_t,pg_pool_t>& get_pools() const {
    return pools;
  }
  const char *get_pool_name(int64_t p) const {
    map<int64_t, string>::const_iterator i = pool_name.find(p);
    if (i != pool_name.end())
      return i->second.c_str();
    return 0;
  }
  bool have_pg_pool(int64_t p) const {
    return pools.count(p);
  }
  const pg_pool_t* get_pg_pool(int64_t p) const {
    map<int64_t, pg_pool_t>::const_iterator i = pools.find(p);
    if (i != pools.end())
      return &i->second;
    return NULL;
  }
  unsigned get_pg_size(pg_t pg) const {
    map<int64_t,pg_pool_t>::const_iterator p = pools.find(pg.pool());
    assert(p != pools.end());
    return p->second.get_size();
  }
  int get_pg_type(pg_t pg) const {
    assert(pools.count(pg.pool()));
    return pools.find(pg.pool())->second.get_type();
  }


  pg_t raw_pg_to_pg(pg_t pg) const {
    assert(pools.count(pg.pool()));
    return pools.find(pg.pool())->second.raw_pg_to_pg(pg);
  }

  // pg -> primary osd
  int get_pg_primary(pg_t pg) const {
    vector<int> group;
    int nrep = pg_to_osds(pg, group);
    if (nrep)
      return group[0];
    return -1;  // we fail!
  }

  // pg -> acting primary osd
  int get_pg_acting_primary(pg_t pg) const {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    if (nrep > 0)
      return group[0];
    return -1;  // we fail!
  }
  int get_pg_acting_tail(pg_t pg) const {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    if (nrep > 0)
      return group[group.size()-1];
    return -1;  // we fail!
  }


  /* what replica # is a given osd? 0 primary, -1 for none. */
  static int calc_pg_rank(int osd, vector<int>& acting, int nrep=0);
  static int calc_pg_role(int osd, vector<int>& acting, int nrep=0);
  
  /* rank is -1 (stray), 0 (primary), 1,2,3,... (replica) */
  int get_pg_acting_rank(pg_t pg, int osd) const {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    return calc_pg_rank(osd, group, nrep);
  }
  /* role is -1 (stray), 0 (primary), 1 (replica) */
  int get_pg_acting_role(pg_t pg, int osd) const {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    return calc_pg_role(osd, group, nrep);
  }


  /*
   * handy helpers to build simple maps...
   */
  void build_simple(CephContext *cct, epoch_t e, uuid_d &fsid,
		    int num_osd, int pg_bits, int pgp_bits);
  int build_simple_from_conf(CephContext *cct, epoch_t e, uuid_d &fsid,
			     int pg_bits, int pgp_bits);
  static void build_simple_crush_map(CephContext *cct, CrushWrapper& crush,
				     map<int, const char*>& poolsets, int num_osd);
  static void build_simple_crush_map_from_conf(CephContext *cct, CrushWrapper& crush,
					       map<int, const char*>& rulesets);

  bool crush_ruleset_in_use(int ruleset) const;

private:
  void print_osd_line(int cur, ostream *out, Formatter *f) const;
public:
  void print(ostream& out) const;
  void print_summary(ostream& out) const;
  void print_tree(ostream *out, Formatter *f) const;

  string get_flag_string() const;
  static string get_flag_string(unsigned flags);
  void dump_json(ostream& out) const;
  void dump(Formatter *f) const;
  static void generate_test_instances(list<OSDMap*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(OSDMap)
WRITE_CLASS_ENCODER_FEATURES(OSDMap::Incremental)

typedef std::tr1::shared_ptr<const OSDMap> OSDMapRef;

inline ostream& operator<<(ostream& out, const OSDMap& m) {
  m.print_summary(out);
  return out;
}


#endif
