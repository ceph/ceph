// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "gtest/gtest.h"
#include "osd/OSDMap.h"
#include "osd/OSDMapMapping.h"
#include "mon/OSDMonitor.h"
#include "mon/PGMap.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include "common/ceph_json.h"

#include <iostream>
#include <cmath>

using namespace std;

int main(int argc, char **argv) {
  map<string,string> defaults = {
    // make sure we have 3 copies, or some tests won't work
    { "osd_pool_default_size", "3" },
    // our map is flat, so just try and split across OSDs, not hosts or whatever
    { "osd_crush_chooseleaf_type", "0" },
  };
  std::vector<const char*> args(argv, argv+argc);
  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class OSDMapTest : public testing::Test,
                   public ::testing::WithParamInterface<std::pair<int, int>> {
  int num_osds = 6;
  bool verbose = false;
public:
  OSDMap osdmap;
  OSDMapMapping mapping;
  const uint64_t my_ec_pool = 1;
  const uint64_t my_rep_pool = 2;

  // Blacklist testing lists
  // I pulled the first two ranges and their start/end points from
  // https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_notation
  static const string range_addrs[];
  static const string ip_addrs[];
  static const string unblocked_ip_addrs[];
  const string EC_RULE_NAME = "erasure";

  OSDMapTest() {}
  void set_verbose(bool v) { verbose = v; }
  bool is_verbose() const {return verbose; }
  void set_up_map(int new_num_osds = 6, bool no_default_pools = false) {
    num_osds = new_num_osds;
    uuid_d fsid;
    osdmap.build_simple(g_ceph_context, 0, fsid, num_osds);
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    pending_inc.fsid = osdmap.get_fsid();
    entity_addrvec_t sample_addrs;
    sample_addrs.v.push_back(entity_addr_t());
    uuid_d sample_uuid;
    for (int i = 0; i < num_osds; ++i) {
      sample_uuid.generate_random();
      sample_addrs.v[0].nonce = i;
      pending_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
      pending_inc.new_up_client[i] = sample_addrs;
      pending_inc.new_up_cluster[i] = sample_addrs;
      pending_inc.new_hb_back_up[i] = sample_addrs;
      pending_inc.new_hb_front_up[i] = sample_addrs;
      pending_inc.new_weight[i] = CEPH_OSD_IN;
      pending_inc.new_uuid[i] = sample_uuid;
    }
    osdmap.apply_incremental(pending_inc);
    if (no_default_pools) // do not create any default pool(s)
      return;

    OSDMap::Incremental new_pool_inc(osdmap.get_epoch() + 1);
    new_pool_inc.new_pool_max = osdmap.get_pool_max();
    new_pool_inc.fsid = osdmap.get_fsid();
    // make an ec pool
    set_ec_pool("ec", new_pool_inc);
    // and a replicated pool
    set_rep_pool("reppool",new_pool_inc);
    osdmap.apply_incremental(new_pool_inc);
  }
  //
  // The following function is currently used only for read balancer which works
  // only on replicated pools. EC pool is created just to keep pool numbers consistent
  // weights are integers in [0..100]
  // [ods#i_weight, osd#i_host] = weights[i]
  // *Note:* hosts are not yet implemented.
  //
  void set_up_map_heterogeneous(const vector<pair<int, int>> &weights, int new_num_hosts) {

    num_osds = weights.size();
    uuid_d fsid;
    osdmap.build_simple(g_ceph_context, 0, fsid, num_osds);
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    pending_inc.fsid = osdmap.get_fsid();
    entity_addrvec_t sample_addrs;
    sample_addrs.v.push_back(entity_addr_t());
    uuid_d sample_uuid;
    for (int i = 0; i < num_osds; ++i) {
      int ow = weights[i].first;
      ceph_assert(ow >= 0 && ow <= 100);
      ow = ow * CEPH_OSD_IN / 100;
      sample_uuid.generate_random();
      sample_addrs.v[0].nonce = i;
      pending_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
      pending_inc.new_up_client[i] = sample_addrs;
      pending_inc.new_up_cluster[i] = sample_addrs;
      pending_inc.new_hb_back_up[i] = sample_addrs;
      pending_inc.new_hb_front_up[i] = sample_addrs;
      pending_inc.new_weight[i] = ow;
      pending_inc.new_uuid[i] = sample_uuid;
    }
    osdmap.apply_incremental(pending_inc);

    OSDMap::Incremental new_pool_inc(osdmap.get_epoch() + 1);
    new_pool_inc.new_pool_max = osdmap.get_pool_max();
    new_pool_inc.fsid = osdmap.get_fsid();
    // make a not needed ec pool just to keep pool numbers consistent
    set_ec_pool("ec", new_pool_inc);
    // and a replicated pool
    set_rep_pool("reppool",new_pool_inc);

    //
    // Force npgs to be a power of 2
    //
    ceph_assert(new_pool_inc.new_pools.contains(my_rep_pool));
    int npgs = new_pool_inc.new_pools[my_rep_pool].get_pg_num();
    float lg = log2(npgs);
    if (ceil(lg) != floor(lg)) {  // npgs is not a power of 2
      cout << "******* Fix npgs from " << npgs << " to " << (1 << (int)ceil(lg)) << std::endl;
      npgs = 1 << (int)ceil(lg);
      new_pool_inc.new_pools[my_rep_pool].set_pg_num(1 << (int)ceil(lg));
      new_pool_inc.new_pools[my_rep_pool].set_pgp_num(1 << (int)ceil(lg));
    }
    osdmap.apply_incremental(new_pool_inc);
  }
  int get_ec_crush_rule() {
    int r = osdmap.crush->get_rule_id(EC_RULE_NAME);
    if (r < 0) {
      r = osdmap.crush->add_simple_rule(
        EC_RULE_NAME, "default", "osd", "",
        "indep", pg_pool_t::TYPE_ERASURE,
        &cerr);
    }
    return r;
  }
  uint64_t set_ec_pool(const string &name, OSDMap::Incremental &new_pool_inc,
                       bool assert_pool_id = true) {
    pg_pool_t empty;
    uint64_t pool_id = ++new_pool_inc.new_pool_max;
    if (assert_pool_id)
      ceph_assert(pool_id == my_ec_pool);
    pg_pool_t *p = new_pool_inc.get_new_pool(pool_id, &empty);
    p->size = 3;
    p->set_pg_num(64);
    p->set_pgp_num(64);
    p->type = pg_pool_t::TYPE_ERASURE;
    p->crush_rule = get_ec_crush_rule();
    new_pool_inc.new_pool_names[pool_id] = name;//"ec";
    return pool_id;
  }
  uint64_t set_rep_pool(const string name, OSDMap::Incremental &new_pool_inc,
                        bool assert_pool_id = true) {
    pg_pool_t empty;
    uint64_t pool_id = ++new_pool_inc.new_pool_max;
    if (assert_pool_id)
      ceph_assert(pool_id == my_rep_pool);
    pg_pool_t *p = new_pool_inc.get_new_pool(pool_id, &empty);
    p->size = 3;
    p->set_pg_num(64);
    p->set_pgp_num(64);
    p->type = pg_pool_t::TYPE_REPLICATED;
    p->crush_rule = 0;
    p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    new_pool_inc.new_pool_names[pool_id] = name;//"reppool";
    return pool_id;
  }

  void balance_capacity(int64_t pid) {
    set<int64_t> only_pools;
    only_pools.insert(pid);
    OSDMap::Incremental pending_inc(osdmap.get_epoch()+1);
    osdmap.calc_pg_upmaps(g_ceph_context,
                          0,
                          100,
                          only_pools,
                          &pending_inc);
    osdmap.apply_incremental(pending_inc);
  }

  void set_pool_read_ratio(uint64_t pid, int64_t ratio) {
    ceph_assert(ratio >= 0 && ratio <= 100);
    OSDMap::Incremental pending_inc(osdmap.get_epoch()+1);
    pg_pool_t *np = nullptr;
    {
      const pg_pool_t *p = osdmap.get_pg_pool(pid);
      np = pending_inc.get_new_pool(pid, p);
      ceph_assert(np != nullptr);
    }
    if (ratio == 0) {
      np->opts.unset(pool_opts_t::READ_RATIO);
    } else {
      np->opts.set(pool_opts_t::READ_RATIO, ratio);
    }
    osdmap.apply_incremental(pending_inc);
  }

  unsigned int get_num_osds() { return num_osds; }
  void get_crush(const OSDMap& tmap, CrushWrapper& newcrush) {
    bufferlist bl;
    tmap.crush->encode(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);
    auto p = bl.cbegin();
    newcrush.decode(p);
  }
  int crush_move(OSDMap& tmap, const string &name, const vector<string> &argvec) {
    map<string,string> loc;
    CrushWrapper::parse_loc_map(argvec, &loc);
    CrushWrapper newcrush;
    get_crush(tmap, newcrush);
    if (!newcrush.name_exists(name)) {
       return -ENOENT;
    }
    int id = newcrush.get_item_id(name);
    int err;
    if (!newcrush.check_item_loc(g_ceph_context, id, loc, (int *)NULL)) {
      if (id >= 0) {
        err = newcrush.create_or_move_item(g_ceph_context, id, 0, name, loc);
      } else {
        err = newcrush.move_bucket(g_ceph_context, id, loc);
      }
      if (err >= 0) {
        OSDMap::Incremental pending_inc(tmap.get_epoch() + 1);
        pending_inc.crush.clear();
        newcrush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
        tmap.apply_incremental(pending_inc);
        err = 0;
      }
    } else {
      // already there
      err = 0;
    }
    return err;
  }
  int crush_rule_create_replicated(const string &name,
                                   const string &root,
                                   const string &type) {
    if (osdmap.crush->rule_exists(name)) {
      return osdmap.crush->get_rule_id(name);
    }
    CrushWrapper newcrush;
    get_crush(osdmap, newcrush);
    string device_class;
    stringstream ss;
    int ruleno = newcrush.add_simple_rule(
              name, root, type, device_class,
              "firstn", pg_pool_t::TYPE_REPLICATED, &ss);
    if (ruleno >= 0) {
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
      osdmap.apply_incremental(pending_inc);
    }
    return ruleno;
  }
  void test_mappings(int pool,
		     int num,
		     vector<int> *any,
		     vector<int> *first,
		     vector<int> *primary) {
    mapping.update(osdmap);
    for (int i=0; i<num; ++i) {
      vector<int> up, acting;
      int up_primary, acting_primary;
      pg_t pgid(i, pool);
      osdmap.pg_to_up_acting_osds(pgid,
				  &up, &up_primary, &acting, &acting_primary);
      for (unsigned j=0; j<acting.size(); ++j)
	(*any)[acting[j]]++;
      if (!acting.empty())
	(*first)[acting[0]]++;
      if (acting_primary >= 0)
	(*primary)[acting_primary]++;

      // compare to precalc mapping
      vector<int> up2, acting2;
      int up_primary2, acting_primary2;
      pgid = osdmap.raw_pg_to_pg(pgid);
      mapping.get(pgid, &up2, &up_primary2, &acting2, &acting_primary2);
      ASSERT_EQ(up, up2);
      ASSERT_EQ(up_primary, up_primary2);
      ASSERT_EQ(acting, acting2);
      ASSERT_EQ(acting_primary, acting_primary2);
    }
    cout << "any: " << *any << std::endl;;
    cout << "first: " << *first << std::endl;;
    cout << "primary: " << *primary << std::endl;;
  }
  void clean_pg_upmaps(CephContext *cct,
                       const OSDMap& om,
                       OSDMap::Incremental& pending_inc) {
    int cpu_num = 8;
    int pgs_per_chunk = 256;
    ThreadPool tp(cct, "BUG_40104::clean_upmap_tp", "clean_upmap_tp", cpu_num);
    tp.start();
    ParallelPGMapper mapper(cct, &tp);
    vector<pg_t> pgs_to_check;
    om.get_upmap_pgs(&pgs_to_check);
    OSDMonitor::CleanUpmapJob job(cct, om, pending_inc);
    mapper.queue(&job, pgs_per_chunk, pgs_to_check);
    job.wait();
    tp.stop();
  }
  void set_primary_affinity_all(float pa) {
    for (uint i = 0 ; i < get_num_osds() ; i++) {
      osdmap.set_primary_affinity(i, int(pa * CEPH_OSD_MAX_PRIMARY_AFFINITY));
    }
  }
  bool score_in_range(float score, uint nosds = 0) {
    if (nosds == 0) {
      nosds = get_num_osds();
    }
    return score >= 1.0 && score <= float(nosds);
  }

  void test_rb_osd_size(const vector <pair<int, int>> &weights, const vector<int> &read_ratios, bool expect_failure, bool reset_rb = false, bool build = true) {

    if (build) {
      set_up_map_heterogeneous(weights, 1);
    }
    int num_osds = osdmap.get_num_osds();

    map<uint64_t,set<pg_t>> orig_prim_pgs_by_osd;
    map<uint64_t,set<pg_t>> pgs_by_osd = osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &orig_prim_pgs_by_osd);
    if (verbose) {
      cout << "PGs distribution:" << std::endl;
      for (auto i = 0 ; i < num_osds ; i++) {
        cout << "osd." << i << ": " << pgs_by_osd[i].size() << std::endl;
      }
    }
    if (reset_rb) {
      OSDMap::Incremental pending_inc(osdmap.get_epoch()+1);
      osdmap.rm_all_upmap_prims(g_ceph_context, &pending_inc, my_rep_pool);
      osdmap.apply_incremental(pending_inc);
    }

    // Make sure capacity is balanced first
    balance_capacity(my_rep_pool);
    pgs_by_osd.clear();
    orig_prim_pgs_by_osd.clear();
    pgs_by_osd = osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &orig_prim_pgs_by_osd);

    map<uint64_t,set<pg_t>> prim_pgs_by_osd;
    if (verbose) {
      cout << "Balanced PG distribution:" << std::endl;
      for (auto i = 0 ; i < num_osds ; i++) {
        cout << "osd." << i << ": " << pgs_by_osd[i].size() << "/" << orig_prim_pgs_by_osd[i].size() << std::endl;
      }
    }

    vector<uint64_t> osds_to_check(num_osds);
    for (auto i = 0 ; i < num_osds ; i++) {
      osds_to_check[i] = i;
    }

    const pg_pool_t *p = osdmap.get_pg_pool(my_rep_pool);
    auto pg_num = p->get_pg_num();

    for (auto rr : read_ratios) {
      map<uint64_t, float> desired_prims;
      set_pool_read_ratio(my_rep_pool, rr);
      if (reset_rb) {
        OSDMap::Incremental pending_inc(osdmap.get_epoch()+1);
        osdmap.rm_all_upmap_prims(g_ceph_context, &pending_inc, my_rep_pool);
        osdmap.apply_incremental(pending_inc);
      }
      int rc = osdmap.calc_desired_primary_distribution_osdsize_opt(g_ceph_context, my_rep_pool,
                                                                    osds_to_check, desired_prims);
      if (expect_failure) {
        ASSERT_TRUE(rc < 0);
        if (verbose) {
          cout << "Can't calculate osd-size-optimized read balancing - this is expected" << std::endl;
        }
        return;
      } else {
        ASSERT_TRUE(rc >= 0);
      }
      if (verbose) {
        cout << ">>>>>Desired primary distribution for read ratio: " << rr << std::endl;
      }
      float total_prims = 0.0;
      map<uint64_t,set<pg_t>> prim_pgs_by_osd;
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd);
      int high_load_before = 0;
      for (auto& [oid, des] : desired_prims) {
        int pgs = pgs_by_osd[oid].size();
        int oprims = orig_prim_pgs_by_osd[oid].size();
        int cur_load = oprims * 100 + (pgs - oprims) * (100 - rr);
        if (verbose) {
          cout << "osd." << oid << ": " << pgs << "/" << des
              <<  " Load = current/desired "
              <<  cur_load << "/"
              << des * 100 + (pgs - des) * (100 - rr) << std::endl;
        }
        ASSERT_LE(des, float(pgs));
        total_prims += des;
        if (cur_load > high_load_before) {
          high_load_before = cur_load;
        }
      }
      ASSERT_TRUE(total_prims < (pg_num + 0.4) && total_prims > (pg_num - 0.4));    // handle rounding errors

      // Balance reads
      OSDMap::Incremental pending_inc(osdmap.get_epoch()+1);
      osdmap.balance_primaries(g_ceph_context, my_rep_pool, &pending_inc, osdmap, OSDMap::RB_OSDSIZEOPT);
      osdmap.apply_incremental(pending_inc);
      prim_pgs_by_osd.clear();
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd);
      if (verbose) {
        cout << "<<<<<PGs distribution:" << std::endl;
      }
      int high_load_after = 0;
      for (auto i = 0 ; i < weights.size() ; i++) {
        int pgs = pgs_by_osd[i].size();
        int prims = prim_pgs_by_osd[i].size();
        int cur_load = prims * 100 + (pgs - prims) * (100 - rr);
        ASSERT_GE(pgs, prims);
        if (verbose) {
          cout << "osd." << i << ": " << pgs << "/" << prims
              << " Load = " << cur_load << std::endl;
        }
        //ASSERT_LE(prim_pgs_by_osd[i].size(), (desired_prims[i] + 1.0));
        //ASSERT_GE(prim_pgs_by_osd[i].size(), (desired_prims[i] - 1.0));
        if (cur_load > high_load_after) {
          high_load_after = cur_load;
        }
      }
      if (verbose) {
        cout << "=== Read ratio: " << rr << " High load before: " << high_load_before << " High load after: " << high_load_after << std::endl;
      }
      ASSERT_LE(high_load_after, high_load_before);
      if (verbose) {
        cout << " ====== end of iteration for read ratio " << rr << std::endl;
      }
    }

    return;
  }
};

TEST_F(OSDMapTest, Create) {
  set_up_map();
  ASSERT_EQ(get_num_osds(), (unsigned)osdmap.get_max_osd());
  ASSERT_EQ(get_num_osds(), osdmap.get_num_in_osds());
}

TEST_F(OSDMapTest, Features) {
  // with EC pool
  set_up_map();
  uint64_t features = osdmap.get_features(CEPH_ENTITY_TYPE_OSD, NULL);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_TUNABLES);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_TUNABLES2);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_TUNABLES3);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_V2);
  ASSERT_TRUE(features & CEPH_FEATURE_OSDHASHPSPOOL);
  ASSERT_TRUE(features & CEPH_FEATURE_OSD_PRIMARY_AFFINITY);

  // clients have a slightly different view
  features = osdmap.get_features(CEPH_ENTITY_TYPE_CLIENT, NULL);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_TUNABLES);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_TUNABLES2);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_TUNABLES3);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_V2);
  ASSERT_TRUE(features & CEPH_FEATURE_OSDHASHPSPOOL);
  ASSERT_TRUE(features & CEPH_FEATURE_OSD_PRIMARY_AFFINITY);

  // remove teh EC pool, but leave the rule.  add primary affinity.
  {
    OSDMap::Incremental new_pool_inc(osdmap.get_epoch() + 1);
    new_pool_inc.old_pools.insert(osdmap.lookup_pg_pool_name("ec"));
    new_pool_inc.new_primary_affinity[0] = 0x8000;
    osdmap.apply_incremental(new_pool_inc);
  }

  features = osdmap.get_features(CEPH_ENTITY_TYPE_MON, NULL);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_TUNABLES);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_TUNABLES2);
  ASSERT_TRUE(features & CEPH_FEATURE_CRUSH_TUNABLES3); // shared bit with primary affinity
  ASSERT_FALSE(features & CEPH_FEATURE_CRUSH_V2);
  ASSERT_TRUE(features & CEPH_FEATURE_OSDHASHPSPOOL);
  ASSERT_TRUE(features & CEPH_FEATURE_OSD_PRIMARY_AFFINITY);

  // FIXME: test tiering feature bits
}

TEST_F(OSDMapTest, MapPG) {
  set_up_map();

  std::cerr << " osdmap.pool_max==" << osdmap.get_pool_max() << std::endl;
  pg_t rawpg(0, my_rep_pool);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  vector<int> old_up_osds, old_acting_osds;
  osdmap.pg_to_up_acting_osds(pgid, old_up_osds, old_acting_osds);
  ASSERT_EQ(old_up_osds, up_osds);
  ASSERT_EQ(old_acting_osds, acting_osds);

  ASSERT_EQ(osdmap.get_pg_pool(my_rep_pool)->get_size(), up_osds.size());
}

TEST_F(OSDMapTest, MapFunctionsMatch) {
  // TODO: make sure pg_to_up_acting_osds and pg_to_acting_osds match
  set_up_map();
  pg_t rawpg(0, my_rep_pool);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  vector<int> up_osds_two, acting_osds_two;

  osdmap.pg_to_up_acting_osds(pgid, up_osds_two, acting_osds_two);

  ASSERT_EQ(up_osds, up_osds_two);
  ASSERT_EQ(acting_osds, acting_osds_two);

  int acting_primary_two;
  osdmap.pg_to_acting_osds(pgid, &acting_osds_two, &acting_primary_two);
  EXPECT_EQ(acting_osds, acting_osds_two);
  EXPECT_EQ(acting_primary, acting_primary_two);
  osdmap.pg_to_acting_osds(pgid, acting_osds_two);
  EXPECT_EQ(acting_osds, acting_osds_two);
}

/** This test must be removed or modified appropriately when we allow
 * other ways to specify a primary. */
TEST_F(OSDMapTest, PrimaryIsFirst) {
  set_up_map();

  pg_t rawpg(0, my_rep_pool);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);
  EXPECT_EQ(up_osds[0], up_primary);
  EXPECT_EQ(acting_osds[0], acting_primary);
}

TEST_F(OSDMapTest, PGTempRespected) {
  set_up_map();

  pg_t rawpg(0, my_rep_pool);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  // copy and swap first and last element in acting_osds
  vector<int> new_acting_osds(acting_osds);
  int first = new_acting_osds[0];
  new_acting_osds[0] = *new_acting_osds.rbegin();
  *new_acting_osds.rbegin() = first;

  // apply pg_temp to osdmap
  OSDMap::Incremental pgtemp_map(osdmap.get_epoch() + 1);
  pgtemp_map.new_pg_temp[pgid] = mempool::osdmap::vector<int>(
    new_acting_osds.begin(), new_acting_osds.end());
  osdmap.apply_incremental(pgtemp_map);

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);
  EXPECT_EQ(new_acting_osds, acting_osds);
}

TEST_F(OSDMapTest, PrimaryTempRespected) {
  set_up_map();

  pg_t rawpg(0, my_rep_pool);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds;
  vector<int> acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  // make second OSD primary via incremental
  OSDMap::Incremental pgtemp_map(osdmap.get_epoch() + 1);
  pgtemp_map.new_primary_temp[pgid] = acting_osds[1];
  osdmap.apply_incremental(pgtemp_map);

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);
  EXPECT_EQ(acting_primary, acting_osds[1]);
}

TEST_F(OSDMapTest, CleanTemps) {
  set_up_map();

  OSDMap::Incremental pgtemp_map(osdmap.get_epoch() + 1);
  OSDMap::Incremental pending_inc(osdmap.get_epoch() + 2);
  pg_t pga = osdmap.raw_pg_to_pg(pg_t(0, my_rep_pool));
  {
    vector<int> up_osds, acting_osds;
    int up_primary, acting_primary;
    osdmap.pg_to_up_acting_osds(pga, &up_osds, &up_primary,
				&acting_osds, &acting_primary);
    pgtemp_map.new_pg_temp[pga] = mempool::osdmap::vector<int>(
      up_osds.begin(), up_osds.end());
    pgtemp_map.new_primary_temp[pga] = up_primary;
  }
  pg_t pgb = osdmap.raw_pg_to_pg(pg_t(1, my_rep_pool));
  {
    vector<int> up_osds, acting_osds;
    int up_primary, acting_primary;
    osdmap.pg_to_up_acting_osds(pgb, &up_osds, &up_primary,
				&acting_osds, &acting_primary);
    pending_inc.new_pg_temp[pgb] = mempool::osdmap::vector<int>(
      up_osds.begin(), up_osds.end());
    pending_inc.new_primary_temp[pgb] = up_primary;
  }

  osdmap.apply_incremental(pgtemp_map);

  OSDMap tmpmap;
  tmpmap.deepish_copy_from(osdmap);
  tmpmap.apply_incremental(pending_inc);
  OSDMap::clean_temps(g_ceph_context, osdmap, tmpmap, &pending_inc);

  EXPECT_TRUE(pending_inc.new_pg_temp.count(pga) &&
	      pending_inc.new_pg_temp[pga].size() == 0);
  EXPECT_EQ(-1, pending_inc.new_primary_temp[pga]);

  EXPECT_TRUE(!pending_inc.new_pg_temp.count(pgb) &&
	      !pending_inc.new_primary_temp.count(pgb));
}

TEST_F(OSDMapTest, KeepsNecessaryTemps) {
  set_up_map();

  pg_t rawpg(0, my_rep_pool);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up_osds, acting_osds;
  int up_primary, acting_primary;

  osdmap.pg_to_up_acting_osds(pgid, &up_osds, &up_primary,
                              &acting_osds, &acting_primary);

  // find unused OSD and stick it in there
  OSDMap::Incremental pgtemp_map(osdmap.get_epoch() + 1);
  // find an unused osd and put it in place of the first one
  int i = 0;
  for(; i != (int)get_num_osds(); ++i) {
    bool in_use = false;
    for (vector<int>::iterator osd_it = up_osds.begin();
	 osd_it != up_osds.end();
	 ++osd_it) {
      if (i == *osd_it) {
	in_use = true;
        break;
      }
    }
    if (!in_use) {
      up_osds[1] = i;
      break;
    }
  }
  if (i == (int)get_num_osds())
    FAIL() << "did not find unused OSD for temp mapping";

  pgtemp_map.new_pg_temp[pgid] = mempool::osdmap::vector<int>(
    up_osds.begin(), up_osds.end());
  pgtemp_map.new_primary_temp[pgid] = up_osds[1];
  osdmap.apply_incremental(pgtemp_map);

  OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);

  OSDMap tmpmap;
  tmpmap.deepish_copy_from(osdmap);
  tmpmap.apply_incremental(pending_inc);
  OSDMap::clean_temps(g_ceph_context, osdmap, tmpmap, &pending_inc);
  EXPECT_FALSE(pending_inc.new_pg_temp.count(pgid));
  EXPECT_FALSE(pending_inc.new_primary_temp.count(pgid));
}

TEST_F(OSDMapTest, PrimaryAffinity) {
  set_up_map();

  int n = get_num_osds();
  for (map<int64_t,pg_pool_t>::const_iterator p = osdmap.get_pools().begin();
       p != osdmap.get_pools().end();
       ++p) {
    int pool = p->first;
    int expect_primary = 10000 / n;
    cout << "pool " << pool << " size " << (int)p->second.size
	 << " expect_primary " << expect_primary << std::endl;
    {
      vector<int> any(n, 0);
      vector<int> first(n, 0);
      vector<int> primary(n, 0);
      test_mappings(pool, 10000, &any, &first, &primary);
      for (int i=0; i<n; ++i) {
	ASSERT_LT(0, any[i]);
	ASSERT_LT(0, first[i]);
	ASSERT_LT(0, primary[i]);
      }
    }

    osdmap.set_primary_affinity(0, 0);
    osdmap.set_primary_affinity(1, 0);
    {
      vector<int> any(n, 0);
      vector<int> first(n, 0);
      vector<int> primary(n, 0);
      test_mappings(pool, 10000, &any, &first, &primary);
      for (int i=0; i<n; ++i) {
	ASSERT_LT(0, any[i]);
	if (i >= 2) {
	  ASSERT_LT(0, first[i]);
	  ASSERT_LT(0, primary[i]);
	} else {
	  if (p->second.is_replicated()) {
	    ASSERT_EQ(0, first[i]);
	  }
	  ASSERT_EQ(0, primary[i]);
	}
      }
    }

    osdmap.set_primary_affinity(0, 0x8000);
    osdmap.set_primary_affinity(1, 0);
    {
      vector<int> any(n, 0);
      vector<int> first(n, 0);
      vector<int> primary(n, 0);
      test_mappings(pool, 10000, &any, &first, &primary);
      int expect = (10000 / (n-2)) / 2; // half weight
      cout << "expect " << expect << std::endl;
      for (int i=0; i<n; ++i) {
	ASSERT_LT(0, any[i]);
	if (i >= 2) {
	  ASSERT_LT(0, first[i]);
	  ASSERT_LT(0, primary[i]);
	} else if (i == 1) {
	  if (p->second.is_replicated()) {
	    ASSERT_EQ(0, first[i]);
	  }
	  ASSERT_EQ(0, primary[i]);
	} else {
	  ASSERT_LT(expect *2/3, primary[0]);
	  ASSERT_GT(expect *4/3, primary[0]);
	}
      }
    }

    osdmap.set_primary_affinity(0, 0x10000);
    osdmap.set_primary_affinity(1, 0x10000);
  }
}

TEST_F(OSDMapTest, get_osd_crush_node_flags) {
  set_up_map();

  for (unsigned i=0; i<get_num_osds(); ++i) {
    ASSERT_EQ(0u, osdmap.get_osd_crush_node_flags(i));
  }

  OSDMap::Incremental inc(osdmap.get_epoch() + 1);
  inc.new_crush_node_flags[-1] = 123u;
  osdmap.apply_incremental(inc);
  for (unsigned i=0; i<get_num_osds(); ++i) {
    ASSERT_EQ(123u, osdmap.get_osd_crush_node_flags(i));
  }
  ASSERT_EQ(0u, osdmap.get_osd_crush_node_flags(1000));

  OSDMap::Incremental inc3(osdmap.get_epoch() + 1);
  inc3.new_crush_node_flags[-1] = 456u;
  osdmap.apply_incremental(inc3);
  for (unsigned i=0; i<get_num_osds(); ++i) {
    ASSERT_EQ(456u, osdmap.get_osd_crush_node_flags(i));
  }
  ASSERT_EQ(0u, osdmap.get_osd_crush_node_flags(1000));

  OSDMap::Incremental inc2(osdmap.get_epoch() + 1);
  inc2.new_crush_node_flags[-1] = 0;
  osdmap.apply_incremental(inc2);
  for (unsigned i=0; i<get_num_osds(); ++i) {
    ASSERT_EQ(0u, osdmap.get_crush_node_flags(i));
  }
}

TEST_F(OSDMapTest, parse_osd_id_list) {
  set_up_map();
  set<int> out;
  set<int> all;
  osdmap.get_all_osds(all);

  ASSERT_EQ(0, osdmap.parse_osd_id_list({"osd.0"}, &out, &cout));
  ASSERT_EQ(1u, out.size());
  ASSERT_EQ(0, *out.begin());

  ASSERT_EQ(0, osdmap.parse_osd_id_list({"1"}, &out, &cout));
  ASSERT_EQ(1u, out.size());
  ASSERT_EQ(1, *out.begin());

  ASSERT_EQ(0, osdmap.parse_osd_id_list({"osd.0","osd.1"}, &out, &cout));
  ASSERT_EQ(2u, out.size());
  ASSERT_EQ(0, *out.begin());
  ASSERT_EQ(1, *out.rbegin());

  ASSERT_EQ(0, osdmap.parse_osd_id_list({"osd.0","1"}, &out, &cout));
  ASSERT_EQ(2u, out.size());
  ASSERT_EQ(0, *out.begin());
  ASSERT_EQ(1, *out.rbegin());

  ASSERT_EQ(0, osdmap.parse_osd_id_list({"*"}, &out, &cout));
  ASSERT_EQ(all.size(), out.size());
  ASSERT_EQ(all, out);

  ASSERT_EQ(0, osdmap.parse_osd_id_list({"all"}, &out, &cout));
  ASSERT_EQ(all, out);

  ASSERT_EQ(0, osdmap.parse_osd_id_list({"any"}, &out, &cout));
  ASSERT_EQ(all, out);

  ASSERT_EQ(-EINVAL, osdmap.parse_osd_id_list({"foo"}, &out, &cout));
  ASSERT_EQ(-EINVAL, osdmap.parse_osd_id_list({"-12"}, &out, &cout));
}

TEST_F(OSDMapTest, CleanPGUpmaps) {
  set_up_map();

  // build a crush rule of type host
  const int expected_host_num = 3;
  int osd_per_host = get_num_osds() / expected_host_num;
  ASSERT_GE(2, osd_per_host);
  int index = 0;
  for (int i = 0; i < (int)get_num_osds(); i++) {
    if (i && i % osd_per_host == 0) {
      ++index;
    }
    stringstream osd_name;
    stringstream host_name;
    vector<string> move_to;
    osd_name << "osd." << i;
    host_name << "host-" << index;
    move_to.push_back("root=default");
    string host_loc = "host=" + host_name.str();
    move_to.push_back(host_loc);
    int r = crush_move(osdmap, osd_name.str(), move_to);
    ASSERT_EQ(0, r);
  }
  const string upmap_rule = "upmap";
  int upmap_rule_no = crush_rule_create_replicated(
    upmap_rule, "default", "host");
  ASSERT_LT(0, upmap_rule_no);

  // create a replicated pool which references the above rule
  OSDMap::Incremental new_pool_inc(osdmap.get_epoch() + 1);
  new_pool_inc.new_pool_max = osdmap.get_pool_max();
  new_pool_inc.fsid = osdmap.get_fsid();
  pg_pool_t empty;
  uint64_t upmap_pool_id = ++new_pool_inc.new_pool_max;
  pg_pool_t *p = new_pool_inc.get_new_pool(upmap_pool_id, &empty);
  p->size = 2;
  p->set_pg_num(64);
  p->set_pgp_num(64);
  p->type = pg_pool_t::TYPE_REPLICATED;
  p->crush_rule = upmap_rule_no;
  p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
  new_pool_inc.new_pool_names[upmap_pool_id] = "upmap_pool";
  osdmap.apply_incremental(new_pool_inc);

  pg_t rawpg(0, upmap_pool_id);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  vector<int> up;
  int up_primary;
  osdmap.pg_to_raw_up(pgid, &up, &up_primary);
  ASSERT_LT(1U, up.size());
  {
    // validate we won't have two OSDs from a same host
    int parent_0 = osdmap.crush->get_parent_of_type(up[0],
      osdmap.crush->get_type_id("host"));
    int parent_1 = osdmap.crush->get_parent_of_type(up[1],
      osdmap.crush->get_type_id("host"));
    ASSERT_TRUE(parent_0 != parent_1);
  }

 {
    // cancel stale upmaps
    osdmap.pg_to_raw_up(pgid, &up, &up_primary);
    int from = -1;
    for (int i = 0; i < (int)get_num_osds(); i++) {
      if (std::find(up.begin(), up.end(), i) == up.end()) {
        from = i;
        break;
      }
    }
    ASSERT_TRUE(from >= 0);
    int to = -1;
    for (int i = 0; i < (int)get_num_osds(); i++) {
      if (std::find(up.begin(), up.end(), i) == up.end() && i != from) {
        to = i;
        break;
      }
    }
    ASSERT_TRUE(to >= 0);
    vector<pair<int32_t,int32_t>> new_pg_upmap_items;
    new_pg_upmap_items.push_back(make_pair(from, to));
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    pending_inc.new_pg_upmap_items[pgid] =
      mempool::osdmap::vector<pair<int32_t,int32_t>>(
        new_pg_upmap_items.begin(), new_pg_upmap_items.end());
    OSDMap nextmap;
    nextmap.deepish_copy_from(osdmap);
    nextmap.apply_incremental(pending_inc);
    ASSERT_TRUE(nextmap.have_pg_upmaps(pgid));
    OSDMap::Incremental new_pending_inc(nextmap.get_epoch() + 1);
    clean_pg_upmaps(g_ceph_context, nextmap, new_pending_inc);
    nextmap.apply_incremental(new_pending_inc);
    ASSERT_TRUE(!nextmap.have_pg_upmaps(pgid));
  }

  {
    // https://tracker.ceph.com/issues/37493
    pg_t ec_pg(0, my_ec_pool);
    pg_t ec_pgid = osdmap.raw_pg_to_pg(ec_pg);
    OSDMap tmpmap; // use a tmpmap here, so we do not dirty origin map..
    int from = -1;
    int to = -1;
    {
      // insert a valid pg_upmap_item
      vector<int> ec_up;
      int ec_up_primary;
      osdmap.pg_to_raw_up(ec_pgid, &ec_up, &ec_up_primary);
      ASSERT_TRUE(!ec_up.empty());
      from = *(ec_up.begin());
      ASSERT_TRUE(from >= 0);
      for (int i = 0; i < (int)get_num_osds(); i++) {
        if (std::find(ec_up.begin(), ec_up.end(), i) == ec_up.end()) {
          to = i;
          break;
        }
      }
      ASSERT_TRUE(to >= 0);
      ASSERT_TRUE(from != to);
      vector<pair<int32_t,int32_t>> new_pg_upmap_items;
      new_pg_upmap_items.push_back(make_pair(from, to));
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      pending_inc.new_pg_upmap_items[ec_pgid] =
      mempool::osdmap::vector<pair<int32_t,int32_t>>(
        new_pg_upmap_items.begin(), new_pg_upmap_items.end());
      tmpmap.deepish_copy_from(osdmap);
      tmpmap.apply_incremental(pending_inc);
      ASSERT_TRUE(tmpmap.have_pg_upmaps(ec_pgid));
    }
    {
      // mark one of the target OSDs of the above pg_upmap_item as down
      OSDMap::Incremental pending_inc(tmpmap.get_epoch() + 1);
      pending_inc.new_state[to] = CEPH_OSD_UP;
      tmpmap.apply_incremental(pending_inc);
      ASSERT_TRUE(!tmpmap.is_up(to));
      ASSERT_TRUE(tmpmap.have_pg_upmaps(ec_pgid));
    }
    {
      // confirm *clean_pg_upmaps* won't do anything bad
      OSDMap::Incremental pending_inc(tmpmap.get_epoch() + 1);
      clean_pg_upmaps(g_ceph_context, tmpmap, pending_inc);
      tmpmap.apply_incremental(pending_inc);
      ASSERT_TRUE(tmpmap.have_pg_upmaps(ec_pgid));
    }
  }

  {
    // http://tracker.ceph.com/issues/37501
    pg_t ec_pg(0, my_ec_pool);
    pg_t ec_pgid = osdmap.raw_pg_to_pg(ec_pg);
    OSDMap tmpmap; // use a tmpmap here, so we do not dirty origin map..
    int from = -1;
    int to = -1;
    {
      // insert a valid pg_upmap_item
      vector<int> ec_up;
      int ec_up_primary;
      osdmap.pg_to_raw_up(ec_pgid, &ec_up, &ec_up_primary);
      ASSERT_TRUE(!ec_up.empty());
      from = *(ec_up.begin());
      ASSERT_TRUE(from >= 0);
      for (int i = 0; i < (int)get_num_osds(); i++) {
        if (std::find(ec_up.begin(), ec_up.end(), i) == ec_up.end()) {
          to = i;
          break;
        }
      }
      ASSERT_TRUE(to >= 0);
      ASSERT_TRUE(from != to);
      vector<pair<int32_t,int32_t>> new_pg_upmap_items;
      new_pg_upmap_items.push_back(make_pair(from, to));
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      pending_inc.new_pg_upmap_items[ec_pgid] =
      mempool::osdmap::vector<pair<int32_t,int32_t>>(
        new_pg_upmap_items.begin(), new_pg_upmap_items.end());
      tmpmap.deepish_copy_from(osdmap);
      tmpmap.apply_incremental(pending_inc);
      ASSERT_TRUE(tmpmap.have_pg_upmaps(ec_pgid));
    }
    {
      // mark one of the target OSDs of the above pg_upmap_item as out
      OSDMap::Incremental pending_inc(tmpmap.get_epoch() + 1);
      pending_inc.new_weight[to] = CEPH_OSD_OUT;
      tmpmap.apply_incremental(pending_inc);
      ASSERT_TRUE(tmpmap.is_out(to));
      ASSERT_TRUE(tmpmap.have_pg_upmaps(ec_pgid));
    }
    {
      // *clean_pg_upmaps* should be able to remove the above *bad* mapping
      OSDMap::Incremental pending_inc(tmpmap.get_epoch() + 1);
      clean_pg_upmaps(g_ceph_context, tmpmap, pending_inc);
      tmpmap.apply_incremental(pending_inc);
      ASSERT_TRUE(!tmpmap.have_pg_upmaps(ec_pgid));
    }
  }

  {
    // http://tracker.ceph.com/issues/37968
    
    // build a temporary crush topology of 2 hosts, 3 osds per host
    OSDMap tmp; // use a tmpmap here, so we do not dirty origin map..
    tmp.deepish_copy_from(osdmap);
    const int expected_host_num = 2;
    int osd_per_host = get_num_osds() / expected_host_num;
    ASSERT_GE(osd_per_host, 3);
    int index = 0;
    for (int i = 0; i < (int)get_num_osds(); i++) {
      if (i && i % osd_per_host == 0) {
        ++index;
      }
      stringstream osd_name;
      stringstream host_name;
      vector<string> move_to;
      osd_name << "osd." << i;
      host_name << "host-" << index;
      move_to.push_back("root=default");
      string host_loc = "host=" + host_name.str();
      move_to.push_back(host_loc);
      auto r = crush_move(tmp, osd_name.str(), move_to);
      ASSERT_EQ(0, r);
    }
      
    // build crush rule
    CrushWrapper crush;
    get_crush(tmp, crush);
    string rule_name = "rule_37968";
    int rule_type = pg_pool_t::TYPE_ERASURE;
    ASSERT_TRUE(!crush.rule_exists(rule_name));
    int rno;
    for (rno = 0; rno < crush.get_max_rules(); rno++) {
      if (!crush.rule_exists(rno))
        break;
    }
    string root_name = "default";
    int root = crush.get_item_id(root_name);
    int steps = 6;
    crush_rule *rule = crush_make_rule(steps, rule_type);
    int step = 0;
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, root, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSE_INDEP, 2, 1 /* host*/); 
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSE_INDEP, 2, 0 /* osd */); 
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    ASSERT_TRUE(step == steps);
    auto r = crush_add_rule(crush.get_crush_map(), rule, rno);
    ASSERT_TRUE(r >= 0);
    crush.set_rule_name(rno, rule_name);
    {
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      pending_inc.crush.clear();
      crush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
      tmp.apply_incremental(pending_inc);
    }

    // create a erasuce-coded pool referencing the above rule
    int64_t pool_37968;
    {
      OSDMap::Incremental new_pool_inc(tmp.get_epoch() + 1);
      new_pool_inc.new_pool_max = tmp.get_pool_max();
      new_pool_inc.fsid = tmp.get_fsid();
      pg_pool_t empty;
      pool_37968 = ++new_pool_inc.new_pool_max;
      pg_pool_t *p = new_pool_inc.get_new_pool(pool_37968, &empty);
      p->size = 4;
      p->set_pg_num(8);
      p->set_pgp_num(8);
      p->type = pg_pool_t::TYPE_ERASURE;
      p->crush_rule = rno;
      p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
      new_pool_inc.new_pool_names[pool_37968] = "pool_37968";
      tmp.apply_incremental(new_pool_inc);
    }

    pg_t ec_pg(0, pool_37968);
    pg_t ec_pgid = tmp.raw_pg_to_pg(ec_pg);
    int from = -1;
    int to = -1;
    {
      // insert a valid pg_upmap_item
      vector<int> ec_up;
      int ec_up_primary;
      tmp.pg_to_raw_up(ec_pgid, &ec_up, &ec_up_primary);
      ASSERT_TRUE(ec_up.size() == 4);
      from = *(ec_up.begin());
      ASSERT_TRUE(from >= 0);
      auto parent = tmp.crush->get_parent_of_type(from, 1 /* host */, rno);
      ASSERT_TRUE(parent < 0);
      // pick an osd of the same parent with *from*
      for (int i = 0; i < (int)get_num_osds(); i++) {
        if (std::find(ec_up.begin(), ec_up.end(), i) == ec_up.end()) {
          auto p = tmp.crush->get_parent_of_type(i, 1 /* host */, rno);
          if (p == parent) {
            to = i;
            break;
          }
        }
      }
      ASSERT_TRUE(to >= 0);
      ASSERT_TRUE(from != to);
      vector<pair<int32_t,int32_t>> new_pg_upmap_items;
      new_pg_upmap_items.push_back(make_pair(from, to));
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      pending_inc.new_pg_upmap_items[ec_pgid] =
        mempool::osdmap::vector<pair<int32_t,int32_t>>(
          new_pg_upmap_items.begin(), new_pg_upmap_items.end());
      tmp.apply_incremental(pending_inc);
      ASSERT_TRUE(tmp.have_pg_upmaps(ec_pgid));
    }
    {
      // *clean_pg_upmaps* should not remove the above upmap_item
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      clean_pg_upmaps(g_ceph_context, tmp, pending_inc);
      tmp.apply_incremental(pending_inc);
      ASSERT_TRUE(tmp.have_pg_upmaps(ec_pgid));
    }
  }

  {
    // TEST pg_upmap
    {
      // STEP-1: enumerate all children of up[0]'s parent,
      // replace up[1] with one of them (other than up[0])
      int parent = osdmap.crush->get_parent_of_type(up[0],
        osdmap.crush->get_type_id("host"));
      set<int> candidates;
      osdmap.crush->get_leaves(osdmap.crush->get_item_name(parent), &candidates);
      ASSERT_LT(1U, candidates.size());
      int replaced_by = -1;
      for (auto c: candidates) {
        if (c != up[0]) {
          replaced_by = c;
          break;
        }
      }
      {
        // Check we can handle a negative pg_upmap value
        vector<int32_t> new_pg_upmap;
        new_pg_upmap.push_back(up[0]);
        new_pg_upmap.push_back(-823648512);
        OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
        pending_inc.new_pg_upmap[pgid] = mempool::osdmap::vector<int32_t>(
            new_pg_upmap.begin(), new_pg_upmap.end());
        osdmap.apply_incremental(pending_inc);
        vector<int> new_up;
        int new_up_primary;
        // crucial call - _apply_upmap should ignore the negative value
        osdmap.pg_to_raw_up(pgid, &new_up, &new_up_primary);
      }
      ASSERT_NE(-1, replaced_by);
      // generate a new pg_upmap item and apply
      vector<int32_t> new_pg_upmap;
      new_pg_upmap.push_back(up[0]);
      new_pg_upmap.push_back(replaced_by); // up[1] -> replaced_by
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      pending_inc.new_pg_upmap[pgid] = mempool::osdmap::vector<int32_t>(
        new_pg_upmap.begin(), new_pg_upmap.end());
      osdmap.apply_incremental(pending_inc);
      {
        // validate pg_upmap is there
        vector<int> new_up;
        int new_up_primary;
        osdmap.pg_to_raw_up(pgid, &new_up, &new_up_primary);
        ASSERT_EQ(new_up.size(), up.size());
        ASSERT_EQ(new_up[0], new_pg_upmap[0]);
        ASSERT_EQ(new_up[1], new_pg_upmap[1]);
        // and we shall have two OSDs from a same host now..
        int parent_0 = osdmap.crush->get_parent_of_type(new_up[0],
          osdmap.crush->get_type_id("host"));
        int parent_1 = osdmap.crush->get_parent_of_type(new_up[1],
          osdmap.crush->get_type_id("host"));
        ASSERT_EQ(parent_0, parent_1);
      }
    }
    {
      // STEP-2: apply cure
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      clean_pg_upmaps(g_ceph_context, osdmap, pending_inc);
      osdmap.apply_incremental(pending_inc);
      {
        // validate pg_upmap is gone (reverted)
        vector<int> new_up;
        int new_up_primary;
        osdmap.pg_to_raw_up(pgid, &new_up, &new_up_primary);
        ASSERT_EQ(new_up, up);
        ASSERT_EQ(new_up_primary, up_primary);
      }
    }
  }

  {
    // TEST pg_upmap_items
    // enumerate all used hosts first
    set<int> parents;
    for (auto u: up) {
      int parent = osdmap.crush->get_parent_of_type(u,
        osdmap.crush->get_type_id("host"));
      ASSERT_GT(0, parent);
      parents.insert(parent);
    }
    int candidate_parent = 0;
    set<int> candidate_children;
    vector<int> up_after_out;
    {
      // STEP-1: try mark out up[1] and all other OSDs from the same host
      int parent = osdmap.crush->get_parent_of_type(up[1],
        osdmap.crush->get_type_id("host"));
      set<int> children;
      osdmap.crush->get_leaves(osdmap.crush->get_item_name(parent),
        &children);
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      for (auto c: children) {
        pending_inc.new_weight[c] = CEPH_OSD_OUT;
      }
      OSDMap tmpmap;
      tmpmap.deepish_copy_from(osdmap);
      tmpmap.apply_incremental(pending_inc);
      vector<int> new_up;
      int new_up_primary;
      tmpmap.pg_to_raw_up(pgid, &new_up, &new_up_primary);
      // verify that we'll have OSDs from a different host..
      int will_choose = -1;
      for (auto o: new_up) {
        int parent = tmpmap.crush->get_parent_of_type(o,
          osdmap.crush->get_type_id("host"));
        if (!parents.count(parent)) {
          will_choose = o;
          candidate_parent = parent; // record
          break;
        }
      }
      ASSERT_LT(-1, will_choose); // it is an OSD!
      ASSERT_NE(candidate_parent, 0);
      osdmap.crush->get_leaves(osdmap.crush->get_item_name(candidate_parent),
        &candidate_children);
      ASSERT_TRUE(candidate_children.count(will_choose));
      candidate_children.erase(will_choose);
      ASSERT_FALSE(candidate_children.empty());
      up_after_out = new_up; // needed for verification..
    }
    {
      // Make sure we can handle a negative pg_upmap_item
      int victim = up[0];
      int replaced_by = -823648512;
      vector<pair<int32_t,int32_t>> new_pg_upmap_items;
      new_pg_upmap_items.push_back(make_pair(victim, replaced_by));
      // apply
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      pending_inc.new_pg_upmap_items[pgid] =
        mempool::osdmap::vector<pair<int32_t,int32_t>>(
        new_pg_upmap_items.begin(), new_pg_upmap_items.end());
      osdmap.apply_incremental(pending_inc);
      vector<int> new_up;
      int new_up_primary;
      // crucial call - _apply_upmap should ignore the negative value
      osdmap.pg_to_raw_up(pgid, &new_up, &new_up_primary);
    }
    {
      // STEP-2: generating a new pg_upmap_items entry by
      // replacing up[0] with one coming from candidate_children
      int victim = up[0];
      int replaced_by = *candidate_children.begin();
      vector<pair<int32_t,int32_t>> new_pg_upmap_items;
      new_pg_upmap_items.push_back(make_pair(victim, replaced_by));
      // apply
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      pending_inc.new_pg_upmap_items[pgid] =
        mempool::osdmap::vector<pair<int32_t,int32_t>>(
        new_pg_upmap_items.begin(), new_pg_upmap_items.end());
      osdmap.apply_incremental(pending_inc);
      {
        // validate pg_upmap_items is there
        vector<int> new_up;
        int new_up_primary;
        osdmap.pg_to_raw_up(pgid, &new_up, &new_up_primary);
        ASSERT_EQ(new_up.size(), up.size());
        ASSERT_TRUE(std::find(new_up.begin(), new_up.end(), replaced_by) !=
          new_up.end());
        // and up[1] too
        ASSERT_TRUE(std::find(new_up.begin(), new_up.end(), up[1]) !=
          new_up.end());
      }
    }
    {
      // STEP-3: mark out up[1] and all other OSDs from the same host
      int parent = osdmap.crush->get_parent_of_type(up[1],
        osdmap.crush->get_type_id("host"));
      set<int> children;
      osdmap.crush->get_leaves(osdmap.crush->get_item_name(parent),
        &children);
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      for (auto c: children) {
        pending_inc.new_weight[c] = CEPH_OSD_OUT;
      }
      osdmap.apply_incremental(pending_inc);
      {
        // validate we have two OSDs from the same host now..
        vector<int> new_up;
        int new_up_primary;
        osdmap.pg_to_raw_up(pgid, &new_up, &new_up_primary);
        ASSERT_EQ(up.size(), new_up.size());
        int parent_0 = osdmap.crush->get_parent_of_type(new_up[0],
          osdmap.crush->get_type_id("host"));
        int parent_1 = osdmap.crush->get_parent_of_type(new_up[1],
          osdmap.crush->get_type_id("host"));
        ASSERT_EQ(parent_0, parent_1);
      } 
    }
    {
      // STEP-4: apply cure
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      clean_pg_upmaps(g_ceph_context, osdmap, pending_inc);
      osdmap.apply_incremental(pending_inc);
      {
        // validate pg_upmap_items is gone (reverted)
        vector<int> new_up;
        int new_up_primary;
        osdmap.pg_to_raw_up(pgid, &new_up, &new_up_primary);
        ASSERT_EQ(new_up, up_after_out);
      }
    }
  }
}

TEST_F(OSDMapTest, BUG_38897) {
  // http://tracker.ceph.com/issues/38897
  // build a fresh map with 12 OSDs, without any default pools
  set_up_map(12, true);
  const string pool_1("pool1");
  const string pool_2("pool2");
  int64_t pool_1_id = -1;

  {
    // build customized crush rule for "pool1"
    string host_name = "host_for_pool_1";
    // build a customized host to capture osd.1~5
    for (int i = 1; i < 5; i++) {
      stringstream osd_name;
      vector<string> move_to;
      osd_name << "osd." << i;
      move_to.push_back("root=default");
      string host_loc = "host=" + host_name;
      move_to.push_back(host_loc);
      auto r = crush_move(osdmap, osd_name.str(), move_to);
      ASSERT_EQ(0, r);
    }
    CrushWrapper crush;
    get_crush(osdmap, crush);
    auto host_id = crush.get_item_id(host_name);
    ASSERT_TRUE(host_id < 0);
    string rule_name = "rule_for_pool1";
    int rule_type = pg_pool_t::TYPE_REPLICATED;
    ASSERT_TRUE(!crush.rule_exists(rule_name));
    int rno;
    for (rno = 0; rno < crush.get_max_rules(); rno++) {
      if (!crush.rule_exists(rno))
        break;
    }
    int steps = 7;
    crush_rule *rule = crush_make_rule(steps, rule_type);
    int step = 0;
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
    // always choose osd.0
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, 0, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    // then pick any other random osds
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, host_id, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_FIRSTN, 2, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    ASSERT_TRUE(step == steps);
    auto r = crush_add_rule(crush.get_crush_map(), rule, rno);
    ASSERT_TRUE(r >= 0);
    crush.set_rule_name(rno, rule_name);
    {
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      pending_inc.crush.clear();
      crush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
      osdmap.apply_incremental(pending_inc);
    }

    // create "pool1"
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    pending_inc.new_pool_max = osdmap.get_pool_max();
    auto pool_id = ++pending_inc.new_pool_max;
    pool_1_id = pool_id;
    pg_pool_t empty;
    auto p = pending_inc.get_new_pool(pool_id, &empty);
    p->size = 3;
    p->min_size = 1;
    p->set_pg_num(3);
    p->set_pgp_num(3);
    p->type = pg_pool_t::TYPE_REPLICATED;
    p->crush_rule = rno;
    p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    pending_inc.new_pool_names[pool_id] = pool_1;
    osdmap.apply_incremental(pending_inc);
    ASSERT_TRUE(osdmap.have_pg_pool(pool_id));
    ASSERT_TRUE(osdmap.get_pool_name(pool_id) == pool_1);
    {
      for (unsigned i = 0; i < 3; i++) {
        // 1.x -> [1]
        pg_t rawpg(i, pool_id);
        pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
        vector<int> up;
        int up_primary;
        osdmap.pg_to_raw_up(pgid, &up, &up_primary);
        ASSERT_TRUE(up.size() == 3);
        ASSERT_TRUE(up[0] == 0);

        // insert a new pg_upmap
        vector<int32_t> new_up;
        // and remap 1.x to osd.1 only
        // this way osd.0 is deemed to be *underfull*
        // and osd.1 is deemed to be *overfull*
        new_up.push_back(1);
        {
          OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
          pending_inc.new_pg_upmap[pgid] = mempool::osdmap::vector<int32_t>(
            new_up.begin(), new_up.end());
          osdmap.apply_incremental(pending_inc);
        }
        osdmap.pg_to_raw_up(pgid, &up, &up_primary);
        ASSERT_TRUE(up.size() == 1);
        ASSERT_TRUE(up[0] == 1);
      }
    }
  }

  {
    // build customized crush rule for "pool2"
    string host_name = "host_for_pool_2";
    // build a customized host to capture osd.6~11
    for (int i = 6; i < (int)get_num_osds(); i++) {
      stringstream osd_name;
      vector<string> move_to;
      osd_name << "osd." << i;
      move_to.push_back("root=default");
      string host_loc = "host=" + host_name;
      move_to.push_back(host_loc);
      auto r = crush_move(osdmap, osd_name.str(), move_to);
      ASSERT_EQ(0, r);
    }
    CrushWrapper crush;
    get_crush(osdmap, crush);
    auto host_id = crush.get_item_id(host_name);
    ASSERT_TRUE(host_id < 0);
    string rule_name = "rule_for_pool2";
    int rule_type = pg_pool_t::TYPE_REPLICATED;
    ASSERT_TRUE(!crush.rule_exists(rule_name));
    int rno;
    for (rno = 0; rno < crush.get_max_rules(); rno++) {
      if (!crush.rule_exists(rno))
        break;
    }
    int steps = 7;
    crush_rule *rule = crush_make_rule(steps, rule_type);
    int step = 0;
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
    // always choose osd.0
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, 0, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    // then pick any other random osds
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, host_id, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_FIRSTN, 2, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    ASSERT_TRUE(step == steps);
    auto r = crush_add_rule(crush.get_crush_map(), rule, rno);
    ASSERT_TRUE(r >= 0);
    crush.set_rule_name(rno, rule_name);
    {
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      pending_inc.crush.clear();
      crush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
      osdmap.apply_incremental(pending_inc);
    }

    // create "pool2"
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    pending_inc.new_pool_max = osdmap.get_pool_max();
    auto pool_id = ++pending_inc.new_pool_max;
    pg_pool_t empty;
    auto p = pending_inc.get_new_pool(pool_id, &empty);
    p->size = 3;
    // include a single PG
    p->set_pg_num(1);
    p->set_pgp_num(1);
    p->type = pg_pool_t::TYPE_REPLICATED;
    p->crush_rule = rno;
    p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    pending_inc.new_pool_names[pool_id] = pool_2;
    osdmap.apply_incremental(pending_inc);
    ASSERT_TRUE(osdmap.have_pg_pool(pool_id));
    ASSERT_TRUE(osdmap.get_pool_name(pool_id) == pool_2);
    pg_t rawpg(0, pool_id);
    pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
    EXPECT_TRUE(!osdmap.have_pg_upmaps(pgid));
    vector<int> up;
    int up_primary;
    osdmap.pg_to_raw_up(pgid, &up, &up_primary);
    ASSERT_TRUE(up.size() == 3);
    ASSERT_TRUE(up[0] == 0);

    {
      // build a pg_upmap_item that will
      // remap pg out from *underfull* osd.0
      vector<pair<int32_t,int32_t>> new_pg_upmap_items;
      new_pg_upmap_items.push_back(make_pair(0, 10)); // osd.0 -> osd.10
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      pending_inc.new_pg_upmap_items[pgid] =
      mempool::osdmap::vector<pair<int32_t,int32_t>>(
        new_pg_upmap_items.begin(), new_pg_upmap_items.end());
      osdmap.apply_incremental(pending_inc);
      ASSERT_TRUE(osdmap.have_pg_upmaps(pgid));
      vector<int> up;
      int up_primary;
      osdmap.pg_to_raw_up(pgid, &up, &up_primary);
      ASSERT_TRUE(up.size() == 3);
      ASSERT_TRUE(up[0] == 10);
    }
  }

  // ready to go
  {
    ASSERT_TRUE(pool_1_id >= 0);
    balance_capacity(pool_1_id);
  }
}

TEST_F(OSDMapTest, BUG_40104) {
  // http://tracker.ceph.com/issues/40104
  int big_osd_num = 5000;
  int big_pg_num = 10000;
  set_up_map(big_osd_num, true);
  int pool_id;
  {
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    pending_inc.new_pool_max = osdmap.get_pool_max();
    pool_id = ++pending_inc.new_pool_max;
    pg_pool_t empty;
    auto p = pending_inc.get_new_pool(pool_id, &empty);
    p->size = 3;
    p->min_size = 1;
    p->set_pg_num(big_pg_num);
    p->set_pgp_num(big_pg_num);
    p->type = pg_pool_t::TYPE_REPLICATED;
    p->crush_rule = 0;
    p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    pending_inc.new_pool_names[pool_id] = "big_pool";
    osdmap.apply_incremental(pending_inc);
    ASSERT_TRUE(osdmap.have_pg_pool(pool_id));
    ASSERT_TRUE(osdmap.get_pool_name(pool_id) == "big_pool");
  }
  {
    // generate pg_upmap_items for each pg
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    for (int i = 0; i < big_pg_num; i++) {
      pg_t rawpg(i, pool_id);
      pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
      vector<int> up;
      int up_primary;
      osdmap.pg_to_raw_up(pgid, &up, &up_primary);
      ASSERT_TRUE(up.size() == 3);
      int victim = up[0];
      int replaced_by = random() % big_osd_num;
      vector<pair<int32_t,int32_t>> new_pg_upmap_items;
      // note that it might or might not be valid, we don't care
      new_pg_upmap_items.push_back(make_pair(victim, replaced_by));
      pending_inc.new_pg_upmap_items[pgid] =
        mempool::osdmap::vector<pair<int32_t,int32_t>>(
          new_pg_upmap_items.begin(), new_pg_upmap_items.end());
    }
    osdmap.apply_incremental(pending_inc);
  }
  {
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    auto start = mono_clock::now();
    clean_pg_upmaps(g_ceph_context, osdmap, pending_inc);
    auto latency = mono_clock::now() - start;
    std::cout << "clean_pg_upmaps (~" << big_pg_num
              << " pg_upmap_items) latency:" << timespan_str(latency)
              << std::endl;
  }
}

TEST_F(OSDMapTest, BUG_42052) {
  // https://tracker.ceph.com/issues/42052
  set_up_map(6, true);
  const string pool_name("pool");
  // build customized crush rule for "pool"
  CrushWrapper crush;
  get_crush(osdmap, crush);
  string rule_name = "rule";
  int rule_type = pg_pool_t::TYPE_REPLICATED;
  ASSERT_TRUE(!crush.rule_exists(rule_name));
  int rno;
  for (rno = 0; rno < crush.get_max_rules(); rno++) {
    if (!crush.rule_exists(rno))
      break;
  }
  int steps = 8;
  crush_rule *rule = crush_make_rule(steps, rule_type);
  int step = 0;
  crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
  crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
  // always choose osd.0, osd.1, osd.2
  crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, 0, 0);
  crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
  crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, 0, 1);
  crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
  crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, 0, 2);
  crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
  ASSERT_TRUE(step == steps);
  auto r = crush_add_rule(crush.get_crush_map(), rule, rno);
  ASSERT_TRUE(r >= 0);
  crush.set_rule_name(rno, rule_name);
  {
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    pending_inc.crush.clear();
    crush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
    osdmap.apply_incremental(pending_inc);
  }

  // create "pool"
  OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
  pending_inc.new_pool_max = osdmap.get_pool_max();
  auto pool_id = ++pending_inc.new_pool_max;
  pg_pool_t empty;
  auto p = pending_inc.get_new_pool(pool_id, &empty);
  p->size = 3;
  p->min_size = 1;
  p->set_pg_num(1);
  p->set_pgp_num(1);
  p->type = pg_pool_t::TYPE_REPLICATED;
  p->crush_rule = rno;
  p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
  pending_inc.new_pool_names[pool_id] = pool_name;
  osdmap.apply_incremental(pending_inc);
  ASSERT_TRUE(osdmap.have_pg_pool(pool_id));
  ASSERT_TRUE(osdmap.get_pool_name(pool_id) == pool_name);
  pg_t rawpg(0, pool_id);
  pg_t pgid = osdmap.raw_pg_to_pg(rawpg);
  {
    // pg_upmap 1.0 [2,3,5]
    vector<int32_t> new_up{2,3,5};
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    pending_inc.new_pg_upmap[pgid] = mempool::osdmap::vector<int32_t>(
      new_up.begin(), new_up.end());
    osdmap.apply_incremental(pending_inc);
  }
  {
    // pg_upmap_items 1.0 [0,3,4,5]
    vector<pair<int32_t,int32_t>> new_pg_upmap_items;
    new_pg_upmap_items.push_back(make_pair(0, 3));
    new_pg_upmap_items.push_back(make_pair(4, 5));
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    pending_inc.new_pg_upmap_items[pgid] =
    mempool::osdmap::vector<pair<int32_t,int32_t>>(
      new_pg_upmap_items.begin(), new_pg_upmap_items.end());
    osdmap.apply_incremental(pending_inc);
  }
  {
    OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
    clean_pg_upmaps(g_ceph_context, osdmap, pending_inc);
    osdmap.apply_incremental(pending_inc);
    ASSERT_FALSE(osdmap.have_pg_upmaps(pgid));
  }
}

TEST_F(OSDMapTest, BUG_42485) {
  set_up_map(60);
  {
    // build a temporary crush topology of 2datacenters, 3racks per dc,
    // 1host per rack, 10osds per host
    OSDMap tmp; // use a tmpmap here, so we do not dirty origin map..
    tmp.deepish_copy_from(osdmap);
    const int expected_host_num = 6;
    int osd_per_host = (int)get_num_osds() / expected_host_num;
    ASSERT_GE(osd_per_host, 10);
    int host_per_dc = 3;
    int index = 0;
    int dc_index = 0;
    for (int i = 0; i < (int)get_num_osds(); i++) {
      if (i && i % osd_per_host == 0) {
        ++index;
      }
      if (i && i % (host_per_dc * osd_per_host) == 0) {
        ++dc_index;
      }
      stringstream osd_name;
      stringstream host_name;
      stringstream rack_name;
      stringstream dc_name;
      vector<string> move_to;
      osd_name << "osd." << i;
      host_name << "host-" << index;
      rack_name << "rack-" << index;
      dc_name << "dc-" << dc_index;
      move_to.push_back("root=default");
      string dc_loc = "datacenter=" + dc_name.str();
      move_to.push_back(dc_loc);
      string rack_loc = "rack=" + rack_name.str();
      move_to.push_back(rack_loc);
      string host_loc = "host=" + host_name.str();
      move_to.push_back(host_loc);
      auto r = crush_move(tmp, osd_name.str(), move_to);
      ASSERT_EQ(0, r);
    }

    // build crush rule
    CrushWrapper crush;
    get_crush(tmp, crush);
    string rule_name = "rule_xeus_993_1";
    int rule_type = pg_pool_t::TYPE_REPLICATED;
    ASSERT_TRUE(!crush.rule_exists(rule_name));
    int rno;
    for (rno = 0; rno < crush.get_max_rules(); rno++) {
      if (!crush.rule_exists(rno))
        break;
    }
    string root_name = "default";
    string dc_1 = "dc-0";
    int dc1 = crush.get_item_id(dc_1);
    string dc_2 = "dc-1";
    int dc2 = crush.get_item_id(dc_2);
    int steps = 8;
    crush_rule *rule = crush_make_rule(steps, rule_type);
    int step = 0;
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, dc1, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_FIRSTN, 2, 3 /* rack */);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, dc2, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_FIRSTN, 2, 3 /* rack */);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    ASSERT_TRUE(step == steps);
    auto r = crush_add_rule(crush.get_crush_map(), rule, rno);
    ASSERT_TRUE(r >= 0);
    crush.set_rule_name(rno, rule_name);
    {
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      pending_inc.crush.clear();
      crush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
      tmp.apply_incremental(pending_inc);
    }
    // create a repliacted pool referencing the above rule
    int64_t pool_xeus_993;
    {
      OSDMap::Incremental new_pool_inc(tmp.get_epoch() + 1);
      new_pool_inc.new_pool_max = tmp.get_pool_max();
      new_pool_inc.fsid = tmp.get_fsid();
      pg_pool_t empty;
      pool_xeus_993 = ++new_pool_inc.new_pool_max;
      pg_pool_t *p = new_pool_inc.get_new_pool(pool_xeus_993, &empty);
      p->size = 4;
      p->set_pg_num(4096);
      p->set_pgp_num(4096);
      p->type = pg_pool_t::TYPE_REPLICATED;
      p->crush_rule = rno;
      p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
      new_pool_inc.new_pool_names[pool_xeus_993] = "pool_xeus_993";
      tmp.apply_incremental(new_pool_inc);
    }

    pg_t rep_pg(0, pool_xeus_993);
    pg_t rep_pgid = tmp.raw_pg_to_pg(rep_pg);
    {
      int from = -1;
      int to = -1;
      vector<int> rep_up;
      int rep_up_primary;
      tmp.pg_to_raw_up(rep_pgid, &rep_up, &rep_up_primary);
      std::cout << "pgid " << rep_up << " up " << rep_up << std::endl;
      ASSERT_TRUE(rep_up.size() == 4);
      from = *(rep_up.begin());
      ASSERT_TRUE(from >= 0);
      auto dc_parent = tmp.crush->get_parent_of_type(from, 8 /* dc */, rno);
      if (dc_parent == dc1)
        dc_parent = dc2;
      else
        dc_parent = dc1;
      auto rack_parent = tmp.crush->get_parent_of_type(from, 3 /* rack */, rno);
      ASSERT_TRUE(dc_parent < 0);
      ASSERT_TRUE(rack_parent < 0);
      set<int> rack_parents;
      for (auto &i: rep_up) {
        if (i == from) continue;
        auto rack_parent = tmp.crush->get_parent_of_type(i, 3 /* rack */, rno);
        rack_parents.insert(rack_parent);
      }
      for (int i = 0; i < (int)get_num_osds(); i++) {
        if (std::find(rep_up.begin(), rep_up.end(), i) == rep_up.end()) {
          auto dc_p = tmp.crush->get_parent_of_type(i, 8 /* dc */, rno);
          auto rack_p = tmp.crush->get_parent_of_type(i, 3 /* rack */, rno);
          if (dc_p == dc_parent &&
              rack_parents.find(rack_p) == rack_parents.end()) {
            to = i;
            break;
          }
        }
      }
      ASSERT_TRUE(to >= 0);
      ASSERT_TRUE(from != to);
      std::cout << "from " << from << " to " << to << std::endl;
      vector<pair<int32_t,int32_t>> new_pg_upmap_items;
      new_pg_upmap_items.push_back(make_pair(from, to));
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      pending_inc.new_pg_upmap_items[rep_pgid] =
        mempool::osdmap::vector<pair<int32_t,int32_t>>(
          new_pg_upmap_items.begin(), new_pg_upmap_items.end());
      tmp.apply_incremental(pending_inc);
      ASSERT_TRUE(tmp.have_pg_upmaps(rep_pgid));
    }
    pg_t rep_pg2(2, pool_xeus_993);
    pg_t rep_pgid2 = tmp.raw_pg_to_pg(rep_pg2);
    {
      pg_t rep_pgid = rep_pgid2;
      vector<int> from_osds{-1, -1};
      vector<int> rep_up;
      int rep_up_primary;
      tmp.pg_to_raw_up(rep_pgid, &rep_up, &rep_up_primary);
      ASSERT_TRUE(rep_up.size() == 4);
      from_osds[0] = *(rep_up.begin());
      from_osds[1] = *(rep_up.rbegin());
      std::cout << "pgid " << rep_pgid2 << " up " << rep_up << std::endl;
      ASSERT_TRUE(*(from_osds.begin()) >= 0);
      ASSERT_TRUE(*(from_osds.rbegin()) >= 0);
      vector<pair<int32_t,int32_t>> new_pg_upmap_items;
      for (auto &from: from_osds) {
        int to = -1;
        auto dc_parent = tmp.crush->get_parent_of_type(from, 8 /* dc */, rno);
        if (dc_parent == dc1)
          dc_parent = dc2;
        else
          dc_parent = dc1;
        auto rack_parent = tmp.crush->get_parent_of_type(from, 3 /* rack */, rno);
        ASSERT_TRUE(dc_parent < 0);
        ASSERT_TRUE(rack_parent < 0);
        set<int> rack_parents;
        for (auto &i: rep_up) {
          if (i == from) continue;
          auto rack_parent = tmp.crush->get_parent_of_type(i, 3 /* rack */, rno);
          rack_parents.insert(rack_parent);
        }
        for (auto &i: new_pg_upmap_items) {
            auto rack_from = tmp.crush->get_parent_of_type(i.first, 3, rno);
            auto rack_to = tmp.crush->get_parent_of_type(i.second, 3, rno);
            rack_parents.insert(rack_from);
            rack_parents.insert(rack_to);
	}
        for (int i = 0; i < (int)get_num_osds(); i++) {
          if (std::find(rep_up.begin(), rep_up.end(), i) == rep_up.end()) {
            auto dc_p = tmp.crush->get_parent_of_type(i, 8 /* dc */, rno);
            auto rack_p = tmp.crush->get_parent_of_type(i, 3 /* rack */, rno);
            if (dc_p == dc_parent &&
                rack_parents.find(rack_p) == rack_parents.end()) {
              to = i;
              break;
            }
          }
        }
        ASSERT_TRUE(to >= 0);
        ASSERT_TRUE(from != to);
        std::cout << "from " << from << " to " << to << std::endl;
        new_pg_upmap_items.push_back(make_pair(from, to));
      }
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      pending_inc.new_pg_upmap_items[rep_pgid] =
        mempool::osdmap::vector<pair<int32_t,int32_t>>(
          new_pg_upmap_items.begin(), new_pg_upmap_items.end());
      tmp.apply_incremental(pending_inc);
      ASSERT_TRUE(tmp.have_pg_upmaps(rep_pgid));
    }
    {
      // *maybe_remove_pg_upmaps* should remove the above upmap_item
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      clean_pg_upmaps(g_ceph_context, tmp, pending_inc);
      tmp.apply_incremental(pending_inc);
      ASSERT_FALSE(tmp.have_pg_upmaps(rep_pgid));
      ASSERT_FALSE(tmp.have_pg_upmaps(rep_pgid2));
    }
  }
}

TEST(PGTempMap, basic)
{
  PGTempMap m;
  pg_t a(1,1);
  for (auto i=3; i<1000; ++i) {
    pg_t x(i, 1);
    m.set(x, {static_cast<int>(i)});
  }
  pg_t b(2,1);
  m.set(a, {1, 2});
  ASSERT_NE(m.find(a), m.end());
  ASSERT_EQ(m.find(a), m.begin());
  ASSERT_EQ(m.find(b), m.end());
  ASSERT_EQ(998u, m.size());
}

TEST_F(OSDMapTest, BUG_43124) {
  set_up_map(200);
  {
    // https://tracker.ceph.com/issues/43124

    // build a temporary crush topology of 5racks,
    // 4 hosts per rack, 10osds per host
    OSDMap tmp; // use a tmpmap here, so we do not dirty origin map..
    tmp.deepish_copy_from(osdmap);
    const int expected_host_num = 20;
    int osd_per_host = (int)get_num_osds() / expected_host_num;
    ASSERT_GE(osd_per_host, 10);
    int host_per_rack = 4;
    int index = 0;
    int rack_index = 0;
    for (int i = 0; i < (int)get_num_osds(); i++) {
      if (i && i % osd_per_host == 0) {
        ++index;
      }
      if (i && i % (host_per_rack * osd_per_host) == 0) {
        ++rack_index;
      }
      stringstream osd_name;
      stringstream host_name;
      stringstream rack_name;
      vector<string> move_to;
      osd_name << "osd." << i;
      host_name << "host-" << index;
      rack_name << "rack-" << rack_index;
      move_to.push_back("root=default");
      string rack_loc = "rack=" + rack_name.str();
      move_to.push_back(rack_loc);
      string host_loc = "host=" + host_name.str();
      move_to.push_back(host_loc);
      auto r = crush_move(tmp, osd_name.str(), move_to);
      ASSERT_EQ(0, r);
    }

    // build crush rule
    CrushWrapper crush;
    get_crush(tmp, crush);
    string rule_name = "rule_angel_1944";
    int rule_type = pg_pool_t::TYPE_ERASURE;
    ASSERT_TRUE(!crush.rule_exists(rule_name));
    int rno;
    for (rno = 0; rno < crush.get_max_rules(); rno++) {
      if (!crush.rule_exists(rno))
        break;
    }
    int steps = 6;
    string root_name = "default";
    int root = crush.get_item_id(root_name);
    crush_rule *rule = crush_make_rule(steps, rule_type);
    int step = 0;
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, root, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSE_FIRSTN, 4, 3 /* rack */);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_INDEP, 3, 1 /* host */);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    ASSERT_TRUE(step == steps);
    auto r = crush_add_rule(crush.get_crush_map(), rule, rno);
    ASSERT_TRUE(r >= 0);
    crush.set_rule_name(rno, rule_name);
    {
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      pending_inc.crush.clear();
      crush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
      tmp.apply_incremental(pending_inc);
    }
    {
      stringstream oss;
      crush.dump_tree(&oss, NULL);
      std::cout << oss.str() << std::endl;
      Formatter *f = Formatter::create("json-pretty");
      f->open_object_section("crush_rules");
      crush.dump_rules(f);
      f->close_section();
      f->flush(cout);
      delete f;
    }
    // create a erasuce-coded pool referencing the above rule
    int64_t pool_angel_1944;
    {
      OSDMap::Incremental new_pool_inc(tmp.get_epoch() + 1);
      new_pool_inc.new_pool_max = tmp.get_pool_max();
      new_pool_inc.fsid = tmp.get_fsid();
      pg_pool_t empty;
      pool_angel_1944 = ++new_pool_inc.new_pool_max;
      pg_pool_t *p = new_pool_inc.get_new_pool(pool_angel_1944, &empty);
      p->size = 12;
      p->set_pg_num(4096);
      p->set_pgp_num(4096);
      p->type = pg_pool_t::TYPE_ERASURE;
      p->crush_rule = rno;
      p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
      new_pool_inc.new_pool_names[pool_angel_1944] = "pool_angel_1944";
      tmp.apply_incremental(new_pool_inc);
    }

    pg_t rep_pg(0, pool_angel_1944);
    pg_t rep_pgid = tmp.raw_pg_to_pg(rep_pg);
    {
      // insert a pg_upmap_item
      int from = -1;
      int to = -1;
      vector<int> rep_up;
      int rep_up_primary;
      tmp.pg_to_raw_up(rep_pgid, &rep_up, &rep_up_primary);
      std::cout << "pgid " << rep_pgid << " up " << rep_up << std::endl;
      ASSERT_TRUE(rep_up.size() == 12);
      from = *(rep_up.begin());
      ASSERT_TRUE(from >= 0);
      auto from_rack = tmp.crush->get_parent_of_type(from, 3 /* rack */, rno);
      set<int> failure_domains;
      for (auto &osd : rep_up) {
        failure_domains.insert(tmp.crush->get_parent_of_type(osd, 1 /* host */, rno));
      }
      for (int i = 0; i < (int)get_num_osds(); i++) {
        if (std::find(rep_up.begin(), rep_up.end(), i) == rep_up.end()) {
          auto to_rack = tmp.crush->get_parent_of_type(i, 3 /* rack */, rno);
          auto to_host = tmp.crush->get_parent_of_type(i, 1 /* host */, rno);
          if (to_rack != from_rack && failure_domains.count(to_host) == 0) {
            to = i;
            break;
          }
        }
      }
      ASSERT_TRUE(to >= 0);
      ASSERT_TRUE(from != to);
      std::cout << "from " << from << " to " << to << std::endl;
      vector<pair<int32_t,int32_t>> new_pg_upmap_items;
      new_pg_upmap_items.push_back(make_pair(from, to));
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      pending_inc.new_pg_upmap_items[rep_pgid] =
        mempool::osdmap::vector<pair<int32_t,int32_t>>(
          new_pg_upmap_items.begin(), new_pg_upmap_items.end());
      tmp.apply_incremental(pending_inc);
      ASSERT_TRUE(tmp.have_pg_upmaps(rep_pgid));
    }
    {
      // *maybe_remove_pg_upmaps* should not remove the above upmap_item
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      clean_pg_upmaps(g_ceph_context, tmp, pending_inc);
      tmp.apply_incremental(pending_inc);
      ASSERT_TRUE(tmp.have_pg_upmaps(rep_pgid));
    }
  }
}

TEST_F(OSDMapTest, BUG_48884)
{

  set_up_map(12);

  unsigned int host_index = 1;
  for (unsigned int x=0; x < get_num_osds();) {
    // Create three hosts with four osds each
    for (unsigned int y=0; y < 4; y++) {
      stringstream osd_name;
      stringstream host_name;
      vector<string> move_to;
      osd_name << "osd." << x;
      host_name << "host-" << host_index;
      move_to.push_back("root=default");
      move_to.push_back("rack=localrack");
      string host_loc = "host=" + host_name.str();
      move_to.push_back(host_loc);
      int r = crush_move(osdmap, osd_name.str(), move_to);
      ASSERT_EQ(0, r);
      x++;
    }
    host_index++;
  }

  CrushWrapper crush;
  get_crush(osdmap, crush);
  auto host_id = crush.get_item_id("localhost");
  crush.remove_item(g_ceph_context, host_id, false);
  OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
  pending_inc.crush.clear();
  crush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
  osdmap.apply_incremental(pending_inc);

  PGMap pgmap;
  osd_stat_t stats, stats_null;
  stats.statfs.total = 500000;
  stats.statfs.available = 50000;
  stats.statfs.omap_allocated = 50000;
  stats.statfs.internal_metadata = 50000;
  stats_null.statfs.total = 0;
  stats_null.statfs.available = 0;
  stats_null.statfs.omap_allocated = 0;
  stats_null.statfs.internal_metadata = 0;
  for (unsigned int x=0; x < get_num_osds(); x++) {
    if (x > 3 && x < 8) {
      pgmap.osd_stat.insert({x,stats_null});
    } else {
      pgmap.osd_stat.insert({x,stats});
    }
  }

  stringstream ss;
  boost::scoped_ptr<Formatter> f(Formatter::create("json-pretty"));
  print_osd_utilization(osdmap, pgmap, ss, f.get(), true, "root");
  JSONParser parser;
  parser.parse(ss.str().c_str(), static_cast<int>(ss.str().size()));
  auto iter = parser.find_first();
  for (const auto& bucket : (*iter)->get_array_elements()) {
    JSONParser parser2;
    parser2.parse(bucket.c_str(), static_cast<int>(bucket.size()));
    auto* obj = parser2.find_obj("name");
    if (obj->get_data().compare("localrack") == 0) {
      obj = parser2.find_obj("kb");
      ASSERT_EQ(obj->get_data(), "3904");
      obj = parser2.find_obj("kb_used");
      ASSERT_EQ(obj->get_data(), "3512");
      obj = parser2.find_obj("kb_used_omap");
      ASSERT_EQ(obj->get_data(), "384");
      obj = parser2.find_obj("kb_used_meta");
      ASSERT_EQ(obj->get_data(), "384");
      obj = parser2.find_obj("kb_avail");
      ASSERT_EQ(obj->get_data(), "384");
    }
  }
}

TEST_P(OSDMapTest, BUG_51842) {
    set_up_map(3, true);
    OSDMap tmp; // use a tmpmap here, so we do not dirty origin map..
    tmp.deepish_copy_from(osdmap);
    for (int i = 0; i < (int)get_num_osds(); i++) {
      stringstream osd_name;
      stringstream host_name;
      vector<string> move_to;
      osd_name << "osd." << i;
      host_name << "host=host-" << i;
      move_to.push_back("root=infra-1706");
      move_to.push_back(host_name.str());
      auto r = crush_move(tmp, osd_name.str(), move_to);
      ASSERT_EQ(0, r);
    }

    // build crush rule
    CrushWrapper crush;
    get_crush(tmp, crush);
    string rule_name = "infra-1706";
    int rule_type = pg_pool_t::TYPE_REPLICATED;
    ASSERT_TRUE(!crush.rule_exists(rule_name));
    int rno;
    for (rno = 0; rno < crush.get_max_rules(); rno++) {
      if (!crush.rule_exists(rno))
        break;
    }
    string root_bucket = "infra-1706";
    int root = crush.get_item_id(root_bucket);
    int steps = 5;
    crush_rule *rule = crush_make_rule(steps, rule_type);
    int step = 0;
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
    crush_rule_set_step(rule, step++, CRUSH_RULE_TAKE, root, 0);
    // note: it's ok to set like 'step chooseleaf_firstn 0 host'
    std::pair<int, int> param = GetParam();
    int rep_num = std::get<0>(param);
    int domain = std::get<1>(param);
    crush_rule_set_step(rule, step++, CRUSH_RULE_CHOOSELEAF_FIRSTN, rep_num, domain);
    crush_rule_set_step(rule, step++, CRUSH_RULE_EMIT, 0, 0);
    ASSERT_TRUE(step == steps);
    auto r = crush_add_rule(crush.get_crush_map(), rule, rno);
    ASSERT_TRUE(r >= 0);
    crush.set_rule_name(rno, rule_name);
    {
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      pending_inc.crush.clear();
      crush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
      tmp.apply_incremental(pending_inc);
    }
    {
      stringstream oss;
      crush.dump_tree(&oss, NULL);
      std::cout << oss.str() << std::endl;
      Formatter *f = Formatter::create("json-pretty");
      f->open_object_section("crush_rules");
      crush.dump_rules(f);
      f->close_section();
      f->flush(cout);
      delete f;
    }
    // create a replicated pool referencing the above rule
    int64_t pool_infra_1706;
    {
      OSDMap::Incremental new_pool_inc(tmp.get_epoch() + 1);
      new_pool_inc.new_pool_max = tmp.get_pool_max();
      new_pool_inc.fsid = tmp.get_fsid();
      pg_pool_t empty;
      pool_infra_1706 = ++new_pool_inc.new_pool_max;
      pg_pool_t *p = new_pool_inc.get_new_pool(pool_infra_1706, &empty);
      p->size = 3;
      p->min_size = 1;
      p->set_pg_num(256);
      p->set_pgp_num(256);
      p->type = pg_pool_t::TYPE_REPLICATED;
      p->crush_rule = rno;
      p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
      new_pool_inc.new_pool_names[pool_infra_1706] = "pool_infra_1706";
      tmp.apply_incremental(new_pool_inc);
    }

    // add upmaps
    pg_t rep_pg(3, pool_infra_1706);
    pg_t rep_pgid = tmp.raw_pg_to_pg(rep_pg);
    pg_t rep_pg2(4, pool_infra_1706);
    pg_t rep_pgid2 = tmp.raw_pg_to_pg(rep_pg2);
    pg_t rep_pg3(6, pool_infra_1706);
    pg_t rep_pgid3 = tmp.raw_pg_to_pg(rep_pg3);
    {
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      pending_inc.new_pg_upmap[rep_pgid] = mempool::osdmap::vector<int32_t>({1,0,2});
      pending_inc.new_pg_upmap[rep_pgid2] = mempool::osdmap::vector<int32_t>({1,2,0});
      pending_inc.new_pg_upmap[rep_pgid3] = mempool::osdmap::vector<int32_t>({1,2,0});
      tmp.apply_incremental(pending_inc);
      ASSERT_TRUE(tmp.have_pg_upmaps(rep_pgid));
      ASSERT_TRUE(tmp.have_pg_upmaps(rep_pgid2));
      ASSERT_TRUE(tmp.have_pg_upmaps(rep_pgid3));
    }

    {
      // now, set pool size to 1
      OSDMap tmpmap;
      tmpmap.deepish_copy_from(tmp);
      OSDMap::Incremental new_pool_inc(tmpmap.get_epoch() + 1);
      pg_pool_t p = *tmpmap.get_pg_pool(pool_infra_1706);
      p.size = 1;
      p.last_change = new_pool_inc.epoch;
      new_pool_inc.new_pools[pool_infra_1706] = p;
      tmpmap.apply_incremental(new_pool_inc);

      OSDMap::Incremental new_pending_inc(tmpmap.get_epoch() + 1);
      clean_pg_upmaps(g_ceph_context, tmpmap, new_pending_inc);
      tmpmap.apply_incremental(new_pending_inc);
      // check pg upmaps
      ASSERT_TRUE(!tmpmap.have_pg_upmaps(rep_pgid));
      ASSERT_TRUE(!tmpmap.have_pg_upmaps(rep_pgid2));
      ASSERT_TRUE(!tmpmap.have_pg_upmaps(rep_pgid3));
    }
    {
      // now, set pool size to 4
      OSDMap tmpmap;
      tmpmap.deepish_copy_from(tmp);
      OSDMap::Incremental new_pool_inc(tmpmap.get_epoch() + 1);
      pg_pool_t p = *tmpmap.get_pg_pool(pool_infra_1706);
      p.size = 4;
      p.last_change = new_pool_inc.epoch;
      new_pool_inc.new_pools[pool_infra_1706] = p;
      tmpmap.apply_incremental(new_pool_inc);

      OSDMap::Incremental new_pending_inc(tmpmap.get_epoch() + 1);
      clean_pg_upmaps(g_ceph_context, tmpmap, new_pending_inc);
      tmpmap.apply_incremental(new_pending_inc);
      // check pg upmaps
      ASSERT_TRUE(!tmpmap.have_pg_upmaps(rep_pgid));
      ASSERT_TRUE(!tmpmap.have_pg_upmaps(rep_pgid2));
      ASSERT_TRUE(!tmpmap.have_pg_upmaps(rep_pgid3));
    }
}

const string OSDMapTest::range_addrs[] = {"198.51.100.0/22", "10.2.5.102/32", "2001:db8::/48",
  "3001:db8::/72", "4001:db8::/30", "5001:db8::/64", "6001:db8::/128", "7001:db8::/127"};
const string OSDMapTest::ip_addrs[] = {"198.51.100.14", "198.51.100.0", "198.51.103.255",
  "10.2.5.102",
  "2001:db8:0:0:0:0:0:0", "2001:db8:0:0:0:0001:ffff:ffff",
  "2001:db8:0:ffff:ffff:ffff:ffff:ffff",
  "3001:db8:0:0:0:0:0:0", "3001:db8:0:0:0:0001:ffff:ffff",
  "3001:db8:0:0:00ff:ffff:ffff:ffff",
  "4001:db8::", "4001:db8:0:0:0:0001:ffff:ffff",
  "4001:dbb:ffff:ffff:ffff:ffff:ffff:ffff",
  "5001:db8:0:0:0:0:0:0", "5001:db8:0:0:0:0:ffff:ffff",
  "5001:db8:0:0:ffff:ffff:ffff:ffff",
  "6001:db8:0:0:0:0:0:0",
  "7001:db8:0:0:0:0:0:0", "7001:db8:0:0:0:0:0:0001"
};
const string OSDMapTest::unblocked_ip_addrs[] = { "0.0.0.0", "1.1.1.1", "192.168.1.1",
  "198.51.99.255", "198.51.104.0",
  "10.2.5.101", "10.2.5.103",
  "2001:db7:ffff:ffff:ffff:ffff:ffff:ffff", "2001:db8:0001::",
  "3001:db7:ffff:ffff:ffff:ffff:ffff:ffff", "3001:db8:0:0:0100::",
  "4001:db7:ffff:ffff:ffff:ffff:ffff:ffff", "4001:dbc::",
  "5001:db7:ffff:ffff:ffff:ffff:ffff:ffff", "5001:db8:0:0001:0:0:0:0", 
  "6001:db8:0:0:0:0:0:0001",
  "7001:db7:ffff:ffff:ffff:ffff:ffff:ffff", "7001:db8:0:0:0:0:0:0002"
};

TEST_F(OSDMapTest, blocklisting_ips) {
  set_up_map(6); //whatever

  OSDMap::Incremental new_blocklist_inc(osdmap.get_epoch() + 1);
  for (const auto& a : ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    new_blocklist_inc.new_blocklist[addr] = ceph_clock_now();
  }
  osdmap.apply_incremental(new_blocklist_inc);

  for (const auto& a: ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    ASSERT_TRUE(osdmap.is_blocklisted(addr, g_ceph_context));
  }
  for (const auto& a: unblocked_ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    ASSERT_FALSE(osdmap.is_blocklisted(addr, g_ceph_context));
  }

  OSDMap::Incremental rm_blocklist_inc(osdmap.get_epoch() + 1);
  for (const auto& a : ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    rm_blocklist_inc.old_blocklist.push_back(addr);
  }
  osdmap.apply_incremental(rm_blocklist_inc);
  for (const auto& a: ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    ASSERT_FALSE(osdmap.is_blocklisted(addr, g_ceph_context));
  }
  for (const auto& a: unblocked_ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    bool blocklisted = osdmap.is_blocklisted(addr, g_ceph_context);
    if (blocklisted) {
      cout << "erroneously blocklisted " << addr << std::endl;
    }
    EXPECT_FALSE(blocklisted);
  }
}

TEST_F(OSDMapTest, blocklisting_ranges) {
  set_up_map(6); //whatever
  OSDMap::Incremental range_blocklist_inc(osdmap.get_epoch() + 1);
  for (const auto& a : range_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.type = entity_addr_t::TYPE_CIDR;
    range_blocklist_inc.new_range_blocklist[addr] = ceph_clock_now();
  }
  osdmap.apply_incremental(range_blocklist_inc);

  for (const auto& a: ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    bool blocklisted = osdmap.is_blocklisted(addr, g_ceph_context);
    if (!blocklisted) {
      cout << "erroneously not blocklisted " << addr << std::endl;
    }
    ASSERT_TRUE(blocklisted);
  }
  for (const auto& a: unblocked_ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    bool blocklisted = osdmap.is_blocklisted(addr, g_ceph_context);
    if (blocklisted) {
      cout << "erroneously blocklisted " << addr << std::endl;
    }
    EXPECT_FALSE(blocklisted);
  }

  OSDMap::Incremental rm_range_blocklist(osdmap.get_epoch() + 1);
  for (const auto& a : range_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.type = entity_addr_t::TYPE_CIDR;
    rm_range_blocklist.old_range_blocklist.push_back(addr);
  }
  osdmap.apply_incremental(rm_range_blocklist);

  for (const auto& a: ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    ASSERT_FALSE(osdmap.is_blocklisted(addr, g_ceph_context));
  }
  for (const auto& a: unblocked_ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    bool blocklisted = osdmap.is_blocklisted(addr, g_ceph_context);
    if (blocklisted) {
      cout << "erroneously blocklisted " << addr << std::endl;
    }
    EXPECT_FALSE(blocklisted);
  }
}

TEST_F(OSDMapTest, blocklisting_everything) {
  set_up_map(6); //whatever
  OSDMap::Incremental range_blocklist_inc(osdmap.get_epoch() + 1);
  entity_addr_t baddr;
  baddr.parse("2001:db8::/0");
  baddr.type = entity_addr_t::TYPE_CIDR;
  range_blocklist_inc.new_range_blocklist[baddr] = ceph_clock_now();
  osdmap.apply_incremental(range_blocklist_inc);

  for (const auto& a: ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    if (addr.is_ipv4()) continue;
    bool blocklisted = osdmap.is_blocklisted(addr, g_ceph_context);
    if (!blocklisted) {
      cout << "erroneously not  blocklisted " << addr << std::endl;
    }
    ASSERT_TRUE(blocklisted);
  }
  for (const auto& a: unblocked_ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    if (addr.is_ipv4()) continue;
    bool blocklisted = osdmap.is_blocklisted(addr, g_ceph_context);
    if (!blocklisted) {
      cout << "erroneously not  blocklisted " << addr << std::endl;
    }
    ASSERT_TRUE(blocklisted);
  }

  OSDMap::Incremental swap_blocklist_inc(osdmap.get_epoch()+1);
  swap_blocklist_inc.old_range_blocklist.push_back(baddr);

  entity_addr_t caddr;
  caddr.parse("1.1.1.1/0");
  caddr.type = entity_addr_t::TYPE_CIDR;
  swap_blocklist_inc.new_range_blocklist[caddr] = ceph_clock_now();
  osdmap.apply_incremental(swap_blocklist_inc);

  for (const auto& a: ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    if (!addr.is_ipv4()) continue;
    bool blocklisted = osdmap.is_blocklisted(addr, g_ceph_context);
    if (!blocklisted) {
      cout << "erroneously not  blocklisted " << addr << std::endl;
    }
    ASSERT_TRUE(blocklisted);
  }
  for (const auto& a: unblocked_ip_addrs) {
    entity_addr_t addr;
    addr.parse(a);
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    if (!addr.is_ipv4()) continue;
    bool blocklisted = osdmap.is_blocklisted(addr, g_ceph_context);
    if (!blocklisted) {
      cout << "erroneously not  blocklisted " << addr << std::endl;
    }
    ASSERT_TRUE(blocklisted);
  }
}

TEST_F(OSDMapTest, ReadBalanceScore1) {
    std::srand ( unsigned ( std::time(0) ) );
    uint osd_rand = rand() % 13;
    set_up_map(6 + osd_rand); //whatever
    auto pools = osdmap.get_pools();
    for (auto &[pid, pg_pool] : pools) {
      const pg_pool_t *pi = osdmap.get_pg_pool(pid);
      if (pi->is_replicated()) {
        //cout << "pool " << pid << " " << pg_pool << std::endl;
        auto replica_count = pi->get_size();
        OSDMap::read_balance_info_t rbi;
        auto rc = osdmap.calc_read_balance_score(g_ceph_context, pid, &rbi);

        // "Normal" score is between 1 and num_osds
        ASSERT_TRUE(rc == 0);
        ASSERT_TRUE(score_in_range(rbi.adjusted_score));
        ASSERT_TRUE(score_in_range(rbi.acting_adj_score));
        ASSERT_TRUE(rbi.err_msg.empty());

        // When all OSDs have primary_affinity 0, score should be 0
        auto num_osds = get_num_osds();
        set_primary_affinity_all(0.);

        rc = osdmap.calc_read_balance_score(g_ceph_context, pid, &rbi);
        ASSERT_TRUE(rc < 0);
        ASSERT_TRUE(rbi.adjusted_score == 0.);
        ASSERT_TRUE(rbi.acting_adj_score == 0.);
        ASSERT_FALSE(rbi.err_msg.empty());

        std::vector<uint> osds;
        for (uint i = 0 ; i < num_osds ; i++) {
          osds.push_back(i);
        }

        // Change primary_affinity of some OSDs to 1 others are 0
        float fratio = 1. / (float)replica_count;
        for (int iter = 0 ; iter < 100 ; iter++) {  // run the test 100 times
          // Create random shuffle of OSDs
          std::random_device seed;
          std::default_random_engine generator(seed());
          std::shuffle(osds.begin(), osds.end(), generator);
          for (uint i = 0 ; i < num_osds ; i++) {
            if ((float(i + 1) / float(num_osds)) < fratio) {
              ASSERT_TRUE(osds[i] < num_osds);
              osdmap.set_primary_affinity(osds[i], CEPH_OSD_MAX_PRIMARY_AFFINITY);
              rc = osdmap.calc_read_balance_score(g_ceph_context, pid, &rbi);

              ASSERT_TRUE(rc < 0);
              ASSERT_TRUE(rbi.adjusted_score == 0.);
              ASSERT_TRUE(rbi.acting_adj_score == 0.);
              ASSERT_FALSE(rbi.err_msg.empty());
            }
            else {
              if (rc < 0) {
                ASSERT_TRUE(rbi.adjusted_score == 0.);
                ASSERT_TRUE(rbi.acting_adj_score == 0.);
                ASSERT_FALSE(rbi.err_msg.empty());
              }
              else {
                ASSERT_TRUE(score_in_range(rbi.acting_adj_score, i + 1));
                ASSERT_TRUE(rbi.err_msg.empty());
              }
            }
          }
          set_primary_affinity_all(0.);
        }
      }
    }

  }

TEST_F(OSDMapTest, ReadBalanceScore2) {
    std::srand ( unsigned ( std::time(0) ) );
    uint osd_num = 6 + rand() % 13;
    set_up_map(osd_num, true);
    for (int i = 0 ; i < 100 ; i++) { //running 100 random tests
      uint num_pa_osds = 0;
      float pa_sum = 0.;
      OSDMap::read_balance_info_t rbi;

      // set pa for all osds
      for (uint j = 0 ; j < osd_num ; j++) {
        uint pa = 1 + rand() % 100;
        if (pa > 80)
          pa = 100;
        if (pa < 20)
          pa = 0;
        float fpa = (float)pa / 100.;
        if (pa > 0) {
          num_pa_osds++;
          pa_sum += fpa;
        }
        osdmap.set_primary_affinity(j, int(fpa * CEPH_OSD_MAX_PRIMARY_AFFINITY));
      }
      float pa_ratio = pa_sum / (float) osd_num;

      // create a pool with the current osdmap configuration
      OSDMap::Incremental new_pool_inc(osdmap.get_epoch() + 1);
      new_pool_inc.new_pool_max = osdmap.get_pool_max();
      new_pool_inc.fsid = osdmap.get_fsid();
      string pool_name = "rep_pool" + stringify(i);
      uint64_t new_pid = set_rep_pool(pool_name, new_pool_inc, false);
      ASSERT_TRUE(new_pid > 0);
      osdmap.apply_incremental(new_pool_inc);

      // now run the test on the pool.
      const pg_pool_t *pi = osdmap.get_pg_pool(new_pid);
      ASSERT_NE(pi, nullptr);
      ASSERT_TRUE(pi->is_replicated());
      float fratio = 1. / (float)pi->get_size();
      auto rc = osdmap.calc_read_balance_score(g_ceph_context, new_pid, &rbi);
      if (pa_ratio < fratio) {
        ASSERT_TRUE(rc < 0);
        ASSERT_FALSE(rbi.err_msg.empty());
        ASSERT_TRUE(rbi.acting_adj_score == 0.);
        ASSERT_TRUE(rbi.adjusted_score == 0.);
      }
      else {
        if (rc < 0) {
          ASSERT_TRUE(rbi.adjusted_score == 0.);
          ASSERT_TRUE(rbi.acting_adj_score == 0.);
          ASSERT_FALSE(rbi.err_msg.empty());
        }
        else {
          if (rbi.err_msg.empty()) {
            ASSERT_TRUE(score_in_range(rbi.acting_adj_score, num_pa_osds));
          }
        }
      }

    }
        //TODO add ReadBalanceScore3 - with weighted osds.

  }

TEST_F(OSDMapTest, read_balance_small_map) {
  // Set up a map with 4 OSDs and default pools
  set_up_map(4);

  const vector<string> test_cases = {"basic", "prim_affinity"};
  for (const auto & test : test_cases) {
    if (test == "prim_affinity") {
      // Make osd.0 off-limits for primaries by giving it prim affinity 0
      OSDMap::Incremental pending_inc0(osdmap.get_epoch() + 1);
      pending_inc0.new_primary_affinity[0] = 0;
      osdmap.apply_incremental(pending_inc0);

      // Ensure osd.0 has no primaries assigned to it
      map<uint64_t,set<pg_t>> prim_pgs_by_osd, acting_prims_by_osd;
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd, &acting_prims_by_osd);
      ASSERT_TRUE(prim_pgs_by_osd[0].size() == 0);
      ASSERT_TRUE(acting_prims_by_osd[0].size() == 0);
    }

    // Make sure capacity is balanced first
    balance_capacity(my_rep_pool);

    // Get read balance score before balancing
    OSDMap::read_balance_info_t rb_info;
    auto rc = osdmap.calc_read_balance_score(g_ceph_context, my_rep_pool, &rb_info);
    ASSERT_TRUE(rc >= 0);
    float read_balance_score_before = rb_info.adjusted_score;

    // Calculate desired prim distributions to verify later
    map<uint64_t,set<pg_t>> prim_pgs_by_osd_2, acting_prims_by_osd_2;
    osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd_2, &acting_prims_by_osd_2);
    vector<uint64_t> osds_to_check;
    for (const auto & [osd, pgs] : prim_pgs_by_osd_2) {
      osds_to_check.push_back(osd);
    }
    map<uint64_t,float> desired_prim_dist;
    rc = osdmap.calc_desired_primary_distribution(g_ceph_context, my_rep_pool,
                                                  osds_to_check, desired_prim_dist);
    ASSERT_TRUE(rc >= 0);

    // Balance reads
    OSDMap::Incremental pending_inc_2(osdmap.get_epoch()+1);
    int num_changes = osdmap.balance_primaries(g_ceph_context, my_rep_pool, &pending_inc_2, osdmap);
    osdmap.apply_incremental(pending_inc_2);

    if (test == "prim_affinity") {
      // Ensure osd.0 still has no primaries assigned to it
      map<uint64_t,set<pg_t>> prim_pgs_by_osd_3, acting_prims_by_osd_3;
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd_3, &acting_prims_by_osd_3);
      ASSERT_TRUE(prim_pgs_by_osd_3[0].size() == 0);
      ASSERT_TRUE(acting_prims_by_osd_3[0].size() == 0);
    }

    // Get read balance score after balancing
    rc = osdmap.calc_read_balance_score(g_ceph_context, my_rep_pool, &rb_info);
    ASSERT_TRUE(rc >= 0);
    float read_balance_score_after = rb_info.adjusted_score;

    // Ensure the score hasn't gotten worse
    ASSERT_TRUE(read_balance_score_after <= read_balance_score_before);

    // Check for improvements
    if (num_changes > 0) {
      ASSERT_TRUE(read_balance_score_after < read_balance_score_before);

      // Check num primaries for each OSD is within range
      map<uint64_t,set<pg_t>> prim_pgs_by_osd_4, acting_prims_by_osd_4;
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd_4, &acting_prims_by_osd_4);
      for (const auto & [osd, primaries] : prim_pgs_by_osd_4) {
        ASSERT_TRUE(primaries.size() >= floor(desired_prim_dist[osd] - 1));
        ASSERT_TRUE(primaries.size() <= ceil(desired_prim_dist[osd] + 1));
      }
    }
  }
}

TEST_F(OSDMapTest, read_balance_large_map) {
  // Set up a map with 60 OSDs and default pools
  set_up_map(60);

  const vector<string> test_cases = {"basic", "prim_affinity"};
  for (const auto & test : test_cases) {
    if (test == "prim_affinity") {
      // Make osd.0 off-limits for primaries by giving it prim affinity 0
      OSDMap::Incremental pending_inc0(osdmap.get_epoch() + 1);
      pending_inc0.new_primary_affinity[0] = 0;
      osdmap.apply_incremental(pending_inc0);

      // Ensure osd.0 has no primaries assigned to it
      map<uint64_t,set<pg_t>> prim_pgs_by_osd, acting_prims_by_osd;
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd, &acting_prims_by_osd);
      ASSERT_TRUE(prim_pgs_by_osd[0].size() == 0);
      ASSERT_TRUE(acting_prims_by_osd[0].size() == 0);
    }

    // Make sure capacity is balanced first
    balance_capacity(my_rep_pool);
  
    // Get read balance score before balancing
    OSDMap::read_balance_info_t rb_info;
    auto rc = osdmap.calc_read_balance_score(g_ceph_context, my_rep_pool, &rb_info);
    ASSERT_TRUE(rc >= 0);
    float read_balance_score_before = rb_info.adjusted_score;

    // Calculate desired prim distributions to verify later
    map<uint64_t,set<pg_t>> prim_pgs_by_osd_2, acting_prims_by_osd_2;
    osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd_2, &acting_prims_by_osd_2);
    vector<uint64_t> osds_to_check;
    for (auto [osd, pgs] : prim_pgs_by_osd_2) {
      osds_to_check.push_back(osd);
    }
    map<uint64_t,float> desired_prim_dist;
    rc = osdmap.calc_desired_primary_distribution(g_ceph_context, my_rep_pool,
                                                  osds_to_check, desired_prim_dist);
    ASSERT_TRUE(rc >= 0);

    // Balance reads
    OSDMap::Incremental pending_inc_2(osdmap.get_epoch()+1);
    int num_changes = osdmap.balance_primaries(g_ceph_context, my_rep_pool, &pending_inc_2, osdmap);
    osdmap.apply_incremental(pending_inc_2);

    if (test == "prim_affinity") {
      // Ensure osd.0 still has no primaries assigned to it
      map<uint64_t,set<pg_t>> prim_pgs_by_osd_3, acting_prims_by_osd_3;
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd_3, &acting_prims_by_osd_3);
      ASSERT_TRUE(prim_pgs_by_osd_3[0].size() == 0);
      ASSERT_TRUE(acting_prims_by_osd_3[0].size() == 0);
    }
  
    // Get read balance score after balancing
    rc = osdmap.calc_read_balance_score(g_ceph_context, my_rep_pool, &rb_info);
    ASSERT_TRUE(rc >= 0);
    float read_balance_score_after = rb_info.adjusted_score;

    // Ensure the score hasn't gotten worse
    ASSERT_TRUE(read_balance_score_after <= read_balance_score_before);

    // Check for improvements
    if (num_changes > 0) {
      ASSERT_TRUE(read_balance_score_after < read_balance_score_before);

      // Check num primaries for each OSD is within range
      map<uint64_t,set<pg_t>> prim_pgs_by_osd_4, acting_prims_by_osd_4;
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd_4, &acting_prims_by_osd_4);
      for (const auto & [osd, primaries] : prim_pgs_by_osd_4) {
        ASSERT_TRUE(primaries.size() >= floor(desired_prim_dist[osd] - 1));
        ASSERT_TRUE(primaries.size() <= ceil(desired_prim_dist[osd] + 1));
      }
    }
  }
}

TEST_F(OSDMapTest, read_balance_random_map) {
  // Set up map with random number of OSDs
  std::srand ( unsigned ( std::time(0) ) );
  uint num_osds = 3 + (rand() % 10);
  ASSERT_TRUE(num_osds >= 3);
  set_up_map(num_osds);

  const vector<string> test_cases = {"basic", "prim_affinity"};
  for (const auto & test : test_cases) {
    uint rand_osd = rand() % num_osds;
    if (test == "prim_affinity") {
      // Make a random OSD off-limits for primaries by giving it prim affinity 0
      ASSERT_TRUE(rand_osd < num_osds);
      OSDMap::Incremental pending_inc0(osdmap.get_epoch() + 1);
      pending_inc0.new_primary_affinity[rand_osd] = 0;
      osdmap.apply_incremental(pending_inc0);

      // Ensure the random OSD has no primaries assigned to it
      map<uint64_t,set<pg_t>> prim_pgs_by_osd, acting_prims_by_osd;
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd, &acting_prims_by_osd);
      ASSERT_TRUE(prim_pgs_by_osd[rand_osd].size() == 0);
      ASSERT_TRUE(acting_prims_by_osd[rand_osd].size() == 0);
    }

    // Make sure capacity is balanced first
    balance_capacity(my_rep_pool);

    // Get read balance score before balancing
    OSDMap::read_balance_info_t rb_info;
    auto rc = osdmap.calc_read_balance_score(g_ceph_context, my_rep_pool, &rb_info);
    ASSERT_TRUE(rc >= 0);
    float read_balance_score_before = rb_info.adjusted_score;

    // Calculate desired prim distributions to verify later
    map<uint64_t,set<pg_t>> prim_pgs_by_osd_2, acting_prims_by_osd_2;
    osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd_2, &acting_prims_by_osd_2);
    vector<uint64_t> osds_to_check;
    for (const auto & [osd, pgs] : prim_pgs_by_osd_2) {
      osds_to_check.push_back(osd);
    }
    map<uint64_t,float> desired_prim_dist;
    rc = osdmap.calc_desired_primary_distribution(g_ceph_context, my_rep_pool,
                                                  osds_to_check, desired_prim_dist);
    ASSERT_TRUE(rc >= 0);

    // Balance reads
    OSDMap::Incremental pending_inc_2(osdmap.get_epoch()+1);
    int num_changes = osdmap.balance_primaries(g_ceph_context, my_rep_pool, &pending_inc_2, osdmap);
    osdmap.apply_incremental(pending_inc_2);

    if (test == "prim_affinity") {
      // Ensure the random OSD still has no primaries assigned to it
      map<uint64_t,set<pg_t>> prim_pgs_by_osd_3, acting_prims_by_osd_3;
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd_3, &acting_prims_by_osd_3);
      ASSERT_TRUE(prim_pgs_by_osd_3[rand_osd].size() == 0);
      ASSERT_TRUE(acting_prims_by_osd_3[rand_osd].size() == 0);
    }

    // Get read balance score after balancing
    rc = osdmap.calc_read_balance_score(g_ceph_context, my_rep_pool, &rb_info);
    ASSERT_TRUE(rc >= 0);
    float read_balance_score_after = rb_info.adjusted_score;

    // Ensure the score hasn't gotten worse
    ASSERT_TRUE(read_balance_score_after <= read_balance_score_before);

    // Check for improvements
    if (num_changes > 0) {
      ASSERT_TRUE(read_balance_score_after < read_balance_score_before);

      // Check num primaries for each OSD is within range
      map<uint64_t,set<pg_t>> prim_pgs_by_osd_4, acting_prims_by_osd_4;
      osdmap.get_pgs_by_osd(g_ceph_context, my_rep_pool, &prim_pgs_by_osd_4, &acting_prims_by_osd_4);
      for (auto [osd, primaries] : prim_pgs_by_osd_4) {
        ASSERT_TRUE(primaries.size() >= floor(desired_prim_dist[osd] - 1));
        ASSERT_TRUE(primaries.size() <= ceil(desired_prim_dist[osd] + 1));
      }
      for (auto [osd, primaries] : prim_pgs_by_osd_4) {
        ASSERT_TRUE(primaries.size() >= floor(desired_prim_dist[osd] - 1));
        ASSERT_TRUE(primaries.size() <= ceil(desired_prim_dist[osd] + 1));
      }
    }
  }
}

TEST_F(OSDMapTest, rb_osdsize_opt_1small_osd) {
  //TO-REMOVE (the comment) - look ar 43124 for examples
  vector <pair<int, int>> weights = {
      {57, 0}, {100, 0}, {100, 0}, {100, 0}, {100, 0},
  };
  vector<int> read_ratios = {10, 50, 70, 80, 100};
  if (is_verbose()) {
    cout << " First iteration - incremental, no reset" << std::endl << "====================" << std::endl << std::endl;
  }
  test_rb_osd_size(weights, read_ratios, false);
  if (is_verbose()) {
    cout << std::endl << " Second iteration - reset between read ratio tests" << std::endl << "====================" << std::endl << std::endl;
  }
  test_rb_osd_size(weights, read_ratios, false, true, false);

  return;
}

TEST_F(OSDMapTest, rb_osdsize_opt_mixed_osds) {
  vector <pair<int, int>> weights = {
    {50, 0}, {50, 0}, {100, 0},  {100, 0}, {50, 0}, {100, 0},
  };
  vector<int> read_ratios = {10, 50, 70, 80, 100};
  if (is_verbose()) {
    cout << " First iteration - incremental, no reset" << std::endl << "====================" << std::endl << std::endl;
  }
  test_rb_osd_size(weights, read_ratios, false);
  if (is_verbose()) {
    cout << std::endl << " Second iteration - reset between read ratio tests" << std::endl << "====================" << std::endl << std::endl;
  }
  test_rb_osd_size(weights, read_ratios, false, true, false);

  return;
}

TEST_F(OSDMapTest, rb_osdsize_opt_1large_osd) {
  vector <pair<int, int>> weights = {
      {25, 0}, {25, 0}, {25, 0}, {25, 0}, {25, 0}, {25, 0}, {100, 0},
  };
  vector<int> read_ratios = {70};
  //expect failure here, so no need for multiple runs
  test_rb_osd_size(weights, read_ratios, true);

  return;
}

TEST_F(OSDMapTest, rb_osdsize_opt_1large_mixed_osds) {
  //TO_REMOVE (the comment) - look ar 43124 for examples
  vector <pair<int, int>> weights = {
      {50, 0}, {70, 0}, {50, 0}, {35, 0}, {35, 0}, {35, 0},
      {50, 0}, {75, 0}, {35, 0}, {50, 0}, {100, 0},
  };
  vector<int> read_ratios = {10, 30, 50, 70, 80, 90, 100};
  if (is_verbose()) {
    cout << " First iteration - incremental, no reset" << std::endl << "====================" << std::endl << std::endl;
  }
  test_rb_osd_size(weights, read_ratios, false);
  if (is_verbose()) {
    cout << std::endl << " Second iteration - reset between read ratio tests" << std::endl << "====================" << std::endl << std::endl;
  }
  test_rb_osd_size(weights, read_ratios, false, true, false);

  return;
}

INSTANTIATE_TEST_SUITE_P(
  OSDMap,
  OSDMapTest,
  ::testing::Values(
    std::make_pair<int, int>(0, 1), // chooseleaf firstn 0 host
   std::make_pair<int, int>(3, 1), // chooseleaf firstn 3 host
    std::make_pair<int, int>(0, 0), // chooseleaf firstn 0 osd
    std::make_pair<int, int>(3, 0)  // chooseleaf firstn 3 osd
  )
);
