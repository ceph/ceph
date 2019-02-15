// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "gtest/gtest.h"
#include "osd/OSDMap.h"
#include "osd/OSDMapMapping.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"

#include <iostream>

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

class OSDMapTest : public testing::Test {
  const static int num_osds = 6;
public:
  OSDMap osdmap;
  OSDMapMapping mapping;
  const uint64_t my_ec_pool = 1;
  const uint64_t my_rep_pool = 2;


  OSDMapTest() {}

  void set_up_map() {
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

    // Create an EC ruleset and a pool using it
    int r = osdmap.crush->add_simple_rule(
      "erasure", "default", "osd", "",
      "indep", pg_pool_t::TYPE_ERASURE,
      &cerr);

    OSDMap::Incremental new_pool_inc(osdmap.get_epoch() + 1);
    new_pool_inc.new_pool_max = osdmap.get_pool_max();
    new_pool_inc.fsid = osdmap.get_fsid();
    pg_pool_t empty;
    // make an ec pool
    uint64_t pool_id = ++new_pool_inc.new_pool_max;
    ceph_assert(pool_id == my_ec_pool);
    pg_pool_t *p = new_pool_inc.get_new_pool(pool_id, &empty);
    p->size = 3;
    p->set_pg_num(64);
    p->set_pgp_num(64);
    p->type = pg_pool_t::TYPE_ERASURE;
    p->crush_rule = r;
    new_pool_inc.new_pool_names[pool_id] = "ec";
    // and a replicated pool
    pool_id = ++new_pool_inc.new_pool_max;
    ceph_assert(pool_id == my_rep_pool);
    p = new_pool_inc.get_new_pool(pool_id, &empty);
    p->size = 3;
    p->set_pg_num(64);
    p->set_pgp_num(64);
    p->type = pg_pool_t::TYPE_REPLICATED;
    p->crush_rule = 0;
    p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
    new_pool_inc.new_pool_names[pool_id] = "reppool";
    osdmap.apply_incremental(new_pool_inc);
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
    nextmap.clean_pg_upmaps(g_ceph_context, &new_pending_inc);
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
      // confirm *maybe_remove_pg_upmaps* won't do anything bad
      OSDMap::Incremental pending_inc(tmpmap.get_epoch() + 1);
      OSDMap nextmap;
      nextmap.deepish_copy_from(tmpmap);
      nextmap.maybe_remove_pg_upmaps(g_ceph_context, tmpmap,
        nextmap, &pending_inc);
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
      // *maybe_remove_pg_upmaps* should be able to remove the above *bad* mapping
      OSDMap::Incremental pending_inc(tmpmap.get_epoch() + 1);
      OSDMap nextmap;
      nextmap.deepish_copy_from(tmpmap);
      nextmap.maybe_remove_pg_upmaps(g_ceph_context, tmpmap,
        nextmap, &pending_inc);
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
      if (!crush.rule_exists(rno) && !crush.ruleset_exists(rno))
        break;
    }
    string root_name = "default";
    int root = crush.get_item_id(root_name);
    int min_size = 3;
    int max_size = 4;
    int steps = 6;
    crush_rule *rule = crush_make_rule(steps, rno, rule_type, min_size, max_size);
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
      // *maybe_remove_pg_upmaps* should not remove the above upmap_item
      OSDMap::Incremental pending_inc(tmp.get_epoch() + 1);
      OSDMap nextmap;
      nextmap.deepish_copy_from(tmp);
      nextmap.maybe_remove_pg_upmaps(g_ceph_context, tmp,
        nextmap, &pending_inc);
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
        ASSERT_TRUE(up.size() == new_up.size());
        ASSERT_TRUE(new_up[0] == new_pg_upmap[0]);
        ASSERT_TRUE(new_up[1] == new_pg_upmap[1]);
        // and we shall have two OSDs from a same host now..
        int parent_0 = osdmap.crush->get_parent_of_type(new_up[0],
          osdmap.crush->get_type_id("host"));
        int parent_1 = osdmap.crush->get_parent_of_type(new_up[1],
          osdmap.crush->get_type_id("host"));
        ASSERT_TRUE(parent_0 == parent_1);
      }
    }
    {
      // STEP-2: apply cure
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      OSDMap tmpmap;
      tmpmap.deepish_copy_from(osdmap);
      tmpmap.apply_incremental(pending_inc);
      osdmap.maybe_remove_pg_upmaps(g_ceph_context, osdmap, tmpmap, &pending_inc);
      osdmap.apply_incremental(pending_inc);
      {
        // validate pg_upmap is gone (reverted)
        vector<int> new_up;
        int new_up_primary;
        osdmap.pg_to_raw_up(pgid, &new_up, &new_up_primary);
        ASSERT_TRUE(new_up == up);
        ASSERT_TRUE(new_up_primary = up_primary);
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
      ASSERT_TRUE(candidate_parent != 0);
      osdmap.crush->get_leaves(osdmap.crush->get_item_name(candidate_parent),
        &candidate_children);
      ASSERT_TRUE(candidate_children.count(will_choose));
      candidate_children.erase(will_choose);
      ASSERT_TRUE(!candidate_children.empty());
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
        ASSERT_TRUE(up.size() == new_up.size());
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
        ASSERT_TRUE(up.size() == new_up.size());
        int parent_0 = osdmap.crush->get_parent_of_type(new_up[0],
          osdmap.crush->get_type_id("host"));
        int parent_1 = osdmap.crush->get_parent_of_type(new_up[1],
          osdmap.crush->get_type_id("host"));
        ASSERT_TRUE(parent_0 == parent_1);
      } 
    }
    {
      // STEP-4: apply cure
      OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
      OSDMap tmpmap;
      tmpmap.deepish_copy_from(osdmap);
      tmpmap.apply_incremental(pending_inc);
      osdmap.maybe_remove_pg_upmaps(g_ceph_context, osdmap, tmpmap,
				    &pending_inc);
      osdmap.apply_incremental(pending_inc);
      {
        // validate pg_upmap_items is gone (reverted)
        vector<int> new_up;
        int new_up_primary;
        osdmap.pg_to_raw_up(pgid, &new_up, &new_up_primary);
        ASSERT_TRUE(new_up == up_after_out);
      }
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

