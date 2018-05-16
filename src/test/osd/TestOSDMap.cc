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
  std::vector<const char*> args(argv, argv+argc);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  // make sure we have 3 copies, or some tests won't work
  g_ceph_context->_conf->set_val("osd_pool_default_size", "3");
  // our map is flat, so just try and split across OSDs, not hosts or whatever
  g_ceph_context->_conf->set_val("osd_crush_chooseleaf_type", "0");
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
    entity_addr_t sample_addr;
    uuid_d sample_uuid;
    for (int i = 0; i < num_osds; ++i) {
      sample_uuid.generate_random();
      sample_addr.nonce = i;
      pending_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
      pending_inc.new_up_client[i] = sample_addr;
      pending_inc.new_up_cluster[i] = sample_addr;
      pending_inc.new_hb_back_up[i] = sample_addr;
      pending_inc.new_hb_front_up[i] = sample_addr;
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
    assert(pool_id == my_ec_pool);
    pg_pool_t *p = new_pool_inc.get_new_pool(pool_id, &empty);
    p->size = 3;
    p->set_pg_num(64);
    p->set_pgp_num(64);
    p->type = pg_pool_t::TYPE_ERASURE;
    p->crush_rule = r;
    new_pool_inc.new_pool_names[pool_id] = "ec";
    // and a replicated pool
    pool_id = ++new_pool_inc.new_pool_max;
    assert(pool_id == my_rep_pool);
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
  void get_crush(CrushWrapper& newcrush) {
    bufferlist bl;
    osdmap.crush->encode(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);
    bufferlist::iterator p = bl.begin();
    newcrush.decode(p);
  }
  int crush_move(const string &name, const vector<string> &argvec) {
    map<string,string> loc;
    CrushWrapper::parse_loc_map(argvec, &loc);
    CrushWrapper newcrush;
    get_crush(newcrush);
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
        OSDMap::Incremental pending_inc(osdmap.get_epoch() + 1);
        pending_inc.crush.clear();
        newcrush.encode(pending_inc.crush, CEPH_FEATURES_SUPPORTED_DEFAULT);
        osdmap.apply_incremental(pending_inc);
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
    get_crush(newcrush);
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

  OSDMap::clean_temps(g_ceph_context, osdmap, &pending_inc);

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

  OSDMap::clean_temps(g_ceph_context, osdmap, &pending_inc);
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
    int r = crush_move(osd_name.str(), move_to);
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
      osdmap.maybe_remove_pg_upmaps(g_ceph_context, osdmap, &pending_inc);
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
      osdmap.maybe_remove_pg_upmaps(g_ceph_context, osdmap, &pending_inc);
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

