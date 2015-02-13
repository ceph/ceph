// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank <info@inktank.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <iostream>
#include <gtest/gtest.h>

#include "include/stringify.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"

#include "crush/CrushWrapper.h"

#include <set>

CrushWrapper *build_indep_map(CephContext *cct, int num_rack, int num_host,
			      int num_osd)
{
  CrushWrapper *c = new CrushWrapper;
  c->create();

  c->set_type_name(5, "root");
  c->set_type_name(4, "row");
  c->set_type_name(3, "rack");
  c->set_type_name(2, "chasis");
  c->set_type_name(1, "host");
  c->set_type_name(0, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		5, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  map<string,string> loc;
  loc["root"] = "default";

  int osd = 0;
  for (int r=0; r<num_rack; ++r) {
    loc["rack"] = string("rack-") + stringify(r);
    for (int h=0; h<num_host; ++h) {
      loc["host"] = string("host-") + stringify(r) + string("-") + stringify(h);
      for (int o=0; o<num_osd; ++o, ++osd) {
	c->insert_item(cct, osd, 1.0, string("osd.") + stringify(osd), loc);
      }
    }
  }
  int ret;
  int ruleno = 0;
  int ruleset = 0;
  ruleno = ruleset;
  ret = c->add_rule(4, ruleset, 123, 1, 20, ruleno);
  assert(ret == ruleno);
  ret = c->set_rule_step(ruleno, 0, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 10, 0);
  assert(ret == 0);
  ret = c->set_rule_step(ruleno, 1, CRUSH_RULE_TAKE, rootno, 0);
  assert(ret == 0);
  ret = c->set_rule_step(ruleno, 2, CRUSH_RULE_CHOOSELEAF_INDEP, CRUSH_CHOOSE_N, 1);
  assert(ret == 0);
  ret = c->set_rule_step(ruleno, 3, CRUSH_RULE_EMIT, 0, 0);
  assert(ret == 0);
  c->set_rule_name(ruleno, "data");

  if (false) {
    Formatter *f = new_formatter("json-pretty");
    f->open_object_section("crush_map");
    c->dump(f);
    f->close_section();
    f->flush(cout);
    delete f;
  }

  return c;
}

int get_num_dups(const vector<int>& v)
{
  std::set<int> s;
  int dups = 0;
  for (unsigned i=0; i<v.size(); ++i) {
    if (s.count(v[i]))
      ++dups;
    else if (v[i] != CRUSH_ITEM_NONE)
      s.insert(v[i]);
  }
  return dups;
}

TEST(CRUSH, indep_toosmall) {
  CrushWrapper *c = build_indep_map(g_ceph_context, 1, 3, 1);
  vector<__u32> weight(c->get_max_devices(), 0x10000);
  c->dump_tree(weight, &cout, NULL);

  for (int x = 0; x < 100; ++x) {
    vector<int> out;
    c->do_rule(0, x, out, 5, weight);
    cout << x << " -> " << out << std::endl;
    int num_none = 0;
    for (unsigned i=0; i<out.size(); ++i) {
      if (out[i] == CRUSH_ITEM_NONE)
	num_none++;
    }
    ASSERT_EQ(2, num_none);
    ASSERT_EQ(0, get_num_dups(out));
  }
  delete c;
}

TEST(CRUSH, indep_basic) {
  CrushWrapper *c = build_indep_map(g_ceph_context, 3, 3, 3);
  vector<__u32> weight(c->get_max_devices(), 0x10000);
  c->dump_tree(weight, &cout, NULL);

  for (int x = 0; x < 100; ++x) {
    vector<int> out;
    c->do_rule(0, x, out, 5, weight);
    cout << x << " -> " << out << std::endl;
    int num_none = 0;
    for (unsigned i=0; i<out.size(); ++i) {
      if (out[i] == CRUSH_ITEM_NONE)
	num_none++;
    }
    ASSERT_EQ(0, num_none);
    ASSERT_EQ(0, get_num_dups(out));
  }
  delete c;
}

TEST(CRUSH, indep_out_alt) {
  CrushWrapper *c = build_indep_map(g_ceph_context, 3, 3, 3);
  vector<__u32> weight(c->get_max_devices(), 0x10000);

  // mark a bunch of osds out
  int num = 3*3*3;
  for (int i=0; i<num / 2; ++i)
    weight[i*2] = 0;
  c->dump_tree(weight, &cout, NULL);

  // need more retries to get 9/9 hosts for x in 0..99
  c->set_choose_total_tries(100);
  for (int x = 0; x < 100; ++x) {
    vector<int> out;
    c->do_rule(0, x, out, 9, weight);
    cout << x << " -> " << out << std::endl;
    int num_none = 0;
    for (unsigned i=0; i<out.size(); ++i) {
      if (out[i] == CRUSH_ITEM_NONE)
	num_none++;
    }
    ASSERT_EQ(0, num_none);
    ASSERT_EQ(0, get_num_dups(out));
  }
  delete c;
}

TEST(CRUSH, indep_out_contig) {
  CrushWrapper *c = build_indep_map(g_ceph_context, 3, 3, 3);
  vector<__u32> weight(c->get_max_devices(), 0x10000);

  // mark a bunch of osds out
  int num = 3*3*3;
  for (int i=0; i<num / 3; ++i)
    weight[i] = 0;
  c->dump_tree(weight, &cout, NULL);

  c->set_choose_total_tries(100);
  for (int x = 0; x < 100; ++x) {
    vector<int> out;
    c->do_rule(0, x, out, 7, weight);
    cout << x << " -> " << out << std::endl;
    int num_none = 0;
    for (unsigned i=0; i<out.size(); ++i) {
      if (out[i] == CRUSH_ITEM_NONE)
	num_none++;
    }
    ASSERT_EQ(1, num_none);
    ASSERT_EQ(0, get_num_dups(out));
  }
  delete c;
}


TEST(CRUSH, indep_out_progressive) {
  CrushWrapper *c = build_indep_map(g_ceph_context, 3, 3, 3);
  c->set_choose_total_tries(100);
  vector<__u32> tweight(c->get_max_devices(), 0x10000);
  c->dump_tree(tweight, &cout, NULL);

  int tchanged = 0;
  for (int x = 1; x < 5; ++x) {
    vector<__u32> weight(c->get_max_devices(), 0x10000);

    std::map<int,unsigned> pos;
    vector<int> prev;
    for (unsigned i=0; i<weight.size(); ++i) {
      vector<int> out;
      c->do_rule(0, x, out, 7, weight);
      cout << "(" << i << "/" << weight.size() << " out) "
	   << x << " -> " << out << std::endl;
      int num_none = 0;
      for (unsigned k=0; k<out.size(); ++k) {
	if (out[k] == CRUSH_ITEM_NONE)
	  num_none++;
      }
      ASSERT_EQ(0, get_num_dups(out));

      // make sure nothing moved
      int moved = 0;
      int changed = 0;
      for (unsigned j=0; j<out.size(); ++j) {
	if (i && out[j] != prev[j]) {
	  ++changed;
	  ++tchanged;
	}
	if (out[j] == CRUSH_ITEM_NONE) {
	  continue;
	}
	if (i && pos.count(out[j])) {
	  // result shouldn't have moved position
	  if (j != pos[out[j]]) {
	    cout << " " << out[j] << " moved from " << pos[out[j]] << " to " << j << std::endl;
	    ++moved;
	  }
	  //ASSERT_EQ(j, pos[out[j]]);
	}
      }
      if (moved || changed)
	cout << " " << moved << " moved, " << changed << " changed" << std::endl;
      ASSERT_LE(moved, 1);
      ASSERT_LE(changed, 3);

      // mark another osd out
      weight[i] = 0;
      prev = out;
      pos.clear();
      for (unsigned j=0; j<out.size(); ++j) {
	if (out[j] != CRUSH_ITEM_NONE)
	  pos[out[j]] = j;
      }
    }
  }
  cout << tchanged << " total changed" << std::endl;

  delete c;
}




int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
