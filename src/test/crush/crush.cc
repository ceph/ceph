// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank <info@inktank.com>
 *
 * LGPL-2.1 (see COPYING-LGPL2.1) or later
 */

#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <set>

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "include/stringify.h"

#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"

using namespace std;

std::unique_ptr<CrushWrapper> build_indep_map(CephContext *cct, int num_rack,
                              int num_host, int num_osd)
{
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);
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
  ret = c->add_rule(ruleno, 4, 123);
  ceph_assert(ret == ruleno);
  ret = c->set_rule_step(ruleno, 0, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 10, 0);
  ceph_assert(ret == 0);
  ret = c->set_rule_step(ruleno, 1, CRUSH_RULE_TAKE, rootno, 0);
  ceph_assert(ret == 0);
  ret = c->set_rule_step(ruleno, 2, CRUSH_RULE_CHOOSELEAF_INDEP, CRUSH_CHOOSE_N, 1);
  ceph_assert(ret == 0);
  ret = c->set_rule_step(ruleno, 3, CRUSH_RULE_EMIT, 0, 0);
  ceph_assert(ret == 0);
  c->set_rule_name(ruleno, "data");

  c->finalize();

  if (false) {
    Formatter *f = Formatter::create("json-pretty");
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
  for (auto n : v) {
    if (s.count(n))
      ++dups;
    else if (n != CRUSH_ITEM_NONE)
      s.insert(n);
  }
  return dups;
}

class CRUSHTest : public ::testing::Test
{
public:
  void SetUp() final
  {
    CephInitParameters params(CEPH_ENTITY_TYPE_CLIENT);
    cct = common_preinit(params, CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  }
  void TearDown() final
  {
    cct->put();
    cct = nullptr;
  }
protected:
  CephContext *cct = nullptr;
};

TEST_F(CRUSHTest, indep_toosmall) {
  std::unique_ptr<CrushWrapper> c(build_indep_map(cct, 1, 3, 1));
  vector<__u32> weight(c->get_max_devices(), 0x10000);
  c->dump_tree(&cout, NULL);

  for (int x = 0; x < 100; ++x) {
    vector<int> out;
    c->do_rule(0, x, out, 5, weight, 0);
    cout << x << " -> " << out << std::endl;
    int num_none = 0;
    for (unsigned i=0; i<out.size(); ++i) {
      if (out[i] == CRUSH_ITEM_NONE)
	num_none++;
    }
    ASSERT_EQ(2, num_none);
    ASSERT_EQ(0, get_num_dups(out));
  }
}

TEST_F(CRUSHTest, indep_basic) {
  std::unique_ptr<CrushWrapper> c(build_indep_map(cct, 3, 3, 3));
  vector<__u32> weight(c->get_max_devices(), 0x10000);
  c->dump_tree(&cout, NULL);

  for (int x = 0; x < 100; ++x) {
    vector<int> out;
    c->do_rule(0, x, out, 5, weight, 0);
    cout << x << " -> " << out << std::endl;
    int num_none = 0;
    for (unsigned i=0; i<out.size(); ++i) {
      if (out[i] == CRUSH_ITEM_NONE)
	num_none++;
    }
    ASSERT_EQ(0, num_none);
    ASSERT_EQ(0, get_num_dups(out));
  }
}

TEST_F(CRUSHTest, indep_out_alt) {
  std::unique_ptr<CrushWrapper> c(build_indep_map(cct, 3, 3, 3));
  vector<__u32> weight(c->get_max_devices(), 0x10000);

  // mark a bunch of osds out
  int num = 3*3*3;
  for (int i=0; i<num / 2; ++i)
    weight[i*2] = 0;
  c->dump_tree(&cout, NULL);

  // need more retries to get 9/9 hosts for x in 0..99
  c->set_choose_total_tries(100);
  for (int x = 0; x < 100; ++x) {
    vector<int> out;
    c->do_rule(0, x, out, 9, weight, 0);
    cout << x << " -> " << out << std::endl;
    int num_none = 0;
    for (unsigned i=0; i<out.size(); ++i) {
      if (out[i] == CRUSH_ITEM_NONE)
	num_none++;
    }
    ASSERT_EQ(0, num_none);
    ASSERT_EQ(0, get_num_dups(out));
  }
}

TEST_F(CRUSHTest, indep_out_contig) {
  std::unique_ptr<CrushWrapper> c(build_indep_map(cct, 3, 3, 3));
  vector<__u32> weight(c->get_max_devices(), 0x10000);

  // mark a bunch of osds out
  int num = 3*3*3;
  for (int i=0; i<num / 3; ++i)
    weight[i] = 0;
  c->dump_tree(&cout, NULL);

  c->set_choose_total_tries(100);
  for (int x = 0; x < 100; ++x) {
    vector<int> out;
    c->do_rule(0, x, out, 7, weight, 0);
    cout << x << " -> " << out << std::endl;
    int num_none = 0;
    for (unsigned i=0; i<out.size(); ++i) {
      if (out[i] == CRUSH_ITEM_NONE)
	num_none++;
    }
    ASSERT_EQ(1, num_none);
    ASSERT_EQ(0, get_num_dups(out));
  }
}


TEST_F(CRUSHTest, indep_out_progressive) {
  std::unique_ptr<CrushWrapper> c(build_indep_map(cct, 3, 3, 3));
  c->set_choose_total_tries(100);
  vector<__u32> tweight(c->get_max_devices(), 0x10000);
  c->dump_tree(&cout, NULL);

  int tchanged = 0;
  for (int x = 1; x < 5; ++x) {
    vector<__u32> weight(c->get_max_devices(), 0x10000);

    std::map<int,unsigned> pos;
    vector<int> prev;
    for (unsigned i=0; i<weight.size(); ++i) {
      vector<int> out;
      c->do_rule(0, x, out, 7, weight, 0);
      cout << "(" << i << "/" << weight.size() << " out) "
	   << x << " -> " << out << std::endl;
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

}

TEST_F(CRUSHTest, straw_zero) {
  // zero weight items should have no effect on placement.

  std::unique_ptr<CrushWrapper> c(new CrushWrapper);
  const int ROOT_TYPE = 1;
  c->set_type_name(ROOT_TYPE, "root");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  int n = 5;
  int items[n], weights[n];
  for (int i=0; i <n; ++i) {
    items[i] = i;
    weights[i] = 0x10000 * (n-i-1);
  }

  c->set_max_devices(n);

  string root_name0("root0");
  int root0;
  EXPECT_EQ(0, c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
			     ROOT_TYPE, n, items, weights, &root0));
  EXPECT_EQ(0, c->set_item_name(root0, root_name0));

  string name0("rule0");
  int rule0 = c->add_simple_rule(name0, root_name0, "osd", "",
				       "firstn", pg_pool_t::TYPE_REPLICATED);
  EXPECT_EQ(0, rule0);

  string root_name1("root1");
  int root1;
  EXPECT_EQ(0, c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
			     ROOT_TYPE, n-1, items, weights, &root1));
  EXPECT_EQ(0, c->set_item_name(root1, root_name1));

  string name1("rule1");
  int rule1 = c->add_simple_rule(name1, root_name1, "osd", "",
				       "firstn", pg_pool_t::TYPE_REPLICATED);
  EXPECT_EQ(1, rule1);

  c->finalize();

  vector<unsigned> reweight(n, 0x10000);
  for (int i=0; i<10000; ++i) {
    vector<int> out0, out1;
    c->do_rule(rule0, i, out0, 1, reweight, 0);
    ASSERT_EQ(1u, out0.size());
    c->do_rule(rule1, i, out1, 1, reweight, 0);
    ASSERT_EQ(1u, out1.size());
    ASSERT_EQ(out0[0], out1[0]);
    //cout << i << "\t" << out0 << "\t" << out1 << std::endl;
  }
}

TEST_F(CRUSHTest, straw_same) {
  // items with the same weight should map about the same as items
  // with very similar weights.
  //
  // give the 0 vector a paired stair pattern, with dup weights.  note
  // that the original straw flaw does not appear when there are 2 of
  // the initial weight, but it does when there is just 1.
  //
  // give the 1 vector a similar stair pattern, but make the same
  // steps weights slightly different (no dups).  this works.
  //
  // compare the result and verify that the resulting mapping is
  // almost identical.

  std::unique_ptr<CrushWrapper> c(new CrushWrapper);
  const int ROOT_TYPE = 1;
  c->set_type_name(ROOT_TYPE, "root");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  int n = 10;
  int items[n], weights[n];
  for (int i=0; i <n; ++i) {
    items[i] = i;
    weights[i] = 0x10000 * ((i+1)/2 + 1);
  }

  c->set_max_devices(n);

  string root_name0("root0");
  int root0;
  EXPECT_EQ(0, c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
			     ROOT_TYPE, n, items, weights, &root0));
  EXPECT_EQ(0, c->set_item_name(root0, root_name0));

  string name0("rule0");
  int rule0 = c->add_simple_rule(name0, root_name0, "osd", "",
				       "firstn", pg_pool_t::TYPE_REPLICATED);
  EXPECT_EQ(0, rule0);

  for (int i=0; i <n; ++i) {
    items[i] = i;
    weights[i] = 0x10000 * ((i+1)/2 + 1) + (i%2)*100;
  }

  string root_name1("root1");
  int root1;
  EXPECT_EQ(0, c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
			     ROOT_TYPE, n, items, weights, &root1));
  EXPECT_EQ(0, c->set_item_name(root1, root_name1));

  string name1("rule1");
  int rule1 = c->add_simple_rule(name1, root_name1, "osd", "",
				       "firstn", pg_pool_t::TYPE_REPLICATED);
  EXPECT_EQ(1, rule1);

  if (0) {
    crush_bucket_straw *sb0 = reinterpret_cast<crush_bucket_straw*>(c->get_crush_map()->buckets[-1-root0]);
    crush_bucket_straw *sb1 = reinterpret_cast<crush_bucket_straw*>(c->get_crush_map()->buckets[-1-root1]);

    for (int i=0; i<n; ++i) {
      cout << i
	   << "\t" << sb0->item_weights[i]
	   << "\t" << sb1->item_weights[i]
	   << "\t"
	   << "\t" << sb0->straws[i]
	   << "\t" << sb1->straws[i]
	   << std::endl;
    }
  }

  if (0) {
    JSONFormatter jf(true);
    jf.open_object_section("crush");
    c->dump(&jf);
    jf.close_section();
    jf.flush(cout);
  }

  c->finalize();

  vector<int> sum0(n, 0), sum1(n, 0);
  vector<unsigned> reweight(n, 0x10000);
  int different = 0;
  int max = 100000;
  for (int i=0; i<max; ++i) {
    vector<int> out0, out1;
    c->do_rule(rule0, i, out0, 1, reweight, 0);
    ASSERT_EQ(1u, out0.size());
    c->do_rule(rule1, i, out1, 1, reweight, 0);
    ASSERT_EQ(1u, out1.size());
    sum0[out0[0]]++;
    sum1[out1[0]]++;
    if (out0[0] != out1[0])
      different++;
  }
  for (int i=0; i<n; ++i) {
    cout << i
	 << "\t" << ((double)weights[i] / (double)weights[0])
	 << "\t" << sum0[i] << "\t" << ((double)sum0[i]/(double)sum0[0])
	 << "\t" << sum1[i] << "\t" << ((double)sum1[i]/(double)sum1[0])
	 << std::endl;
  }
  double ratio = ((double)different / (double)max);
  cout << different << " of " << max << " = "
       << ratio
       << " different" << std::endl;
  ASSERT_LT(ratio, .001);
}

double calc_straw2_stddev(int *weights, int n, bool verbose)
{
  std::unique_ptr<CrushWrapper> c(new CrushWrapper);
  const int ROOT_TYPE = 2;
  c->set_type_name(ROOT_TYPE, "root");
  const int HOST_TYPE = 1;
  c->set_type_name(HOST_TYPE, "host");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  int items[n];
  for (int i=0; i <n; ++i) {
    items[i] = i;
  }

  c->set_max_devices(n);

  string root_name0("root0");
  int root0;
  crush_bucket *b0 = crush_make_bucket(c->get_crush_map(),
				       CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
				       ROOT_TYPE, n, items, weights);
  crush_add_bucket(c->get_crush_map(), 0, b0, &root0);
  c->set_item_name(root0, root_name0);

  string name0("rule0");
  int rule0 = c->add_simple_rule(name0, root_name0, "osd", "",
				       "firstn", pg_pool_t::TYPE_REPLICATED);

  int sum[n];
  double totalweight = 0;
  vector<unsigned> reweight(n);
  for (int i=0; i<n; ++i) {
    sum[i] = 0;
    reweight[i] = 0x10000;
    totalweight += weights[i];
  }
  totalweight /= (double)0x10000;
  double avgweight = totalweight / n;

  c->finalize();

  int total = 1000000;
  for (int i=0; i<total; ++i) {
    vector<int> out;
    c->do_rule(rule0, i, out, 1, reweight, 0);
    sum[out[0]]++;
  }

  double expected = (double)total / (double)n;
  if (verbose)
    cout << "expect\t\t\t" << expected << std::endl;
  double stddev = 0;
  double exptotal = 0;
  if (verbose)
    cout << "osd\tweight\tcount\tadjusted\n";
  std::streamsize p = cout.precision();
  cout << std::setprecision(4);
  for (int i=0; i<n; ++i) {
    double w = (double)weights[i] / (double)0x10000;
    double adj = (double)sum[i] * avgweight / w;
    stddev += (adj - expected) * (adj - expected);
    exptotal += adj;
    if (verbose)
      cout << i
	   << "\t" << w
	   << "\t" << sum[i]
	   << "\t" << (int)adj
	   << std::endl;
  }
  cout << std::setprecision(p);
  {
    stddev = sqrt(stddev / (double)n);
    if (verbose)
      cout << "std dev " << stddev << std::endl;

    double p = 1.0 / (double)n;
    double estddev = sqrt(exptotal * p * (1.0 - p));
    if (verbose)
      cout << "     vs " << estddev << "\t(expected)" << std::endl;
  }
  return stddev;
}

TEST_F(CRUSHTest, straw2_stddev)
{
  int n = 15;
  int weights[n];
  cout << "maxskew\tstddev\n";
  for (double step = 1.0; step < 2; step += .25) {
    int w = 0x10000;
    for (int i = 0; i < n; ++i) {
      weights[i] = w;
      w *= step;
    }
    double stddev = calc_straw2_stddev(weights, n, true);
    cout << ((double)weights[n-1]/(double)weights[0])
	 << "\t" << stddev << std::endl;
  }
}

TEST_F(CRUSHTest, straw2_reweight) {
  // when we adjust the weight of an item in a straw2 bucket,
  // we should *only* see movement from or to that item, never
  // between other items.
  int weights[] = {
    0x10000,
    0x10000,
    0x20000,
    0x20000,
    0x30000,
    0x50000,
    0x8000,
    0x20000,
    0x10000,
    0x10000,
    0x20000,
    0x10000,
    0x10000,
    0x20000,
    0x300000,
    0x10000,
    0x20000
  };
  int n = 15;

  std::unique_ptr<CrushWrapper> c(new CrushWrapper);
  const int ROOT_TYPE = 2;
  c->set_type_name(ROOT_TYPE, "root");
  const int HOST_TYPE = 1;
  c->set_type_name(HOST_TYPE, "host");
  const int OSD_TYPE = 0;
  c->set_type_name(OSD_TYPE, "osd");

  int items[n];
  for (int i=0; i <n; ++i) {
    items[i] = i;
    //weights[i] = 0x10000;
  }

  c->set_max_devices(n);

  string root_name0("root0");
  int root0;
  crush_bucket *b0 = crush_make_bucket(c->get_crush_map(),
				       CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
				       ROOT_TYPE, n, items, weights);
  EXPECT_EQ(0, crush_add_bucket(c->get_crush_map(), 0, b0, &root0));
  EXPECT_EQ(0, c->set_item_name(root0, root_name0));

  string name0("rule0");
  int rule0 = c->add_simple_rule(name0, root_name0, "osd", "",
				       "firstn", pg_pool_t::TYPE_REPLICATED);
  EXPECT_EQ(0, rule0);

  int changed = 1;
  weights[changed] = weights[changed] / 10 * (rand() % 10);

  string root_name1("root1");
  int root1;
  crush_bucket *b1 = crush_make_bucket(c->get_crush_map(),
				       CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
				       ROOT_TYPE, n, items, weights);
  EXPECT_EQ(0, crush_add_bucket(c->get_crush_map(), 0, b1, &root1));
  EXPECT_EQ(0, c->set_item_name(root1, root_name1));

  string name1("rule1");
  int rule1 = c->add_simple_rule(name1, root_name1, "osd", "",
				       "firstn", pg_pool_t::TYPE_REPLICATED);
  EXPECT_EQ(1, rule1);

  int sum[n];
  double totalweight = 0;
  vector<unsigned> reweight(n);
  for (int i=0; i<n; ++i) {
    sum[i] = 0;
    reweight[i] = 0x10000;
    totalweight += weights[i];
  }
  totalweight /= (double)0x10000;
  double avgweight = totalweight / n;

  c->finalize();

  int total = 1000000;
  for (int i=0; i<total; ++i) {
    vector<int> out0, out1;
    c->do_rule(rule0, i, out0, 1, reweight, 0);
    ASSERT_EQ(1u, out0.size());

    c->do_rule(rule1, i, out1, 1, reweight, 0);
    ASSERT_EQ(1u, out1.size());

    sum[out1[0]]++;
    //sum[rand()%n]++;

    if (out1[0] == changed) {
      ASSERT_EQ(changed, out0[0]);
    } else if (out0[0] != changed) {
      ASSERT_EQ(out0[0], out1[0]);
    }
  }

  double expected = (double)total / (double)n;
  cout << "expect\t\t\t" << expected << std::endl;
  double stddev = 0;
  cout << "osd\tweight\tcount\tadjusted\n";
  std::streamsize p = cout.precision();
  cout << std::setprecision(4);
  for (int i=0; i<n; ++i) {
    double w = (double)weights[i] / (double)0x10000;
    double adj = (double)sum[i] * avgweight / w;
    stddev += (adj - expected) * (adj - expected);
    cout << i
	 << "\t" << w
	 << "\t" << sum[i]
	 << "\t" << (int)adj
	 << std::endl;
  }
  cout << std::setprecision(p);
  {
    stddev = sqrt(stddev / (double)n);
    cout << "std dev " << stddev << std::endl;

    double p = 1.0 / (double)n;
    double estddev = sqrt((double)total * p * (1.0 - p));
    cout << "     vs " << estddev << std::endl;
  }
}
