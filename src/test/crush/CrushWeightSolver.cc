// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Clyso Technologies Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <cmath>
#include <numeric>
#include <sstream>
#include <gtest/gtest.h>

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "include/stringify.h"
#include "osd/osd_types.h"

#include "crush/CrushWeightSolver.h"
#include "crush/CrushWrapper.h"

using namespace ceph::crush;
using weights_t = std::vector<double>;

namespace {

weights_t repeat(std::initializer_list<std::pair<double, int>> groups)
{
  weights_t w;
  for (auto& [weight, count] : groups) {
    w.insert(w.end(), count, weight);
  }
  return w;
}

double sum(const weights_t& w)
{
  return std::accumulate(w.begin(), w.end(), 0.0);
}

struct layout_t {
  const char *name;
  weights_t w;
  unsigned r;
};

const std::vector<layout_t> small_layouts = {
  {"3,3,3,1 x3", {3, 3, 3, 1}, 3},
  {"3,3,3,1 x2", {3, 3, 3, 1}, 2},
  {"12 hosts x3", repeat({{1, 9}, {0.25, 3}}), 3},
  {"6 racks x3", repeat({{2, 1}, {1, 5}}), 3},
  {"10 hosts 4+2", repeat({{1, 6}, {0.5, 4}}), 6},
  {"with a zero", {4, 0, 2, 2, 1}, 2},
};

} // anonymous namespace

TEST(CrushWeightSolver, inclusion_matches_enumeration)
{
  for (auto& l : small_layouts) {
    SCOPED_TRACE(l.name);
    auto positions = position_probabilities(
      std::vector<weights_t>(l.r, l.w));
    auto inclusion = inclusion_probabilities(l.w, l.r);
    for (unsigned i = 0; i < l.w.size(); i++) {
      double expected = 0;
      for (auto& p : positions) {
        expected += p[i];
      }
      EXPECT_NEAR(expected, inclusion[i], 1e-10);
    }
  }
}

TEST(CrushWeightSolver, plain_weights_favour_small_items)
{
  weights_t w = {3, 3, 3, 1};
  auto inclusion = inclusion_probabilities(w, 3);
  // the anomaly: 1.40 times its share
  EXPECT_NEAR(1.4048, inclusion[3] / (3 * w[3] / sum(w)), 1e-3);
  EXPECT_NEAR(0.9550, inclusion[0] / (3 * w[0] / sum(w)), 1e-3);
}

TEST(CrushWeightSolver, flat)
{
  auto layouts = small_layouts;
  layouts.push_back({"40 hosts 8+3", repeat({{1, 20}, {0.7, 12}, {0.4, 8}}), 11});
  layouts.push_back({"300 hosts x3", repeat({{1, 200}, {0.5, 60}, {0.1, 40}}), 3});
  for (auto& l : layouts) {
    SCOPED_TRACE(l.name);
    auto flat = solve_flat_weights(l.w, l.r);
    ASSERT_EQ(l.w.size(), flat.size());
    EXPECT_NEAR(sum(l.w), sum(flat), 1e-9);
    auto inclusion = inclusion_probabilities(flat, l.r);
    for (unsigned i = 0; i < l.w.size(); i++) {
      EXPECT_NEAR(l.r * l.w[i] / sum(l.w), inclusion[i], 1e-8);
      if (l.w[i] == 0) {
        EXPECT_EQ(0, flat[i]);
      }
    }
  }
}

TEST(CrushWeightSolver, positional)
{
  for (auto& l : small_layouts) {
    if (l.r > 3) {
      continue;
    }
    SCOPED_TRACE(l.name);
    auto positional = solve_positional_weights(l.w, l.r);
    ASSERT_EQ(l.r, positional.size());
    // the first position is the bucket's own weights
    for (unsigned i = 0; i < l.w.size(); i++) {
      EXPECT_NEAR(l.w[i], positional[0][i], 1e-12);
    }
    auto probabilities = position_probabilities(positional);
    for (unsigned k = 0; k < l.r; k++) {
      EXPECT_NEAR(sum(l.w), sum(positional[k]), 1e-9);
      for (unsigned i = 0; i < l.w.size(); i++) {
        EXPECT_NEAR(l.w[i] / sum(l.w), probabilities[k][i], 1e-8);
      }
    }
  }
}

TEST(CrushWeightSolver, uniform_weights_are_left_alone)
{
  weights_t w(8, 1.5);
  auto flat = solve_flat_weights(w, 3);
  ASSERT_EQ(w.size(), flat.size());
  for (auto x : flat) {
    EXPECT_NEAR(1.5, x, 1e-9);
  }
  auto positional = solve_positional_weights(w, 3);
  ASSERT_EQ(3u, positional.size());
  for (auto& p : positional) {
    for (auto x : p) {
      EXPECT_NEAR(1.5, x, 1e-9);
    }
  }
}

TEST(CrushWeightSolver, infeasible)
{
  // an item with more than 1/r of the weight
  EXPECT_TRUE(solve_flat_weights({3, 3, 2}, 3).empty());
  EXPECT_TRUE(solve_positional_weights({3, 3, 2}, 3).empty());
  // fewer items than picks
  EXPECT_TRUE(solve_flat_weights({1, 1, 0}, 3).empty());
  EXPECT_TRUE(solve_positional_weights({1, 1}, 3).empty());
  EXPECT_TRUE(solve_flat_weights({}, 3).empty());
  EXPECT_TRUE(solve_flat_weights({0, 0, 0}, 2).empty());
  EXPECT_TRUE(solve_flat_weights({1, 1, 1}, 0).empty());
  // one pick: nothing to correct
  EXPECT_EQ(weights_t({3, 1}), solve_flat_weights({3, 1}, 1));
  // too expensive to enumerate
  EXPECT_TRUE(solve_positional_weights(weights_t(400, 1.0), 6).empty());
  // too many distinct weights
  weights_t all_differ;
  for (int i = 0; i < 2000; i++) {
    all_differ.push_back(1 + i / 2000.0);
  }
  EXPECT_TRUE(solve_flat_weights(all_differ, 3).empty());
  // but not with a larger budget
  all_differ.resize(150);
  EXPECT_TRUE(solve_flat_weights(all_differ, 3, 1e4).empty());
  EXPECT_FALSE(solve_flat_weights(all_differ, 3).empty());
}

/*
 * The real thing: a CRUSH map, a weight-set set through
 * choose_args_adjust_item_weightf() as the monitor does, and crush_do_rule().
 */
class CrushWeightSolverMap : public ::testing::Test
{
public:
  void SetUp() final {
    CephInitParameters params(CEPH_ENTITY_TYPE_CLIENT);
    cct = common_preinit(params, CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
    cct->_conf.set_val("debug_crush", "0");
  }
  void TearDown() final {
    cct->put();
    cct = nullptr;
  }

protected:
  CephContext *cct = nullptr;
  static constexpr int inputs = 200000;

  // one OSD per host, hosts in racks of rack_size
  void build(CrushWrapper& c, const weights_t& w, int rack_size,
             const std::string& mode, int rule_type) {
    c.create();
    c.set_tunables_optimal();
    c.set_type_name(0, "osd");
    c.set_type_name(1, "host");
    c.set_type_name(2, "rack");
    c.set_type_name(3, "root");
    int rootno;
    c.add_bucket(0, CRUSH_BUCKET_STRAW2, CRUSH_HASH_RJENKINS1,
		 3, 0, nullptr, nullptr, &rootno);
    c.set_item_name(rootno, "default");
    for (unsigned i = 0; i < w.size(); i++) {
      std::map<std::string, std::string> loc = {
        {"host", "host" + stringify(i)},
        {"rack", "rack" + stringify(i / rack_size)},
        {"root", "default"}};
      ASSERT_EQ(0, c.insert_item(cct, i, w[i], "osd." + stringify(i), loc));
    }
    ASSERT_EQ(0, c.add_simple_rule("rule", "default", "host", "", mode,
                                   rule_type));
    c.finalize();
  }

  // weight-set 0: weights[position][osd]
  void set_weight_set(CrushWrapper& c, const std::vector<weights_t>& weights) {
    ASSERT_TRUE(c.create_choose_args(0, weights.size()));
    auto cmap = c.choose_args_get(0);
    for (unsigned i = 0; i < weights[0].size(); i++) {
      weights_t per_position;
      for (auto& p : weights) {
        per_position.push_back(p[i]);
      }
      std::stringstream ss;
      // returns the number of buckets it changed
      ASSERT_LT(0, c.choose_args_adjust_item_weightf(cct, cmap, i,
                                                     per_position, &ss))
        << ss.str();
    }
  }

  // share of the data and of the first position, each against w_i / W
  void map_all(const CrushWrapper& c, const weights_t& w, unsigned r,
               uint64_t choose_args, weights_t *data, weights_t *first) {
    std::vector<__u32> reweight(w.size(), 0x10000);
    std::vector<int> all(w.size(), 0), primary(w.size(), 0);
    int placed = 0;
    for (int x = 0; x < inputs; x++) {
      std::vector<int> out;
      c.do_rule(0, x, out, r, reweight, choose_args);
      ASSERT_EQ(r, out.size());
      for (auto o : out) {
        ASSERT_NE(CRUSH_ITEM_NONE, o);
        all[o]++;
        placed++;
      }
      primary[out[0]]++;
    }
    data->clear();
    first->clear();
    for (unsigned i = 0; i < w.size(); i++) {
      const double share = w[i] / sum(w);
      data->push_back(all[i] / (placed * share));
      first->push_back(primary[i] / (inputs * share));
    }
  }

  // mean over the items of one weight
  static double mean(const weights_t& w, const weights_t& v, double weight) {
    double total = 0;
    int n = 0;
    for (unsigned i = 0; i < w.size(); i++) {
      if (w[i] == weight) {
        total += v[i];
        n++;
      }
    }
    return total / n;
  }
};

TEST_F(CrushWeightSolverMap, replicated_positional)
{
  const weights_t w = repeat({{1, 10}, {0.25, 6}});
  const unsigned r = 3;
  CrushWrapper c;
  build(c, w, 4, "firstn", pg_pool_t::TYPE_REPLICATED);
  weights_t data, first;

  map_all(c, w, r, 1, &data, &first);   // no weight-set 1: plain weights
  EXPECT_LT(1.04, mean(w, data, 0.25));
  EXPECT_GT(0.995, mean(w, data, 1));

  auto solved = solve_positional_weights(w, r);
  ASSERT_EQ(r, solved.size());
  set_weight_set(c, solved);
  map_all(c, w, r, 0, &data, &first);
  EXPECT_NEAR(1, mean(w, data, 0.25), 0.01);
  EXPECT_NEAR(1, mean(w, data, 1), 0.005);
  EXPECT_NEAR(1, mean(w, first, 0.25), 0.02);
  EXPECT_NEAR(1, mean(w, first, 1), 0.01);
}

TEST_F(CrushWeightSolverMap, replicated_flat)
{
  const weights_t w = repeat({{1, 10}, {0.25, 6}});
  const unsigned r = 3;
  CrushWrapper c;
  build(c, w, 4, "firstn", pg_pool_t::TYPE_REPLICATED);
  auto solved = solve_flat_weights(w, r);
  ASSERT_EQ(w.size(), solved.size());
  set_weight_set(c, {solved});
  weights_t data, first;
  map_all(c, w, r, 0, &data, &first);
  EXPECT_NEAR(1, mean(w, data, 0.25), 0.01);
  EXPECT_NEAR(1, mean(w, data, 1), 0.005);
  // the price of one weight for all positions
  EXPECT_GT(0.97, mean(w, first, 0.25));
}

TEST_F(CrushWeightSolverMap, erasure_flat)
{
  const weights_t w = repeat({{1, 8}, {0.5, 6}});
  const unsigned r = 6;
  CrushWrapper c;
  build(c, w, 4, "indep", pg_pool_t::TYPE_ERASURE);
  weights_t data, first;

  map_all(c, w, r, 1, &data, &first);
  EXPECT_LT(1.05, mean(w, data, 0.5));

  auto solved = solve_flat_weights(w, r);
  ASSERT_EQ(w.size(), solved.size());
  set_weight_set(c, {solved});
  map_all(c, w, r, 0, &data, &first);
  EXPECT_NEAR(1, mean(w, data, 0.5), 0.01);
  EXPECT_NEAR(1, mean(w, data, 1), 0.005);
}
