// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

// PeerFeatureGate: which clients the mons turn away at authentication,
// the second strikes of older msgr2 clients, and the cluster log line.

#include "gtest/gtest.h"

#include "include/ceph_features.h"
#include "mon/PeerFeatureGate.h"

using kind_t = PeerFeatureGate::peer_t::kind_t;

static constexpr uint64_t F = CEPH_FEATUREMASK_SERVER_SQUID;

static PeerFeatureGate::peer_t peer(kind_t kind, uint64_t features,
                                    uint64_t global_id = 0)
{
  PeerFeatureGate::peer_t p;
  p.kind = kind;
  p.features = features;
  p.global_id = global_id;
  return p;
}

TEST(PeerFeatureGate, Msgr1)
{
  PeerFeatureGate g;
  auto now = ceph::mono_clock::now();
  ASSERT_FALSE(g.check(F, peer(kind_t::MSGR1, F), now));
  auto r = g.check(F, peer(kind_t::MSGR1, 0), now);
  ASSERT_TRUE(r);
  ASSERT_EQ(F, r->missing);
  ASSERT_NE(std::string::npos, r->reason.find("requires features 0x"));
}

TEST(PeerFeatureGate, Msgr2)
{
  PeerFeatureGate g;
  auto now = ceph::mono_clock::now();
  // its features are not known yet: only a strike refuses it
  ASSERT_FALSE(g.check(F, peer(kind_t::UNKNOWN, 0, 7), now));
}

TEST(PeerFeatureGate, SecondStrike)
{
  PeerFeatureGate g;
  auto now = ceph::mono_clock::now();
  auto old = peer(kind_t::UNKNOWN, 0, 7);
  g.strike(7, F, now);
  auto r = g.check(F, old, now);
  ASSERT_TRUE(r);
  ASSERT_EQ(F, r->missing);
  // a forged global_id gets only its own connection refused; the entry stays
  ASSERT_TRUE(g.check(F, old, now + std::chrono::seconds(1)));
  // a msgr1 client says what it has, and is judged by that
  ASSERT_FALSE(g.check(F, peer(kind_t::MSGR1, F, 7), now));
  // only what is still required counts
  ASSERT_FALSE(g.check(0, old, now));
  // the strike expires
  ASSERT_FALSE(g.check(F, old, now + PeerFeatureGate::strike_window));
  // nothing to remember for global_id 0 or nothing missing
  g.strike(0, F, now);
  g.strike(8, 0, now);
  ASSERT_FALSE(g.check(F, peer(kind_t::UNKNOWN, 0, 0), now));
  ASSERT_FALSE(g.check(F, peer(kind_t::UNKNOWN, 0, 8), now));
}

TEST(PeerFeatureGate, StrikeAgainExtends)
{
  PeerFeatureGate g;
  auto now = ceph::mono_clock::now();
  auto old = peer(kind_t::UNKNOWN, 0, 7);
  g.strike(7, F, now);
  g.strike(7, F, now + std::chrono::seconds(30));
  // expiring the first entry must not drop the second
  g.strike(9, F, now + PeerFeatureGate::strike_window + std::chrono::seconds(1));
  ASSERT_TRUE(g.check(F, old,
                      now + PeerFeatureGate::strike_window + std::chrono::seconds(1)));
}

TEST(PeerFeatureGate, StrikesAreBounded)
{
  PeerFeatureGate g;
  auto now = ceph::mono_clock::now();
  for (uint64_t gid = 1; gid <= PeerFeatureGate::max_strikes + 1; ++gid) {
    g.strike(gid, F, now);
  }
  // the oldest went to make room
  ASSERT_FALSE(g.check(F, peer(kind_t::UNKNOWN, 0, 1), now));
  ASSERT_TRUE(g.check(F, peer(kind_t::UNKNOWN, 0, 2), now));
  ASSERT_TRUE(g.check(F, peer(kind_t::UNKNOWN, 0, PeerFeatureGate::max_strikes + 1),
                      now));
}

TEST(PeerFeatureGate, LogLine)
{
  PeerFeatureGate g;
  auto now = ceph::mono_clock::now();
  ASSERT_FALSE(g.take_log_line(now));
  g.count_refusal("v2:1.2.3.4:5/6", "reason one");
  auto line = g.take_log_line(now);
  ASSERT_TRUE(line);
  ASSERT_NE(std::string::npos, line->find("turned away 1 connection attempt(s)"));
  ASSERT_NE(std::string::npos, line->find("v2:1.2.3.4:5/6"));
  // at most one line an interval; the count carries over
  g.count_refusal("a", "r");
  g.count_refusal("b", "reason two");
  ASSERT_FALSE(g.take_log_line(now + std::chrono::seconds(1)));
  line = g.take_log_line(now + PeerFeatureGate::log_interval);
  ASSERT_TRUE(line);
  ASSERT_NE(std::string::npos, line->find("turned away 2 connection attempt(s)"));
  ASSERT_NE(std::string::npos, line->find("(latest at b): reason two"));
  ASSERT_FALSE(g.take_log_line(now + 3 * PeerFeatureGate::log_interval));
}
