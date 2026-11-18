// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

// Ceph - scalable distributed file system
//
// This is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License version 2.1, as published by the Free Software
// Foundation.  See file COPYING.

// Tests that a snaprealm split is divided into MClientSnap messages whose
// fronts stay within the kernel's CEPH_MSG_MAX_FRONT_LEN.
// https://tracker.ceph.com/issues/76530

#include <vector>

#include "gtest/gtest.h"

#include "messages/MClientSnap.h"

using chunk_t = MClientSnap::chunk_t;
using plan_t = MClientSnap::split_plan_t;

static void expect_covers(size_t n_inos, size_t n_realms,
                          const std::vector<chunk_t>& chunks) {
  size_t ino_off = 0;
  size_t realm_off = 0;
  for (const auto& chunk : chunks) {
    EXPECT_EQ(realm_off, chunk.realm_off);
    EXPECT_EQ(ino_off, chunk.ino_off);
    EXPECT_TRUE(chunk.ino_count || chunk.realm_count);
    realm_off += chunk.realm_count;
    ino_off += chunk.ino_count;
  }
  EXPECT_EQ(n_realms, realm_off);
  EXPECT_EQ(n_inos, ino_off);
}

static void expect_chunks_fit(size_t trace_len, size_t limit,
                              const std::vector<chunk_t>& chunks) {
  for (const auto& chunk : chunks) {
    const size_t bytes = MClientSnap::front_len(chunk.ino_count,
                                                chunk.realm_count,
                                                trace_len);
    EXPECT_LE(bytes, limit);
    EXPECT_TRUE(MClientSnap::front_fits(chunk.ino_count, chunk.realm_count,
                                        trace_len, limit));
  }
}

TEST(MClientSnap, small_split_is_one_message) {
  std::vector<chunk_t> chunks;
  const auto plan = MClientSnap::plan_split(4, 1, 64, chunks);
  EXPECT_EQ(plan_t::SINGLE, plan);
  EXPECT_TRUE(chunks.empty());
  EXPECT_TRUE(MClientSnap::front_fits(4, 1, 64));
}

TEST(MClientSnap, reported_quota_split_is_chunked) {
  // Front of 36345824 is a 24-byte head plus 4543225 raw inode numbers.
  constexpr size_t n_inos = 4543225;
  constexpr size_t trace_len = 0;
  EXPECT_EQ(36345824u,
            MClientSnap::front_len(n_inos, 0, trace_len));
  EXPECT_GT(MClientSnap::front_len(n_inos, 0, trace_len),
            MClientSnap::MAX_FRONT_LEN);

  std::vector<chunk_t> chunks;
  const auto plan = MClientSnap::plan_split(n_inos, 0, trace_len, chunks);
  EXPECT_EQ(plan_t::CHUNKED, plan);
  EXPECT_GT(chunks.size(), 1u);
  expect_covers(n_inos, 0, chunks);
  expect_chunks_fit(trace_len, MClientSnap::MAX_FRONT_LEN, chunks);

  // A short snap trace, as a real notify carries one, still chunks.
  constexpr size_t traced = 128;
  std::vector<chunk_t> traced_chunks;
  EXPECT_EQ(plan_t::CHUNKED,
            MClientSnap::plan_split(n_inos, 2, traced, traced_chunks));
  expect_covers(n_inos, 2, traced_chunks);
  expect_chunks_fit(traced, MClientSnap::MAX_FRONT_LEN, traced_chunks);
}

TEST(MClientSnap, realms_and_inos_share_chunks) {
  // header 24 + trace 8 leaves room for exactly 3 eight-byte entries.
  constexpr size_t trace_len = 8;
  constexpr size_t limit = sizeof(ceph_mds_snap_head) + trace_len + 3 * 8;
  std::vector<chunk_t> chunks;
  const auto plan = MClientSnap::plan_split(5, 5, trace_len, chunks, limit);
  EXPECT_EQ(plan_t::CHUNKED, plan);
  expect_covers(5, 5, chunks);
  expect_chunks_fit(trace_len, limit, chunks);
  ASSERT_GE(chunks.size(), 2u);
  // Realms are emitted before inodes.
  EXPECT_GT(chunks.front().realm_count, 0u);
  EXPECT_GT(chunks.back().ino_count, 0u);
  EXPECT_EQ(0u, chunks.back().realm_count);
}

TEST(MClientSnap, trace_alone_cannot_be_split) {
  std::vector<chunk_t> chunks;
  const size_t trace_len = MClientSnap::MAX_FRONT_LEN;
  const auto plan = MClientSnap::plan_split(10, 1, trace_len, chunks);
  EXPECT_EQ(plan_t::UNSPLITTABLE, plan);
  EXPECT_TRUE(chunks.empty());
}

TEST(MClientSnap, encoded_front_matches_planner) {
  constexpr size_t n_inos = 1000;
  constexpr size_t n_realms = 3;
  const std::string trace(32, 't');

  auto m = ceph::make_message<MClientSnap>(CEPH_SNAP_OP_SPLIT);
  m->head.split = 42;
  m->bl.append(trace);
  for (size_t i = 0; i < n_inos; ++i) {
    m->split_inos.push_back(inodeno_t(i + 1));
  }
  for (size_t i = 0; i < n_realms; ++i) {
    m->split_realms.push_back(inodeno_t(100000 + i));
  }

  const size_t expect = MClientSnap::front_len(n_inos, n_realms, trace.size());
  m->encode(0, 0);
  EXPECT_EQ(expect, m->get_payload().length());
  EXPECT_LE(m->get_payload().length(), MClientSnap::MAX_FRONT_LEN);

  // A full chunk sits within the limit, and one more entry would not.
  const size_t room = MClientSnap::max_split_entries(trace.size());
  EXPECT_TRUE(MClientSnap::front_fits(room, 0, trace.size()));
  EXPECT_FALSE(MClientSnap::front_fits(room + 1, 0, trace.size()));
  const size_t slack = MClientSnap::MAX_FRONT_LEN -
                       MClientSnap::front_len(room, 0, trace.size());
  EXPECT_LT(slack, sizeof(uint64_t));

  const auto inos = m->split_inos;
  const auto realms = m->split_realms;
  const auto trace_len = m->bl.length();
  m->split_inos.clear();
  m->split_realms.clear();
  m->bl.clear();
  m->decode_payload();
  ASSERT_EQ(inos.size(), m->split_inos.size());
  ASSERT_EQ(realms.size(), m->split_realms.size());
  for (size_t i = 0; i < inos.size(); ++i) {
    EXPECT_EQ(uint64_t(inos[i]), uint64_t(m->split_inos[i]));
  }
  for (size_t i = 0; i < realms.size(); ++i) {
    EXPECT_EQ(uint64_t(realms[i]), uint64_t(m->split_realms[i]));
  }
  EXPECT_EQ(trace_len, m->bl.length());
  EXPECT_EQ(uint64_t(42), uint64_t(m->head.split));
  EXPECT_EQ(CEPH_SNAP_OP_SPLIT, int(uint32_t(m->head.op)));
}
