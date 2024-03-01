// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include "osd/PGTransaction.h"
#include "osd/ECTransaction.h"

#include "test/unit.cc"

struct mydpp : public DoutPrefixProvider {
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "foo"; }
  CephContext *get_cct() const override { return g_ceph_context; }
  unsigned get_subsys() const override { return ceph_subsys_osd; }
} dpp;

#define dout_context g_ceph_context

TEST(ectransaction, two_writes_separated)
{
  hobject_t h;
  PGTransactionUPtr t(new PGTransaction);
  bufferlist a, b;
  t->create(h);
  a.append_zero(565760);
  t->write(h, 0, a.length(), a, 0);
  b.append_zero(2437120);
  t->write(h, 669856, b.length(), b, 0);

  ECUtil::stripe_info_t sinfo(2, 8192);
  auto plan = ECTransaction::get_write_plan(
    sinfo,
    *t,
    [&](const hobject_t &i) {
      ECUtil::HashInfoRef ref(new ECUtil::HashInfo(1));
      return ref;
    },
    &dpp);
  generic_derr << "to_read " << plan.to_read << dendl;
  generic_derr << "will_write " << plan.will_write << dendl;

  ASSERT_EQ(0u, plan.to_read.size());
  ASSERT_EQ(1u, plan.will_write.size());
}

TEST(ectransaction, two_writes_nearby)
{
  hobject_t h;
  PGTransactionUPtr t(new PGTransaction);
  bufferlist a, b;
  t->create(h);

  // two nearby writes, both partly touching the same 8192-byte stripe
  ECUtil::stripe_info_t sinfo(2, 8192);
  a.append_zero(565760);
  t->write(h, 0, a.length(), a, 0);
  b.append_zero(2437120);
  t->write(h, 569856, b.length(), b, 0);

  auto plan = ECTransaction::get_write_plan(
    sinfo,
    *t,
    [&](const hobject_t &i) {
      ECUtil::HashInfoRef ref(new ECUtil::HashInfo(1));
      return ref;
    },
    &dpp);
  generic_derr << "to_read " << plan.to_read << dendl;
  generic_derr << "will_write " << plan.will_write << dendl;

  ASSERT_EQ(0u, plan.to_read.size());
  ASSERT_EQ(1u, plan.will_write.size());
}

TEST(ectransaction, many_writes)
{
  hobject_t h;
  PGTransactionUPtr t(new PGTransaction);
  bufferlist a, b;
  a.append_zero(512);
  b.append_zero(4096);
  t->create(h);

  ECUtil::stripe_info_t sinfo(2, 8192);
  // write 2801664~512
  // write 2802176~512
  // write 2802688~512
  // write 2803200~512
  t->write(h, 2801664, a.length(), a, 0);
  t->write(h, 2802176, a.length(), a, 0);
  t->write(h, 2802688, a.length(), a, 0);
  t->write(h, 2803200, a.length(), a, 0);

  // write 2805760~4096
  // write 2809856~4096
  // write 2813952~4096
  t->write(h, 2805760, b.length(), b, 0);
  t->write(h, 2809856, b.length(), b, 0);
  t->write(h, 2813952, b.length(), b, 0);

  auto plan = ECTransaction::get_write_plan(
    sinfo,
    *t,
    [&](const hobject_t &i) {
      ECUtil::HashInfoRef ref(new ECUtil::HashInfo(1));
      return ref;
    },
    &dpp);
  generic_derr << "to_read " << plan.to_read << dendl;
  generic_derr << "will_write " << plan.will_write << dendl;

  ASSERT_EQ(0u, plan.to_read.size());
  ASSERT_EQ(1u, plan.will_write.size());
}
