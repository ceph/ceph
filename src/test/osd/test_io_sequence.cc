// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include "common/io_exerciser/IoOp.h"
#include "common/io_exerciser/IoSequence.h"

using ceph::io_exerciser::IoSequence;
using ceph::io_exerciser::OpType;
using ceph::io_exerciser::Sequence;

// A sequence asked not to check consistency must never issue a Consistency
// op: RadosIo has no consistency checker for a replicated pool.
TEST(IoSequence, NoConsistencyOpsUnlessRequested) {
  for (int s = static_cast<int>(Sequence::SEQUENCE_BEGIN);
       s < static_cast<int>(Sequence::SEQUENCE_END); s++) {
    const Sequence seq = static_cast<Sequence>(s);
    if (seq == Sequence::SEQUENCE_SEQ10) {
      continue;  // erasure coded pools only, see EcIoSequence
    }
    for (int seed = 1; seed <= 3; seed++) {
      auto sequence = IoSequence::generate_sequence(seq, {1, 32}, seed, false);
      ASSERT_TRUE(sequence) << seq;
      int steps = 0;
      for (auto op = sequence->next(); op->getOpType() != OpType::Done;
           op = sequence->next()) {
        EXPECT_NE(op->getOpType(), OpType::Consistency)
            << seq << " seed " << seed << " step " << steps;
        ASSERT_LT(++steps, 10000000) << seq << " did not finish";
      }
    }
  }
}

TEST(IoSequence, Seq16ChecksConsistencyWhenRequested) {
  auto sequence = IoSequence::generate_sequence(Sequence::SEQUENCE_SEQ16,
                                                {1, 32}, 1, true);
  int consistency_ops = 0;
  for (auto op = sequence->next(); op->getOpType() != OpType::Done;
       op = sequence->next()) {
    if (op->getOpType() == OpType::Consistency) {
      consistency_ops++;
    }
  }
  EXPECT_GT(consistency_ops, 0);
}
