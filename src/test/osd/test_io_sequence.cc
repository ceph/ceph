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

// ObjectModel only stops a read overlapping an in-flight Write, so a
// sequence must issue a barrier between an op that rewrites a whole object
// (copy, truncate, create, remove) and the next read of that object.
// Without it a balanced read can reach a replica before that op does and
// return the old object.
TEST(IoSequence, NoReadWhileWholeObjectWriteInFlight) {
  for (int s = static_cast<int>(Sequence::SEQUENCE_BEGIN);
       s < static_cast<int>(Sequence::SEQUENCE_END); s++) {
    const Sequence seq = static_cast<Sequence>(s);
    if (seq == Sequence::SEQUENCE_SEQ10) {
      continue;
    }
    for (int seed = 1; seed <= 3; seed++) {
      auto sequence = IoSequence::generate_sequence(seq, {1, 32}, seed, false);
      int primary = 0;
      bool in_flight[2] = {false, false};
      int steps = 0;
      for (auto op = sequence->next(); op->getOpType() != OpType::Done;
           op = sequence->next(), steps++) {
        switch (op->getOpType()) {
          case OpType::Barrier:
            in_flight[0] = in_flight[1] = false;
            break;
          case OpType::Swap:
            primary = 1 - primary;
            break;
          case OpType::Copy:
            in_flight[1 - primary] = true;
            break;
          case OpType::Create:
          case OpType::Remove:
          case OpType::Truncate:
          case OpType::TruncateWrite:
          case OpType::TruncateWrite2:
          case OpType::TruncateWrite3:
            in_flight[primary] = true;
            break;
          case OpType::Read:
          case OpType::Read2:
          case OpType::Read3:
            EXPECT_FALSE(in_flight[primary])
                << seq << " seed " << seed << " step " << steps;
            break;
          default:
            break;
        }
      }
    }
  }
}
