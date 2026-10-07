// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "common/io_exerciser/IoOp.h"
#include "common/io_exerciser/IoSequence.h"

using ceph::io_exerciser::IoSequence;
using ceph::io_exerciser::OpType;
using ceph::io_exerciser::Sequence;

static std::vector<Sequence> supported_sequences() {
  std::vector<Sequence> sequences;
  for (Sequence s = Sequence::SEQUENCE_BEGIN; s != Sequence::SEQUENCE_END;
       s = IoSequence::generate_sequence(s, {1, 32}, 1, false)
               ->getNextSupportedSequenceId()) {
    sequences.push_back(s);
  }
  return sequences;
}

// Returns the op types of a whole run. The step cap makes a sequence that
// never reaches Done fail with its name instead of hanging until the ctest
// timeout; the longest sequence here takes about 30,000 steps.
static std::vector<OpType> run_sequence(Sequence seq, int seed,
                                        bool check_consistency) {
  std::vector<OpType> ops;
  auto sequence = IoSequence::generate_sequence(seq, {1, 32}, seed,
                                                check_consistency);
  if (!sequence) {
    ADD_FAILURE() << seq << " not generated";
    return ops;
  }
  for (auto op = sequence->next(); op->getOpType() != OpType::Done;
       op = sequence->next()) {
    ops.push_back(op->getOpType());
    if (ops.size() >= 1000000) {
      ADD_FAILURE() << seq << " seed " << seed << " did not finish";
      break;
    }
  }
  return ops;
}

// A sequence asked not to check consistency must never issue a Consistency
// op: RadosIo has no consistency checker for a replicated pool.
TEST(IoSequence, NoConsistencyOpsUnlessRequested) {
  for (Sequence seq : supported_sequences()) {
    for (int seed = 1; seed <= 3; seed++) {
      auto ops = run_sequence(seq, seed, false);
      for (size_t step = 0; step < ops.size(); step++) {
        EXPECT_NE(ops[step], OpType::Consistency)
            << seq << " seed " << seed << " step " << step;
      }
    }
  }
}

TEST(IoSequence, Seq16ChecksConsistencyWhenRequested) {
  auto ops = run_sequence(Sequence::SEQUENCE_SEQ16, 1, true);
  EXPECT_GT(std::count(ops.begin(), ops.end(), OpType::Consistency), 0);
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
