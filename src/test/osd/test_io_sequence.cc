// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <new>
#include <vector>

#include "common/io_exerciser/IoOp.h"
#include "common/io_exerciser/IoSequence.h"

using ceph::io_exerciser::IoSequence;
using ceph::io_exerciser::OpType;
using ceph::io_exerciser::Seq15;
using ceph::io_exerciser::Sequence;

namespace ceph::io_exerciser {
static void PrintTo(OpType op, std::ostream* os) {
  *os << fmt::format("{}", op);
}
}  // namespace ceph::io_exerciser

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
static std::vector<OpType> run_sequence(IoSequence& sequence) {
  std::vector<OpType> ops;
  for (auto op = sequence.next(); op->getOpType() != OpType::Done;
       op = sequence.next()) {
    ops.push_back(op->getOpType());
    if (ops.size() >= 1000000) {
      ADD_FAILURE() << sequence.get_name_with_seqseed() << " did not finish";
      break;
    }
  }
  return ops;
}

static std::vector<OpType> run_sequence(Sequence seq, int seed,
                                        bool check_consistency) {
  auto sequence = IoSequence::generate_sequence(seq, {1, 32}, seed,
                                                check_consistency);
  if (!sequence) {
    ADD_FAILURE() << seq << " not generated";
    return {};
  }
  return run_sequence(*sequence);
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

// A sequence must not depend on what the memory it is constructed in held:
// build Seq15 in zeroed memory and in memory full of 5 (Seq15's
// SWAP_TO_PRIMARY stage) and expect the same ops.
TEST(IoSequence, Seq15IgnoresPriorMemoryContents) {
  alignas(Seq15) uint32_t zeroed[sizeof(Seq15) / sizeof(uint32_t) + 1] = {};
  alignas(Seq15) uint32_t filled[sizeof(Seq15) / sizeof(uint32_t) + 1];
  for (auto& word : filled) {
    word = 5;
  }
  auto* a = new (zeroed) Seq15({1, 32}, 1, false);
  auto* b = new (filled) Seq15({1, 32}, 1, false);
  auto a_ops = run_sequence(*a);
  auto b_ops = run_sequence(*b);
  a->~Seq15();
  b->~Seq15();
  EXPECT_EQ(a_ops, b_ops);
}

// ObjectModel only stops a read overlapping an in-flight Write, so a
// sequence must issue a barrier between an op that rewrites a whole object
// (copy, truncate, create, remove) and the next read of that object.
// Without it a balanced read can reach a replica before that op does and
// return the old object.
TEST(IoSequence, NoReadWhileWholeObjectWriteInFlight) {
  for (Sequence seq : supported_sequences()) {
    for (int seed = 1; seed <= 3; seed++) {
      auto ops = run_sequence(seq, seed, false);
      int primary = 0;
      bool in_flight[2] = {false, false};
      for (size_t step = 0; step < ops.size(); step++) {
        switch (ops[step]) {
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
          case OpType::SparseRead:
          case OpType::Consistency:
            EXPECT_FALSE(in_flight[primary])
                << seq << " seed " << seed << " step " << step;
            break;
          case OpType::Done:
          case OpType::Write:
          case OpType::Write2:
          case OpType::Write3:
          case OpType::Append:
          case OpType::FailedWrite:
          case OpType::FailedWrite2:
          case OpType::FailedWrite3:
          case OpType::InjectReadError:
          case OpType::InjectWriteError:
          case OpType::ClearReadErrorInject:
          case OpType::ClearWriteErrorInject:
            break;
        }
      }
    }
  }
}
