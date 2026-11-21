#include "EcIoSequence.h"

#include <algorithm>
#include <memory>

using IoOp = ceph::io_exerciser::IoOp;
using Sequence = ceph::io_exerciser::Sequence;
using IoSequence = ceph::io_exerciser::IoSequence;
using EcIoSequence = ceph::io_exerciser::EcIoSequence;
using ReadInjectSequence = ceph::io_exerciser::ReadInjectSequence;

bool EcIoSequence::is_supported(Sequence sequence) const { return true; }

std::unique_ptr<IoSequence> EcIoSequence::generate_sequence(
    Sequence sequence, std::pair<int, int> obj_size_range,
    std::optional<std::pair<int, int>> km,
    std::optional<std::pair<std::string_view, std::string_view>> mappinglayers,
    int seed,
    bool check_consistency) {
  switch (sequence) {
    case Sequence::SEQUENCE_SEQ0:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ1:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ2:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ3:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ4:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ5:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ6:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ7:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ8:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ9:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ11:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ12:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ13:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ14:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ15:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ16:
      [[fallthrough]];
    case Sequence::SEQUENCE_SEQ17:
      return std::make_unique<ReadInjectSequence>(obj_size_range, seed,
                                                  sequence, km, mappinglayers, check_consistency);
    case Sequence::SEQUENCE_SEQ10:
      return std::make_unique<Seq10>(obj_size_range, seed, km, mappinglayers, check_consistency);
    case Sequence::SEQUENCE_SEQ19:
      return std::make_unique<Seq19>(obj_size_range, seed, km, mappinglayers, check_consistency);
    default:
      ceph_abort_msg("Unrecognised sequence");
  }
}

EcIoSequence::EcIoSequence(std::pair<int, int> obj_size_range, int seed, bool check_consistency)
    : IoSequence(obj_size_range, seed, check_consistency),
      setup_inject(false),
      clear_inject(false),
      shard_to_inject(std::nullopt) {}

void EcIoSequence::select_random_data_shard_to_inject_read_error(
    std::optional<std::pair<int, int>> km,
    std::optional<std::pair<std::string_view, std::string_view>>
        mappinglayers) {
  if (km) {
    shard_to_inject = rng(km->first - 1);
  }
  if (mappinglayers) {
    int count = std::ranges::count(mappinglayers->first, 'D');
    int data_shard_position = rng(count - 1);
    shard_to_inject = 0;
    for (int i = 0; i < data_shard_position; i++) {
      shard_to_inject =
          std::distance(std::find(mappinglayers->first.begin(),
                                  mappinglayers->first.end(), *shard_to_inject),
                        mappinglayers->first.begin());
      ceph_assert(*shard_to_inject != std::string::npos);
    }
  }
  setup_inject = true;
}

void EcIoSequence::select_random_data_shard_to_inject_write_error(
    std::optional<std::pair<int, int>> km,
    std::optional<std::pair<std::string_view, std::string_view>>
        mappinglayers) {
  // Write errors do not support injecting to the primary OSD
  if (km) {
    shard_to_inject = rng(1, km->first - 1);
  }
  if (mappinglayers) {
    int count = std::ranges::count(mappinglayers->first, 'D');
    if (mappinglayers->first[0] == 'D') {
      count--;
    }
    int data_shard_position = rng(1, count - 1);
    shard_to_inject = 0;
    for (int i = 1; i < data_shard_position; i++) {
      shard_to_inject =
          std::distance(std::find(mappinglayers->first.begin(),
                                  mappinglayers->first.end(), *shard_to_inject),
                        mappinglayers->first.begin());
      ceph_assert(*shard_to_inject != std::string::npos);
    }
  }
  setup_inject = true;
}

void EcIoSequence::select_random_shard_to_inject_read_error(
    std::optional<std::pair<int, int>> km,
    std::optional<std::pair<std::string_view, std::string_view>>
        mappinglayers) {
  if (km) {
    shard_to_inject = rng(km->first + km->second - 1);
  }
  if (mappinglayers) {
    int count = std::ranges::count(mappinglayers->first, 'D');
    int data_shard_position = rng(count - 1);
    shard_to_inject = 0;
    for (int i = 0; i < data_shard_position; i++) {
      shard_to_inject =
          std::distance(std::find(mappinglayers->first.begin(),
                                  mappinglayers->first.end(), *shard_to_inject),
                        mappinglayers->first.begin());
      ceph_assert(*shard_to_inject != std::string::npos);
    }
  }
  setup_inject = true;
}

void EcIoSequence::select_random_shard_to_inject_write_error(
    std::optional<std::pair<int, int>> km,
    std::optional<std::pair<std::string_view, std::string_view>>
        mappinglayers) {
  // Write errors do not support injecting to the primary OSD
  if (km) {
    shard_to_inject = rng(1, km->first + km->second - 1);
  }
  if (mappinglayers) {
    int count = std::ranges::count(mappinglayers->first, 'D');
    if (mappinglayers->first[0] == 'D') {
      count--;
    }
    int data_shard_position = rng(count - 1);
    shard_to_inject = 0;
    for (int i = 0; i < data_shard_position; i++) {
      shard_to_inject =
          std::distance(std::find(mappinglayers->first.begin(),
                                  mappinglayers->first.end(), *shard_to_inject),
                        mappinglayers->first.begin());
      ceph_assert(*shard_to_inject != std::string::npos);
    }
  }
  setup_inject = true;
}

void EcIoSequence::generate_random_read_inject_type() {
  inject_op_type = static_cast<InjectOpType>(
      rng(static_cast<int>(InjectOpType::ReadEIO),
          static_cast<int>(InjectOpType::ReadMissingShard)));
}

void EcIoSequence::generate_random_write_inject_type() {
  inject_op_type = static_cast<InjectOpType>(
      rng(static_cast<int>(InjectOpType::WriteFailAndRollback),
          static_cast<int>(InjectOpType::WriteOSDAbort)));
}

ceph::io_exerciser::ReadInjectSequence::ReadInjectSequence(
    std::pair<int, int> obj_size_range,
    int seed,
    Sequence s,
    std::optional<std::pair<int, int>> km,
    std::optional<std::pair<std::string_view, std::string_view>> mappinglayers,
    bool check_consistency)
    : EcIoSequence(obj_size_range, seed, check_consistency) {
  child_sequence = IoSequence::generate_sequence(s, obj_size_range, seed, check_consistency);
  select_random_data_shard_to_inject_read_error(km, mappinglayers);
  generate_random_read_inject_type();
}

Sequence ceph::io_exerciser::ReadInjectSequence::get_id() const {
  return child_sequence->get_id();
}

std::string ceph::io_exerciser::ReadInjectSequence::get_name() const {
  std::string injected_shard = "UNKNOWN";
  if (shard_to_inject) {
    injected_shard = std::to_string(*shard_to_inject);
  }

  return child_sequence->get_name() +
         " running with read errors injected on shard " + injected_shard;
}

std::unique_ptr<IoOp> ReadInjectSequence::next() {
  step++;

  if (next_op) {
    std::unique_ptr<IoOp> ret_op = nullptr;
    next_op.swap(ret_op);
    return ret_op;
  }

  std::unique_ptr<IoOp> child_op = child_sequence->next();

  switch (child_op->getOpType()) {
    case OpType::Remove:
      next_op.swap(child_op);
      ceph_assert(shard_to_inject.has_value());
      switch (inject_op_type) {
        case InjectOpType::ReadEIO:
          return ClearReadErrorInjectOp::generate(*shard_to_inject, 0);
        case InjectOpType::ReadMissingShard:
          return ClearReadErrorInjectOp::generate(*shard_to_inject, 1);
        case InjectOpType::WriteFailAndRollback:
          return ClearWriteErrorInjectOp::generate(*shard_to_inject, 0);
        case InjectOpType::WriteOSDAbort:
          return ClearWriteErrorInjectOp::generate(*shard_to_inject, 3);
        case InjectOpType::None:
          [[fallthrough]];
        default:
          ceph_abort_msg("Unsupported operation");
      }
      break;
    case OpType::Create:
      switch (inject_op_type) {
        case InjectOpType::ReadEIO:
          next_op = InjectReadErrorOp::generate(
              *shard_to_inject, 0, 0, std::numeric_limits<uint64_t>::max());
          break;
        case InjectOpType::ReadMissingShard:
          next_op = InjectReadErrorOp::generate(
              *shard_to_inject, 1, 0, std::numeric_limits<uint64_t>::max());
          break;
        case InjectOpType::WriteFailAndRollback:
          next_op = InjectWriteErrorOp::generate(
              *shard_to_inject, 0, 0, std::numeric_limits<uint64_t>::max());
          break;
        case InjectOpType::WriteOSDAbort:
          next_op = InjectWriteErrorOp::generate(
              *shard_to_inject, 3, 0, std::numeric_limits<uint64_t>::max());
          break;
        case InjectOpType::None:
          [[fallthrough]];
        default:
          ceph_abort_msg("Unsupported operation");
      }
      break;
    default:
      // Do nothing in default case
      break;
  }

  return child_op;
}

std::unique_ptr<ceph::io_exerciser::IoOp>
ceph::io_exerciser::ReadInjectSequence::_next() {
  ceph_abort_msg(
      "Should not reach this point, "
      "this sequence should only consume complete sequences");

  return DoneOp::generate();
}

ceph::io_exerciser::Seq10::Seq10(
    std::pair<int, int> obj_size_range, int seed,
    std::optional<std::pair<int, int>> km,
    std::optional<std::pair<std::string_view, std::string_view>> mappinglayers,
    bool check_consistency)
    : EcIoSequence(obj_size_range, seed, check_consistency),
      offset(0),
      length(1),
      inject_error_done(false),
      failed_write_done(false),
      read_done(false),
      successful_write_done(false),
      test_all_lengths(false),  // Only test length(1) due to time constraints
      test_all_sizes(
          false)  // Only test obj_size(rand()) due to time constraints
{
  select_random_shard_to_inject_write_error(km, mappinglayers);
  // We will inject specifically as part of our sequence in this sequence
  setup_inject = false;
  if (!test_all_sizes) {
    select_random_object_size();
  }
}

Sequence ceph::io_exerciser::Seq10::get_id() const {
  return Sequence::SEQUENCE_SEQ10;
}

std::string ceph::io_exerciser::Seq10::get_name() const {
  return "Sequential writes of length " + std::to_string(length) +
         " with queue depth 1"
         " first injecting a failed write and read it to ensure it rolls back, "
         "then"
         " successfully writing the data and reading the write the ensure it "
         "is applied";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq10::_next() {
  if (!inject_error_done) {
    inject_error_done = true;
    barrier = true;
    return InjectWriteErrorOp::generate(*shard_to_inject, 0, 0,
                                        std::numeric_limits<uint64_t>::max());
  } else if (!failed_write_done) {
    failed_write_done = true;
    read_done = false;
    barrier = true;
    return SingleFailedWriteOp::generate(offset, length);
  } else if (failed_write_done && !read_done) {
    read_done = true;
    barrier = true;
    return SingleReadOp::generate(offset, length);
  } else if (!clear_inject_done) {
    clear_inject_done = true;
    barrier = true;
    return ClearWriteErrorInjectOp::generate(*shard_to_inject, 0);
  } else if (!successful_write_done) {
    successful_write_done = true;
    read_done = false;
    barrier = true;
    return SingleWriteOp::generate(offset, length);
  } else if (successful_write_done && !read_done) {
    read_done = true;
    return SingleReadOp::generate(offset, length);
  } else if (successful_write_done && read_done) {
    offset++;
    inject_error_done = false;
    failed_write_done = false;
    read_done = false;
    clear_inject_done = false;
    successful_write_done = false;

    if (offset + length >= obj_size) {
      if (!test_all_lengths) {
        remove = true;
        done = true;
        return BarrierOp::generate();
      }

      offset = 0;
      length++;
      if (length > obj_size) {
        if (!test_all_sizes) {
          remove = true;
          done = true;
          return BarrierOp::generate();
        }

        length = 1;
        return increment_object_size();
      }
    }

    return BarrierOp::generate();
  } else {
    ceph_abort_msg("Sequence in undefined state. Aborting");
    return DoneOp::generate();
  }
}

// Seq19: for each of a small number of WriteTruncate/WriteTruncate2 cases, on
// a freshly created object of WRITE_TRUNCATE_OBJECT_SIZE blocks:
//   1. Inject a write error
//   2. Issue the failed WriteTruncate and read the object to ensure it was
//      rolled back
//   3. Clear the injected error
//   4. Issue the same WriteTruncate successfully and read the object to
//      ensure it was applied
// Like Seq10, this is kept short because every case needs its own error
// injection, so only three cases are used (one failed write each):
//   - WriteTruncate: write at the end of the object, truncate it away
//   - WriteTruncate: write at the start of the object, truncate to grow it
//   - WriteTruncate2: write across the smallest truncate size, then shrink
//     to it and grow to the largest truncate size
// A failed write must lie within the object, so every write ends at or
// before the object size.
ceph::io_exerciser::Seq19::Seq19(
    std::pair<int, int> obj_size_range, int seed,
    std::optional<std::pair<int, int>> km,
    std::optional<std::pair<std::string_view, std::string_view>> mappinglayers,
    bool check_consistency)
    : EcIoSequence(
          std::make_pair(static_cast<int>(WRITE_TRUNCATE_OBJECT_SIZE),
                         static_cast<int>(WRITE_TRUNCATE_OBJECT_SIZE)),
          seed, check_consistency),
      cases({{20, 4, WRITE_TRUNCATE_MIN_SIZE, std::nullopt},
             {0, 1, WRITE_TRUNCATE_MAX_SIZE, std::nullopt},
             {8, 5, WRITE_TRUNCATE_MIN_SIZE, WRITE_TRUNCATE_MAX_SIZE}}),
      case_idx(0),
      stage(Stage::INJECT_ERROR) {
  select_random_shard_to_inject_write_error(km, mappinglayers);
  // We inject specifically as part of our sequence in this sequence
  setup_inject = false;
}

Sequence ceph::io_exerciser::Seq19::get_id() const {
  return Sequence::SEQUENCE_SEQ18;
}

std::string ceph::io_exerciser::Seq19::get_name() const {
  return "WriteTruncate and WriteTruncate2 operations with edge case write "
         "locations and truncate sizes on objects of fixed size, first "
         "injecting a failed write and reading it to ensure it rolls back, "
         "then successfully writing the data and reading it to ensure it is "
         "applied";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq19::_next() {
  const WriteTruncateCase& c = cases[case_idx];
  barrier = true;
  switch (stage) {
    case Stage::INJECT_ERROR:
      stage = Stage::FAILED_WRITE_TRUNCATE;
      return InjectWriteErrorOp::generate(*shard_to_inject, 0, 0,
                                          std::numeric_limits<uint64_t>::max());
    case Stage::FAILED_WRITE_TRUNCATE:
      stage = Stage::READ_AFTER_FAILURE;
      if (c.size2) {
        return DoubleFailedWriteTruncateOp::generate(c.offset, c.length,
                                                     c.size1, *c.size2);
      }
      return SingleFailedWriteTruncateOp::generate(c.offset, c.length, c.size1);
    case Stage::READ_AFTER_FAILURE:
      stage = Stage::CLEAR_INJECT;
      return SingleReadOp::generate(0, WRITE_TRUNCATE_OBJECT_SIZE);
    case Stage::CLEAR_INJECT:
      stage = Stage::WRITE_TRUNCATE;
      return ClearWriteErrorInjectOp::generate(*shard_to_inject, 0);
    case Stage::WRITE_TRUNCATE:
      stage = Stage::READ_AFTER_SUCCESS;
      if (c.size2) {
        return DoubleWriteTruncateOp::generate(c.offset, c.length, c.size1,
                                               *c.size2);
      }
      return SingleWriteTruncateOp::generate(c.offset, c.length, c.size1);
    case Stage::READ_AFTER_SUCCESS:
      stage = Stage::RESET;
      consistency = check_consistency;
      return SingleReadOp::generate(0, c.final_size());
    case Stage::RESET:
    default:
      stage = Stage::INJECT_ERROR;
      case_idx++;
      remove = true;
      if (case_idx >= cases.size()) {
        done = true;
      } else {
        create = true;
      }
      return BarrierOp::generate();
  }
}
