#include "EcIoSequence.h"

#include <memory>

using IoOp = ceph::io_exerciser::IoOp;
using Sequence = ceph::io_exerciser::Sequence;
using IoSequence = ceph::io_exerciser::IoSequence;
using EcIoSequence = ceph::io_exerciser::EcIoSequence;
using ReadInjectSequence = ceph::io_exerciser::ReadInjectSequence;

bool EcIoSequence::is_supported(Sequence sequence) const
{
  return true;
}

std::unique_ptr<IoSequence> EcIoSequence::generate_sequence(Sequence sequence,
                                                            std::pair<int,int> obj_size_range,
                                                            int k,
                                                            int m,
                                                            int seed)
{
  switch(sequence)
  {
    case Sequence::SEQUENCE_SEQ0:
      [[ fallthrough ]];
    case Sequence::SEQUENCE_SEQ1:
      [[ fallthrough ]];
    case Sequence::SEQUENCE_SEQ2:
      [[ fallthrough ]];
    case Sequence::SEQUENCE_SEQ3:
      [[ fallthrough ]];
    case Sequence::SEQUENCE_SEQ4:
      [[ fallthrough ]];
    case Sequence::SEQUENCE_SEQ5:
      [[ fallthrough ]];
    case Sequence::SEQUENCE_SEQ6:
      [[ fallthrough ]];
    case Sequence::SEQUENCE_SEQ7:
      [[ fallthrough ]];
    case Sequence::SEQUENCE_SEQ8:
      [[ fallthrough ]];
    case Sequence::SEQUENCE_SEQ9:
      return std::make_unique<ReadInjectSequence>(obj_size_range, seed,
                                                  sequence, k, m);
    case Sequence::SEQUENCE_SEQ10:
      return std::make_unique<Seq10>(obj_size_range, seed, k, m);
    default:
      ceph_abort_msg("Unrecognised sequence");
  }
}

EcIoSequence::EcIoSequence(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed),
  setup_inject(false), clear_inject(false), shard_to_inject(std::nullopt)
{

}

void EcIoSequence::select_random_data_shard_to_inject_read_error(int k, int m)
{
  shard_to_inject = rng(k - 1);
  setup_inject = true;
}

void EcIoSequence::select_random_data_shard_to_inject_write_error(int k, int m)
{
  // Write errors do not support injecting to the primary OSD
  shard_to_inject = rng(1, k - 1);
  setup_inject = true;
}

void EcIoSequence::select_random_shard_to_inject_read_error(int k, int m)
{
  shard_to_inject = rng(k + m - 1);
  setup_inject = true;
}

void EcIoSequence::select_random_shard_to_inject_write_error(int k, int m)
{
  // Write errors do not support injecting to the primary OSD
  shard_to_inject = rng(1, k + m - 1);
  setup_inject = true;
}

void EcIoSequence::generate_random_read_inject_type()
{
  inject_op_type = static_cast<InjectOpType>(rng(static_cast<int>(InjectOpType::ReadEIO),
                                                 static_cast<int>(InjectOpType::ReadMissingShard)));
}

void EcIoSequence::generate_random_write_inject_type()
{
  inject_op_type = static_cast<InjectOpType>(rng(static_cast<int>(InjectOpType::WriteFailAndRollback),
                                                 static_cast<int>(InjectOpType::WriteOSDAbort)));
}

ceph::io_exerciser::ReadInjectSequence::ReadInjectSequence(std::pair<int,int> obj_size_range,
                                                           int seed,
                                                           Sequence s,
                                                           int k, int m) :
  EcIoSequence(obj_size_range, seed)
{
  child_sequence = IoSequence::generate_sequence(s, obj_size_range, seed);
  select_random_data_shard_to_inject_read_error(k, m);
  generate_random_read_inject_type();
}

Sequence ceph::io_exerciser::ReadInjectSequence::get_id() const
{
  return child_sequence->get_id();
}

std::string ceph::io_exerciser::ReadInjectSequence::get_name() const
{
  return child_sequence->get_name() +
    " running with read errors injected on shard "
    + std::to_string(*shard_to_inject);
}

std::unique_ptr<IoOp> ReadInjectSequence::next()
{
  step++;

  if (nextOp)
  {
    std::unique_ptr<IoOp> retOp = nullptr;
    nextOp.swap(retOp);
    return retOp;
  }

  std::unique_ptr<IoOp> childOp = child_sequence->next();

  switch(childOp->getOpType())
  {
    case OpType::Remove:
      nextOp.swap(childOp);
        switch(inject_op_type)
        {
          case InjectOpType::ReadEIO:
            return ClearReadErrorInjectOp::generate(*shard_to_inject, 0);
          case InjectOpType::ReadMissingShard:
            return ClearReadErrorInjectOp::generate(*shard_to_inject, 1);
          case InjectOpType::WriteFailAndRollback:
            return ClearWriteErrorInjectOp::generate(*shard_to_inject, 0);
          case InjectOpType::WriteOSDAbort:
            return ClearWriteErrorInjectOp::generate(*shard_to_inject, 3);
          case InjectOpType::None:
            [[ fallthrough ]];
          default:
            ceph_abort_msg("Unsupported operation");
        }
      break;
    case OpType::Create:
      switch(inject_op_type)
      {
        case InjectOpType::ReadEIO:
          nextOp = InjectReadErrorOp::generate(*shard_to_inject, 0, 0, std::numeric_limits<uint64_t>::max());
          break;
        case InjectOpType::ReadMissingShard:
          nextOp = InjectReadErrorOp::generate(*shard_to_inject, 1, 0, std::numeric_limits<uint64_t>::max());
          break;
        case InjectOpType::WriteFailAndRollback:
          nextOp = InjectWriteErrorOp::generate(*shard_to_inject, 0, 0, std::numeric_limits<uint64_t>::max());
          break;
        case InjectOpType::WriteOSDAbort:
          nextOp = InjectWriteErrorOp::generate(*shard_to_inject, 3, 0, std::numeric_limits<uint64_t>::max());
          break;
        case InjectOpType::None:
          [[ fallthrough ]];
        default:
          ceph_abort_msg("Unsupported operation");
      }
    break;
    default:
      // Do nothing in default case
      break;
  }

  return childOp;
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::ReadInjectSequence::_next()
{
  ceph_abort_msg("Should not reach this point, "
                 "this sequence should only consume complete sequences");

  return DoneOp::generate();
}



ceph::io_exerciser::Seq10::Seq10(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  EcIoSequence(obj_size_range, seed), offset(0), length(1),
  failed_write_done(false), read_done(false), successful_write_done(false),
  test_all_lengths(false), // Only test length(1) due to time constraints
  test_all_sizes(false) // Only test obj_size(rand()) due to time constraints
{
  select_random_shard_to_inject_write_error(k, m);
  // We will inject specifically as part of our sequence in this sequence
  setup_inject = false;
  if (!test_all_sizes)
  {
    select_random_object_size();
  }
}

Sequence ceph::io_exerciser::Seq10::get_id() const
{
  return Sequence::SEQUENCE_SEQ10;
}

std::string ceph::io_exerciser::Seq10::get_name() const
{
  return "Sequential writes of length " + std::to_string(length) +
    " with queue depth 1"
    " first injecting a failed write and read it to ensure it rolls back, then"
    " successfully writing the data and reading the write the ensure it is applied";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq10::_next()
{
  if (!inject_error_done)
  {
    inject_error_done = true;
    return InjectWriteErrorOp::generate(*shard_to_inject, 0, 0,
                                        std::numeric_limits<uint64_t>::max());
  }
  else if (!failed_write_done)
  {
    failed_write_done = true;
    read_done = false;
    barrier = true;
    return SingleFailedWriteOp::generate(offset, length);
  }
  else if (failed_write_done && !read_done)
  {
    read_done = true;
    barrier = true;
    return SingleReadOp::generate(offset, length);
  }
  else if (!clear_inject_done)
  {
    clear_inject_done = true;
    return ClearWriteErrorInjectOp::generate(*shard_to_inject, 0);
  }
  else if (!successful_write_done)
  {
    successful_write_done = true;
    read_done = false;
    barrier = true;
    return SingleWriteOp::generate(offset, length);
  }
  else if (successful_write_done && !read_done)
  {
    read_done = true;
    return SingleReadOp::generate(offset, length);
  }
  else if (successful_write_done && read_done)
  {
    offset++;
    inject_error_done = false;
    failed_write_done = false;
    read_done = false;
    clear_inject_done = false;
    successful_write_done = false;

    if (offset + length >= obj_size) {
      if (!test_all_lengths)
      {
        done = true;
        return BarrierOp::generate();
      }

      offset = 0;
      length++;
      if (length > obj_size) {
        if (!test_all_sizes)
        {
          done = true;
          return BarrierOp::generate();
        }

        length = 1;
        return increment_object_size();
      }
    }

    return BarrierOp::generate();
  }
  else
  {
    ceph_abort_msg("Sequence in undefined state. Aborting");
    return DoneOp::generate();
  }
}