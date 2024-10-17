#include "IoSequence.h"

using IoOp = ceph::io_exerciser::IoOp;
using Sequence = ceph::io_exerciser::Sequence;
using IoSequence = ceph::io_exerciser::IoSequence;

std::ostream& ceph::io_exerciser::operator<<(std::ostream& os, const Sequence& seq)
{
  switch (seq)
  {
    case Sequence::SEQUENCE_SEQ0:
      os << "SEQUENCE_SEQ0";
      break;
    case Sequence::SEQUENCE_SEQ1:
      os << "SEQUENCE_SEQ1";
      break;
    case Sequence::SEQUENCE_SEQ2:
      os << "SEQUENCE_SEQ2";
      break;
    case Sequence::SEQUENCE_SEQ3:
      os << "SEQUENCE_SEQ3";
      break;
    case Sequence::SEQUENCE_SEQ4:
      os << "SEQUENCE_SEQ4";
      break;
    case Sequence::SEQUENCE_SEQ5:
      os << "SEQUENCE_SEQ5";
      break;
    case Sequence::SEQUENCE_SEQ6:
      os << "SEQUENCE_SEQ6";
      break;
    case Sequence::SEQUENCE_SEQ7:
      os << "SEQUENCE_SEQ7";
      break;
    case Sequence::SEQUENCE_SEQ8:
      os << "SEQUENCE_SEQ8";
      break;
    case Sequence::SEQUENCE_SEQ9:
      os << "SEQUENCE_SEQ9";
      break;
    case Sequence::SEQUENCE_SEQ10:
      os << "SEQUENCE_SEQ10";
      break;
    case Sequence::SEQUENCE_SEQ11:
      os << "SEQUENCE_SEQ11";
      break;
    case Sequence::SEQUENCE_SEQ12:
      os << "SEQUENCE_SEQ12";
      break;
    case Sequence::SEQUENCE_SEQ13:
      os << "SEQUENCE_SEQ13";
      break;
    case Sequence::SEQUENCE_SEQ14:
      os << "SEQUENCE_SEQ14";
      break;
    case Sequence::SEQUENCE_SEQ15:
      os << "SEQUENCE_SEQ15";
      break;
    case Sequence::SEQUENCE_SEQ16:
      os << "SEQUENCE_SEQ16";
      break;
    case Sequence::SEQUENCE_SEQ17:
      os << "SEQUENCE_SEQ17";
      break;
    case Sequence::SEQUENCE_SEQ18:
      os << "SEQUENCE_SEQ18";
      break;
    case Sequence::SEQUENCE_SEQ19:
      os << "SEQUENCE_SEQ19";
      break;
    case Sequence::SEQUENCE_END:
      os << "SEQUENCE_END";
      break;
  }
  return os;
}

std::unique_ptr<IoSequence> IoSequence::generate_sequence(Sequence s,
                                                          std::pair<int,int> obj_size_range,
                                                          int k,
                                                          int m,
                                                          int seed)
{
  switch (s) {
    case Sequence::SEQUENCE_SEQ0:
      return std::make_unique<Seq0>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ1:
      return std::make_unique<Seq1>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ2:
      return std::make_unique<Seq2>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ3:
      return std::make_unique<Seq3>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ4:
      return std::make_unique<Seq4>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ5:
      return std::make_unique<Seq5>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ6:
      return std::make_unique<Seq6>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ7:
      return std::make_unique<Seq7>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ8:
      return std::make_unique<Seq8>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ9:
      return std::make_unique<Seq9>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ10:
      return std::make_unique<Seq10>(obj_size_range, seed, k, m);
    case Sequence::SEQUENCE_SEQ11:
      return std::make_unique<Seq11>(obj_size_range, seed, k, m);
    case Sequence::SEQUENCE_SEQ12:
      return std::make_unique<Seq12>(obj_size_range, seed, k, m);
    case Sequence::SEQUENCE_SEQ13:
      return std::make_unique<Seq13>(obj_size_range, seed, k, m);
    case Sequence::SEQUENCE_SEQ14:
      return std::make_unique<Seq14>(obj_size_range, seed, k, m);
    case Sequence::SEQUENCE_SEQ15:
      return std::make_unique<Seq15>(obj_size_range, seed, k, m);
    case Sequence::SEQUENCE_SEQ16:
      return std::make_unique<Seq16>(obj_size_range, seed, k, m);
    case Sequence::SEQUENCE_SEQ17:
      return std::make_unique<Seq17>(obj_size_range, seed, k, m);
    case Sequence::SEQUENCE_SEQ18:
      return std::make_unique<Seq18>(obj_size_range, seed, k, m);
    case Sequence::SEQUENCE_SEQ19:
      return std::make_unique<Seq19>(obj_size_range, seed, k, m);
    default:
      break;
  }
  return nullptr;
}

IoSequence::IoSequence(std::pair<int,int> obj_size_range,
                       int seed) :
        min_obj_size(obj_size_range.first), max_obj_size(obj_size_range.second),
        create(true), barrier(false), done(false), remove(false),
        setup_inject(false), clear_inject(false),
        obj_size(min_obj_size), shard_to_inject(std::nullopt), step(-1), seed(seed)
{
  rng.seed(seed);
}

int IoSequence::get_step() const
{
  return step;
}

int IoSequence::get_seed() const
{
  return seed;
}

void IoSequence::set_min_object_size(uint64_t size)
{
  min_obj_size = size;
  if (obj_size < size) {
    obj_size = size;
    if (obj_size > max_obj_size) {
      done = true;
    }
  }
}

void IoSequence::set_max_object_size(uint64_t size)
{
  max_obj_size = size;
  if (obj_size > size) {
    done = true;
  }
}

void IoSequence::select_random_object_size()
{
  if (max_obj_size != min_obj_size) {
    obj_size = min_obj_size + rng(max_obj_size - min_obj_size);
  }
}

std::unique_ptr<IoOp> IoSequence::increment_object_size()
{
  obj_size++;
  if (obj_size > max_obj_size) {
    done = true;
  }
  create = true;
  barrier = true;
  remove = true;
  return BarrierOp::generate();
}

void IoSequence::select_random_data_shard_to_inject_read_error(int k, int m)
{
  shard_to_inject = rng(k);
  setup_inject = true;
}

void IoSequence::select_random_data_shard_to_inject_write_error(int k, int m)
{
  shard_to_inject = rng(1, k);
  setup_inject = true;
}

void IoSequence::select_random_shard_to_inject_read_error(int k, int m)
{
  shard_to_inject = rng(k + m);
  setup_inject = true;
}

void IoSequence::select_random_shard_to_inject_write_error(int k, int m)
{
  shard_to_inject = rng(1, k + m);
  setup_inject = true;
}

void IoSequence::generate_random_read_inject_type()
{
  inject_op_type = static_cast<InjectOpType>(rng(static_cast<int>(InjectOpType::ReadEIO),
                                                 static_cast<int>(InjectOpType::ReadMissingShard)));
}

void IoSequence::generate_random_write_inject_type()
{
  inject_op_type = static_cast<InjectOpType>(rng(static_cast<int>(InjectOpType::WriteFailAndRollback),
                                                 static_cast<int>(InjectOpType::WriteOSDAbort)));
}

std::unique_ptr<IoOp> IoSequence::next()
{
  step++;
  if (clear_inject && inject_op_type != InjectOpType::None) {
    clear_inject = false;
    switch(inject_op_type)
    {
      case InjectOpType::None:
        ceph_abort();
      case InjectOpType::ReadEIO:
        return ClearReadErrorInjectOp::generate(*shard_to_inject, 0);
      case InjectOpType::ReadMissingShard:
        return ClearReadErrorInjectOp::generate(*shard_to_inject, 1);
      case InjectOpType::WriteFailAndRollback:
        return ClearWriteErrorInjectOp::generate(*shard_to_inject, 0);
      case InjectOpType::WriteOSDAbort:
        return ClearWriteErrorInjectOp::generate(*shard_to_inject, 3);
      default:
        ceph_abort_msg("Unsupported operation");
    }
  }
  if (remove) {
    remove = false;
    return RemoveOp::generate();
  }
  if (barrier) {
    barrier = false;
    return BarrierOp::generate();
  }
  if (done) {
    return DoneOp::generate();
  }
  if (create) {
    create = false;
    barrier = true;
    return CreateOp::generate(obj_size);
  }
  if (setup_inject && inject_op_type != InjectOpType::None) {
    setup_inject = false;
    switch(inject_op_type)
    {
      case InjectOpType::None:
        ceph_abort();
      case InjectOpType::ReadEIO:
        return InjectReadErrorOp::generate(*shard_to_inject, 0, 0, std::numeric_limits<uint64_t>::max());
      case InjectOpType::ReadMissingShard:
        return InjectReadErrorOp::generate(*shard_to_inject, 1, 0, std::numeric_limits<uint64_t>::max());
      case InjectOpType::WriteFailAndRollback:
        return InjectWriteErrorOp::generate(*shard_to_inject, 0, 0, std::numeric_limits<uint64_t>::max());
      case InjectOpType::WriteOSDAbort:
        return InjectWriteErrorOp::generate(*shard_to_inject, 3, 0, std::numeric_limits<uint64_t>::max());
      default:
        ceph_abort_msg("Unsupported operation");
    }
  }
  return _next();
}



ceph::io_exerciser::Seq0::Seq0(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed), offset(0)
{
  select_random_object_size();
  length = 1 + rng(obj_size - 1);
}

std::string ceph::io_exerciser::Seq0::get_name() const
{
  return "Sequential reads of length " + std::to_string(length) +
    " with queue depth 1 (seqseed " + std::to_string(get_seed()) + ")";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq0::_next()
{
  std::unique_ptr<IoOp> r;
  if (offset >= obj_size) {
    done = true;
    barrier = true;
    remove = true;
    return BarrierOp::generate();
  }
  if (offset + length > obj_size) {
    r = SingleReadOp::generate(offset, obj_size - offset);
  } else {
    r = SingleReadOp::generate(offset, length);
  }
  offset += length;
  return r;
}



ceph::io_exerciser::Seq1::Seq1(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed)
{
  select_random_object_size();
  count = 3 * obj_size;
}

std::string ceph::io_exerciser::Seq1::get_name() const
{
  return "Random offset, random length read/write I/O with queue depth 1 (seqseed "
    + std::to_string(get_seed()) + ")";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq1::_next()
{
  barrier = true;
  if (count-- == 0) {
    done = true;
    remove = true;
    return BarrierOp::generate();
  }

  uint64_t offset = rng(obj_size - 1);
  uint64_t length = 1 + rng(obj_size - 1 - offset);

  if (rng(2) != 0)
  {
    return SingleWriteOp::generate(offset, length);
  }
  else
  {
    return SingleReadOp::generate(offset, length);
  }
}



ceph::io_exerciser::Seq2::Seq2(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed), offset(0), length(0) {}

std::string ceph::io_exerciser::Seq2::get_name() const
{
  return "Permutations of offset and length read I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq2::_next()
{
  length++;
  if (length > obj_size - offset) {
    length = 1;
    offset++;
    if (offset >= obj_size) {
      offset = 0;
      length = 0;
      return increment_object_size();
    }
  }
  return SingleReadOp::generate(offset, length);
}



ceph::io_exerciser::Seq3::Seq3(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed), offset1(0), offset2(0)
{
  set_min_object_size(2);
}

std::string ceph::io_exerciser::Seq3::get_name() const
{
  return "Permutations of offset 2-region 1-block read I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq3::_next()
{
  offset2++;
  if (offset2 >= obj_size - offset1) {
    offset2 = 1;
    offset1++;
    if (offset1 + 1 >= obj_size) {
      offset1 = 0;
      offset2 = 0;
      return increment_object_size();
    }
  }
  return DoubleReadOp::generate(offset1, 1, offset1 + offset2, 1);
}



ceph::io_exerciser::Seq4::Seq4(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed), offset1(0), offset2(1)
{
  set_min_object_size(3);
}

std::string ceph::io_exerciser::Seq4::get_name() const
{
  return "Permutations of offset 3-region 1-block read I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq4::_next()
{
  offset2++;
  if (offset2 >= obj_size - offset1) {
    offset2 = 2;
    offset1++;
    if (offset1 + 2 >= obj_size) {
      offset1 = 0;
      offset2 = 1;
      return increment_object_size();
    }
  }
  return TripleReadOp::generate(offset1, 1, (offset1 + offset2), 1, (offset1 * 2 + offset2) / 2, 1);
}



ceph::io_exerciser::Seq5::Seq5(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed), offset(0), length(1),
  doneread(false), donebarrier(false) {}

std::string ceph::io_exerciser::Seq5::get_name() const
{
  return "Permutation of length sequential writes";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq5::_next()
{
  if (offset >= obj_size) {
    if (!doneread) {
      if (!donebarrier) {
        donebarrier = true;
        return BarrierOp::generate();
      }
      doneread = true;
      barrier = true;
      return SingleReadOp::generate(0, obj_size);
    }
    doneread = false;
    donebarrier = false;
    offset = 0;
    length++;
    if (length > obj_size) {
      length = 1;
      return increment_object_size();
    }
  }
  uint64_t io_len = (offset + length > obj_size) ? (obj_size - offset) : length;
  std::unique_ptr<IoOp> r = SingleWriteOp::generate(offset, io_len);
  offset += io_len;
  return r;
}



ceph::io_exerciser::Seq6::Seq6(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed), offset(0), length(1),
  doneread(false), donebarrier(false) {}

std::string ceph::io_exerciser::Seq6::get_name() const
{
  return "Permutation of length sequential writes, different alignment";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq6::_next()
{
  if (offset >= obj_size) {
    if (!doneread) {
      if (!donebarrier) {
        donebarrier = true;
        return BarrierOp::generate();
      }
      doneread = true;
      barrier = true;
      return SingleReadOp::generate(0, obj_size);
    }
    doneread = false;
    donebarrier = false;
    offset = 0;
    length++;
    if (length > obj_size) {
      length = 1;
      return increment_object_size();
    }
  }
  uint64_t io_len = (offset == 0) ? (obj_size % length) : length;
  if (io_len == 0) {
    io_len = length;
  }
  std::unique_ptr<IoOp> r = SingleWriteOp::generate(offset, io_len);
  offset += io_len;
  return r;
}



ceph::io_exerciser::Seq7::Seq7(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed)
{
  set_min_object_size(2);
  offset = obj_size;
}

std::string ceph::io_exerciser::Seq7::get_name() const
{
  return "Permutations of offset 2-region 1-block writes";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq7::_next()
{
  if (!doneread) {
    if (!donebarrier) {
      donebarrier = true;
      return BarrierOp::generate();
    }
    doneread = true;
    barrier = true;
    return SingleReadOp::generate(0, obj_size);
  }
  if (offset == 0) {
    doneread = false;
    donebarrier = false;
    offset = obj_size+1;
    return increment_object_size();
  }
  offset--;
  if (offset == obj_size/2) {
    return _next();
  }
  doneread = false;
  donebarrier = false;
  return DoubleReadOp::generate(offset, 1, obj_size/2, 1);
}



ceph::io_exerciser::Seq8::Seq8(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed), offset1(0), offset2(1)
{
  set_min_object_size(3);
}

std::string ceph::io_exerciser::Seq8::get_name() const
{
  return "Permutations of offset 3-region 1-block write I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq8::_next()
{
  if (!doneread) {
    if (!donebarrier) {
      donebarrier = true;
      return BarrierOp::generate();
    }
    doneread = true;
    barrier = true;
    return SingleReadOp::generate(0, obj_size);
  }
  offset2++;
  if (offset2 >= obj_size - offset1) {
    offset2 = 2;
    offset1++;
    if (offset1 + 2 >= obj_size) {
      offset1 = 0;
      offset2 = 1;
      return increment_object_size();
    }
  }
  doneread = false;
  donebarrier = false;
  return TripleWriteOp::generate(offset1, 1, offset1 + offset2, 1, (offset1 * 2 + offset2)/2, 1);
}



ceph::io_exerciser::Seq9::Seq9(std::pair<int,int> obj_size_range, int seed) :
  IoSequence(obj_size_range, seed), offset(0), length(0)
{

}

std::string ceph::io_exerciser::Seq9::get_name() const
{
  return "Permutations of offset and length write I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq9::_next()
{
  if (!doneread) {
    if (!donebarrier) {
      donebarrier = true;
      return BarrierOp::generate();
    }
    doneread = true;
    barrier = true;
    return SingleReadOp::generate(0, obj_size);
  }
  length++;
  if (length > obj_size - offset) {
    length = 1;
    offset++;
    if (offset >= obj_size) {
      offset = 0;
      length = 0;
      return increment_object_size();
    }
  }
  doneread = false;
  donebarrier = false;
  return SingleWriteOp::generate(offset, length);
}



ceph::io_exerciser::Seq10::Seq10(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  Seq0(obj_size_range, seed)
{
  select_random_data_shard_to_inject_read_error(k, m);
  generate_random_read_inject_type();
}

std::string ceph::io_exerciser::Seq10::get_name() const
{
  return Seq0::get_name() +
    " running with read errors injected on shard " + std::to_string(*shard_to_inject);
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq10::_next()
{
  std::unique_ptr<ceph::io_exerciser::IoOp> op = Seq0::_next();
  setup_inject = create;
  clear_inject = remove;
  return op;
}



ceph::io_exerciser::Seq11::Seq11(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  Seq1(obj_size_range, seed)
{
  select_random_shard_to_inject_write_error(k, m);
  generate_random_write_inject_type();
}

std::string ceph::io_exerciser::Seq11::get_name() const
{
  return Seq1::get_name() +
    " running with write errors injected on shard " + std::to_string(*shard_to_inject);
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq11::_next()
{
  std::unique_ptr<ceph::io_exerciser::IoOp> op = Seq1::_next();
  setup_inject = create;
  clear_inject = remove;
  return op;
}



ceph::io_exerciser::Seq12::Seq12(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  Seq2(obj_size_range, seed)
{
  select_random_data_shard_to_inject_read_error(k, m);
  generate_random_read_inject_type();
}

std::string ceph::io_exerciser::Seq12::get_name() const
{
  return Seq2::get_name() +
    " running with read errors injected on shard " + std::to_string(*shard_to_inject);
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq12::_next()
{
  std::unique_ptr<ceph::io_exerciser::IoOp> op = Seq2::_next();
  setup_inject = create;
  clear_inject = remove;
  return op;
}



ceph::io_exerciser::Seq13::Seq13(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  Seq3(obj_size_range, seed)
{
  select_random_data_shard_to_inject_read_error(k, m);
  generate_random_read_inject_type();
}

std::string ceph::io_exerciser::Seq13::get_name() const
{
  return Seq3::get_name() +
    " running with read errors injected on shard " + std::to_string(*shard_to_inject);
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq13::_next()
{
  std::unique_ptr<ceph::io_exerciser::IoOp> op = Seq3::_next();
  setup_inject = create;
  clear_inject = remove;
  return op;
}



ceph::io_exerciser::Seq14::Seq14(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  Seq4(obj_size_range, seed)
{
  select_random_data_shard_to_inject_read_error(k, m);
  generate_random_read_inject_type();
}

std::string ceph::io_exerciser::Seq14::get_name() const
{
  return Seq4::get_name() +
    " running with read errors injected on shard " + std::to_string(*shard_to_inject);
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq14::_next()
{
  std::unique_ptr<ceph::io_exerciser::IoOp> op = Seq4::_next();
  setup_inject = create;
  clear_inject = remove;
  return op;
}



ceph::io_exerciser::Seq15::Seq15(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  Seq5(obj_size_range, seed)
{
  select_random_data_shard_to_inject_read_error(k, m);
  generate_random_read_inject_type();
}

std::string ceph::io_exerciser::Seq15::get_name() const
{
  return Seq5::get_name() +
    " running with read errors injected on shard " + std::to_string(*shard_to_inject);
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq15::_next()
{
  std::unique_ptr<ceph::io_exerciser::IoOp> op = Seq5::_next();
  setup_inject = create;
  clear_inject = remove;
  return op;
}



ceph::io_exerciser::Seq16::Seq16(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  Seq6(obj_size_range, seed)
{
  select_random_shard_to_inject_write_error(k, m);
  generate_random_write_inject_type();
}

std::string ceph::io_exerciser::Seq16::get_name() const
{
  return Seq6::get_name() +
    " running with write errors injected on shard " + std::to_string(*shard_to_inject);
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq16::_next()
{
  std::unique_ptr<ceph::io_exerciser::IoOp> op = Seq6::_next();
  setup_inject = create;
  clear_inject = remove;
  return op;
}



ceph::io_exerciser::Seq17::Seq17(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  Seq7(obj_size_range, seed)
{
  select_random_shard_to_inject_write_error(k, m);
  generate_random_write_inject_type();
}

std::string ceph::io_exerciser::Seq17::get_name() const
{
  return Seq7::get_name() +
    " running with read errors injected on shard " + std::to_string(*shard_to_inject);
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq17::_next()
{
  std::unique_ptr<ceph::io_exerciser::IoOp> op = Seq7::_next();
  setup_inject = create;
  clear_inject = remove;
  return op;
}



ceph::io_exerciser::Seq18::Seq18(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  Seq8(obj_size_range, seed)
{
  select_random_shard_to_inject_write_error(k, m);
  generate_random_write_inject_type();
}

std::string ceph::io_exerciser::Seq18::get_name() const
{
  return Seq8::get_name() +
    " running with read errors injected on shard " + std::to_string(*shard_to_inject);
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq18::_next()
{
  std::unique_ptr<ceph::io_exerciser::IoOp> op = Seq8::_next();
  setup_inject = create;
  clear_inject = remove;
  return op;
}



ceph::io_exerciser::Seq19::Seq19(std::pair<int,int> obj_size_range, int seed,
                                 int k, int m) :
  Seq9(obj_size_range, seed)
{
  select_random_shard_to_inject_write_error(k, m);
  generate_random_write_inject_type();
}

std::string ceph::io_exerciser::Seq19::get_name() const
{
  return Seq9::get_name() +
    " running with read errors injected on shard " + std::to_string(*shard_to_inject);
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq19::_next()
{
  std::unique_ptr<ceph::io_exerciser::IoOp> op = Seq9::_next();
  setup_inject = create;
  clear_inject = remove;
  return op;
}