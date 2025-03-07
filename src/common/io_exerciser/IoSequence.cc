#include "IoSequence.h"
#include <algorithm>

using IoOp = ceph::io_exerciser::IoOp;
using Sequence = ceph::io_exerciser::Sequence;
using IoSequence = ceph::io_exerciser::IoSequence;

std::ostream& ceph::io_exerciser::operator<<(std::ostream& os,
                                             const Sequence& seq) {
  switch (seq) {
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
    case Sequence::SEQUENCE_END:
      os << "SEQUENCE_END";
      break;
  }
  return os;
}

bool IoSequence::is_supported(Sequence sequence) const {
  return sequence != Sequence::SEQUENCE_SEQ10;
}

std::unique_ptr<IoSequence> IoSequence::generate_sequence(
    Sequence s, std::pair<int, int> obj_size_range, int seed) {
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
      ceph_abort_msg(
          "Sequence 10 only supported for erasure coded pools "
          "through the EcIoSequence interface");
      return nullptr;
    case Sequence::SEQUENCE_SEQ11:
      return std::make_unique<Seq11>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ12:
      return std::make_unique<Seq12>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ13:
      return std::make_unique<Seq13>(obj_size_range, seed);
    case Sequence::SEQUENCE_SEQ14:
      return std::make_unique<Seq14>(obj_size_range, seed);
    default:
      break;
  }
  return nullptr;
}

IoSequence::IoSequence(std::pair<int, int> obj_size_range, int seed)
    : min_obj_size(obj_size_range.first),
      max_obj_size(obj_size_range.second),
      create(true),
      barrier(false),
      done(false),
      remove(false),
      obj_size(min_obj_size),
      step(-1),
      seed(seed) {
  rng.seed(seed);
}

std::string ceph::io_exerciser::IoSequence::get_name_with_seqseed() const {
  return get_name() + " (seqseed " + std::to_string(get_seed()) + ")";
}

int IoSequence::get_step() const { return step; }

int IoSequence::get_seed() const { return seed; }

void IoSequence::set_min_object_size(uint64_t size) {
  min_obj_size = size;
  if (obj_size < size) {
    obj_size = size;
    if (obj_size > max_obj_size) {
      done = true;
    }
  }
}

void IoSequence::set_max_object_size(uint64_t size) {
  max_obj_size = size;
  if (obj_size > size) {
    done = true;
  }
}

void IoSequence::select_random_object_size() {
  if (max_obj_size != min_obj_size) {
    obj_size = min_obj_size + rng(max_obj_size - min_obj_size);
  }
}

std::unique_ptr<IoOp> IoSequence::increment_object_size() {
  obj_size++;
  if (obj_size > max_obj_size) {
    done = true;
  }
  create = true;
  barrier = true;
  remove = true;
  return BarrierOp::generate();
}

Sequence IoSequence::getNextSupportedSequenceId() const {
  Sequence sequence = get_id();
  ++sequence;
  for (; sequence < Sequence::SEQUENCE_END; ++sequence) {
    if (is_supported(sequence)) {
      return sequence;
    }
  }

  return Sequence::SEQUENCE_END;
}

std::unique_ptr<IoOp> IoSequence::next() {
  step++;
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
  return _next();
}

ceph::io_exerciser::Seq0::Seq0(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed), offset(0) {
  select_random_object_size();
  length = 1 + rng(obj_size - 1);
}

Sequence ceph::io_exerciser::Seq0::get_id() const {
  return Sequence::SEQUENCE_SEQ0;
}

std::string ceph::io_exerciser::Seq0::get_name() const {
  return "Sequential reads of length " + std::to_string(length) +
         " with queue depth 1";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq0::_next() {
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

ceph::io_exerciser::Seq1::Seq1(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed) {
  select_random_object_size();
  count = 3 * obj_size;
}

Sequence ceph::io_exerciser::Seq1::get_id() const {
  return Sequence::SEQUENCE_SEQ1;
}

std::string ceph::io_exerciser::Seq1::get_name() const {
  return "Random offset, random length read/write I/O with queue depth 1";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq1::_next() {
  barrier = true;
  if (count-- == 0) {
    done = true;
    remove = true;
    return BarrierOp::generate();
  }

  uint64_t offset = rng(obj_size - 1);
  uint64_t length = 1 + rng(obj_size - 1 - offset);

  if (rng(2) != 0) {
    return SingleWriteOp::generate(offset, length);
  } else {
    return SingleReadOp::generate(offset, length);
  }
}

ceph::io_exerciser::Seq2::Seq2(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed), offset(0), length(0) {}

Sequence ceph::io_exerciser::Seq2::get_id() const {
  return Sequence::SEQUENCE_SEQ2;
}

std::string ceph::io_exerciser::Seq2::get_name() const {
  return "Permutations of offset and length read I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq2::_next() {
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

ceph::io_exerciser::Seq3::Seq3(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed), offset1(0), offset2(0) {
  set_min_object_size(2);
}

Sequence ceph::io_exerciser::Seq3::get_id() const {
  return Sequence::SEQUENCE_SEQ3;
}

std::string ceph::io_exerciser::Seq3::get_name() const {
  return "Permutations of offset 2-region 1-block read I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq3::_next() {
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

ceph::io_exerciser::Seq4::Seq4(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed), offset1(0), offset2(1) {
  set_min_object_size(3);
}

Sequence ceph::io_exerciser::Seq4::get_id() const {
  return Sequence::SEQUENCE_SEQ4;
}

std::string ceph::io_exerciser::Seq4::get_name() const {
  return "Permutations of offset 3-region 1-block read I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq4::_next() {
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
  return TripleReadOp::generate(offset1, 1, (offset1 + offset2), 1,
                                (offset1 * 2 + offset2) / 2, 1);
}

ceph::io_exerciser::Seq5::Seq5(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed),
      offset(0),
      length(1),
      doneread(false),
      donebarrier(false) {}

Sequence ceph::io_exerciser::Seq5::get_id() const {
  return Sequence::SEQUENCE_SEQ5;
}

std::string ceph::io_exerciser::Seq5::get_name() const {
  return "Permutation of length sequential writes";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq5::_next() {
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

ceph::io_exerciser::Seq6::Seq6(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed),
      offset(0),
      length(1),
      doneread(false),
      donebarrier(false) {}

Sequence ceph::io_exerciser::Seq6::get_id() const {
  return Sequence::SEQUENCE_SEQ6;
}

std::string ceph::io_exerciser::Seq6::get_name() const {
  return "Permutation of length sequential writes, different alignment";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq6::_next() {
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

ceph::io_exerciser::Seq7::Seq7(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed) {
  set_min_object_size(2);
  offset = obj_size;
}

Sequence ceph::io_exerciser::Seq7::get_id() const {
  return Sequence::SEQUENCE_SEQ7;
}

std::string ceph::io_exerciser::Seq7::get_name() const {
  return "Permutations of offset 2-region 1-block writes";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq7::_next() {
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
    offset = obj_size + 1;
    return increment_object_size();
  }
  offset--;
  if (offset == obj_size / 2) {
    return _next();
  }
  doneread = false;
  donebarrier = false;
  return DoubleReadOp::generate(offset, 1, obj_size / 2, 1);
}

ceph::io_exerciser::Seq8::Seq8(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed), offset1(0), offset2(1) {
  set_min_object_size(3);
}

Sequence ceph::io_exerciser::Seq8::get_id() const {
  return Sequence::SEQUENCE_SEQ8;
}

std::string ceph::io_exerciser::Seq8::get_name() const {
  return "Permutations of offset 3-region 1-block write I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq8::_next() {
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
  return TripleWriteOp::generate(offset1, 1, offset1 + offset2, 1,
                                 (offset1 * 2 + offset2) / 2, 1);
}

ceph::io_exerciser::Seq9::Seq9(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed), offset(0), length(0) {}

Sequence ceph::io_exerciser::Seq9::get_id() const {
  return Sequence::SEQUENCE_SEQ9;
}

std::string ceph::io_exerciser::Seq9::get_name() const {
  return "Permutations of offset and length write I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq9::_next() {
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

ceph::io_exerciser::Seq11::Seq11(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed),
      count(0),
      doneread(false),
      donebarrier(false) {}

Sequence ceph::io_exerciser::Seq11::get_id() const {
  return Sequence::SEQUENCE_SEQ11;
}

std::string ceph::io_exerciser::Seq11::get_name() const {
  return "Permutations of length sequential append I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq11::_next() {
  if (count >= 16) {
    if (!doneread) {
      if (!donebarrier) {
        donebarrier = true;
        return BarrierOp::generate();
      }
      doneread = true;
      return SingleReadOp::generate(0, obj_size * (count + 1));
    }
    doneread = false;
    donebarrier = false;
    count = 0;
    return increment_object_size();
  }
  count++;
  return SingleAppendOp::generate(obj_size);
}

ceph::io_exerciser::Seq12::Seq12(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed), count(0), overlap(1), doneread(false) {}

Sequence ceph::io_exerciser::Seq12::get_id() const {
  return Sequence::SEQUENCE_SEQ12;
}

std::string ceph::io_exerciser::Seq12::get_name() const {
  return "Permutations of length sequential overwrite+append I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq12::_next() {
  if (count >= 16) {
    if (!doneread) {
      doneread = true;
      return SingleReadOp::generate(0, obj_size * (count + 1));
    }
    doneread = false;
    count = 0;
    overlap++;
    if (overlap > obj_size) {
      overlap = 1;
      return increment_object_size();
    } else {
      create = true;
      barrier = true;
      remove = true;
      return BarrierOp::generate();
    }
  }
  count++;
  barrier = true;
  return SingleWriteOp::generate((count * obj_size) - overlap,
                                 obj_size + overlap);
}

ceph::io_exerciser::Seq13::Seq13(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(obj_size_range, seed), count(0), gap(1), doneread(false) {
  set_min_object_size(2);
}

Sequence ceph::io_exerciser::Seq13::get_id() const {
  return Sequence::SEQUENCE_SEQ13;
}

std::string ceph::io_exerciser::Seq13::get_name() const {
  return "Permutations of length sequential gap+append I/O";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq13::_next() {
  if (count >= 16) {
    if (!doneread) {
      doneread = true;
      return SingleReadOp::generate(0, obj_size * (count + 1));
    }
    doneread = false;
    count = 0;
    gap++;
    if (gap >= obj_size) {
      gap = 1;
      return increment_object_size();
    } else {
      create = true;
      barrier = true;
      remove = true;
      return BarrierOp::generate();
    }
  }
  count++;
  barrier = true;
  return SingleWriteOp::generate((count * obj_size) + gap, obj_size - gap);
}

ceph::io_exerciser::Seq14::Seq14(std::pair<int, int> obj_size_range, int seed)
    : IoSequence(std::make_pair(0, obj_size_range.second), seed),
      offset(0),
      step(1) {
  startrng = std::default_random_engine(seed);
  target_obj_size = std::max(obj_size_range.first, 3);
  if (target_obj_size > max_obj_size) {
    done = true;
  }
  setup_starts();
}

void ceph::io_exerciser::Seq14::setup_starts() {
  starts.resize(step);
  for (size_t i = 0; i < step; i++) {
    starts[i] = i;
  }
  std::shuffle(std::begin(starts), std::end(starts), startrng);
  startidx = 0;
  offset = starts[0];
}

Sequence ceph::io_exerciser::Seq14::get_id() const {
  return Sequence::SEQUENCE_SEQ14;
}

std::string ceph::io_exerciser::Seq14::get_name() const {
  return "Permutations of gap stepped write I/Os";
}

std::unique_ptr<ceph::io_exerciser::IoOp> ceph::io_exerciser::Seq14::_next() {
  if (offset >= target_obj_size) {
    if (!doneread) {
      doneread = true;
      return SingleReadOp::generate(0, current_size);
    }
    doneread = false;
    startidx++;
    if (startidx >= starts.size()) {
      step++;
      current_size = 0;
      create = true;
      barrier = true;
      remove = true;
      if (step >= target_obj_size) {
        step = 1;
        target_obj_size++;
        if (target_obj_size > max_obj_size) {
          done = true;
        }
      }
      setup_starts();
    } else {
      offset = starts[startidx];
    }
    return BarrierOp::generate();
  }
  std::unique_ptr<IoOp> r = SingleWriteOp::generate(offset, 1);
  current_size = std::max(current_size, offset + 1);
  offset += step;
  if (offset >= target_obj_size) {
    barrier = true;
  }
  return r;
}
