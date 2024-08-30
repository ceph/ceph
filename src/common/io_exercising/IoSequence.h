// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "IoOp.h"

/* Overview
 *
 * enum Sequence
 *   Enumeration of the different sequences
 *
 * class IoSequence
 *   Virtual class. IoSequences generate a stream of IoOPs.
 *   Sequences typically exhastively test permutations of
 *   offset and length to allow validation of code such as
 *   Erasure Coding. An IoSequence does not determine
 *   whether I/Os are issued sequentially or in parallel,
 *   it must generate barrier I/Os where operations must
 *   be serialized.
 *
 * class Seq*
 *   Implementations of IoSequence. Each class generates
 *   a different sequence of I/O.
 *
 * generate_sequence
 *   Create an IoSequence
 */

enum Sequence {
  SEQUENCE_SEQ0,
  SEQUENCE_SEQ1,
  SEQUENCE_SEQ2,
  SEQUENCE_SEQ3,
  SEQUENCE_SEQ4,
  SEQUENCE_SEQ5,
  SEQUENCE_SEQ6,
  SEQUENCE_SEQ7,
  SEQUENCE_SEQ8,
  SEQUENCE_SEQ9,
  //
  SEQUENCE_END,
  SEQUENCE_BEGIN = SEQUENCE_SEQ0
};
inline Sequence operator++( Sequence& s )
{
  return s = (Sequence)(((int)(s) + 1));
}

/* I/O Sequences */

class IoSequence {
protected:
  uint64_t min_obj_size;
  uint64_t max_obj_size;
  bool create;
  bool barrier;
  bool done;
  bool remove;
  uint64_t obj_size;
  int step;
  int seed;
  ceph::util::random_number_generator<int> rng =
    ceph::util::random_number_generator<int>();

  IoSequence(std::pair<int,int> obj_size_range, int seed) :
    min_obj_size(obj_size_range.first), max_obj_size(obj_size_range.second),
    create(true), barrier(false), done(false), remove(false),
    obj_size(min_obj_size), step(-1), seed(seed)
  {
    rng.seed(seed);
  }

  virtual std::unique_ptr<IoOp> _next() = 0;

  void select_random_object_size()
  {
    if (max_obj_size != min_obj_size) {
      obj_size = min_obj_size + rng(max_obj_size - min_obj_size);
    }
  }

  void set_min_object_size(uint64_t size)
  {
    min_obj_size = size;
    if (obj_size < size) {
      obj_size = size;
      if (obj_size > max_obj_size) {
        done = true;
      }
    }
  }

  void set_max_object_size(uint64_t size)
  {
    max_obj_size = size;
    if (obj_size > size) {
      done = true;
    }
  }

  std::unique_ptr<IoOp> increment_object_size()
  {
    obj_size++;
    if (obj_size > max_obj_size) {
      done = true;
    }
    create = true;
    barrier = true;
    remove = true;
    return IoOp::generate_barrier();
  }

public:
  virtual ~IoSequence() = default;

  std::unique_ptr<IoOp> next()
  {
    step++;
    if (remove) {
      remove = false;
      return IoOp::generate_remove();
    }
    if (barrier) {
      barrier = false;
      return IoOp::generate_barrier();
    }
    if (done) {
      return IoOp::generate_done();
    }
    if (create) {
      create = false;
      barrier = true;
      return IoOp::generate_create(obj_size);
    }
    return _next();
  }

  virtual std::string get_name() = 0;

  int get_step()
  {
    return step;
  }

  int get_seed()
  {
    return seed;
  }

  static std::unique_ptr<IoSequence> generate_sequence(
    Sequence s, std::pair<int,int> obj_size_range, int seed );
};

class Seq0: public IoSequence {
private:
  uint64_t offset;
  uint64_t length;

public:
  Seq0(std::pair<int,int> obj_size_range, int seed) :
    IoSequence(obj_size_range, seed), offset(0)
  {
    select_random_object_size();
    length = 1 + rng(obj_size - 1);
  }

  std::string get_name()
  {
    return "Sequential reads of length " + std::to_string(length) +
      " with queue depth 1 (seqseed " + std::to_string(get_seed()) + ")";
  }

  std::unique_ptr<IoOp> _next() override
  {
    std::unique_ptr<IoOp> r;
    if (offset >= obj_size) {
      done = true;
      barrier = true;
      remove = true;
      return IoOp::generate_barrier();
    }
    if (offset + length > obj_size) {
      r = IoOp::generate_read(offset, obj_size - offset);
    } else {
      r = IoOp::generate_read(offset, length);
    }
    offset += length;
    return r;
  }
};

class Seq1: public IoSequence {
private:
  int count;
  
public:
  Seq1(std::pair<int,int> obj_size_range, int seed) :
    IoSequence(obj_size_range, seed)
  {
    select_random_object_size();
    count = 3 * obj_size;
  }

  std::string get_name()
  {
    return "Random offset, random length read/write I/O with queue depth 1 (seqseed " +
      std::to_string(get_seed()) + ")";
  }

  std::unique_ptr<IoOp> _next() override
  {
    barrier = true;
    if (count-- == 0) {
      done = true;
      remove = true;
      return IoOp::generate_barrier();
    }

    uint64_t offset = rng(obj_size - 1);
    uint64_t length = 1 + rng(obj_size - 1 - offset);
    return (rng(2) != 0) ? IoOp::generate_write(offset, length) :
      IoOp::generate_read(offset, length);
  }
};
  
class Seq2: public IoSequence {
private:
  uint64_t offset;
  uint64_t length;

public:
  Seq2(std::pair<int,int> obj_size_range, int seed) :
    IoSequence(obj_size_range, seed), offset(0), length(0) {}

  std::string get_name()
  {
    return "Permutations of offset and length read I/O";
  }

  std::unique_ptr<IoOp> _next() override
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
    return IoOp::generate_read(offset, length);
  }
};

class Seq3: public IoSequence {
public:
  uint64_t offset1;
  uint64_t offset2;

  Seq3(std::pair<int,int> obj_size_range, int seed) :
    IoSequence(obj_size_range, seed), offset1(0), offset2(0)
  {
    set_min_object_size(2);
  }

  std::string get_name()
  {
    return "Permutations of offset 2-region 1-block read I/O";
  }

  std::unique_ptr<IoOp> _next() override
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
    return IoOp::generate_read2(offset1, 1, offset1 + offset2, 1);
  }
};

class Seq4: public IoSequence {
public:
  uint64_t offset1;
  uint64_t offset2;

  Seq4(std::pair<int,int> obj_size_range, int seed) :
    IoSequence(obj_size_range, seed), offset1(0), offset2(1)
  {
    set_min_object_size(3);
  }

  std::string get_name()
  {
    return "Permutations of offset 3-region 1-block read I/O";
  }

  std::unique_ptr<IoOp> _next() override
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
    return IoOp::generate_read3(offset1, 1,
				offset1 + offset2, 1,
				(offset1 * 2 + offset2)/2, 1);
  }
};

class Seq5: public IoSequence {
public:
  uint64_t offset;
  uint64_t length;
  bool doneread;
  bool donebarrier;  
  Seq5(std::pair<int,int> obj_size_range, int seed) :
    IoSequence(obj_size_range, seed), offset(0), length(1),
    doneread(false), donebarrier(false) {}

  std::string get_name()
  {
    return "Permutation of length sequential writes";
  }
  
  std::unique_ptr<IoOp> _next() override
  {
    if (offset >= obj_size) {
      if (!doneread) {
	if (!donebarrier) {
          donebarrier = true;
	  return IoOp::generate_barrier();
	}
	doneread = true;
	barrier = true;
	return IoOp::generate_read(0, obj_size);
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
    uint64_t io_len = (offset + length > obj_size) ?
      (obj_size - offset) : length;
    std::unique_ptr<IoOp> r = IoOp::generate_write(offset, io_len);
    offset += io_len;
    return r;
  }
};

class Seq6: public IoSequence {
public:
  uint64_t offset;
  uint64_t length;
  bool doneread;
  bool donebarrier;  

  Seq6(std::pair<int,int> obj_size_range, int seed) :
    IoSequence(obj_size_range, seed), offset(0), length(1),
    doneread(false), donebarrier(false) {}

  std::string get_name()
  {
    return "Permutation of length sequential writes, different alignment";
  }
  
  std::unique_ptr<IoOp> _next() override
  {
    if (offset >= obj_size) {
      if (!doneread) {
	if (!donebarrier) {
	  donebarrier = true;
	  return IoOp::generate_barrier();
	}
	doneread = true;
	barrier = true;
	return IoOp::generate_read(0, obj_size);
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
    std::unique_ptr<IoOp> r = IoOp::generate_write(offset, io_len);
    offset += io_len;
    return r;
  }
};

class Seq7: public IoSequence {
public:
  uint64_t offset;
  bool doneread = true;
  bool donebarrier = false;

  Seq7(std::pair<int,int> obj_size_range, int seed) :
    IoSequence(obj_size_range, seed)
  {
    set_min_object_size(2);
    offset = obj_size;
  }

  std::string get_name()
  {
    return "Permutations of offset 2-region 1-block writes";
  }

  std::unique_ptr<IoOp> _next() override
  {
    if (!doneread) {
      if (!donebarrier) {
	donebarrier = true;
	return IoOp::generate_barrier();
      }
      doneread = true;
      barrier = true;
      return IoOp::generate_read(0, obj_size);
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
    return IoOp::generate_write2(offset, 1, obj_size/2, 1);
  }
};

class Seq8: public IoSequence {
public:
  uint64_t offset1;
  uint64_t offset2;
  bool doneread = true;
  bool donebarrier = false;

  Seq8(std::pair<int,int> obj_size_range, int seed) :
    IoSequence(obj_size_range, seed), offset1(0), offset2(1)
  {
    set_min_object_size(3);
  }

  std::string get_name()
  {
    return "Permutations of offset 3-region 1-block write I/O";
  }

  std::unique_ptr<IoOp> _next() override
  {
    if (!doneread) {
      if (!donebarrier) {
	donebarrier = true;
	return IoOp::generate_barrier();
      }
      doneread = true;
      barrier = true;
      return IoOp::generate_read(0, obj_size);
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
    return IoOp::generate_write3(offset1, 1,
				 offset1 + offset2, 1,
				 (offset1 * 2 + offset2)/2, 1);
  }
};

class Seq9: public IoSequence {
private:
  uint64_t offset;
  uint64_t length;
  bool doneread = true;
  bool donebarrier = false;

public:
  Seq9(std::pair<int,int> obj_size_range, int seed) :
    IoSequence(obj_size_range, seed), offset(0), length(0) {}

  std::string get_name()
  {
    return "Permutations of offset and length write I/O";
  }

  std::unique_ptr<IoOp> _next() override
  {
    if (!doneread) {
      if (!donebarrier) {
	donebarrier = true;
	return IoOp::generate_barrier();
      }
      doneread = true;
      barrier = true;
      return IoOp::generate_read(0, obj_size);
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
    return IoOp::generate_write(offset, length);
  }
};

std::unique_ptr<IoSequence> IoSequence::generate_sequence(
  Sequence s, std::pair<int,int> obj_size_range, int seed )
{
  switch (s) {
  case SEQUENCE_SEQ0:
    return std::make_unique<Seq0>(obj_size_range, seed);
  case SEQUENCE_SEQ1:
    return std::make_unique<Seq1>(obj_size_range, seed);
  case SEQUENCE_SEQ2:
    return std::make_unique<Seq2>(obj_size_range, seed);
  case SEQUENCE_SEQ3:
    return std::make_unique<Seq3>(obj_size_range, seed);
  case SEQUENCE_SEQ4:
    return std::make_unique<Seq4>(obj_size_range, seed);
  case SEQUENCE_SEQ5:
    return std::make_unique<Seq5>(obj_size_range, seed);
  case SEQUENCE_SEQ6:
    return std::make_unique<Seq6>(obj_size_range, seed);
  case SEQUENCE_SEQ7:
    return std::make_unique<Seq7>(obj_size_range, seed);
  case SEQUENCE_SEQ8:
    return std::make_unique<Seq8>(obj_size_range, seed);
  case SEQUENCE_SEQ9:
    return std::make_unique<Seq9>(obj_size_range, seed);
  default:
    break;
  }
  return nullptr;
}
