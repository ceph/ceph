// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "IoOp.h"
#include "include/random.h"

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

namespace ceph {
namespace io_exerciser {

enum class Sequence {
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
  SEQUENCE_SEQ10,
  SEQUENCE_SEQ11,
  SEQUENCE_SEQ12,
  SEQUENCE_SEQ13,
  SEQUENCE_SEQ14,

  SEQUENCE_END,
  SEQUENCE_BEGIN = SEQUENCE_SEQ0
};

inline Sequence operator++(Sequence& s) {
  return s = (Sequence)(((int)(s) + 1));
}

std::ostream& operator<<(std::ostream& os, const Sequence& seq);

/* I/O Sequences */

class IoSequence {
 public:
  virtual ~IoSequence() = default;

  virtual Sequence get_id() const = 0;
  virtual std::string get_name_with_seqseed() const;
  virtual std::string get_name() const = 0;
  int get_step() const;
  int get_seed() const;

  virtual Sequence getNextSupportedSequenceId() const;
  virtual std::unique_ptr<IoOp> next();

  virtual bool is_supported(Sequence sequence) const;
  static std::unique_ptr<IoSequence> generate_sequence(
      Sequence s, std::pair<int, int> obj_size_range, int seed);

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

  IoSequence(std::pair<int, int> obj_size_range, int seed);

  virtual std::unique_ptr<IoOp> _next() = 0;

  void set_min_object_size(uint64_t size);
  void set_max_object_size(uint64_t size);
  void select_random_object_size();
  std::unique_ptr<IoOp> increment_object_size();
};

class Seq0 : public IoSequence {
 public:
  Seq0(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  uint64_t offset;
  uint64_t length;
};

class Seq1 : public IoSequence {
 public:
  Seq1(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  int count;
};

class Seq2 : public IoSequence {
 public:
  Seq2(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  uint64_t offset;
  uint64_t length;
};

class Seq3 : public IoSequence {
 public:
  Seq3(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  uint64_t offset1;
  uint64_t offset2;
};

class Seq4 : public IoSequence {
 public:
  Seq4(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  uint64_t offset1;
  uint64_t offset2;
};

class Seq5 : public IoSequence {
 public:
  Seq5(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  uint64_t offset;
  uint64_t length;
  bool doneread;
  bool donebarrier;
};

class Seq6 : public IoSequence {
 public:
  Seq6(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  uint64_t offset;
  uint64_t length;
  bool doneread;
  bool donebarrier;
};

class Seq7 : public IoSequence {
 public:
  Seq7(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  uint64_t offset;
  bool doneread = true;
  bool donebarrier = false;
};

class Seq8 : public IoSequence {
 public:
  Seq8(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  uint64_t offset1;
  uint64_t offset2;
  bool doneread = true;
  bool donebarrier = false;
};

class Seq9 : public IoSequence {
 private:
  uint64_t offset;
  uint64_t length;
  bool doneread = true;
  bool donebarrier = false;

 public:
  Seq9(std::pair<int, int> obj_size_range, int seed);

      Sequence get_id() const override;
      std::string get_name() const override;
      std::unique_ptr<IoOp> _next() override;
    };

class Seq11 : public IoSequence {
 private:
  uint64_t count;
  bool doneread = true;
  bool donebarrier = false;

 public:
  Seq11(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;
};

class Seq12 : public IoSequence {
 private:
  uint64_t count;
  uint64_t overlap;
  bool doneread = true;
  bool donebarrier = false;

 public:
  Seq12(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;
};

class Seq13 : public IoSequence {
 private:
  uint64_t count;
  uint64_t gap;
  bool doneread = true;
  bool donebarrier = false;

 public:
  Seq13(std::pair<int, int> obj_size_range, int seed);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;
};

class Seq14 : public IoSequence {
 private:
  uint64_t offset;
  uint64_t step;
  uint64_t target_obj_size;
  uint64_t current_size = 0;
  std::default_random_engine startrng;
  std::vector<uint64_t> starts;
  size_t startidx;
  bool doneread = false;

 public:
  Seq14(std::pair<int, int> obj_size_range, int seed);

  void setup_starts();
  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;
};
}  // namespace io_exerciser
}  // namespace ceph
