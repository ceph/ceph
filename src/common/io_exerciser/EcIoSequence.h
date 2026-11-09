#pragma once

#include "IoSequence.h"

namespace ceph {
namespace io_exerciser {
class EcIoSequence : public IoSequence {
 public:
  virtual bool is_supported(Sequence sequence) const override;
  static std::unique_ptr<IoSequence> generate_sequence(
      Sequence s, std::pair<int, int> obj_size_range,
      std::optional<std::pair<int, int>> km,
      std::optional<std::pair<std::string_view, std::string_view>>
          mappinglayers,
      int seed,
      bool check_consistency);

 protected:
  bool setup_inject;
  bool clear_inject;
  std::optional<uint64_t> shard_to_inject;
  InjectOpType inject_op_type;

  EcIoSequence(std::pair<int, int> obj_size_range, int seed, bool check_consistency);

  // Writes cannot be sent to injected on shard zero, so selections seperated
  // out
  void select_random_data_shard_to_inject_read_error(
      std::optional<std::pair<int, int>> km,
      std::optional<std::pair<std::string_view, std::string_view>>
          mappinglayers);
  void select_random_data_shard_to_inject_write_error(
      std::optional<std::pair<int, int>> km,
      std::optional<std::pair<std::string_view, std::string_view>>
          mappinglayers);
  void select_random_shard_to_inject_read_error(
      std::optional<std::pair<int, int>> km,
      std::optional<std::pair<std::string_view, std::string_view>>
          mappinglayers);
  void select_random_shard_to_inject_write_error(
      std::optional<std::pair<int, int>> km,
      std::optional<std::pair<std::string_view, std::string_view>>
          mappinglayers);
  void generate_random_read_inject_type();
  void generate_random_write_inject_type();
};

class ReadInjectSequence : public EcIoSequence {
 public:
  ReadInjectSequence(
      std::pair<int, int> obj_size_range, int seed, Sequence s,
      std::optional<std::pair<int, int>> km,
      std::optional<std::pair<std::string_view, std::string_view>>
          mappinglayers,
      bool check_consistency);

  Sequence get_id() const override;
  std::string get_name() const override;
  virtual std::unique_ptr<IoOp> next() override;
  std::unique_ptr<IoOp> _next() override;

 private:
  std::unique_ptr<IoSequence> child_sequence;
  std::unique_ptr<IoOp> next_op;
};

class Seq10 : public EcIoSequence {
 public:
  Seq10(std::pair<int, int> obj_size_range, int seed,
        std::optional<std::pair<int, int>> km,
        std::optional<std::pair<std::string_view, std::string_view>>
            mappinglayers,
        bool check_consistency);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  uint64_t offset;
  uint64_t length;

  bool inject_error_done;
  bool failed_write_done;
  bool read_done;
  bool clear_inject_done;
  bool successful_write_done;
  bool test_all_lengths;
  bool test_all_sizes;
};

// Seq19: for each WriteTruncate/WriteTruncate2 case, first injects a write
// error and issues a FailedWriteTruncate/FailedWriteTruncate2, reading the
// object to ensure it rolled back. Then clears the injection and issues the
// same operation successfully, reading the object to ensure it was applied.
class Seq19 : public EcIoSequence {
 public:
  Seq19(std::pair<int, int> obj_size_range, int seed,
        std::optional<std::pair<int, int>> km,
        std::optional<std::pair<std::string_view, std::string_view>>
            mappinglayers,
        bool check_consistency);

  Sequence get_id() const override;
  std::string get_name() const override;
  std::unique_ptr<IoOp> _next() override;

 private:
  enum class Stage {
    INJECT_ERROR,
    FAILED_WRITE_TRUNCATE,
    READ_AFTER_FAILURE,
    CLEAR_INJECT,
    WRITE_TRUNCATE,
    READ_AFTER_SUCCESS,
    RESET
  };

  std::vector<WriteTruncateCase> cases;
  size_t case_idx;
  Stage stage;
};
}  // namespace io_exerciser
}  // namespace ceph