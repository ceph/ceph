#pragma once

#include "Model.h"

/* Overview
 *
 * class ObjectModel
 *   An IoExerciser. Tracks the data stored in an object, applies
 *   IoOp's to update the model. Polices that I/Os that are
 *   permitted to run in parallel do not break rules. Provides
 *   interface to query state of object. State can be encoded
 *   and decoded
 *
 */

namespace ceph {
namespace io_exerciser {
/* Model of an object to track its data contents */

class ObjectModel : public Model {
 private:
  bool created;
  std::vector<int> contents;
  ceph::util::random_number_generator<int> rng =
      ceph::util::random_number_generator<int>();

  // Track read and write I/Os that can be submitted in
  // parallel to detect violations:
  //
  // * Read may not overlap with a parallel write
  // * Write may not overlap with a parallel read or write
  // * Create / remove may not be in parallel with read or write
  //
  // Fix broken test cases by adding barrier ops to restrict
  // I/O exercisers from issuing conflicting ops in parallel
  interval_set<uint64_t> reads;
  interval_set<uint64_t> writes;

 public:
  ObjectModel(const std::string& oid, uint64_t block_size, int seed);

  int get_seed(uint64_t offset) const;
  std::vector<int> get_seed_offsets(int seed) const;

  std::string to_string(int mask = -1) const;

  bool readyForIoOp(IoOp& op);
  void applyIoOp(IoOp& op);

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
};
}  // namespace io_exerciser
}  // namespace ceph