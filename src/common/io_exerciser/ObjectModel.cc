#include "ObjectModel.h"
#include "IoOp.h"

#include <algorithm>
#include <map>
#include <execution>
#include <iterator>
#include <random>

using ObjectModel = ceph::io_exerciser::ObjectModel;

ObjectModel::ObjectModel(const std::string& primary_oid, const std::string& secondary_oid,
                         uint64_t block_size, int seed, bool is_replicated_pool,
                         bool delete_objects)
    : Model(primary_oid, secondary_oid, block_size, delete_objects),
      primary_created(false),
      secondary_created(false),
      allocation_mode(is_replicated_pool ? AllocationMode::Replicated
                                         : AllocationMode::ErasureCoded),
      rng(seed) {}

int ObjectModel::get_seed(uint64_t offset) const {
  ceph_assert(offset < primary_contents.size());
  return primary_contents[offset];
}

uint64_t ObjectModel::get_primary_size() const {
  return primary_contents.size();
}

std::vector<int> ObjectModel::get_seed_offsets(int seed) const {
  std::vector<int> offsets;
  for (size_t i = 0; i < primary_contents.size(); i++) {
    if (primary_contents[i] == seed) {
      offsets.push_back(i);
    }
  }

  return offsets;
}

std::map<uint64_t, uint64_t> ObjectModel::get_expected_extent_map() const {
  ceph_assert(primary_created);
  std::map<uint64_t, uint64_t> extent_map;
  uint64_t size = primary_allocated.size();
  uint64_t i = 0;
  while (i < size) {
    if (primary_allocated[i]) {
      uint64_t start = i;
      while (i < size && primary_allocated[i]) {
        ++i;
      }
      extent_map.emplace(start * block_size, (i - start) * block_size);
    } else {
      ++i;
    }
  }
  return extent_map;
}

std::string ObjectModel::to_string(int mask) const {
  if (!primary_created) {
    return "Object does not exist";
  }
  std::string result = "{";
  for (uint64_t i = 0; i < primary_contents.size(); i++) {
    if (i != 0) {
      result += ",";
    }
    result += std::to_string(primary_contents[i] & mask);
  }
  result += "}";
  return result;
}

bool ObjectModel::readyForIoOp(IoOp& op) { return true; }

void ObjectModel::applyIoOp(IoOp& op) {
  auto generate_random = [&rng = rng]() {
    constexpr int64_t min = 1;
    constexpr int64_t max = static_cast<int64_t>(std::numeric_limits<int>::max());
    constexpr uint64_t range = static_cast<uint64_t>(max - min + 1);
    uint64_t rand_value = rng();
    return static_cast<int64_t>(rand_value % range + min);
  };

  auto verify_and_record_read_op =
      [&primary_contents = primary_contents,
       &primary_created = primary_created,
       &num_io = num_io,
       &reads = reads,
       &writes = writes]<OpType opType, int N>(ReadWriteOp<opType, N>& readOp) {
        ceph_assert(primary_created);
        for (int i = 0; i < N; i++) {
          ceph_assert(readOp.offset[i] + readOp.length[i] <= primary_contents.size());
          // Not allowed: read overlapping with parallel write
          ceph_assert(!writes.intersects(readOp.offset[i], readOp.length[i]));
          reads.union_insert(readOp.offset[i], readOp.length[i]);
        }
        num_io++;
      };

  auto ensure_size = [&primary_contents = primary_contents,
                      &primary_allocated = primary_allocated](uint64_t size) {
    if (size > primary_contents.size()) {
      primary_contents.resize(size);
      primary_allocated.resize(size, false);
    }
  };

  const uint64_t ec_alloc_unit = std::max<uint64_t>(1, 4096 / block_size);
  const AllocationMode allocation_mode_local = allocation_mode;

  auto mark_allocated = [&primary_allocated = primary_allocated,
                         allocation_mode = allocation_mode,
                         ec_alloc_unit](uint64_t offset, uint64_t length) {
    uint64_t alloc_start = offset;
    uint64_t alloc_end = offset + length;
    if (allocation_mode == AllocationMode::ErasureCoded) {
      alloc_start = p2align(offset, ec_alloc_unit);
      alloc_end = p2roundup(offset + length, ec_alloc_unit);
    }
    alloc_end = std::min(alloc_end, primary_allocated.size());
    std::fill(std::next(primary_allocated.begin(), alloc_start),
              std::next(primary_allocated.begin(), alloc_end),
              true);
  };

  auto zero_with_allocation =
      [&primary_contents = primary_contents,
       &primary_allocated = primary_allocated,
       allocation_mode = allocation_mode,
       ec_alloc_unit](uint64_t zero_start, uint64_t zero_end) {
        if (zero_start >= primary_contents.size()) {
          return;
        }

        zero_end = std::min<uint64_t>(zero_end, primary_contents.size());
        if (allocation_mode == AllocationMode::Replicated) {
          std::fill(std::next(primary_contents.begin(), zero_start),
                    std::next(primary_contents.begin(), zero_end),
                    0);
          std::fill(std::next(primary_allocated.begin(), zero_start),
                    std::next(primary_allocated.begin(), zero_end),
                    false);
          return;
        }

        const uint64_t hole_start = p2roundup(zero_start, ec_alloc_unit);
        const uint64_t hole_end = p2align(zero_end, ec_alloc_unit);

        std::fill(std::next(primary_contents.begin(), zero_start),
                  std::next(primary_contents.begin(), hole_start),
                  0);
        std::fill(std::next(primary_allocated.begin(), zero_start),
                  std::next(primary_allocated.begin(), hole_start),
                  true);
        std::fill(std::next(primary_contents.begin(), hole_end),
                  std::next(primary_contents.begin(), zero_end),
                  0);
        std::fill(std::next(primary_allocated.begin(), hole_end),
                  std::next(primary_allocated.begin(), zero_end),
                  true);
        std::fill(std::next(primary_contents.begin(), hole_start),
                  std::next(primary_contents.begin(), hole_end),
                  0);
        std::fill(std::next(primary_allocated.begin(), hole_start),
                  std::next(primary_allocated.begin(), hole_end),
                  false);
      };

  auto verify_write_and_record_and_generate_seed =
      [&generate_random, &primary_contents = primary_contents,
       &primary_created = primary_created,
       &num_io = num_io,
       &reads = reads,
       &writes = writes,
       &ensure_size,
       &mark_allocated,
       allocation_mode_local,
       ec_alloc_unit]<OpType opType, int N>(ReadWriteOp<opType, N> writeOp) {
         // Auto-create the object on first write, mirroring librados semantics.
         if (!primary_created) {
           primary_created = true;
         }
         for (int i = 0; i < N; i++) {
           ceph_assert(!reads.intersects(writeOp.offset[i], writeOp.length[i]));
           ceph_assert(!writes.intersects(writeOp.offset[i], writeOp.length[i]));
           writes.union_insert(writeOp.offset[i], writeOp.length[i]);
           uint64_t alloc_end =
               (allocation_mode_local == AllocationMode::ErasureCoded)
                   ? p2roundup(writeOp.offset[i] + writeOp.length[i],
                               ec_alloc_unit)
                   : writeOp.offset[i] + writeOp.length[i];
           ensure_size(alloc_end);
           std::generate(std::execution::seq,
                         std::next(primary_contents.begin(), writeOp.offset[i]),
                         std::next(primary_contents.begin(),
                                   writeOp.offset[i] + writeOp.length[i]),
                         generate_random);
           mark_allocated(writeOp.offset[i], writeOp.length[i]);
         }
         num_io++;
       };

  auto verify_zero_and_record =
      [&primary_created = primary_created,
       &primary_contents = primary_contents,
       &primary_allocated = primary_allocated,
       &num_io = num_io,
       &reads = reads,
       &writes = writes,
       &zero_with_allocation]<OpType opType, int N>(ReadWriteOp<opType, N> writeOp) {
         if (!primary_created) {
           primary_created = true;
         }
         for (int i = 0; i < N; i++) {
           ceph_assert(!reads.intersects(writeOp.offset[i], writeOp.length[i]));
           ceph_assert(!writes.intersects(writeOp.offset[i], writeOp.length[i]));
           writes.union_insert(writeOp.offset[i], writeOp.length[i]);
           zero_with_allocation(writeOp.offset[i], writeOp.offset[i] + writeOp.length[i]);
           if (writeOp.offset[i] + writeOp.length[i] >= primary_contents.size()) {
             primary_contents.resize(writeOp.offset[i]);
             primary_allocated.resize(writeOp.offset[i]);
           }
         }
         num_io++;
       };

  auto verify_failed_write_and_record =
      [&primary_contents = primary_contents,
       &primary_created = primary_created,
       &num_io = num_io,
       &reads = reads,
       &writes = writes]<OpType opType, int N>(ReadWriteOp<opType, N> writeOp) {
        // Ensure write should still be valid, even though we are expecting OSD
        // failure
        ceph_assert(primary_created);
        for (int i = 0; i < N; i++) {
          // Not allowed: write overlapping with parallel read or write
          ceph_assert(!reads.intersects(writeOp.offset[i], writeOp.length[i]));
          ceph_assert(!writes.intersects(writeOp.offset[i], writeOp.length[i]));
          writes.union_insert(writeOp.offset[i], writeOp.length[i]);
          ceph_assert(writeOp.offset[i] + writeOp.length[i] <= primary_contents.size());
        }
        num_io++;
      };

  switch (op.getOpType()) {
    case OpType::Barrier:
      reads.clear();
      writes.clear();
      break;

    case OpType::Swap: {
      bool temp = primary_created;
      primary_created = secondary_created;
      secondary_created = temp;
      primary_contents.swap(secondary_contents);
      primary_allocated.swap(secondary_allocated);
      reads.clear();
      writes.clear();
    } break;

    case OpType::Copy:
      ceph_assert(primary_created && secondary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      // The target object may be larger than the source - however, it will be replaced by a new object rather than overwriting
      // and padding the old object. Therefore, the target object should now be the same size as the source object.
      secondary_contents.resize(primary_contents.size());
      secondary_allocated.resize(primary_allocated.size());
      std::copy(primary_contents.begin(), primary_contents.end(), secondary_contents.begin());
      std::copy(primary_allocated.begin(), primary_allocated.end(), secondary_allocated.begin());
      break;

    case OpType::Create:
      ceph_assert(!primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      primary_created = true;
      primary_contents.resize(static_cast<CreateOp&>(op).size);
      primary_allocated.resize(static_cast<CreateOp&>(op).size, true);
      std::generate(std::execution::seq, primary_contents.begin(), primary_contents.end(),
                    generate_random);
      break;

    case OpType::Truncate: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      auto new_size = static_cast<TruncateOp&>(op).size;
      if (allocation_mode == AllocationMode::ErasureCoded && new_size < primary_allocated.size()) {
        const uint64_t alloc_end = std::min(
            p2roundup(new_size, ec_alloc_unit),
            primary_allocated.size());
        std::vector<bool> tail_alloc;
        if (new_size < alloc_end) {
          tail_alloc.assign(
              primary_allocated.begin() + new_size,
              primary_allocated.begin() + alloc_end);
        }
        primary_contents.resize(new_size);
        primary_allocated.resize(new_size, false);
        if (!tail_alloc.empty()) {
          primary_contents.resize(alloc_end, 0);
          primary_allocated.resize(alloc_end, false);
          for (size_t i = 0; i < tail_alloc.size(); ++i) {
            primary_allocated[new_size + i] = tail_alloc[i];
          }
        }
      } else {
        primary_contents.resize(new_size);
        primary_allocated.resize(new_size, false);
      }
    } break;

    case OpType::Remove: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      if (!delete_objects) {
        const std::string new_primary_oid = primary_oid_base + "_" + std::to_string(++num_objects);
        set_primary_oid(new_primary_oid);
      }
      primary_created = false;
      primary_contents.resize(0);
      primary_allocated.resize(0);
    } break;
      
    case OpType::Read: {
      SingleReadOp& readOp = static_cast<SingleReadOp&>(op);
      verify_and_record_read_op(readOp);
    } break;
    case OpType::Read2: {
      DoubleReadOp& readOp = static_cast<DoubleReadOp&>(op);
      verify_and_record_read_op(readOp);
    } break;
    case OpType::Read3: {
      TripleReadOp& readOp = static_cast<TripleReadOp&>(op);
      verify_and_record_read_op(readOp);
    } break;

    case OpType::Write: {
      SingleWriteOp& writeOp = static_cast<SingleWriteOp&>(op);
      verify_write_and_record_and_generate_seed(writeOp);
    } break;
    case OpType::Write2: {
      DoubleWriteOp& writeOp = static_cast<DoubleWriteOp&>(op);
      verify_write_and_record_and_generate_seed(writeOp);
    } break;
    case OpType::Write3: {
      TripleWriteOp& writeOp = static_cast<TripleWriteOp&>(op);
      verify_write_and_record_and_generate_seed(writeOp);
    } break;

    case OpType::Append: {
      SingleAppendOp& appendOp = static_cast<SingleAppendOp&>(op);
      appendOp.offset[0] = primary_contents.size();
      verify_write_and_record_and_generate_seed(appendOp);
    } break;

    case OpType::TruncateWrite: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      SingleTruncateWriteOp& truncWriteOp = static_cast<SingleTruncateWriteOp&>(op);
      auto new_size = truncWriteOp.size;
      auto old_size = primary_contents.size();
      bool expand = new_size > old_size;
      primary_contents.resize(new_size);
      if (expand) {
        std::generate(std::execution::seq, primary_contents.begin() + old_size,
                      primary_contents.end(), generate_random);
      }
      // Now apply the write operations
      for (int i = 0; i < 1; i++) {
        ceph_assert(!reads.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        ceph_assert(!writes.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        writes.union_insert(truncWriteOp.offset[i], truncWriteOp.length[i]);
        if (truncWriteOp.offset[i] + truncWriteOp.length[i] > primary_contents.size()) {
          primary_contents.resize(truncWriteOp.offset[i] + truncWriteOp.length[i]);
        }
        std::generate(std::execution::seq,
                      std::next(primary_contents.begin(), truncWriteOp.offset[i]),
                      std::next(primary_contents.begin(),
                                truncWriteOp.offset[i] + truncWriteOp.length[i]),
                      generate_random);
      }
      num_io++;
    } break;
    case OpType::TruncateWrite2: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      DoubleTruncateWriteOp& truncWriteOp = static_cast<DoubleTruncateWriteOp&>(op);
      auto new_size = truncWriteOp.size;
      auto old_size = primary_contents.size();
      bool expand = new_size > old_size;
      primary_contents.resize(new_size);
      if (expand) {
        std::generate(std::execution::seq, primary_contents.begin() + old_size,
                      primary_contents.end(), generate_random);
      }
      // Now apply the write operations
      for (int i = 0; i < 2; i++) {
        ceph_assert(!reads.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        ceph_assert(!writes.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        writes.union_insert(truncWriteOp.offset[i], truncWriteOp.length[i]);
        if (truncWriteOp.offset[i] + truncWriteOp.length[i] > primary_contents.size()) {
          primary_contents.resize(truncWriteOp.offset[i] + truncWriteOp.length[i]);
        }
        std::generate(std::execution::seq,
                      std::next(primary_contents.begin(), truncWriteOp.offset[i]),
                      std::next(primary_contents.begin(),
                                truncWriteOp.offset[i] + truncWriteOp.length[i]),
                      generate_random);
      }
      num_io++;
    } break;
    case OpType::TruncateWrite3: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      TripleTruncateWriteOp& truncWriteOp = static_cast<TripleTruncateWriteOp&>(op);
      auto new_size = truncWriteOp.size;
      auto old_size = primary_contents.size();
      bool expand = new_size > old_size;
      primary_contents.resize(new_size);
      if (expand) {
        std::generate(std::execution::seq, primary_contents.begin() + old_size,
                      primary_contents.end(), generate_random);
      }
      // Now apply the write operations
      for (int i = 0; i < 3; i++) {
        ceph_assert(!reads.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        ceph_assert(!writes.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        writes.union_insert(truncWriteOp.offset[i], truncWriteOp.length[i]);
        if (truncWriteOp.offset[i] + truncWriteOp.length[i] > primary_contents.size()) {
          primary_contents.resize(truncWriteOp.offset[i] + truncWriteOp.length[i]);
        }
        std::generate(std::execution::seq,
                      std::next(primary_contents.begin(), truncWriteOp.offset[i]),
                      std::next(primary_contents.begin(),
                                truncWriteOp.offset[i] + truncWriteOp.length[i]),
                      generate_random);
      }
      num_io++;
    } break;

    case OpType::FailedWrite: {
      ceph_assert(primary_created);
      SingleWriteOp& writeOp = static_cast<SingleWriteOp&>(op);
      verify_failed_write_and_record(writeOp);
    } break;
    case OpType::FailedWrite2: {
      ceph_assert(primary_created);
      DoubleWriteOp& writeOp = static_cast<DoubleWriteOp&>(op);
      verify_failed_write_and_record(writeOp);
    } break;
    case OpType::FailedWrite3: {
      ceph_assert(primary_created);
      TripleWriteOp& writeOp = static_cast<TripleWriteOp&>(op);
      verify_failed_write_and_record(writeOp);
    } break;
    case OpType::WriteZeroData: {
      WriteZeroDataOp& wzdOp = static_cast<WriteZeroDataOp&>(op);
      if (!primary_created) {
        primary_created = true;
      }
      ceph_assert(!reads.intersects(wzdOp.offset[0], wzdOp.length[0]));
      ceph_assert(!writes.intersects(wzdOp.offset[0], wzdOp.length[0]));
      writes.union_insert(wzdOp.offset[0], wzdOp.length[0]);
      {
        uint64_t alloc_end =
            (allocation_mode == AllocationMode::ErasureCoded)
                ? p2roundup(wzdOp.offset[0] + wzdOp.length[0], ec_alloc_unit)
                : wzdOp.offset[0] + wzdOp.length[0];
        ensure_size(alloc_end);
      }
      std::fill(std::next(primary_contents.begin(), wzdOp.offset[0]),
                std::next(primary_contents.begin(),
                          wzdOp.offset[0] + wzdOp.length[0]),
                0);
      mark_allocated(wzdOp.offset[0], wzdOp.length[0]);
      num_io++;
    } break;
    case OpType::Zero: {
      ZeroOp& zeroOp = static_cast<ZeroOp&>(op);
      verify_zero_and_record(zeroOp);
    } break;
    case OpType::Zero2: {
      DoubleZeroOp& zeroOp = static_cast<DoubleZeroOp&>(op);
      verify_zero_and_record(zeroOp);
    } break;
    case OpType::WriteAndZero: {
      if (!primary_created) {
        primary_created = true;
      }
      WriteAndZeroOp& wzOp = static_cast<WriteAndZeroOp&>(op);
      ceph_assert(!reads.intersects(wzOp.write_offset, wzOp.write_length));
      ceph_assert(!writes.intersects(wzOp.write_offset, wzOp.write_length));
      ceph_assert(!reads.intersects(wzOp.zero_offset, wzOp.zero_length));
      ceph_assert(!writes.intersects(wzOp.zero_offset, wzOp.zero_length));
      writes.union_insert(wzOp.write_offset, wzOp.write_length);
      writes.union_insert(wzOp.zero_offset, wzOp.zero_length);
      uint64_t write_end = wzOp.write_offset + wzOp.write_length;
      {
        uint64_t alloc_end =
            (allocation_mode == AllocationMode::ErasureCoded)
                ? p2roundup(write_end, ec_alloc_unit)
                : write_end;
        ensure_size(alloc_end);
      }
      std::generate(std::execution::seq,
                    std::next(primary_contents.begin(), wzOp.write_offset),
                    std::next(primary_contents.begin(), write_end),
                    generate_random);
      mark_allocated(wzOp.write_offset, wzOp.write_length);
      zero_with_allocation(wzOp.zero_offset, wzOp.zero_offset + wzOp.zero_length);
      if (wzOp.zero_offset + wzOp.zero_length >= primary_contents.size()) {
        primary_contents.resize(wzOp.zero_offset);
        primary_allocated.resize(wzOp.zero_offset);
      }
      num_io++;
    } break;
    case OpType::ZeroAndTruncate: {
      if (!primary_created) {
        primary_created = true;
      }
      ZeroAndTruncateOp& ztOp = static_cast<ZeroAndTruncateOp&>(op);
      ceph_assert(!reads.intersects(ztOp.zero_offset, ztOp.zero_length));
      ceph_assert(!writes.intersects(ztOp.zero_offset, ztOp.zero_length));
      writes.union_insert(ztOp.zero_offset, ztOp.zero_length);
      ensure_size(ztOp.zero_offset + ztOp.zero_length);
      if (ztOp.zero_offset + ztOp.zero_length >= primary_contents.size()) {
        primary_contents.resize(ztOp.zero_offset);
        primary_allocated.resize(ztOp.zero_offset);
      } else {
        zero_with_allocation(ztOp.zero_offset, ztOp.zero_offset + ztOp.zero_length);
      }
      primary_contents.resize(ztOp.truncate_size);
      primary_allocated.resize(ztOp.truncate_size, false);
      num_io++;
    } break;
    case OpType::Mapext: {
      MapextOp& mop = static_cast<MapextOp&>(op);
      ceph_assert(primary_created);
      if (mop.length[0] > 0) {
        ceph_assert(!writes.intersects(mop.offset[0], mop.length[0]));
        reads.union_insert(mop.offset[0], mop.length[0]);
      }
      num_io++;
    } break;
    default:
      break;
  }
}
