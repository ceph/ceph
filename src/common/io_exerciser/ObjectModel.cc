#include "ObjectModel.h"
#include "IoOp.h"

#include <algorithm>
#include <iterator>
#include <random>

using ObjectModel = ceph::io_exerciser::ObjectModel;

ObjectModel::ObjectModel(const std::string& primary_oid, const std::string& secondary_oid,
                         uint64_t block_size, int seed, bool delete_objects)
    : Model(primary_oid, secondary_oid, block_size, delete_objects),
      primary_created(false), secondary_created(false), rng(seed) {}

int ObjectModel::get_seed(uint64_t offset) const {
  auto it = write_seeds.find(offset);
  if (it != write_seeds.end()) {
    return it->second;
  }
  ceph_assert(offset < primary_contents.size());
  return primary_contents[offset];
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

  auto verify_write_and_record_and_generate_seed =
      [&generate_random, &primary_contents = primary_contents,
       &write_seeds = write_seeds,
       &primary_created = primary_created,
       &num_io = num_io,
       &reads = reads,
       &writes = writes]<OpType opType, int N>(ReadWriteOp<opType, N> writeOp) {
        ceph_assert(primary_created);
        for (int i = 0; i < N; i++) {
          // Not allowed: write overlapping with parallel read or write
          ceph_assert(!reads.intersects(writeOp.offset[i], writeOp.length[i]));
          ceph_assert(!writes.intersects(writeOp.offset[i], writeOp.length[i]));
          writes.union_insert(writeOp.offset[i], writeOp.length[i]);
          if (writeOp.offset[i] + writeOp.length[i] > primary_contents.size()) {
            primary_contents.resize(writeOp.offset[i] + writeOp.length[i]);
          }
          for (uint64_t b = writeOp.offset[i];
               b < writeOp.offset[i] + writeOp.length[i]; b++) {
            int s = generate_random();
            primary_contents[b] = s;
            write_seeds[b] = s;
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
      write_seeds.clear();
      break;

    case OpType::Swap: {
      bool temp = primary_created;
      primary_created = secondary_created;
      secondary_created = temp;
      primary_contents.swap(secondary_contents);
      reads.clear();
      writes.clear();
      write_seeds.clear();
    } break;

    case OpType::Copy:
      ceph_assert(primary_created && secondary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      write_seeds.clear();
      // The target object may be larger than the source - however, it will be replaced by a new object rather than overwriting
      // and padding the old object. Therefore, the target object should now be the same size as the source object.
      secondary_contents.resize(primary_contents.size());
      std::copy(primary_contents.begin(), primary_contents.end(), secondary_contents.begin());
      break;

    case OpType::Create:
      ceph_assert(!primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      write_seeds.clear();
      primary_created = true;
      primary_contents.resize(static_cast<CreateOp&>(op).size);
      std::generate(primary_contents.begin(), primary_contents.end(),
                    generate_random);
      break;

    case OpType::Truncate: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      write_seeds.clear();
      auto new_size = static_cast<TruncateOp&>(op).size;
      primary_contents.resize(new_size);
      // Expanding via truncate leaves newly added blocks as 0 (zeroed).
    } break;

    case OpType::Remove: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      write_seeds.clear();
      if (!delete_objects) {
        const std::string new_primary_oid = primary_oid_base + "_" + std::to_string(++num_objects);
        set_primary_oid(new_primary_oid);
      }
      primary_created = false;
      primary_contents.resize(0);
    } break;
      
    case OpType::Read: {
      write_seeds.clear();
      SingleReadOp& readOp = static_cast<SingleReadOp&>(op);
      verify_and_record_read_op(readOp);
    } break;
    case OpType::SparseRead: {
      // SparseRead skips primary_created/size assertions because it may target
      // a pre-existing object of unknown size (e.g. interactive mode).
      // Still track ranges for concurrent-io conflict detection.
      write_seeds.clear();
      SingleSparseReadOp& readOp = static_cast<SingleSparseReadOp&>(op);
      ceph_assert(!writes.intersects(readOp.offset[0], readOp.length[0]));
      reads.union_insert(readOp.offset[0], readOp.length[0]);
      num_io++;
    } break;
    case OpType::Read2: {
      write_seeds.clear();
      DoubleReadOp& readOp = static_cast<DoubleReadOp&>(op);
      verify_and_record_read_op(readOp);
    } break;
    case OpType::Read3: {
      write_seeds.clear();
      TripleReadOp& readOp = static_cast<TripleReadOp&>(op);
      verify_and_record_read_op(readOp);
    } break;

    case OpType::Write: {
      ceph_assert(primary_created);
      SingleWriteOp& writeOp = static_cast<SingleWriteOp&>(op);
      verify_write_and_record_and_generate_seed(writeOp);
    } break;
    case OpType::Write2: {
      ceph_assert(primary_created);
      DoubleWriteOp& writeOp = static_cast<DoubleWriteOp&>(op);
      verify_write_and_record_and_generate_seed(writeOp);
    } break;
    case OpType::Write3: {
      ceph_assert(primary_created);
      TripleWriteOp& writeOp = static_cast<TripleWriteOp&>(op);
      verify_write_and_record_and_generate_seed(writeOp);
    } break;

    case OpType::Append: {
      ceph_assert(primary_created);
      SingleAppendOp& appendOp = static_cast<SingleAppendOp&>(op);
      appendOp.offset[0] = primary_contents.size();
      verify_write_and_record_and_generate_seed(appendOp);
    } break;

    case OpType::TruncateWrite: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      SingleTruncateWriteOp& truncWriteOp = static_cast<SingleTruncateWriteOp&>(op);
      // Blocks that the truncate adds have seed 0, so they read as zeros.
      primary_contents.resize(truncWriteOp.size);
      // Now apply the write operations
      for (int i = 0; i < 1; i++) {
        ceph_assert(!reads.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        ceph_assert(!writes.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        writes.union_insert(truncWriteOp.offset[i], truncWriteOp.length[i]);
        if (truncWriteOp.offset[i] + truncWriteOp.length[i] > primary_contents.size()) {
          primary_contents.resize(truncWriteOp.offset[i] + truncWriteOp.length[i]);
        }
        for (uint64_t b = truncWriteOp.offset[i];
             b < truncWriteOp.offset[i] + truncWriteOp.length[i]; b++) {
          int s = generate_random();
          primary_contents[b] = s;
          write_seeds[b] = s;
        }
      }
      num_io++;
    } break;
    case OpType::TruncateWrite2: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      DoubleTruncateWriteOp& truncWriteOp = static_cast<DoubleTruncateWriteOp&>(op);
      // Blocks that the truncate adds have seed 0, so they read as zeros.
      primary_contents.resize(truncWriteOp.size);
      // Now apply the write operations
      for (int i = 0; i < 2; i++) {
        ceph_assert(!reads.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        ceph_assert(!writes.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        writes.union_insert(truncWriteOp.offset[i], truncWriteOp.length[i]);
        if (truncWriteOp.offset[i] + truncWriteOp.length[i] > primary_contents.size()) {
          primary_contents.resize(truncWriteOp.offset[i] + truncWriteOp.length[i]);
        }
        for (uint64_t b = truncWriteOp.offset[i];
             b < truncWriteOp.offset[i] + truncWriteOp.length[i]; b++) {
          int s = generate_random();
          primary_contents[b] = s;
          write_seeds[b] = s;
        }
      }
      num_io++;
    } break;
    case OpType::TruncateWrite3: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      TripleTruncateWriteOp& truncWriteOp = static_cast<TripleTruncateWriteOp&>(op);
      // Blocks that the truncate adds have seed 0, so they read as zeros.
      primary_contents.resize(truncWriteOp.size);
      // Now apply the write operations
      for (int i = 0; i < 3; i++) {
        ceph_assert(!reads.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        ceph_assert(!writes.intersects(truncWriteOp.offset[i], truncWriteOp.length[i]));
        writes.union_insert(truncWriteOp.offset[i], truncWriteOp.length[i]);
        if (truncWriteOp.offset[i] + truncWriteOp.length[i] > primary_contents.size()) {
          primary_contents.resize(truncWriteOp.offset[i] + truncWriteOp.length[i]);
        }
        for (uint64_t b = truncWriteOp.offset[i];
             b < truncWriteOp.offset[i] + truncWriteOp.length[i]; b++) {
          int s = generate_random();
          primary_contents[b] = s;
          write_seeds[b] = s;
        }
      }
      num_io++;
    } break;

    case OpType::FailedWrite: {
      ceph_assert(primary_created);
      write_seeds.clear();
      SingleWriteOp& writeOp = static_cast<SingleWriteOp&>(op);
      verify_failed_write_and_record(writeOp);
    } break;
    case OpType::FailedWrite2: {
      ceph_assert(primary_created);
      write_seeds.clear();
      DoubleWriteOp& writeOp = static_cast<DoubleWriteOp&>(op);
      verify_failed_write_and_record(writeOp);
    } break;
    case OpType::FailedWrite3: {
      ceph_assert(primary_created);
      write_seeds.clear();
      TripleWriteOp& writeOp = static_cast<TripleWriteOp&>(op);
      verify_failed_write_and_record(writeOp);
    } break;
    case OpType::WriteTruncate: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      SingleWriteTruncateOp& writeTruncOp = static_cast<SingleWriteTruncateOp&>(op);
      ceph_assert(!reads.intersects(writeTruncOp.offset, writeTruncOp.length));
      ceph_assert(!writes.intersects(writeTruncOp.offset, writeTruncOp.length));
      writes.union_insert(writeTruncOp.offset, writeTruncOp.length);
      if (writeTruncOp.offset + writeTruncOp.length > primary_contents.size()) {
        primary_contents.resize(writeTruncOp.offset + writeTruncOp.length);
      }
      for (uint64_t b = writeTruncOp.offset;
           b < writeTruncOp.offset + writeTruncOp.length; b++) {
        int s = generate_random();
        primary_contents[b] = s;
        write_seeds[b] = s;
      }
      primary_contents.resize(writeTruncOp.size[0]);
      num_io++;
    } break;
    case OpType::WriteTruncate2: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      DoubleWriteTruncateOp& writeTruncOp = static_cast<DoubleWriteTruncateOp&>(op);
      ceph_assert(!reads.intersects(writeTruncOp.offset, writeTruncOp.length));
      ceph_assert(!writes.intersects(writeTruncOp.offset, writeTruncOp.length));
      writes.union_insert(writeTruncOp.offset, writeTruncOp.length);
      if (writeTruncOp.offset + writeTruncOp.length > primary_contents.size()) {
        primary_contents.resize(writeTruncOp.offset + writeTruncOp.length);
      }
      for (uint64_t b = writeTruncOp.offset;
           b < writeTruncOp.offset + writeTruncOp.length; b++) {
        int s = generate_random();
        primary_contents[b] = s;
        write_seeds[b] = s;
      }
      for (int i = 0; i < 2; i++) {
        primary_contents.resize(writeTruncOp.size[i]);
      }
      num_io++;
    } break;
    case OpType::FailedWriteTruncate: {
      ceph_assert(primary_created);
      write_seeds.clear();
      SingleFailedWriteTruncateOp& writeTruncOp = static_cast<SingleFailedWriteTruncateOp&>(op);
      ceph_assert(!reads.intersects(writeTruncOp.offset, writeTruncOp.length));
      ceph_assert(!writes.intersects(writeTruncOp.offset, writeTruncOp.length));
      writes.union_insert(writeTruncOp.offset, writeTruncOp.length);
      ceph_assert(writeTruncOp.offset + writeTruncOp.length <= primary_contents.size());
      num_io++;
    } break;
    case OpType::FailedWriteTruncate2: {
      ceph_assert(primary_created);
      write_seeds.clear();
      DoubleFailedWriteTruncateOp& writeTruncOp = static_cast<DoubleFailedWriteTruncateOp&>(op);
      ceph_assert(!reads.intersects(writeTruncOp.offset, writeTruncOp.length));
      ceph_assert(!writes.intersects(writeTruncOp.offset, writeTruncOp.length));
      writes.union_insert(writeTruncOp.offset, writeTruncOp.length);
      ceph_assert(writeTruncOp.offset + writeTruncOp.length <= primary_contents.size());
      num_io++;
    } break;
    default:
      break;
  }
}
