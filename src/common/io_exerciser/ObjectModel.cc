#include "ObjectModel.h"
#include "IoOp.h"

#include <algorithm>
#include <execution>
#include <iterator>

#include "common/debug.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

using ObjectModel = ceph::io_exerciser::ObjectModel;

ObjectModel::ObjectModel(const std::string& primary_oid, const std::string& secondary_oid, uint64_t block_size, int seed)
    : Model(primary_oid, secondary_oid, block_size), primary_created(false), secondary_created(false) {
  rng.seed(seed);
}

int ObjectModel::get_seed(uint64_t offset) const {
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
    int64_t result = static_cast<int64_t>(rand_value % range + min);
    dout(0) << "S1 rand_value: " << rand_value << " range: " 
            << range << " result: " << result  << " addr: " << &rng << dendl;
    return result;
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
          std::generate(std::execution::seq,
                        std::next(primary_contents.begin(), writeOp.offset[i]),
                        std::next(primary_contents.begin(),
                                  writeOp.offset[i] + writeOp.length[i]),
                        generate_random);
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
      std::copy(primary_contents.begin(), primary_contents.end(), secondary_contents.begin());
      break;

    case OpType::Create:
      ceph_assert(!primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      primary_created = true;
      primary_contents.resize(static_cast<CreateOp&>(op).size);
      std::generate(std::execution::seq, primary_contents.begin(), primary_contents.end(),
                    generate_random);
      break;

    case OpType::Truncate: {
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      auto new_size = static_cast<TruncateOp&>(op).size;
      auto old_size = primary_contents.size();
      bool expand = new_size > old_size;
      primary_contents.resize(new_size);
      // Yes, truncate CAN be used to make an object bigger!
      if (expand) {
        std::generate(std::execution::seq, primary_contents.begin() + new_size, primary_contents.end(),
                      generate_random);
      }
    } break;

    case OpType::Remove:
      ceph_assert(primary_created);
      ceph_assert(reads.empty());
      ceph_assert(writes.empty());
      primary_created = false;
      primary_contents.resize(0);
      break;
      
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
    default:
      break;
  }
}
