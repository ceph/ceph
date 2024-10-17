#include "ObjectModel.h"

#include <algorithm>
#include <execution>
#include <iterator>

using ObjectModel = ceph::io_exerciser::ObjectModel;

ObjectModel::ObjectModel(const std::string& oid, uint64_t block_size, int seed) :
  Model(oid, block_size), created(false)
{
  rng.seed(seed);
}

int ObjectModel::get_seed(uint64_t offset) const
{
  ceph_assert(offset < contents.size());
  return contents[offset];
}

std::vector<int> ObjectModel::get_seed_offsets(int seed) const
{
  std::vector<int> offsets;
  for (size_t i = 0; i < contents.size(); i++)
  {
    if (contents[i] == seed)
    {
      offsets.push_back(i);
    }
  }

  return offsets;
}

std::string ObjectModel::to_string(int mask) const
{
  if (!created) {
    return "Object does not exist";
  }
  std::string result = "{";
  for (uint64_t i = 0; i < contents.size(); i++) {
    if (i != 0) {
      result += ",";
    }
    result += std::to_string(contents[i] & mask);
  }
  result += "}";
  return result;
}

bool ObjectModel::readyForIoOp(IoOp& op)
{
  return true;
}

void ObjectModel::applyIoOp(IoOp& op)
{
  auto generate_random = [&rng = rng]() {
    return rng();
  };

  switch (op.getOpType()) {
  case OpType::Barrier:
    reads.clear();
    writes.clear();
    break;

  case OpType::Create:
    ceph_assert(!created);
    ceph_assert(reads.empty());
    ceph_assert(writes.empty());
    created = true;
    contents.resize(static_cast<CreateOp&>(op).size);
    std::generate(std::execution::seq, contents.begin(), contents.end(),
                  generate_random);
    break;

  case OpType::Remove:
    ceph_assert(created);
    ceph_assert(reads.empty());
    ceph_assert(writes.empty());
    created = false;
    contents.resize(0);
    break;

  case OpType::Read3:
  {
    ceph_assert(created);
    TripleReadOp& readOp = static_cast<TripleReadOp&>(op);
    for (int i = 0; i < 3; i++)
    {
      ceph_assert(readOp.offset[i] + readOp.length[i] <= contents.size());
      // Not allowed: read overlapping with parallel write
      ceph_assert(!writes.intersects(readOp.offset[i], readOp.length[i]));
      reads.union_insert(readOp.offset[i], readOp.length[i]);
    }
    num_io++;
  }
  break;
  case OpType::Read2:
  {
    ceph_assert(created);
    DoubleReadOp& readOp = static_cast<DoubleReadOp&>(op);
    for (int i = 0; i < 2; i++)
    {
      ceph_assert(readOp.offset[i] + readOp.length[i] <= contents.size());
      // Not allowed: read overlapping with parallel write
      ceph_assert(!writes.intersects(readOp.offset[i], readOp.length[i]));
      reads.union_insert(readOp.offset[i], readOp.length[i]);
    }
    num_io++;
  }
  break;

  case OpType::Read:
  {
    ceph_assert(created);
    SingleReadOp& readOp = static_cast<SingleReadOp&>(op);
    ceph_assert(readOp.offset[0] + readOp.length[0] <= contents.size());
    // Not allowed: read overlapping with parallel write
    ceph_assert(!writes.intersects(readOp.offset[0], readOp.length[0]));
    reads.union_insert(readOp.offset[0], readOp.length[0]);
    num_io++;
  }
  break;

  case OpType::Write3:
  {
    ceph_assert(created);
    TripleWriteOp& writeOp = static_cast<TripleWriteOp&>(op);
    for (int i = 0; i < 3; i++)
    {
      // Not allowed: write overlapping with parallel read or write
      ceph_assert(!reads.intersects(writeOp.offset[i], writeOp.length[i]));
      ceph_assert(!writes.intersects(writeOp.offset[i], writeOp.length[i]));
      writes.union_insert(writeOp.offset[i], writeOp.length[i]);
      ceph_assert(writeOp.offset[i] + writeOp.length[i] <= contents.size());
      std::generate(std::execution::seq,
                    std::next(contents.begin(), writeOp.offset[i]),
                    std::next(contents.begin(), writeOp.offset[i] + writeOp.length[i]),
                    generate_random);
    }
    num_io++;
  }
  break;

  case OpType::Write2:
  {
    ceph_assert(created);
    DoubleWriteOp& writeOp = static_cast<DoubleWriteOp&>(op);
    for (int i = 0; i < 2; i++)
    {
      // Not allowed: write overlapping with parallel read or write
      ceph_assert(!reads.intersects(writeOp.offset[i], writeOp.length[i]));
      ceph_assert(!writes.intersects(writeOp.offset[i], writeOp.length[i]));
      writes.union_insert(writeOp.offset[i], writeOp.length[i]);
      ceph_assert(writeOp.offset[i] + writeOp.length[i] <= contents.size());
      std::generate(std::execution::seq,
                    std::next(contents.begin(), writeOp.offset[i]),
                    std::next(contents.begin(), writeOp.offset[i] + writeOp.length[i]),
                    generate_random);
    }
    num_io++;
  }
  break;

  case OpType::Write:
  {
    ceph_assert(created);
    SingleWriteOp& writeOp = static_cast<SingleWriteOp&>(op);
    // Not allowed: write overlapping with parallel read or write
    ceph_assert(!reads.intersects(writeOp.offset[0], writeOp.length[0]));
    ceph_assert(!writes.intersects(writeOp.offset[0], writeOp.length[0]));
    writes.union_insert(writeOp.offset[0], writeOp.length[0]);
    ceph_assert(writeOp.offset[0] + writeOp.length[0] <= contents.size());
    std::generate(std::execution::seq,
                  std::next(contents.begin(), writeOp.offset[0]),
                  std::next(contents.begin(), writeOp.offset[0] + writeOp.length[0]),
                  generate_random);

    num_io++;
  }
  break;
  default:
    break;
  }
}

void ObjectModel::encode(ceph::buffer::list& bl) const {
  ENCODE_START(1, 1, bl);
  encode(created, bl);
  if (created) {
    encode(contents, bl);
  }
  ENCODE_FINISH(bl);
}

void ObjectModel::decode(ceph::buffer::list::const_iterator& bl) {
  DECODE_START(1, bl);
  DECODE_OLDEST(1);
  decode(created, bl);
  if (created) {
    decode(contents, bl);
  } else {
    contents.resize(0);
  }
  DECODE_FINISH(bl);
}
