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

  switch (op.op) {
  case OpType::BARRIER:
    reads.clear();
    writes.clear();
    break;

  case OpType::CREATE:
    ceph_assert(!created);
    ceph_assert(reads.empty());
    ceph_assert(writes.empty());
    created = true;
    contents.resize(op.length1);
    std::generate(std::execution::seq, contents.begin(), contents.end(),
                  generate_random);
    break;

  case OpType::REMOVE:
    ceph_assert(created);
    ceph_assert(reads.empty());
    ceph_assert(writes.empty());
    created = false;
    contents.resize(0);
    break;

  case OpType::READ3:
    ceph_assert(created);
    ceph_assert(op.offset3 + op.length3 <= contents.size());
    // Not allowed: read overlapping with parallel write
    ceph_assert(!writes.intersects(op.offset3, op.length3));
    reads.union_insert(op.offset3, op.length3);
    [[fallthrough]];

  case OpType::READ2:
    ceph_assert(created);
    ceph_assert(op.offset2 + op.length2 <= contents.size());
    // Not allowed: read overlapping with parallel write
    ceph_assert(!writes.intersects(op.offset2, op.length2));
    reads.union_insert(op.offset2, op.length2);
    [[fallthrough]];

  case OpType::READ:
    ceph_assert(created);
    ceph_assert(op.offset1 + op.length1 <= contents.size());
    // Not allowed: read overlapping with parallel write
    ceph_assert(!writes.intersects(op.offset1, op.length1));
    reads.union_insert(op.offset1, op.length1);
    num_io++;
    break;

  case OpType::WRITE3:
    ceph_assert(created);
    // Not allowed: write overlapping with parallel read or write
    ceph_assert(!reads.intersects(op.offset3, op.length3));
    ceph_assert(!writes.intersects(op.offset3, op.length3));
    writes.union_insert(op.offset3, op.length3);
    ceph_assert(op.offset3 + op.length3 <= contents.size());
    std::generate(std::execution::seq,
                  std::next(contents.begin(), op.offset3),
                  std::next(contents.begin(), op.offset3 + op.length3),
                  generate_random);
    [[fallthrough]];

  case OpType::WRITE2:
    ceph_assert(created);
    // Not allowed: write overlapping with parallel read or write
    ceph_assert(!reads.intersects(op.offset2, op.length2));
    ceph_assert(!writes.intersects(op.offset2, op.length2));
    writes.union_insert(op.offset2, op.length2);
    ceph_assert(op.offset2 + op.length2 <= contents.size());
    std::generate(std::execution::seq,
                  std::next(contents.begin(), op.offset2),
                  std::next(contents.begin(), op.offset2 + op.length2),
                  generate_random);
    [[fallthrough]];

  case OpType::WRITE:
    ceph_assert(created);
    // Not allowed: write overlapping with parallel read or write
    ceph_assert(!reads.intersects(op.offset1, op.length1));
    ceph_assert(!writes.intersects(op.offset1, op.length1));
    writes.union_insert(op.offset1, op.length1);
    ceph_assert(op.offset1 + op.length1 <= contents.size());
    std::generate(std::execution::seq,
                  std::next(contents.begin(), op.offset1),
                  std::next(contents.begin(), op.offset1 + op.length1),
                  generate_random);
    num_io++;
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
