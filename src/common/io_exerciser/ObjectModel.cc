#include "ObjectModel.h"

ceph::io_exerciser::ObjectModel::ObjectModel(const std::string oid, uint64_t block_size, int seed) :
  Model(oid, block_size), created(false)
{
  rng.seed(seed);
}

int ceph::io_exerciser::ObjectModel::get_seed(uint64_t offset) const
{
  ceph_assert(offset < contents.size());
  return contents[offset];
}

std::vector<int> ceph::io_exerciser::ObjectModel::get_seed_offsets(int seed) const
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

std::string ceph::io_exerciser::ObjectModel::to_string(int mask) const
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

bool ceph::io_exerciser::ObjectModel::readyForIoOp(IoOp& op)
{
  return true;
}

void ceph::io_exerciser::ObjectModel::applyIoOp(IoOp& op)
{
  switch (op.op) {
  case BARRIER:
    reads.clear();
    writes.clear();
    break;

  case CREATE:
    ceph_assert(!created);
    ceph_assert(reads.empty());
    ceph_assert(writes.empty());
    created = true;
    contents.resize(op.length1);
    for (uint64_t i = 0; i < contents.size(); i++) {
      contents[i] = rng();
    }
    break;

  case REMOVE:
    ceph_assert(created);
    ceph_assert(reads.empty());
    ceph_assert(writes.empty());
    created = false;
    contents.resize(0);
    break;

  case READ3:
    ceph_assert(created);
    ceph_assert(op.offset3 + op.length3 <= contents.size());
    // Not allowed: read overlapping with parallel write
    ceph_assert(!writes.intersects(op.offset3, op.length3));
    reads.union_insert(op.offset3, op.length3);
    [[fallthrough]];

  case READ2:
    ceph_assert(created);
    ceph_assert(op.offset2 + op.length2 <= contents.size());
    // Not allowed: read overlapping with parallel write
    ceph_assert(!writes.intersects(op.offset2, op.length2));
    reads.union_insert(op.offset2, op.length2);
    [[fallthrough]];

  case READ:
    ceph_assert(created);
    ceph_assert(op.offset1 + op.length1 <= contents.size());
    // Not allowed: read overlapping with parallel write
    ceph_assert(!writes.intersects(op.offset1, op.length1));
    reads.union_insert(op.offset1, op.length1);
    num_io++;
    break;
    
  case WRITE3:
    ceph_assert(created);
    // Not allowed: write overlapping with parallel read or write
    ceph_assert(!reads.intersects(op.offset3, op.length3));
    ceph_assert(!writes.intersects(op.offset3, op.length3));
    writes.union_insert(op.offset3, op.length3);
    for (uint64_t i = op.offset3; i < op.offset3 + op.length3; i++) {
      ceph_assert(i < contents.size());
      contents[i] = rng();
    }
    [[fallthrough]];

  case WRITE2:
    ceph_assert(created);
    // Not allowed: write overlapping with parallel read or write
    ceph_assert(!reads.intersects(op.offset2, op.length2));
    ceph_assert(!writes.intersects(op.offset2, op.length2));
    writes.union_insert(op.offset2, op.length2);
    for (uint64_t i = op.offset2; i < op.offset2 + op.length2; i++) {
      ceph_assert(i < contents.size());
      contents[i] = rng();
    }
    [[fallthrough]];
    
  case WRITE:
    ceph_assert(created);
    // Not allowed: write overlapping with parallel read or write
    ceph_assert(!reads.intersects(op.offset1, op.length1));
    ceph_assert(!writes.intersects(op.offset1, op.length1));
    writes.union_insert(op.offset1, op.length1);
    for (uint64_t i = op.offset1; i < op.offset1 + op.length1; i++) {
      ceph_assert(i < contents.size());
      contents[i] = rng();
    }
    num_io++;
    break;
  default:
    break;
  }
}
  
void ceph::io_exerciser::ObjectModel::encode(ceph::buffer::list& bl) const {
  ENCODE_START(1, 1, bl);
  encode(created, bl);
  if (created) {
    encode(contents, bl);
  }
  ENCODE_FINISH(bl);
}    

void ceph::io_exerciser::ObjectModel::decode(ceph::buffer::list::const_iterator& bl) {
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
