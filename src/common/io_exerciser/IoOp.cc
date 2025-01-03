#include "IoOp.h"

using IoOp = ceph::io_exerciser::IoOp;

IoOp::IoOp( OpType op,
            uint64_t offset1, uint64_t length1,
            uint64_t offset2, uint64_t length2,
            uint64_t offset3, uint64_t length3) :
  op(op),
  offset1(offset1), length1(length1),
  offset2(offset2), length2(length2),
  offset3(offset3), length3(length3)
{

}

std::string IoOp::value_to_string(uint64_t v) const
{
  if (v < 1024 || (v % 1024) != 0) {
    return std::to_string(v);
  }else if (v < 1024*1024 || (v % (1024 * 1024)) != 0 ) {
    return std::to_string(v / 1024) + "K";
  }else{
    return std::to_string(v / 1024 / 1024) + "M";
  }
}

std::unique_ptr<IoOp> IoOp
  ::generate_done() {

    return std::make_unique<IoOp>(OpType::Done);
}

std::unique_ptr<IoOp> IoOp
  ::generate_barrier() {

  return std::make_unique<IoOp>(OpType::BARRIER);
}

std::unique_ptr<IoOp> IoOp
  ::generate_create(uint64_t size) {

  return std::make_unique<IoOp>(OpType::CREATE,0,size);
}

std::unique_ptr<IoOp> IoOp
  ::generate_remove() {

  return std::make_unique<IoOp>(OpType::REMOVE);
}

std::unique_ptr<IoOp> IoOp
  ::generate_read(uint64_t offset, uint64_t length) {

  return std::make_unique<IoOp>(OpType::READ, offset, length);
}

std::unique_ptr<IoOp> IoOp
  ::generate_read2(uint64_t offset1, uint64_t length1,
                   uint64_t offset2, uint64_t length2) {

  if (offset1 < offset2) {
    ceph_assert( offset1 + length1 <= offset2 );
  } else {
    ceph_assert( offset2 + length2 <= offset1 );
  }

  return std::make_unique<IoOp>(OpType::READ2,
                                offset1, length1,
                                offset2, length2);
}

std::unique_ptr<IoOp> IoOp
  ::generate_read3(uint64_t offset1, uint64_t length1,
                   uint64_t offset2, uint64_t length2,
                   uint64_t offset3, uint64_t length3) {

  if (offset1 < offset2) {
    ceph_assert( offset1 + length1 <= offset2 );
  } else {
    ceph_assert( offset2 + length2 <= offset1 );
  }
  if (offset1 < offset3) {
    ceph_assert( offset1 + length1 <= offset3 );
  } else {
    ceph_assert( offset3 + length3 <= offset1 );
  }
  if (offset2 < offset3) {
    ceph_assert( offset2 + length2 <= offset3 );
  } else {
    ceph_assert( offset3 + length3 <= offset2 );
  }
  return std::make_unique<IoOp>(OpType::READ3,
                                offset1, length1,
                                offset2, length2,
                                offset3, length3);
}

std::unique_ptr<IoOp> IoOp::generate_write(uint64_t offset, uint64_t length) {
  return std::make_unique<IoOp>(OpType::WRITE, offset, length);
}

std::unique_ptr<IoOp> IoOp::generate_write2(uint64_t offset1, uint64_t length1,
                                            uint64_t offset2, uint64_t length2) {
  if (offset1 < offset2) {
    ceph_assert( offset1 + length1 <= offset2 );
  } else {
    ceph_assert( offset2 + length2 <= offset1 );
  }
  return std::make_unique<IoOp>(OpType::WRITE2,
                                offset1, length1,
                                offset2, length2);
}

std::unique_ptr<IoOp> IoOp::generate_write3(uint64_t offset1, uint64_t length1, 
                                            uint64_t offset2, uint64_t length2,
                                            uint64_t offset3, uint64_t length3) {
  if (offset1 < offset2) {
    ceph_assert( offset1 + length1 <= offset2 );
  } else {
    ceph_assert( offset2 + length2 <= offset1 );
  }
  if (offset1 < offset3) {
    ceph_assert( offset1 + length1 <= offset3 );
  } else {
    ceph_assert( offset3 + length3 <= offset1 );
  }
  if (offset2 < offset3) {
    ceph_assert( offset2 + length2 <= offset3 );
  } else {
    ceph_assert( offset3 + length3 <= offset2 );
  }
  return std::make_unique<IoOp>(OpType::WRITE3,
                                offset1, length1,
                                offset2, length2,
                                offset3, length3);
}

bool IoOp::done() {
  return (op == OpType::Done);
}

std::string IoOp::to_string(uint64_t block_size) const
{
  switch (op) {
  case OpType::Done:
    return "Done";
  case OpType::BARRIER:
    return "Barrier";
  case OpType::CREATE:
    return "Create (size=" + value_to_string(length1 * block_size) + ")";
  case OpType::REMOVE:
    return "Remove";
  case OpType::READ:
    return "Read (offset=" + value_to_string(offset1 * block_size) +
           ",length=" + value_to_string(length1 * block_size) + ")";
  case OpType::READ2:
    return "Read2 (offset1=" + value_to_string(offset1 * block_size) +
           ",length1=" + value_to_string(length1 * block_size) +
           ",offset2=" + value_to_string(offset2 * block_size) +
           ",length2=" + value_to_string(length2 * block_size) + ")";
  case OpType::READ3:
    return "Read3 (offset1=" + value_to_string(offset1 * block_size) +
           ",length1=" + value_to_string(length1 * block_size) +
           ",offset2=" + value_to_string(offset2 * block_size) +
           ",length2=" + value_to_string(length2 * block_size) +
           ",offset3=" + value_to_string(offset3 * block_size) +
           ",length3=" + value_to_string(length3 * block_size) + ")";
  case OpType::WRITE:
    return "Write (offset=" + value_to_string(offset1 * block_size) +
           ",length=" + value_to_string(length1 * block_size) + ")";
  case OpType::WRITE2:
    return "Write2 (offset1=" + value_to_string(offset1 * block_size) +
           ",length1=" + value_to_string(length1 * block_size) +
           ",offset2=" + value_to_string(offset2 * block_size) +
           ",length2=" + value_to_string(length2 * block_size) + ")";
  case OpType::WRITE3:
    return "Write3 (offset1=" + value_to_string(offset1 * block_size) +
           ",length1=" + value_to_string(length1 * block_size) +
           ",offset2=" + value_to_string(offset2 * block_size) +
           ",length2=" + value_to_string(length2 * block_size) +
           ",offset3=" + value_to_string(offset3 * block_size) +
           ",length3=" + value_to_string(length3 * block_size) + ")";
  default:
    break;
  }
  return "Unknown";
}