#include "IoOp.h"

#include "fmt/format.h"
#include "include/ceph_assert.h"

using IoOp = ceph::io_exerciser::IoOp;
using OpType = ceph::io_exerciser::OpType;

using DoneOp = ceph::io_exerciser::DoneOp;
using BarrierOp = ceph::io_exerciser::BarrierOp;
using CreateOp = ceph::io_exerciser::CreateOp;
using RemoveOp = ceph::io_exerciser::RemoveOp;
using SingleReadOp = ceph::io_exerciser::SingleReadOp;
using DoubleReadOp = ceph::io_exerciser::DoubleReadOp;
using TripleReadOp = ceph::io_exerciser::TripleReadOp;
using SingleWriteOp = ceph::io_exerciser::SingleWriteOp;
using DoubleWriteOp = ceph::io_exerciser::DoubleWriteOp;
using TripleWriteOp = ceph::io_exerciser::TripleWriteOp;
using SingleFailedWriteOp = ceph::io_exerciser::SingleFailedWriteOp;
using DoubleFailedWriteOp = ceph::io_exerciser::DoubleFailedWriteOp;
using TripleFailedWriteOp = ceph::io_exerciser::TripleFailedWriteOp;
using SingleAppendOp = ceph::io_exerciser::SingleAppendOp;
using TruncateOp = ceph::io_exerciser::TruncateOp;

namespace {
std::string value_to_string(uint64_t v) {
  if (v < 1024 || (v % 1024) != 0) {
    return std::to_string(v);
  } else if (v < 1024 * 1024 || (v % (1024 * 1024)) != 0) {
    return std::to_string(v / 1024) + "K";
  } else {
    return std::to_string(v / 1024 / 1024) + "M";
  }
}
}  // namespace

IoOp::IoOp() {}

template <OpType opType>
ceph::io_exerciser::TestOp<opType>::TestOp() : IoOp() {}

DoneOp::DoneOp() : TestOp<OpType::Done>() {}

std::string DoneOp::to_string(uint64_t block_size) const { return "Done"; }

std::unique_ptr<DoneOp> DoneOp::generate() {
  return std::make_unique<DoneOp>();
}

BarrierOp::BarrierOp() : TestOp<OpType::Barrier>() {}

std::unique_ptr<BarrierOp> BarrierOp::generate() {
  return std::make_unique<BarrierOp>();
}

std::string BarrierOp::to_string(uint64_t block_size) const {
  return "Barrier";
}

CreateOp::CreateOp(uint64_t size) : TestOp<OpType::Create>(), size(size) {}

std::unique_ptr<CreateOp> CreateOp::generate(uint64_t size) {
  return std::make_unique<CreateOp>(size);
}

std::string CreateOp::to_string(uint64_t block_size) const {
  return "Create (size=" + value_to_string(size * block_size) + ")";
}

RemoveOp::RemoveOp() : TestOp<OpType::Remove>() {}

std::unique_ptr<RemoveOp> RemoveOp::generate() {
  return std::make_unique<RemoveOp>();
}

std::string RemoveOp::to_string(uint64_t block_size) const { return "Remove"; }

template <OpType opType, int numIOs>
ceph::io_exerciser::ReadWriteOp<opType, numIOs>::ReadWriteOp(
    std::array<uint64_t, numIOs>&& offset,
    std::array<uint64_t, numIOs>&& length)
    : TestOp<opType>(), offset(offset), length(length) {
  auto compare = [](uint64_t offset1, uint64_t length1, uint64_t offset2,
                    uint64_t length2) {
    if (offset1 < offset2) {
      ceph_assert(offset1 + length1 <= offset2);
    } else {
      ceph_assert(offset2 + length2 <= offset1);
    }
  };

  if (numIOs > 1) {
    for (int i = 0; i < numIOs - 1; i++) {
      for (int j = i + 1; j < numIOs; j++) {
        compare(offset[i], length[i], offset[j], length[j]);
      }
    }
  }
}

template <OpType opType, int numIOs>
std::string ceph::io_exerciser::ReadWriteOp<opType, numIOs>::to_string(
    uint64_t block_size) const {
  std::string offset_length_desc;
  std::string length_desc;
  if (numIOs > 0) {
    offset_length_desc += fmt::format(
        "offset1={}", value_to_string(this->offset[0] * block_size));
    length_desc += fmt::format("length1={}",
                               value_to_string(this->length[0] * block_size));
    offset_length_desc += "," + length_desc;
    for (int i = 1; i < numIOs; i++) {
      std::string length;
      offset_length_desc += fmt::format(
          ",offset{}={}", i + 1, value_to_string(this->offset[i] * block_size));
      length += fmt::format(",length{}={}", i + 1,
                            value_to_string(this->length[i] * block_size));
      length_desc += length;
      offset_length_desc += length;
    }
  }
  switch (opType) {
    case OpType::Read:
      [[fallthrough]];
    case OpType::Read2:
      [[fallthrough]];
    case OpType::Read3:
      return fmt::format("Read{} ({})", numIOs, offset_length_desc);
    case OpType::Write:
      [[fallthrough]];
    case OpType::Write2:
      [[fallthrough]];
    case OpType::Write3:
      return fmt::format("Write{} ({})", numIOs, offset_length_desc);
    case OpType::Append:
      return fmt::format("Append{} ({})", numIOs, length_desc);
    case OpType::FailedWrite:
      [[fallthrough]];
    case OpType::FailedWrite2:
      [[fallthrough]];
    case OpType::FailedWrite3:
      return fmt::format("FailedWrite{} ({})", numIOs, offset_length_desc);
    default:
      ceph_abort_msg(
          fmt::format("Unsupported op type by ReadWriteOp ({})", opType));
  }
}

SingleReadOp::SingleReadOp(uint64_t offset, uint64_t length)
    : ReadWriteOp<OpType::Read, 1>({offset}, {length}) {}

std::unique_ptr<SingleReadOp> SingleReadOp::generate(uint64_t offset,
                                                     uint64_t length) {
  return std::make_unique<SingleReadOp>(offset, length);
}

DoubleReadOp::DoubleReadOp(uint64_t offset1, uint64_t length1, uint64_t offset2,
                           uint64_t length2)
    : ReadWriteOp<OpType::Read2, 2>({offset1, offset2}, {length1, length2}) {}

std::unique_ptr<DoubleReadOp> DoubleReadOp::generate(uint64_t offset1,
                                                     uint64_t length1,
                                                     uint64_t offset2,
                                                     uint64_t length2) {
  return std::make_unique<DoubleReadOp>(offset1, length1, offset2, length2);
}

TripleReadOp::TripleReadOp(uint64_t offset1, uint64_t length1, uint64_t offset2,
                           uint64_t length2, uint64_t offset3, uint64_t length3)
    : ReadWriteOp<OpType::Read3, 3>({offset1, offset2, offset3},
                                    {length1, length2, length3}) {}

std::unique_ptr<TripleReadOp> TripleReadOp::generate(
    uint64_t offset1, uint64_t length1, uint64_t offset2, uint64_t length2,
    uint64_t offset3, uint64_t length3) {
  return std::make_unique<TripleReadOp>(offset1, length1, offset2, length2,
                                        offset3, length3);
}

SingleWriteOp::SingleWriteOp(uint64_t offset, uint64_t length)
    : ReadWriteOp<OpType::Write, 1>({offset}, {length}) {}

std::unique_ptr<SingleWriteOp> SingleWriteOp::generate(uint64_t offset,
                                                       uint64_t length) {
  return std::make_unique<SingleWriteOp>(offset, length);
}

DoubleWriteOp::DoubleWriteOp(uint64_t offset1, uint64_t length1,
                             uint64_t offset2, uint64_t length2)
    : ReadWriteOp<OpType::Write2, 2>({offset1, offset2}, {length1, length2}) {}

std::unique_ptr<DoubleWriteOp> DoubleWriteOp::generate(uint64_t offset1,
                                                       uint64_t length1,
                                                       uint64_t offset2,
                                                       uint64_t length2) {
  return std::make_unique<DoubleWriteOp>(offset1, length1, offset2, length2);
}

TripleWriteOp::TripleWriteOp(uint64_t offset1, uint64_t length1,
                             uint64_t offset2, uint64_t length2,
                             uint64_t offset3, uint64_t length3)
    : ReadWriteOp<OpType::Write3, 3>({offset1, offset2, offset3},
                                     {length1, length2, length3}) {}

std::unique_ptr<TripleWriteOp> TripleWriteOp::generate(
    uint64_t offset1, uint64_t length1, uint64_t offset2, uint64_t length2,
    uint64_t offset3, uint64_t length3) {
  return std::make_unique<TripleWriteOp>(offset1, length1, offset2, length2,
                                         offset3, length3);
}

SingleAppendOp::SingleAppendOp(uint64_t length)
    : ReadWriteOp<OpType::Append, 1>({0}, {length}) {}

std::unique_ptr<SingleAppendOp> SingleAppendOp::generate(uint64_t length) {
  return std::make_unique<SingleAppendOp>(length);
}

TruncateOp::TruncateOp(uint64_t size)
    : TestOp<OpType::Truncate>(), size(size) {}

std::unique_ptr<TruncateOp> TruncateOp::generate(uint64_t size) {
  return std::make_unique<TruncateOp>(size);
}

std::string TruncateOp::to_string(uint64_t block_size) const {
  return "Truncate (size=" + value_to_string(size * block_size) + ")";
}

SingleFailedWriteOp::SingleFailedWriteOp(uint64_t offset, uint64_t length)
    : ReadWriteOp<OpType::FailedWrite, 1>({offset}, {length}) {}

std::unique_ptr<SingleFailedWriteOp> SingleFailedWriteOp::generate(
    uint64_t offset, uint64_t length) {
  return std::make_unique<SingleFailedWriteOp>(offset, length);
}

DoubleFailedWriteOp::DoubleFailedWriteOp(uint64_t offset1, uint64_t length1,
                                         uint64_t offset2, uint64_t length2)
    : ReadWriteOp<OpType::FailedWrite2, 2>({offset1, offset2},
                                           {length1, length2}) {}

std::unique_ptr<DoubleFailedWriteOp> DoubleFailedWriteOp::generate(
    uint64_t offset1, uint64_t length1, uint64_t offset2, uint64_t length2) {
  return std::make_unique<DoubleFailedWriteOp>(offset1, length1, offset2,
                                               length2);
}

TripleFailedWriteOp::TripleFailedWriteOp(uint64_t offset1, uint64_t length1,
                                         uint64_t offset2, uint64_t length2,
                                         uint64_t offset3, uint64_t length3)
    : ReadWriteOp<OpType::FailedWrite3, 3>({offset1, offset2, offset3},
                                           {length1, length2, length3}) {}

std::unique_ptr<TripleFailedWriteOp> TripleFailedWriteOp::generate(
    uint64_t offset1, uint64_t length1, uint64_t offset2, uint64_t length2,
    uint64_t offset3, uint64_t length3) {
  return std::make_unique<TripleFailedWriteOp>(offset1, length1, offset2,
                                               length2, offset3, length3);
}

template <ceph::io_exerciser::OpType opType>
ceph::io_exerciser::InjectErrorOp<opType>::InjectErrorOp(
    int shard, const std::optional<uint64_t>& type,
    const std::optional<uint64_t>& when,
    const std::optional<uint64_t>& duration)
    : TestOp<opType>(),
      shard(shard),
      type(type),
      when(when),
      duration(duration) {}

template <ceph::io_exerciser::OpType opType>
std::string ceph::io_exerciser::InjectErrorOp<opType>::to_string(
    uint64_t blocksize) const {
  std::string_view inject_type = get_inject_type_string();
  return fmt::format(
      "Inject {} error on shard {} of type {}"
      " after {} successful inject(s) lasting {} inject(s)",
      inject_type, shard, type.value_or(0), when.value_or(0),
      duration.value_or(1));
}

ceph::io_exerciser::InjectReadErrorOp::InjectReadErrorOp(
    int shard, const std::optional<uint64_t>& type,
    const std::optional<uint64_t>& when,
    const std::optional<uint64_t>& duration)
    : InjectErrorOp<OpType::InjectReadError>(shard, type, when, duration) {}

std::unique_ptr<ceph::io_exerciser::InjectReadErrorOp>
ceph::io_exerciser ::InjectReadErrorOp::generate(
    int shard, const std::optional<uint64_t>& type,
    const std::optional<uint64_t>& when,
    const std::optional<uint64_t>& duration) {
  return std::make_unique<InjectReadErrorOp>(shard, type, when, duration);
}

ceph::io_exerciser::InjectWriteErrorOp::InjectWriteErrorOp(
    int shard, const std::optional<uint64_t>& type,
    const std::optional<uint64_t>& when,
    const std::optional<uint64_t>& duration)
    : InjectErrorOp<OpType::InjectWriteError>(shard, type, when, duration) {}

std::unique_ptr<ceph::io_exerciser::InjectWriteErrorOp>
ceph::io_exerciser ::InjectWriteErrorOp::generate(
    int shard, const std::optional<uint64_t>& type,
    const std::optional<uint64_t>& when,
    const std::optional<uint64_t>& duration) {
  return std::make_unique<InjectWriteErrorOp>(shard, type, when, duration);
}

template <ceph::io_exerciser::OpType opType>
ceph::io_exerciser::ClearErrorInjectOp<opType>::ClearErrorInjectOp(
    int shard, const std::optional<uint64_t>& type)
    : TestOp<opType>(), shard(shard), type(type) {}

template <ceph::io_exerciser::OpType opType>
std::string ceph::io_exerciser::ClearErrorInjectOp<opType>::to_string(
    uint64_t blocksize) const {
  std::string_view inject_type = get_inject_type_string();
  return fmt::format("Clear {} injects on shard {} of type {}", inject_type,
                     shard, type.value_or(0));
}

ceph::io_exerciser::ClearReadErrorInjectOp::ClearReadErrorInjectOp(
    int shard, const std::optional<uint64_t>& type)
    : ClearErrorInjectOp<OpType::ClearReadErrorInject>(shard, type) {}

std::unique_ptr<ceph::io_exerciser::ClearReadErrorInjectOp>
ceph::io_exerciser ::ClearReadErrorInjectOp::generate(
    int shard, const std::optional<uint64_t>& type) {
  return std::make_unique<ClearReadErrorInjectOp>(shard, type);
}

ceph::io_exerciser::ClearWriteErrorInjectOp::ClearWriteErrorInjectOp(
    int shard, const std::optional<uint64_t>& type)
    : ClearErrorInjectOp<OpType::ClearWriteErrorInject>(shard, type) {}

std::unique_ptr<ceph::io_exerciser::ClearWriteErrorInjectOp>
ceph::io_exerciser ::ClearWriteErrorInjectOp::generate(
    int shard, const std::optional<uint64_t>& type) {
  return std::make_unique<ClearWriteErrorInjectOp>(shard, type);
}
