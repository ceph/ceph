#pragma once

#include <fmt/format.h>
#include <include/ceph_assert.h>

/* Overview
 *
 * enum OpType
 *   Enumeration of different types of I/O operation
 *
 */

namespace ceph {
namespace io_exerciser {
enum class OpType {
  Done,                  // End of I/O sequence
  Barrier,               // Barrier - all prior I/Os must complete
  Create,                // Create object and pattern with data
  Remove,                // Remove object
  Read,                  // Read
  Read2,                 // Two reads in a single op
  Read3,                 // Three reads in a single op
  Write,                 // Write
  Write2,                // Two writes in a single op
  Write3,                // Three writes in a single op
  Append,                // Append
  Truncate,              // Truncate
  FailedWrite,           // A write which should fail
  FailedWrite2,          // Two writes in one op which should fail
  FailedWrite3,          // Three writes in one op which should fail
  InjectReadError,       // Op to tell OSD to inject read errors
  InjectWriteError,      // Op to tell OSD to inject write errors
  ClearReadErrorInject,  // Op to tell OSD to clear read error injects
  ClearWriteErrorInject  // Op to tell OSD to clear write error injects
};

enum class InjectOpType {
  None,
  ReadEIO,
  ReadMissingShard,
  WriteFailAndRollback,
  WriteOSDAbort
};
}  // namespace io_exerciser
}  // namespace ceph

template <>
struct fmt::formatter<ceph::io_exerciser::OpType> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  auto format(ceph::io_exerciser::OpType opType,
              fmt::format_context& ctx) const -> fmt::format_context::iterator {
    switch (opType) {
      case ceph::io_exerciser::OpType::Done:
        return fmt::format_to(ctx.out(), "Done");
      case ceph::io_exerciser::OpType::Barrier:
        return fmt::format_to(ctx.out(), "Barrier");
      case ceph::io_exerciser::OpType::Create:
        return fmt::format_to(ctx.out(), "Create");
      case ceph::io_exerciser::OpType::Remove:
        return fmt::format_to(ctx.out(), "Remove");
      case ceph::io_exerciser::OpType::Read:
        return fmt::format_to(ctx.out(), "Read");
      case ceph::io_exerciser::OpType::Read2:
        return fmt::format_to(ctx.out(), "Read2");
      case ceph::io_exerciser::OpType::Read3:
        return fmt::format_to(ctx.out(), "Read3");
      case ceph::io_exerciser::OpType::Write:
        return fmt::format_to(ctx.out(), "Write");
      case ceph::io_exerciser::OpType::Write2:
        return fmt::format_to(ctx.out(), "Write2");
      case ceph::io_exerciser::OpType::Write3:
        return fmt::format_to(ctx.out(), "Write3");
      case ceph::io_exerciser::OpType::Append:
        return fmt::format_to(ctx.out(), "Append");
      case ceph::io_exerciser::OpType::Truncate:
        return fmt::format_to(ctx.out(), "Truncate");
      case ceph::io_exerciser::OpType::FailedWrite:
        return fmt::format_to(ctx.out(), "FailedWrite");
      case ceph::io_exerciser::OpType::FailedWrite2:
        return fmt::format_to(ctx.out(), "FailedWrite2");
      case ceph::io_exerciser::OpType::FailedWrite3:
        return fmt::format_to(ctx.out(), "FailedWrite3");
      case ceph::io_exerciser::OpType::InjectReadError:
        return fmt::format_to(ctx.out(), "InjectReadError");
      case ceph::io_exerciser::OpType::InjectWriteError:
        return fmt::format_to(ctx.out(), "InjectWriteError");
      case ceph::io_exerciser::OpType::ClearReadErrorInject:
        return fmt::format_to(ctx.out(), "ClearReadErrorInject");
      case ceph::io_exerciser::OpType::ClearWriteErrorInject:
        return fmt::format_to(ctx.out(), "ClearWriteErrorInject");
      default:
        ceph_abort_msg("Unknown OpType");
        return fmt::format_to(ctx.out(), "Unknown OpType");
    }
  }
};
