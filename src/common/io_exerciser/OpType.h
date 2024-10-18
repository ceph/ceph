#pragma once

/* Overview
 *
 * enum OpType
 *   Enumeration of different types of I/O operation
 *
 */

namespace ceph {
  namespace io_exerciser {

    enum class OpType {
      Done,                   // End of I/O sequence
      Barrier,                // Barrier - all prior I/Os must complete
      Create,                 // Create object and pattern with data
      Remove,                 // Remove object
      Read,                   // Read
      Read2,                  // Two reads in a single op
      Read3,                  // Three reads in a single op
      Write,                  // Write
      Write2,                 // Two writes in a single op
      Write3,                 // Three writes in a single op
      InjectReadError,        // Op to tell OSD to inject read errors
      InjectWriteError,       // Op to tell OSD to inject write errors
      ClearReadErrorInject,   // Op to tell OSD to clear read error injects
      ClearWriteErrorInject   // Op to tell OSD to clear write error injects
    };

    enum class InjectOpType {
      None,
      ReadEIO,
      ReadMissingShard,
      WriteFailAndRollback,
      WriteOSDAbort
    };
  }
}