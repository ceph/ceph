// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

// Pluggable consumer of the desired export set computed by
// compose_exports(). The reconciler hands a vector<DesiredExport>
// to the sink each cycle; the sink converges Ganesha (or, in
// tests, an in-memory log) to that set.
//
// Implementations:
//   RecordingGaneshaSink — appends every apply() call to an
//                          in-memory log. Used by reconciler
//                          unit tests to assert what the
//                          reconciler emitted without needing
//                          a running Ganesha.
//   DbusGaneshaSink     — production sink (separate commit). Talks
//                          to the local Ganesha via D-Bus on the
//                          `org.ganesha.nfsd.exportmgr`
//                          interface, emitting AddExport /
//                          UpdateExport / RemoveExport per the
//                          diff between desired and last-applied.
//                          See doc/dev/radosgw/s3_files_api.rst.
//   GrpcGaneshaSink     — *future*. Once upstream Ganesha lands
//                          export-management gRPC APIs (gerrit
//                          change I500c278f...), this sink can
//                          replace D-Bus and unlock remote /
//                          centralized reconciler topologies.

#pragma once

#include <mutex>
#include <vector>

#include "desired_export.h"

namespace rgw::file_state {

class GaneshaSink {
 public:
  virtual ~GaneshaSink() = default;

  // Converge Ganesha to `desired`. The sink is responsible for
  // diffing against its prior state and emitting only the
  // necessary mutations (Add/Update/Remove). Idempotent on the
  // same input.
  //
  // May be called from the reconciler's worker thread; sinks
  // must be safe under that single-writer assumption (concurrent
  // calls are not made).
  virtual void apply(std::vector<DesiredExport> desired) = 0;
};

// In-memory recording sink for tests. Appends every apply() call
// to an internal log; tests assert on the log's contents. Thread-
// safe so threading-aware reconciler tests can run without
// extra synchronization on the test side.
class RecordingGaneshaSink final : public GaneshaSink {
 public:
  void apply(std::vector<DesiredExport> desired) override;

  // Snapshot of every apply() call seen so far. Returns a copy
  // so callers don't race with new apply()s.
  std::vector<std::vector<DesiredExport>> calls() const;

  // Convenience: the most recent apply()'s desired set, or an
  // empty vector if no apply() has happened yet.
  std::vector<DesiredExport> last() const;

  // Total number of apply() calls observed.
  std::size_t call_count() const;

  // Reset the log. Useful between phases of a long test.
  void clear();

 private:
  mutable std::mutex mu_;
  std::vector<std::vector<DesiredExport>> calls_;
};

}  // namespace rgw::file_state
