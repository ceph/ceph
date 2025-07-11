// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

/// \file neorados/cls/fifo/entry.h
///
/// \brief Entries returned by FIFO list
///
/// Part of the public FIFO interface but needed by `FIFOImpl`.

#pragma once

#include "include/buffer.h"
#include "common/ceph_time.h"

namespace neorados::cls::fifo {
/// Entries to be returned by the list operation
struct entry {
  /// Data stored in the entry
  ceph::buffer::list data;
  /// Marker (for trimming, continuing list operations)
  std::string marker;
  /// Time stored (set by the OSD)
  ceph::real_time mtime;
};

inline std::ostream& operator <<(std::ostream& m, const entry& e) {
  return m << "[data: " << e.data
	   << ", marker: " << e.marker
	   << ", mtime: " << e.mtime << "]";
}
} // neorados::cls::fifo
