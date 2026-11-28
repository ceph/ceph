// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Clyso Technologies Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <ctime>
#include <string>
#include <vector>

namespace ceph {
class Formatter;

/// Userspace live patches (libpulp) applied to the running process.
namespace livepatch {

struct patch_t {
  std::string id;      ///< patch id, hex, as printed by `ulp dump`
  std::string target;  ///< patched object: the executable or a library
  std::string file;    ///< patch container (.so) that was loaded
  time_t applied = 0;
};

/// true if this process can be live patched (libpulp is loaded)
bool enabled();

/// name and version of the live patch runtime, e.g. "libpulp 0.3.17";
/// empty if not enabled()
std::string runtime_version();

/// live patches currently applied, oldest first; empty if the runtime's
/// version is not one whose patch list layout is known
std::vector<patch_t> list();

/// dump "livepatch_enabled", "livepatch_runtime" and a "livepatches" array
void dump(Formatter *f);

} // namespace livepatch
} // namespace ceph
