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

#include "ganesha_sink.h"

namespace rgw::file_state {

void RecordingGaneshaSink::apply(std::vector<DesiredExport> desired) {
  std::lock_guard lock(mu_);
  calls_.push_back(std::move(desired));
}

std::vector<std::vector<DesiredExport>>
RecordingGaneshaSink::calls() const {
  std::lock_guard lock(mu_);
  return calls_;
}

std::vector<DesiredExport> RecordingGaneshaSink::last() const {
  std::lock_guard lock(mu_);
  if (calls_.empty()) return {};
  return calls_.back();
}

std::size_t RecordingGaneshaSink::call_count() const {
  std::lock_guard lock(mu_);
  return calls_.size();
}

void RecordingGaneshaSink::clear() {
  std::lock_guard lock(mu_);
  calls_.clear();
}

}  // namespace rgw::file_state
