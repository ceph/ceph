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

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "gpfs/gpfs.h"

class DoutPrefixProvider;

namespace rgw { namespace sal { namespace nsfs {

enum class SafeResult {
  OK = 0,
  MISMATCH = 1,
  ERROR = 2,
};

class FSStrategy {
public:
  virtual ~FSStrategy() = default;

  /* atomically publish a temp file (O_TMPFILE fd) to a directory entry,
   * replacing any existing entry with that name */
  virtual int link_temp_file(int temp_fd, int dir_fd,
                             const std::string& name,
                             const DoutPrefixProvider* dpp) = 0;

  /* CAS link: link src to dst, verify inode+mtime match expected;
   * undo on mismatch */
  virtual SafeResult safe_link(int src_dir_fd, const std::string& src_name,
                               int dst_dir_fd, const std::string& dst_name,
                               uint64_t expected_mtime_ns,
                               uint64_t expected_ino) = 0;

  /* CAS unlink: remove entry only if it matches expected inode+mtime */
  virtual SafeResult safe_unlink(int dir_fd, const std::string& name,
                                 int tmp_dir_fd,
                                 uint64_t expected_mtime_ns,
                                 uint64_t expected_ino) = 0;

  virtual const char* name() const = 0;
};

class POSIXStrategy : public FSStrategy {
public:
  int link_temp_file(int temp_fd, int dir_fd,
                     const std::string& name,
                     const DoutPrefixProvider* dpp) override;

  SafeResult safe_link(int src_dir_fd, const std::string& src_name,
                       int dst_dir_fd, const std::string& dst_name,
                       uint64_t expected_mtime_ns,
                       uint64_t expected_ino) override;

  SafeResult safe_unlink(int dir_fd, const std::string& name,
                         int tmp_dir_fd,
                         uint64_t expected_mtime_ns,
                         uint64_t expected_ino) override;

  const char* name() const override { return "posix"; }
};

class GPFSStrategy : public FSStrategy {
  void* dl_handle;
  decltype(&gpfs_linkat) fn_linkat;
  decltype(&gpfs_linkatif) fn_linkatif;
  decltype(&gpfs_unlinkat) fn_unlinkat;

  GPFSStrategy(void* dl, decltype(&gpfs_linkat) la,
               decltype(&gpfs_linkatif) lai,
               decltype(&gpfs_unlinkat) ua)
    : dl_handle(dl), fn_linkat(la), fn_linkatif(lai), fn_unlinkat(ua) {}

public:
  ~GPFSStrategy() override;

  static std::unique_ptr<GPFSStrategy> try_create(
    const DoutPrefixProvider* dpp);

  int link_temp_file(int temp_fd, int dir_fd,
                     const std::string& name,
                     const DoutPrefixProvider* dpp) override;

  SafeResult safe_link(int src_dir_fd, const std::string& src_name,
                       int dst_dir_fd, const std::string& dst_name,
                       uint64_t expected_mtime_ns,
                       uint64_t expected_ino) override;

  SafeResult safe_unlink(int dir_fd, const std::string& name,
                         int tmp_dir_fd,
                         uint64_t expected_mtime_ns,
                         uint64_t expected_ino) override;

  const char* name() const override { return "gpfs"; }
};

} } } // namespace rgw::sal::nsfs
