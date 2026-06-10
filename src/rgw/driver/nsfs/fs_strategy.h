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
#include <vector>

#include <boost/container/flat_map.hpp>

#include "gpfs/gpfs.h"

class DoutPrefixProvider;

namespace rgw { namespace sal { namespace nsfs {

using xattr_map_t = boost::container::flat_map<std::string, std::string>;

enum class SafeResult {
  OK = 0,
  MISMATCH = 1,
  ERROR = 2,
};

/* RAII handle returned by FSStrategy::version_lock().
 * Destructor releases the lock. */
class VersionLockHandle {
public:
  virtual ~VersionLockHandle() = default;
  VersionLockHandle(const VersionLockHandle&) = delete;
  VersionLockHandle& operator=(const VersionLockHandle&) = delete;
protected:
  VersionLockHandle() = default;
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
  virtual SafeResult safe_link(const DoutPrefixProvider* dpp,
                               int src_dir_fd, const std::string& src_name,
                               int dst_dir_fd, const std::string& dst_name,
                               uint64_t expected_mtime_ns,
                               uint64_t expected_ino) = 0;

  /* CAS unlink: remove entry only if it matches expected inode+mtime */
  virtual SafeResult safe_unlink(const DoutPrefixProvider* dpp,
                                 int dir_fd, const std::string& name,
                                 int tmp_dir_fd,
                                 uint64_t expected_mtime_ns,
                                 uint64_t expected_ino) = 0;

  /* clone a file's data — GPFS can use clone_snap+clone_copy (CoW,
   * experimental) or fall back to copy_file_range / read+write.
   *
   * The GPFS clone path (clone_snap + clone_copy) keeps data at rest
   * and produces a mutable destination with independent xattrs.
   * However, clone parents are immutable and must be explicitly
   * cleaned up via cleanup_clone() when the child is removed or
   * overwritten.  A GPFS extension for automatic clone-parent
   * garbage collection would eliminate this lifecycle burden; until
   * then, the clone path is experimental. */
  virtual int clone_file(const DoutPrefixProvider* dpp,
                         int src_dir_fd, const std::string& src_name,
                         int dst_dir_fd, const std::string& dst_name) = 0;

  /* remove the hidden clone parent associated with name, if any.
   * Call when an object created by clone_file is deleted or
   * overwritten.  No-op when no clone parent exists or when the
   * strategy does not use GPFS clones. */
  virtual void cleanup_clone(const DoutPrefixProvider* dpp,
                             int dir_fd, const std::string& name) = 0;

  /* acquire an exclusive lock for versioned object operations.
   * lock_fd is an open fd on the .versions/.lock file.
   *
   * POSIX: OFD write lock (per-fd, single-node).
   * GPFS/LWE: cluster-wide exclusive right via the GPFS token
   *   manager, falling back to OFD on failure.
   *
   * Returns an RAII handle — the lock is released when the
   * handle is destroyed. */
  virtual std::unique_ptr<VersionLockHandle> version_lock(
    const DoutPrefixProvider* dpp, int lock_fd) = 0;

  /* batch xattr operations.
   *
   * POSIX: loops over flistxattr/fgetxattr/fsetxattr/fremovexattr.
   * GPFS: packs multiple gpfsGetSetXAttr_t structs into a single
   *   gpfs_fcntl call, reducing N+1 syscalls to 1.
   *
   * names in xattr_map_t are the full on-disk names (e.g.
   * "user.rgw.etag"); the caller handles any prefix mapping. */
  virtual int get_xattrs(const DoutPrefixProvider* dpp, int fd,
                         xattr_map_t& attrs) = 0;

  virtual int set_xattrs(const DoutPrefixProvider* dpp, int fd,
                         const xattr_map_t& attrs) = 0;

  virtual int remove_xattrs(const DoutPrefixProvider* dpp, int fd,
                            const std::vector<std::string>& names) = 0;

  virtual const char* name() const = 0;
};

class POSIXStrategy : public FSStrategy {
public:
  int link_temp_file(int temp_fd, int dir_fd,
                     const std::string& name,
                     const DoutPrefixProvider* dpp) override;

  SafeResult safe_link(const DoutPrefixProvider* dpp,
                       int src_dir_fd, const std::string& src_name,
                       int dst_dir_fd, const std::string& dst_name,
                       uint64_t expected_mtime_ns,
                       uint64_t expected_ino) override;

  SafeResult safe_unlink(const DoutPrefixProvider* dpp,
                         int dir_fd, const std::string& name,
                         int tmp_dir_fd,
                         uint64_t expected_mtime_ns,
                         uint64_t expected_ino) override;

  int clone_file(const DoutPrefixProvider* dpp,
                 int src_dir_fd, const std::string& src_name,
                 int dst_dir_fd, const std::string& dst_name) override;

  void cleanup_clone(const DoutPrefixProvider* dpp,
                     int dir_fd, const std::string& name) override {}

  std::unique_ptr<VersionLockHandle> version_lock(
    const DoutPrefixProvider* dpp, int lock_fd) override;

  int get_xattrs(const DoutPrefixProvider* dpp, int fd,
                 xattr_map_t& attrs) override;
  int set_xattrs(const DoutPrefixProvider* dpp, int fd,
                 const xattr_map_t& attrs) override;
  int remove_xattrs(const DoutPrefixProvider* dpp, int fd,
                    const std::vector<std::string>& names) override;

  const char* name() const override { return "posix"; }
};

class GPFSStrategy : public FSStrategy {
  void* dl_handle;
  void* dmapi_handle;
  decltype(&gpfs_linkat) fn_linkat;
  decltype(&gpfs_linkatif) fn_linkatif;
  decltype(&gpfs_unlinkat) fn_unlinkat;
  decltype(&gpfs_clone_snap) fn_clone_snap;
  decltype(&gpfs_clone_copy) fn_clone_copy;
  decltype(&gpfs_clone_unsnap) fn_clone_unsnap;
  using gpfs_fcntl_t = int(*)(gpfs_file_t, void*);
  gpfs_fcntl_t fn_fcntl;
  bool clone_enabled;
  bool batch_xattrs_enabled;

  /* LWE cluster-wide locking via GPFS token manager.
   * dm_fd_to_handle/dm_handle_free come from libdmapi.so;
   * gpfs_lwe_* come from libgpfs.so. */
  using dm_fd_to_handle_t = int(*)(int, void**, size_t*);
  using dm_handle_free_t = void(*)(void*, size_t);
  using lwe_create_session_t = int(*)(gpfs_lwe_sessid_t, char*,
                                      gpfs_lwe_sessid_t*);
  using lwe_destroy_session_t = int(*)(gpfs_lwe_sessid_t);
  using lwe_request_right_t = int(*)(gpfs_lwe_sessid_t, void*, size_t,
                                     unsigned int, unsigned int,
                                     gpfs_lwe_token_t*);
  using lwe_release_right_t = int(*)(gpfs_lwe_sessid_t, void*, size_t,
                                     gpfs_lwe_token_t);

  dm_fd_to_handle_t fn_dm_fd_to_handle;
  dm_handle_free_t fn_dm_handle_free;
  lwe_create_session_t fn_lwe_create_session;
  lwe_destroy_session_t fn_lwe_destroy_session;
  lwe_request_right_t fn_lwe_request_right;
  lwe_release_right_t fn_lwe_release_right;

  gpfs_lwe_sessid_t lwe_session;
  bool lwe_enabled;

  GPFSStrategy(void* dl, void* dmapi_dl,
               decltype(&gpfs_linkat) la,
               decltype(&gpfs_linkatif) lai,
               decltype(&gpfs_unlinkat) ua,
               decltype(&gpfs_clone_snap) cs,
               decltype(&gpfs_clone_copy) cc,
               decltype(&gpfs_clone_unsnap) cu,
               gpfs_fcntl_t fc,
               bool clone_en, bool batch_en,
               dm_fd_to_handle_t dm_fd, dm_handle_free_t dm_free,
               lwe_create_session_t lcs, lwe_destroy_session_t lds,
               lwe_request_right_t lrr, lwe_release_right_t lrl,
               gpfs_lwe_sessid_t session, bool lwe_en)
    : dl_handle(dl), dmapi_handle(dmapi_dl),
      fn_linkat(la), fn_linkatif(lai), fn_unlinkat(ua),
      fn_clone_snap(cs), fn_clone_copy(cc), fn_clone_unsnap(cu),
      fn_fcntl(fc), clone_enabled(clone_en),
      batch_xattrs_enabled(batch_en),
      fn_dm_fd_to_handle(dm_fd), fn_dm_handle_free(dm_free),
      fn_lwe_create_session(lcs), fn_lwe_destroy_session(lds),
      fn_lwe_request_right(lrr), fn_lwe_release_right(lrl),
      lwe_session(session), lwe_enabled(lwe_en) {}

public:
  ~GPFSStrategy() override;

  static std::unique_ptr<GPFSStrategy> try_create(
    const DoutPrefixProvider* dpp, const std::string& dl_path,
    bool clone_enabled, bool lwe_enabled, bool batch_xattrs);

  int link_temp_file(int temp_fd, int dir_fd,
                     const std::string& name,
                     const DoutPrefixProvider* dpp) override;

  SafeResult safe_link(const DoutPrefixProvider* dpp,
                       int src_dir_fd, const std::string& src_name,
                       int dst_dir_fd, const std::string& dst_name,
                       uint64_t expected_mtime_ns,
                       uint64_t expected_ino) override;

  SafeResult safe_unlink(const DoutPrefixProvider* dpp,
                         int dir_fd, const std::string& name,
                         int tmp_dir_fd,
                         uint64_t expected_mtime_ns,
                         uint64_t expected_ino) override;

  int clone_file(const DoutPrefixProvider* dpp,
                 int src_dir_fd, const std::string& src_name,
                 int dst_dir_fd, const std::string& dst_name) override;

  void cleanup_clone(const DoutPrefixProvider* dpp,
                     int dir_fd, const std::string& name) override;

  std::unique_ptr<VersionLockHandle> version_lock(
    const DoutPrefixProvider* dpp, int lock_fd) override;

  int get_xattrs(const DoutPrefixProvider* dpp, int fd,
                 xattr_map_t& attrs) override;
  int set_xattrs(const DoutPrefixProvider* dpp, int fd,
                 const xattr_map_t& attrs) override;
  int remove_xattrs(const DoutPrefixProvider* dpp, int fd,
                    const std::vector<std::string>& names) override;

  const char* name() const override { return "gpfs"; }

  bool has_clone() const {
    return clone_enabled && fn_clone_snap && fn_clone_copy && fn_clone_unsnap;
  }

  bool has_lwe() const {
    return lwe_enabled && fn_dm_fd_to_handle && fn_dm_handle_free &&
           fn_lwe_request_right && fn_lwe_release_right;
  }

  bool has_batch_xattrs() const {
    return batch_xattrs_enabled && fn_fcntl;
  }
};

} } } // namespace rgw::sal::nsfs
