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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <sys/types.h>

#include <boost/container/flat_map.hpp>

#include "gpfs/gpfs.h"

class DoutPrefixProvider;

namespace rgw { namespace sal { namespace nsfs {

using xattr_map_t = boost::container::flat_map<std::string, std::string>;

/* Names the strategy layer creates on disk which are not objects.  They
 * are declared here rather than inline at their use sites so that the
 * listing paths can suppress them without duplicating the knowledge --
 * a name added here and nowhere else would otherwise start appearing in
 * NFS readdir and S3 LIST. */
inline constexpr std::string_view TMP_LINK_PREFIX = ".tmp_link_";
inline constexpr std::string_view UNLINK_TMP_PREFIX = ".unlink_tmp_";
inline constexpr std::string_view CLONE_PARENT_PREFIX = ".clone_parent.";

/* Bounds on the I/O size FSStrategy::copy_range() uses when it has to fall
 * back to a buffered copy.  See preferred_io_size(). */
inline constexpr size_t MIN_COPY_IO_SIZE = 1u << 20;
inline constexpr size_t MAX_COPY_IO_SIZE = 4u << 20;

/* Try a real copy_file_range in dir and see whether it consumed no space:
 * the one question a strategy cannot answer about itself.  Returns false on
 * any error, so an unwritable or odd directory reads as "does not share",
 * which is the conservative answer -- it only costs a fallback that would
 * have worked. */
bool probe_shares_extents(const DoutPrefixProvider* dpp,
                          const std::string& dir);

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

  /* copy a byte range between two files, by the cheapest means the
   * filesystem offers:  copy_file_range(2), which shares extents where
   * that is supported (XFS with reflink), and a buffered loop where it is
   * not.  GPFS answers EOPNOTSUPP for copy_file_range at every
   * granularity, so there the fallback is the only path -- see
   * probes/results-cowprobe-2026-09-22.txt.
   *
   * Offsets are explicit;  neither file's position is read or disturbed.
   * Implemented once here rather than once per strategy, because the only
   * thing a strategy varies is preferred_io_size(). */
  virtual int copy_range(const DoutPrefixProvider* dpp,
                         int src_fd, off_t src_off,
                         int dst_fd, off_t dst_off,
                         off_t len);

  /* I/O size for copy_range()'s buffered fallback:  the file's
   * st_blksize, clamped to [MIN_COPY_IO_SIZE, MAX_COPY_IO_SIZE].
   *
   * The clamp is the measurement, not caution.  GPFS reports st_blksize
   * 4 MiB, its block size and the right answer;  XFS and tmpfs report
   * 4096, and honouring that would be sixteen times worse than the 64 KiB
   * this replaced.  Above 4 MiB the cost rises again everywhere.  See
   * probes/results-copyprobe-2026-09-23.txt. */
  virtual size_t preferred_io_size(int fd) const;

  /* test hook:  make copy_range() skip copy_file_range and take the
   * buffered path.  On a filesystem that shares extents the fallback is
   * otherwise unreachable, so the only path GPFS ever takes would never
   * run under test here.  Driven by the "inject-buffered-copy" driver
   * hint;  never by config. */
  void set_force_buffered_copy(bool on) { force_buffered_copy = on; }

  /* bytes copied through the buffered fallback, reset when read.  This is
   * what lets a test assert that the fallback actually ran, rather than
   * that a flag was set:  on a filesystem which shares extents the two are
   * otherwise indistinguishable from outside. */
  uint64_t take_buffered_bytes() { return buffered_copy_bytes.exchange(0); }

  /* Does the namespace's filesystem share extents, i.e. does
   * copy_file_range(2) actually clone rather than copy?  Established once
   * at driver startup by probe_shares_extents() and recorded here, because
   * it is not a property of the strategy:  POSIXStrategy serves XFS with
   * reflink (yes) alongside ext4 and tmpfs (no), and GPFS answers no today
   * (probes/results-cowprobe-2026-09-22.txt) but is not promised to forever.
   * Measuring costs one small file at mount and needs no strategy to make a
   * claim it cannot check.
   *
   * Reported, not acted on:  copy_range() still tries copy_file_range and
   * falls back, so a strategy constructed outside the driver -- the xattr
   * fallbacks build POSIXStrategy() temporaries -- is not misled by the
   * default. */
  bool shares_extents() const { return fs_shares_extents; }
  void set_shares_extents(bool on) { fs_shares_extents = on; }

protected:
  /* clone by copying, which is what every strategy falls back to when its
   * cheap path is unavailable.  Here rather than on POSIXStrategy because
   * GPFSStrategy used to reach it by constructing a POSIXStrategy()
   * temporary, and a temporary is a different object:  it does not carry
   * the injected fallback flag, and the bytes it copies are accounted to
   * something that is destroyed on the next line.  Called on `this`, the
   * fallback belongs to the strategy the driver actually holds. */
  int clone_file_by_copy(const DoutPrefixProvider* dpp,
                         int src_dir_fd, const std::string& src_name,
                         int dst_dir_fd, const std::string& dst_name,
                         bool excl);

  int clone_fd_by_copy(const DoutPrefixProvider* dpp, int src_fd,
                       int dst_dir_fd, const std::string& dst_name);

  int copy_buffered(const DoutPrefixProvider* dpp, size_t bufsz,
                    int src_fd, off_t src_off,
                    int dst_fd, off_t dst_off, off_t len);

  bool force_buffered_copy{false};
  bool fs_shares_extents{false};
  std::atomic<uint64_t> buffered_copy_bytes{0};

public:

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
  /* excl: fail with -EEXIST rather than overwrite an existing
   * destination.  shadow forks pass it so that two instances racing to
   * fork the same object cannot clobber each other;  the loser joins
   * the winner's shadow instead. */
  virtual int clone_file(const DoutPrefixProvider* dpp,
                         int src_dir_fd, const std::string& src_name,
                         int dst_dir_fd, const std::string& dst_name,
                         bool excl = false) = 0;

  /* clone from an open source fd — used when the source pathname is
   * no longer valid (e.g. versioned self-copy after demote). */
  virtual int clone_fd(const DoutPrefixProvider* dpp,
                       int src_fd,
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

  /* Can this strategy move a name cheaply, i.e. is renameat() the right
   * way to relocate an object?  Default no, so a strategy opts in rather
   * than inherits the behaviour.  GPFS deliberately does not:  rgw-nfs on
   * Spectrum Scale is not a productization target and this is provisional
   * code, so it should not reach those deployments.  See
   * RENAME_DESIGN.md. */
  virtual bool can_rename() const { return false; }
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
                 int dst_dir_fd, const std::string& dst_name,
                 bool excl = false) override;

  int clone_fd(const DoutPrefixProvider* dpp,
               int src_fd,
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
  bool can_rename() const override { return true; }
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
    const std::string& base_path,
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
                 int dst_dir_fd, const std::string& dst_name,
                 bool excl = false) override;

  int clone_fd(const DoutPrefixProvider* dpp,
               int src_fd,
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
