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

#include "fs_strategy.h"

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <dlfcn.h>
#include <fcntl.h>
#include <linux/stat.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/dout.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace sal { namespace nsfs {

static uint64_t statx_mtime_ns(const struct statx& stx)
{
  return (uint64_t)stx.stx_mtime.tv_sec * 1000000000ULL
       + stx.stx_mtime.tv_nsec;
}

static bool stat_matches(int fd, const std::string& name,
                         uint64_t expected_mtime_ns, uint64_t expected_ino)
{
  struct statx stx;
  int ret = statx(fd, name.c_str(), 0, STATX_INO | STATX_MTIME, &stx);
  if (ret < 0) {
    return false;
  }
  return statx_mtime_ns(stx) == expected_mtime_ns &&
         stx.stx_ino == expected_ino;
}

/* --- POSIXStrategy ---------------------------------------------------- */

int POSIXStrategy::link_temp_file(int temp_fd, int dir_fd,
                                  const std::string& name,
                                  const DoutPrefixProvider* dpp)
{
  char temp_file_path[PATH_MAX];
  snprintf(temp_file_path, PATH_MAX, "/proc/self/fd/%d", temp_fd);

  /* link the O_TMPFILE into the directory under a temp name, then
   * rename to the final name — two-step because linkat(2) cannot
   * atomically replace an existing entry */
  std::string tmp_name = ".tmp_link_" +
    std::to_string(getpid()) + "_" + std::to_string(temp_fd);

  int ret = ::linkat(AT_FDCWD, temp_file_path,
                     dir_fd, tmp_name.c_str(), AT_SYMLINK_FOLLOW);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: linkat for temp file: "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  ret = ::renameat(dir_fd, tmp_name.c_str(), dir_fd, name.c_str());
  if (ret < 0) {
    ret = errno;
    ::unlinkat(dir_fd, tmp_name.c_str(), 0);
    ldpp_dout(dpp, 0) << "ERROR: renameat for temp file: "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

SafeResult POSIXStrategy::safe_link(int src_dir_fd,
                                    const std::string& src_name,
                                    int dst_dir_fd,
                                    const std::string& dst_name,
                                    uint64_t expected_mtime_ns,
                                    uint64_t expected_ino)
{
  int ret = ::linkat(src_dir_fd, src_name.c_str(),
                     dst_dir_fd, dst_name.c_str(), 0);
  if (ret < 0) {
    if (errno == EEXIST) {
      return SafeResult::MISMATCH;
    }
    return SafeResult::ERROR;
  }
  if (stat_matches(dst_dir_fd, dst_name, expected_mtime_ns, expected_ino)) {
    return SafeResult::OK;
  }
  ::unlinkat(dst_dir_fd, dst_name.c_str(), 0);
  return SafeResult::MISMATCH;
}

SafeResult POSIXStrategy::safe_unlink(int dir_fd, const std::string& name,
                                      int tmp_dir_fd,
                                      uint64_t expected_mtime_ns,
                                      uint64_t expected_ino)
{
  static std::atomic<uint64_t> counter{0};
  std::string tmp_name = ".unlink_tmp_" +
    std::to_string(getpid()) + "_" + std::to_string(counter.fetch_add(1));

  int ret = ::renameat(dir_fd, name.c_str(),
                       tmp_dir_fd, tmp_name.c_str());
  if (ret < 0) {
    if (errno == ENOENT) {
      return SafeResult::OK;
    }
    return SafeResult::ERROR;
  }
  if (stat_matches(tmp_dir_fd, tmp_name, expected_mtime_ns, expected_ino)) {
    ::unlinkat(tmp_dir_fd, tmp_name.c_str(), 0);
    return SafeResult::OK;
  }
  ::renameat(tmp_dir_fd, tmp_name.c_str(), dir_fd, name.c_str());
  return SafeResult::MISMATCH;
}

/* --- GPFSStrategy ----------------------------------------------------- */

static const char* gpfs_dl_path = "/usr/lpp/mmfs/lib/libgpfs.so";

GPFSStrategy::~GPFSStrategy()
{
  if (dl_handle) {
    dlclose(dl_handle);
  }
}

std::unique_ptr<GPFSStrategy> GPFSStrategy::try_create(
  const DoutPrefixProvider* dpp)
{
  void* dl = dlopen(gpfs_dl_path, RTLD_NOW | RTLD_LOCAL);
  if (!dl) {
    ldpp_dout(dpp, 5) << "gpfs: dlopen " << gpfs_dl_path
      << " failed: " << dlerror() << dendl;
    return nullptr;
  }

  auto la = reinterpret_cast<decltype(&gpfs_linkat)>(
    dlsym(dl, "gpfs_linkat"));
  auto lai = reinterpret_cast<decltype(&gpfs_linkatif)>(
    dlsym(dl, "gpfs_linkatif"));
  auto ua = reinterpret_cast<decltype(&gpfs_unlinkat)>(
    dlsym(dl, "gpfs_unlinkat"));

  if (!la || !lai || !ua) {
    ldpp_dout(dpp, 0) << "gpfs: dlsym failed — missing symbols in "
      << gpfs_dl_path << dendl;
    dlclose(dl);
    return nullptr;
  }

  ldpp_dout(dpp, 1) << "gpfs: loaded " << gpfs_dl_path << dendl;
  return std::unique_ptr<GPFSStrategy>(new GPFSStrategy(dl, la, lai, ua));
}

int GPFSStrategy::link_temp_file(int temp_fd, int dir_fd,
                                 const std::string& name,
                                 const DoutPrefixProvider* dpp)
{
  /* gpfs_linkat with AT_EMPTY_PATH links an open fd directly into
   * the namespace, atomically replacing any existing entry */
  int ret = fn_linkat(temp_fd, "", dir_fd, name.c_str(), AT_EMPTY_PATH);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: gpfs_linkat for temp file: "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }
  return 0;
}

SafeResult GPFSStrategy::safe_link(int src_dir_fd,
                                   const std::string& src_name,
                                   int dst_dir_fd,
                                   const std::string& dst_name,
                                   uint64_t expected_mtime_ns,
                                   uint64_t expected_ino)
{
  /* open the source to get an fd for linkatif's inode verification */
  int src_fd = ::openat(src_dir_fd, src_name.c_str(), O_RDONLY);
  if (src_fd < 0) {
    if (errno == ENOENT) {
      return SafeResult::MISMATCH;
    }
    return SafeResult::ERROR;
  }

  /* gpfs_linkatif atomically replaces dst only if its inode matches
   * the replacefd — no post-check needed */
  int ret = fn_linkatif(src_dir_fd, src_name.c_str(),
                        dst_dir_fd, dst_name.c_str(),
                        0, src_fd);
  ::close(src_fd);

  if (ret < 0) {
    if (errno == EEXIST) {
      return SafeResult::MISMATCH;
    }
    return SafeResult::ERROR;
  }
  return SafeResult::OK;
}

SafeResult GPFSStrategy::safe_unlink(int dir_fd, const std::string& name,
                                     int tmp_dir_fd,
                                     uint64_t expected_mtime_ns,
                                     uint64_t expected_ino)
{
  /* open the target to get an fd; gpfs_unlinkat removes the entry
   * only if its inode matches this fd */
  int fd = ::openat(dir_fd, name.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT) {
      return SafeResult::OK;
    }
    return SafeResult::ERROR;
  }

  /* verify inode+mtime before attempting the unlink */
  struct statx stx;
  int ret = statx(fd, "", AT_EMPTY_PATH, STATX_INO | STATX_MTIME, &stx);
  if (ret < 0 ||
      statx_mtime_ns(stx) != expected_mtime_ns ||
      stx.stx_ino != expected_ino) {
    ::close(fd);
    return SafeResult::MISMATCH;
  }

  ret = fn_unlinkat(dir_fd, name.c_str(), fd);
  ::close(fd);

  if (ret < 0) {
    if (errno == EEXIST) {
      return SafeResult::MISMATCH;
    }
    return SafeResult::ERROR;
  }
  return SafeResult::OK;
}

} } } // namespace rgw::sal::nsfs
