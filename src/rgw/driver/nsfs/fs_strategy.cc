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
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <linux/stat.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/xattr.h>
#include <unistd.h>

#include "gpfs/gpfs.h"
#include "gpfs/gpfs_fcntl.h"

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

SafeResult POSIXStrategy::safe_link(const DoutPrefixProvider* dpp,
                                    int src_dir_fd,
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

SafeResult POSIXStrategy::safe_unlink(const DoutPrefixProvider* dpp,
                                      int dir_fd, const std::string& name,
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

static int copy_file_data(int src_fd, int dst_fd, off64_t size)
{
  off64_t soff = 0, doff = 0;
  while (size > 0) {
    ssize_t copied = ::copy_file_range(src_fd, &soff, dst_fd, &doff, size, 0);
    if (copied == 0) {
      return (soff > 0) ? 0 : -EIO;
    }
    if (copied < 0) {
      if (errno == EXDEV || errno == ENOSYS || errno == EOPNOTSUPP) {
        char buf[65536];
        ::lseek(src_fd, soff, SEEK_SET);
        while (size > 0) {
          ssize_t nr = ::read(src_fd, buf,
                              std::min(size, (off64_t)sizeof(buf)));
          if (nr <= 0) return -EIO;
          ssize_t nw = ::write(dst_fd, buf, nr);
          if (nw != nr) return -EIO;
          size -= nr;
        }
        return 0;
      }
      return -errno;
    }
    size -= copied;
  }
  return 0;
}

/* OFD (open file description) lock handle — per-fd, works across
 * threads and local processes but NOT across GPFS cluster nodes. */
class OFDLockHandle : public VersionLockHandle {
  int fd;
public:
  explicit OFDLockHandle(int fd) : fd(fd) {
    if (fd >= 0) {
      struct flock fl{};
      fl.l_type = F_WRLCK;
      fl.l_whence = SEEK_SET;
      ::fcntl(fd, F_OFD_SETLKW, &fl);
    }
  }
  ~OFDLockHandle() override {
    if (fd >= 0) {
      struct flock fl{};
      fl.l_type = F_UNLCK;
      fl.l_whence = SEEK_SET;
      ::fcntl(fd, F_OFD_SETLK, &fl);
      ::close(fd);
    }
  }
};

std::unique_ptr<VersionLockHandle> POSIXStrategy::version_lock(
  const DoutPrefixProvider* dpp, int lock_fd)
{
  return std::make_unique<OFDLockHandle>(lock_fd);
}

int POSIXStrategy::get_xattrs(const DoutPrefixProvider* dpp, int fd,
                              xattr_map_t& attrs)
{
  char namebuf[64 * 1024];
  ssize_t list_len = ::flistxattr(fd, namebuf, sizeof(namebuf));
  if (list_len < 0) {
    int err = errno;
    ldpp_dout(dpp, 0) << "ERROR: flistxattr failed: "
      << cpp_strerror(err) << dendl;
    return -err;
  }
  if (list_len == 0) {
    return 0;
  }

  char* p = namebuf;
  ssize_t remaining = list_len;
  while (remaining > 0) {
    std::string name(p);
    ssize_t keylen = name.size() + 1;

    ssize_t vallen = ::fgetxattr(fd, p, nullptr, 0);
    if (vallen < 0) {
      if (errno == ENODATA || errno == EACCES || errno == ERANGE) {
        remaining -= keylen;
        p += keylen;
        continue;
      }
      int err = errno;
      ldpp_dout(dpp, 0) << "ERROR: fgetxattr " << p
        << " failed: " << cpp_strerror(err) << dendl;
      return -err;
    }

    std::string value(vallen, '\0');
    if (vallen > 0) {
      ssize_t got = ::fgetxattr(fd, p, &value[0], vallen);
      if (got < 0) {
        remaining -= keylen;
        p += keylen;
        continue;
      }
      value.resize(got);
    }

    attrs.emplace(std::move(name), std::move(value));
    remaining -= keylen;
    p += keylen;
  }
  return 0;
}

int POSIXStrategy::set_xattrs(const DoutPrefixProvider* dpp, int fd,
                              const xattr_map_t& attrs)
{
  for (auto& [name, value] : attrs) {
    int ret = ::fsetxattr(fd, name.c_str(), value.data(), value.size(), 0);
    if (ret < 0) {
      int err = errno;
      ldpp_dout(dpp, 0) << "ERROR: fsetxattr " << name
        << " failed: " << cpp_strerror(err) << dendl;
      return -err;
    }
  }
  return 0;
}

int POSIXStrategy::remove_xattrs(const DoutPrefixProvider* dpp, int fd,
                                 const std::vector<std::string>& names)
{
  for (auto& name : names) {
    int ret = ::fremovexattr(fd, name.c_str());
    if (ret < 0 && errno != ENODATA) {
      int err = errno;
      ldpp_dout(dpp, 0) << "ERROR: fremovexattr " << name
        << " failed: " << cpp_strerror(err) << dendl;
      return -err;
    }
  }
  return 0;
}

int POSIXStrategy::clone_file(const DoutPrefixProvider* dpp,
                              int src_dir_fd, const std::string& src_name,
                              int dst_dir_fd, const std::string& dst_name)
{
  int src_fd = ::openat(src_dir_fd, src_name.c_str(), O_RDONLY);
  if (src_fd < 0) {
    int err = errno;
    ldpp_dout(dpp, 0) << "ERROR: clone_file: openat src=" << src_name
      << " failed: " << cpp_strerror(err) << dendl;
    return -err;
  }

  struct statx stx;
  int ret = statx(src_fd, "", AT_EMPTY_PATH, STATX_SIZE, &stx);
  if (ret < 0) {
    int err = errno;
    ::close(src_fd);
    ldpp_dout(dpp, 0) << "ERROR: clone_file: statx src=" << src_name
      << " failed: " << cpp_strerror(err) << dendl;
    return -err;
  }

  int dst_fd = ::openat(dst_dir_fd, dst_name.c_str(),
                         O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (dst_fd < 0) {
    int err = errno;
    ::close(src_fd);
    ldpp_dout(dpp, 0) << "ERROR: clone_file: openat dst=" << dst_name
      << " failed: " << cpp_strerror(err) << dendl;
    return -err;
  }

  ret = copy_file_data(src_fd, dst_fd, stx.stx_size);
  ::close(src_fd);
  ::close(dst_fd);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: clone_file: copy " << src_name
      << " → " << dst_name << " failed: " << cpp_strerror(-ret) << dendl;
    ::unlinkat(dst_dir_fd, dst_name.c_str(), 0);
  }
  return ret;
}

int POSIXStrategy::clone_fd(const DoutPrefixProvider* dpp,
                            int src_fd,
                            int dst_dir_fd, const std::string& dst_name)
{
  struct statx stx;
  if (statx(src_fd, "", AT_EMPTY_PATH, STATX_SIZE, &stx) < 0) {
    int err = errno;
    ldpp_dout(dpp, 0) << "ERROR: clone_fd: statx src failed: "
      << cpp_strerror(err) << dendl;
    return -err;
  }

  int tmp_fd = ::openat(dst_dir_fd, ".", O_TMPFILE | O_WRONLY, 0644);
  if (tmp_fd < 0) {
    int err = errno;
    ldpp_dout(dpp, 0) << "ERROR: clone_fd: O_TMPFILE failed: "
      << cpp_strerror(err) << dendl;
    return -err;
  }

  ::lseek(src_fd, 0, SEEK_SET);
  int ret = copy_file_data(src_fd, tmp_fd, stx.stx_size);
  if (ret < 0) {
    ::close(tmp_fd);
    ldpp_dout(dpp, 0) << "ERROR: clone_fd: copy -> " << dst_name
      << " failed: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = link_temp_file(tmp_fd, dst_dir_fd, dst_name, dpp);
  ::close(tmp_fd);
  return ret;
}

/* --- GPFSStrategy ----------------------------------------------------- */

GPFSStrategy::~GPFSStrategy()
{
  if (lwe_session != GPFS_LWE_NO_SESSION && fn_lwe_destroy_session) {
    fn_lwe_destroy_session(lwe_session);
  }
  if (dmapi_handle) {
    dlclose(dmapi_handle);
  }
  if (dl_handle) {
    dlclose(dl_handle);
  }
}

std::unique_ptr<GPFSStrategy> GPFSStrategy::try_create(
  const DoutPrefixProvider* dpp, const std::string& dl_path,
  const std::string& base_path,
  bool clone_enabled, bool lwe_enabled, bool batch_xattrs)
{
  /*
   * Verify the data root is actually a GPFS filesystem.  The GPFS
   * shared library may be installed (e.g. for client tools) even
   * when the data directory lives on XFS or ext4.
   *
   * TODO: when multi-account lands with per-account filesets on
   * potentially different filesystems, this check will need to be
   * per-fileset rather than a single check at init time.
   */
  struct statfs sfs;
  if (::statfs(base_path.c_str(), &sfs) == 0) {
    if (sfs.f_type != GPFS_SUPER_MAGIC) {
      ldpp_dout(dpp, 1) << "gpfs: base_path " << base_path
        << " is not a GPFS filesystem (f_type=0x"
        << std::hex << sfs.f_type << std::dec
        << "), falling back to POSIX strategy" << dendl;
      return nullptr;
    }
  } else {
    ldpp_dout(dpp, 0) << "gpfs: statfs(" << base_path
      << ") failed: " << cpp_strerror(errno) << dendl;
    return nullptr;
  }

  void* dl = dlopen(dl_path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!dl) {
    ldpp_dout(dpp, 5) << "gpfs: dlopen " << dl_path
      << " failed: " << dlerror() << dendl;
    return nullptr;
  }

  auto la = reinterpret_cast<decltype(&gpfs_linkat)>(
    dlsym(dl, "gpfs_linkat"));
  auto lai = reinterpret_cast<decltype(&gpfs_linkatif)>(
    dlsym(dl, "gpfs_linkatif"));
  auto ua = reinterpret_cast<decltype(&gpfs_unlinkat)>(
    dlsym(dl, "gpfs_unlinkat"));
  auto cs = reinterpret_cast<decltype(&gpfs_clone_snap)>(
    dlsym(dl, "gpfs_clone_snap"));
  auto cc = reinterpret_cast<decltype(&gpfs_clone_copy)>(
    dlsym(dl, "gpfs_clone_copy"));
  auto cu = reinterpret_cast<decltype(&gpfs_clone_unsnap)>(
    dlsym(dl, "gpfs_clone_unsnap"));

  auto fc = reinterpret_cast<GPFSStrategy::gpfs_fcntl_t>(
    dlsym(dl, "gpfs_fcntl"));

  if (!la || !lai || !ua) {
    ldpp_dout(dpp, 0) << "gpfs: dlsym failed — missing symbols in "
      << dl_path << dendl;
    dlclose(dl);
    return nullptr;
  }

  if (!cs || !cc || !cu) {
    ldpp_dout(dpp, 5) << "gpfs: clone symbols not available, "
      << "clone_file will fall back to copy" << dendl;
  }

  /* LWE cluster-wide locking.
   *
   * GPFS Lightweight Events (LWE) provide cluster-wide exclusive
   * access rights coordinated by the GPFS token manager.  Unlike
   * OFD/POSIX locks which only serialize within a single kernel,
   * LWE rights are enforced across all nodes mounting the same
   * GPFS filesystem — required for multi-gateway deployments.
   *
   * We dlopen libdmapi.so for dm_fd_to_handle/dm_handle_free
   * (converts an fd to the opaque DMAPI handle that LWE APIs
   * require), then dlsym the LWE session and right management
   * functions from libgpfs.so itself. */
  dm_fd_to_handle_t dm_fd = nullptr;
  dm_handle_free_t dm_free = nullptr;
  lwe_create_session_t lcs = nullptr;
  lwe_destroy_session_t lds = nullptr;
  lwe_request_right_t lrr = nullptr;
  lwe_release_right_t lrl = nullptr;
  gpfs_lwe_sessid_t session = GPFS_LWE_NO_SESSION;
  void* dmapi_dl = nullptr;
  bool lwe_ok = false;

  if (lwe_enabled) {
    dmapi_dl = dlopen("libdmapi.so", RTLD_NOW | RTLD_LOCAL);
    if (!dmapi_dl) {
      ldpp_dout(dpp, 5) << "gpfs: dlopen libdmapi.so failed: "
        << dlerror() << ", LWE locking will fall back to OFD" << dendl;
    } else {
      dm_fd = reinterpret_cast<dm_fd_to_handle_t>(
        dlsym(dmapi_dl, "dm_fd_to_handle"));
      dm_free = reinterpret_cast<dm_handle_free_t>(
        dlsym(dmapi_dl, "dm_handle_free"));
    }

    lcs = reinterpret_cast<lwe_create_session_t>(
      dlsym(dl, "gpfs_lwe_create_session"));
    lds = reinterpret_cast<lwe_destroy_session_t>(
      dlsym(dl, "gpfs_lwe_destroy_session"));
    lrr = reinterpret_cast<lwe_request_right_t>(
      dlsym(dl, "gpfs_lwe_request_right"));
    lrl = reinterpret_cast<lwe_release_right_t>(
      dlsym(dl, "gpfs_lwe_release_right"));

    if (dm_fd && dm_free && lcs && lds && lrr && lrl) {
      char sess_info[] = "ceph-rgw-nsfs";
      int ret = lcs(GPFS_LWE_NO_SESSION, sess_info, &session);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "gpfs: lwe_create_session failed: "
          << cpp_strerror(errno)
          << ", LWE locking will fall back to OFD" << dendl;
        session = GPFS_LWE_NO_SESSION;
      } else {
        lwe_ok = true;
      }
    } else {
      ldpp_dout(dpp, 5) << "gpfs: LWE/DMAPI symbols not available, "
        << "version locking will fall back to OFD" << dendl;
    }
  }

  bool batch_ok = batch_xattrs && fc;
  if (batch_xattrs && !fc) {
    ldpp_dout(dpp, 5) << "gpfs: gpfs_fcntl symbol not available, "
      << "batch xattr ops will fall back to POSIX" << dendl;
  }

  std::string features;
  if (clone_enabled) features += " clone";
  if (lwe_ok) features += " lwe";
  if (batch_ok) features += " batch-xattrs";
  ldpp_dout(dpp, 1) << "gpfs: loaded " << dl_path
    << (features.empty() ? "" : " (" + features.substr(1) + ")") << dendl;

  return std::unique_ptr<GPFSStrategy>(
    new GPFSStrategy(dl, dmapi_dl, la, lai, ua, cs, cc, cu, fc,
                     clone_enabled, batch_ok,
                     dm_fd, dm_free, lcs, lds, lrr, lrl, session, lwe_ok));
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

SafeResult GPFSStrategy::safe_link(const DoutPrefixProvider* dpp,
                                   int src_dir_fd,
                                   const std::string& src_name,
                                   int dst_dir_fd,
                                   const std::string& dst_name,
                                   uint64_t expected_mtime_ns,
                                   uint64_t expected_ino)
{
  /* verify the source still matches expectations before linking */
  if (!stat_matches(src_dir_fd, src_name, expected_mtime_ns, expected_ino)) {
    ldpp_dout(dpp, 5) << "gpfs safe_link: source mismatch or gone"
      << " src=" << src_name << dendl;
    return SafeResult::MISMATCH;
  }

  /* gpfs_linkatif with replacefd=0 skips the destination inode check,
   * creating a new link or unconditionally replacing an existing one */
  int ret = fn_linkatif(src_dir_fd, src_name.c_str(),
                        dst_dir_fd, dst_name.c_str(),
                        0, 0);
  int err = errno;

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: gpfs_linkatif failed: "
      << cpp_strerror(err)
      << " src=" << src_name << " dst=" << dst_name << dendl;
    if (err == EEXIST) {
      return SafeResult::MISMATCH;
    }
    return SafeResult::ERROR;
  }
  return SafeResult::OK;
}

SafeResult GPFSStrategy::safe_unlink(const DoutPrefixProvider* dpp,
                                     int dir_fd, const std::string& name,
                                     int tmp_dir_fd,
                                     uint64_t expected_mtime_ns,
                                     uint64_t expected_ino)
{
  /* open the target to get an fd; gpfs_unlinkat removes the entry
   * only if its inode matches this fd */
  int fd = ::openat(dir_fd, name.c_str(), O_RDONLY);
  if (fd < 0) {
    int err = errno;
    if (err == ENOENT) {
      return SafeResult::OK;
    }
    ldpp_dout(dpp, 0) << "ERROR: gpfs safe_unlink: openat " << name
      << " failed: " << cpp_strerror(err) << dendl;
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
  int err = errno;
  ::close(fd);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: gpfs_unlinkat failed: "
      << cpp_strerror(err) << " name=" << name << dendl;
    if (err == EEXIST) {
      return SafeResult::MISMATCH;
    }
    return SafeResult::ERROR;
  }
  return SafeResult::OK;
}

static std::string fd_path(int dir_fd, const std::string& name)
{
  char proc[64];
  char resolved[PATH_MAX];
  snprintf(proc, sizeof(proc), "/proc/self/fd/%d", dir_fd);
  ssize_t len = ::readlink(proc, resolved, sizeof(resolved) - 1);
  if (len < 0) {
    return {};
  }
  resolved[len] = '\0';
  return std::string(resolved) + "/" + name;
}

/* hidden name for clone parent files — dot-prefix keeps them
 * out of bucket listings */
static std::string clone_parent_name(const std::string& name)
{
  return ".clone_parent." + name;
}

int GPFSStrategy::clone_file(const DoutPrefixProvider* dpp,
                             int src_dir_fd, const std::string& src_name,
                             int dst_dir_fd, const std::string& dst_name)
{
  if (!has_clone()) {
    ldpp_dout(dpp, 10) << "gpfs clone_file: clone symbols not available, "
      << "falling back to copy" << dendl;
    return POSIXStrategy().clone_file(dpp, src_dir_fd, src_name,
                                      dst_dir_fd, dst_name);
  }

  std::string src_path = fd_path(src_dir_fd, src_name);
  std::string dst_path = fd_path(dst_dir_fd, dst_name);
  if (src_path.empty() || dst_path.empty()) {
    ldpp_dout(dpp, 0) << "ERROR: gpfs clone_file: could not resolve paths"
      << dendl;
    return -EINVAL;
  }

  /* Experimental GPFS clone path (see fs_strategy.h for caveats).
   *
   * snap the source into a hidden immutable clone parent in the
   * destination directory, then clone_copy to produce a mutable
   * child.  The clone child gets its own inode with independent
   * xattrs, sharing only data blocks with the parent.
   *
   * The snap parent must persist as long as the child exists.
   * cleanup_clone() handles removal when the child is deleted or
   * overwritten.  Without a GPFS auto-gc extension for orphaned
   * clone parents, cleanup depends on callers invoking
   * cleanup_clone() on every path that destroys the child. */
  std::string snap_name = clone_parent_name(dst_name);
  std::string snap_path = fd_path(dst_dir_fd, snap_name);
  if (snap_path.empty()) {
    ldpp_dout(dpp, 0) << "ERROR: gpfs clone_file: could not resolve snap path"
      << dendl;
    return -EINVAL;
  }

  /* clean up any stale parent from a previous clone of the same name */
  cleanup_clone(dpp, dst_dir_fd, dst_name);

  int ret = fn_clone_snap(src_path.c_str(), snap_path.c_str());
  if (ret < 0) {
    int err = errno;
    ldpp_dout(dpp, 5) << "gpfs clone_file: clone_snap " << src_name
      << " failed: " << cpp_strerror(err)
      << ", falling back to copy" << dendl;
    return POSIXStrategy().clone_file(dpp, src_dir_fd, src_name,
                                      dst_dir_fd, dst_name);
  }

  ret = fn_clone_copy(snap_path.c_str(), dst_path.c_str());
  if (ret < 0) {
    int err = errno;
    ldpp_dout(dpp, 5) << "gpfs clone_file: clone_copy " << src_name
      << " → " << dst_name << " failed: " << cpp_strerror(err)
      << ", falling back to copy" << dendl;
    cleanup_clone(dpp, dst_dir_fd, dst_name);
    return POSIXStrategy().clone_file(dpp, src_dir_fd, src_name,
                                      dst_dir_fd, dst_name);
  }

  ldpp_dout(dpp, 10) << "gpfs clone_file: cloned " << src_name
    << " → " << dst_name << dendl;
  return 0;
}

int GPFSStrategy::clone_fd(const DoutPrefixProvider* dpp,
                           int src_fd,
                           int dst_dir_fd, const std::string& dst_name)
{
  return POSIXStrategy().clone_fd(dpp, src_fd, dst_dir_fd, dst_name);
}

void GPFSStrategy::cleanup_clone(const DoutPrefixProvider* dpp,
                                 int dir_fd, const std::string& name)
{
  if (!fn_clone_unsnap) {
    return;
  }

  std::string snap_name = clone_parent_name(name);
  int fd = ::openat(dir_fd, snap_name.c_str(), O_RDONLY);
  if (fd < 0) {
    return;
  }

  int ret = fn_clone_unsnap(fd);
  int err = errno;
  ::close(fd);

  if (ret < 0) {
    /* unsnap fails if the parent still has children — this is
     * expected if the child hasn't been unlinked yet, or if
     * multiple clones share the same parent.  Log at debug
     * level; a future sweep can retry. */
    ldpp_dout(dpp, 10) << "gpfs cleanup_clone: unsnap "
      << snap_name << " failed: " << cpp_strerror(err)
      << " (may still have children)" << dendl;
    return;
  }

  ret = ::unlinkat(dir_fd, snap_name.c_str(), 0);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "gpfs cleanup_clone: unlink "
      << snap_name << " failed: " << cpp_strerror(errno) << dendl;
  } else {
    ldpp_dout(dpp, 10) << "gpfs cleanup_clone: removed "
      << snap_name << dendl;
  }
}

/* --- GPFS batch xattr operations ----------------------------------------
 *
 * gpfs_fcntl accepts a buffer starting with gpfsFcntlHeader_t followed
 * by one or more typed structs.  For xattrs, each gpfsGetSetXAttr_t
 * carries one name+value pair.  We pack as many as fit into a single
 * GPFS_MAX_FCNTL_LENGTH (64KB) buffer, reducing N syscalls to 1.
 *
 * Buffer layout for GET_XATTR:
 *   [header][getset1 + name\0 + padding][getset2 + name\0 + padding]...
 *
 * For SET_XATTR:
 *   [header][getset1 + name\0 + padding + value + padding]...
 *
 * Name and value are each rounded up to 8-byte alignment. */

static size_t align8(size_t n) { return (n + 7) & ~7; }

int GPFSStrategy::get_xattrs(const DoutPrefixProvider* dpp, int fd,
                             xattr_map_t& attrs)
{
  if (!has_batch_xattrs()) {
    return POSIXStrategy().get_xattrs(dpp, fd, attrs);
  }

  /* first, list xattr names via gpfs_fcntl LIST_XATTR */
  struct {
    gpfsFcntlHeader_t hdr;
    gpfsListXAttr_t list;
    char buf[GPFS_MAX_FCNTL_LENGTH - sizeof(gpfsFcntlHeader_t)
             - sizeof(gpfsListXAttr_t)];
  } list_arg;
  memset(&list_arg, 0, sizeof(list_arg));
  list_arg.hdr.totalLength = sizeof(list_arg);
  list_arg.hdr.fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
  list_arg.list.structLen = sizeof(list_arg.list) + sizeof(list_arg.buf);
  list_arg.list.structType = GPFS_FCNTL_LIST_XATTR;
  list_arg.list.bufferLen = sizeof(list_arg.buf);

  int ret = fn_fcntl(fd, &list_arg);
  if (ret < 0) {
    int err = errno;
    ldpp_dout(dpp, 5) << "gpfs get_xattrs: LIST_XATTR failed: "
      << cpp_strerror(err) << ", falling back to POSIX" << dendl;
    return POSIXStrategy().get_xattrs(dpp, fd, attrs);
  }

  /* parse the length-prefixed name list */
  std::vector<std::string> names;
  char* p = list_arg.list.buffer;
  int remaining = list_arg.list.bufferLen;
  while (remaining > 0 && *p != '\0') {
    uint8_t namelen = static_cast<uint8_t>(*p);
    p++; remaining--;
    if (namelen > remaining) break;
    names.emplace_back(p, namelen);
    p += namelen; remaining -= namelen;
  }

  if (names.empty()) {
    return 0;
  }

  /*
   * Batch GET_XATTR with multi-round overflow.
   *
   * We start with a modest per-value buffer (4KB).  If a value is
   * larger (ERR_BUFFER_TOO_SMALL), we retry that name alone with
   * the full XATTR_MAX_VALUELEN.  When entries don't fit in the
   * 64KB gpfs_fcntl buffer, we flush the current batch and start
   * a new round with the remaining names.
   */
  static constexpr size_t DEFAULT_VAL_SPACE = 4096;

  char get_buf[GPFS_MAX_FCNTL_LENGTH];
  struct xattr_slot {
    size_t struct_off;
    size_t name_idx;
  };

  size_t ni = 0;
  while (ni < names.size()) {
    auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(get_buf);
    memset(hdr, 0, sizeof(*hdr));
    hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

    size_t off = sizeof(gpfsFcntlHeader_t);
    std::vector<xattr_slot> slots;

    while (ni < names.size()) {
      size_t namelen = names[ni].size() + 1;
      size_t padded_name = align8(namelen);
      size_t val_space = align8(DEFAULT_VAL_SPACE);
      size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + val_space;

      if (off + entry_size > sizeof(get_buf)) {
        break;
      }

      auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(get_buf + off);
      memset(ea, 0, entry_size);
      ea->structLen = entry_size;
      ea->structType = GPFS_FCNTL_GET_XATTR;
      ea->nameLen = namelen;
      ea->bufferLen = padded_name + val_space;
      ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
      memcpy(ea->buffer, names[ni].c_str(), namelen);

      slots.push_back({off, ni});
      off += entry_size;
      ni++;
    }

    if (slots.empty()) {
      break;
    }

    hdr->totalLength = off;
    ret = fn_fcntl(fd, get_buf);
    if (ret < 0) {
      int err = errno;
      ldpp_dout(dpp, 5) << "gpfs get_xattrs: batch GET_XATTR failed: "
        << cpp_strerror(err) << ", falling back to POSIX" << dendl;
      return POSIXStrategy().get_xattrs(dpp, fd, attrs);
    }

    for (auto& slot : slots) {
      auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(get_buf + slot.struct_off);
      if (ea->errReasonCode == GPFS_FCNTL_ERR_BUFFER_TOO_SMALL) {
        /* retry this name alone with full buffer */
        char retry_buf[GPFS_MAX_FCNTL_LENGTH];
        auto* rh = reinterpret_cast<gpfsFcntlHeader_t*>(retry_buf);
        memset(rh, 0, sizeof(*rh));
        rh->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

        auto& rname = names[slot.name_idx];
        size_t rnamelen = rname.size() + 1;
        size_t rpadded_name = align8(rnamelen);
        size_t rval_space = align8(GPFS_FCNTL_XATTR_MAX_VALUELEN);
        size_t rentry_size = sizeof(gpfsGetSetXAttr_t) + rpadded_name + rval_space;

        auto* rea = reinterpret_cast<gpfsGetSetXAttr_t*>(retry_buf + sizeof(gpfsFcntlHeader_t));
        memset(rea, 0, rentry_size);
        rea->structLen = rentry_size;
        rea->structType = GPFS_FCNTL_GET_XATTR;
        rea->nameLen = rnamelen;
        rea->bufferLen = rpadded_name + rval_space;
        rea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
        memcpy(rea->buffer, rname.c_str(), rnamelen);

        rh->totalLength = sizeof(gpfsFcntlHeader_t) + rentry_size;
        int rret = fn_fcntl(fd, retry_buf);
        if (rret == 0 && rea->errReasonCode == 0 && rea->bufferLen > 0) {
          char* valp = rea->buffer + rpadded_name;
          attrs.try_emplace(rname, std::string(valp, rea->bufferLen));
        }
      } else if (ea->errReasonCode == 0 && ea->bufferLen > 0) {
        size_t padded_name = align8(ea->nameLen);
        char* valp = ea->buffer + padded_name;
        attrs.try_emplace(names[slot.name_idx],
                          std::string(valp, ea->bufferLen));
      }
    }
  }

  ldpp_dout(dpp, 20) << "gpfs get_xattrs: batch read "
    << attrs.size() << " xattrs" << dendl;
  return 0;
}

int GPFSStrategy::set_xattrs(const DoutPrefixProvider* dpp, int fd,
                             const xattr_map_t& attrs)
{
  if (!has_batch_xattrs() || attrs.empty()) {
    return POSIXStrategy().set_xattrs(dpp, fd, attrs);
  }

  char buf[GPFS_MAX_FCNTL_LENGTH];
  auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf);
  memset(hdr, 0, sizeof(*hdr));
  hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

  size_t off = sizeof(gpfsFcntlHeader_t);

  for (auto& [name, value] : attrs) {
    size_t namelen = name.size() + 1;
    size_t padded_name = align8(namelen);
    size_t padded_value = align8(value.size());
    size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name + padded_value;

    if (off + entry_size > sizeof(buf)) {
      /* flush what we have so far */
      hdr->totalLength = off;
      int ret = fn_fcntl(fd, buf);
      if (ret < 0) {
        int err = errno;
        ldpp_dout(dpp, 5) << "gpfs set_xattrs: batch SET_XATTR failed: "
          << cpp_strerror(err) << ", falling back to POSIX" << dendl;
        return POSIXStrategy().set_xattrs(dpp, fd, attrs);
      }
      off = sizeof(gpfsFcntlHeader_t);
      memset(hdr, 0, sizeof(*hdr));
      hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;
    }

    auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf + off);
    memset(ea, 0, entry_size);
    ea->structLen = entry_size;
    ea->structType = GPFS_FCNTL_SET_XATTR;
    ea->nameLen = namelen;
    ea->bufferLen = value.size();
    ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
    memcpy(ea->buffer, name.c_str(), namelen);
    memcpy(ea->buffer + padded_name, value.data(), value.size());

    off += entry_size;
  }

  hdr->totalLength = off;
  int ret = fn_fcntl(fd, buf);
  if (ret < 0) {
    int err = errno;
    ldpp_dout(dpp, 5) << "gpfs set_xattrs: batch SET_XATTR failed: "
      << cpp_strerror(err) << ", falling back to POSIX" << dendl;
    return POSIXStrategy().set_xattrs(dpp, fd, attrs);
  }

  ldpp_dout(dpp, 20) << "gpfs set_xattrs: batch wrote "
    << attrs.size() << " xattrs" << dendl;
  return 0;
}

int GPFSStrategy::remove_xattrs(const DoutPrefixProvider* dpp, int fd,
                                const std::vector<std::string>& names)
{
  if (!has_batch_xattrs() || names.empty()) {
    return POSIXStrategy().remove_xattrs(dpp, fd, names);
  }

  char buf[GPFS_MAX_FCNTL_LENGTH];
  auto* hdr = reinterpret_cast<gpfsFcntlHeader_t*>(buf);
  memset(hdr, 0, sizeof(*hdr));
  hdr->fcntlVersion = GPFS_FCNTL_CURRENT_VERSION;

  size_t off = sizeof(gpfsFcntlHeader_t);

  for (auto& name : names) {
    size_t namelen = name.size() + 1;
    size_t padded_name = align8(namelen);
    size_t entry_size = sizeof(gpfsGetSetXAttr_t) + padded_name;

    if (off + entry_size > sizeof(buf)) break;

    auto* ea = reinterpret_cast<gpfsGetSetXAttr_t*>(buf + off);
    memset(ea, 0, entry_size);
    ea->structLen = entry_size;
    ea->structType = GPFS_FCNTL_SET_XATTR;
    ea->nameLen = namelen;
    ea->bufferLen = -1;
    ea->flags = GPFS_FCNTL_XATTRFLAG_NONE;
    memcpy(ea->buffer, name.c_str(), namelen);

    off += entry_size;
  }

  hdr->totalLength = off;
  int ret = fn_fcntl(fd, buf);
  if (ret < 0) {
    int err = errno;
    ldpp_dout(dpp, 5) << "gpfs remove_xattrs: batch delete failed: "
      << cpp_strerror(err) << ", falling back to POSIX" << dendl;
    return POSIXStrategy().remove_xattrs(dpp, fd, names);
  }

  ldpp_dout(dpp, 20) << "gpfs remove_xattrs: batch deleted "
    << names.size() << " xattrs" << dendl;
  return 0;
}

/* LWE lock handle — holds a cluster-wide exclusive right via the
 * GPFS token manager.  The right is released when the handle is
 * destroyed, then the DMAPI handle is freed. */
class LWELockHandle : public VersionLockHandle {
  int(*fn_release)(gpfs_lwe_sessid_t, void*, size_t, gpfs_lwe_token_t);
  void(*fn_handle_free)(void*, size_t);
  gpfs_lwe_sessid_t session;
  void* hanp;
  size_t hlen;
  gpfs_lwe_token_t token;
  int fd;
public:
  LWELockHandle(decltype(fn_release) rel,
                decltype(fn_handle_free) hfree,
                gpfs_lwe_sessid_t sid,
                void* h, size_t hl, gpfs_lwe_token_t tok, int fd)
    : fn_release(rel), fn_handle_free(hfree),
      session(sid), hanp(h), hlen(hl), token(tok), fd(fd) {}

  ~LWELockHandle() override {
    fn_release(session, hanp, hlen, token);
    fn_handle_free(hanp, hlen);
    ::close(fd);
  }
};

std::unique_ptr<VersionLockHandle> GPFSStrategy::version_lock(
  const DoutPrefixProvider* dpp, int lock_fd)
{
  if (!has_lwe()) {
    ldpp_dout(dpp, 10) << "gpfs version_lock: LWE not available, "
      << "falling back to OFD" << dendl;
    return std::make_unique<OFDLockHandle>(lock_fd);
  }

  void* hanp = nullptr;
  size_t hlen = 0;
  int ret = fn_dm_fd_to_handle(lock_fd, &hanp, &hlen);
  if (ret < 0) {
    ldpp_dout(dpp, 5) << "gpfs version_lock: dm_fd_to_handle failed: "
      << cpp_strerror(errno)
      << ", falling back to OFD" << dendl;
    return std::make_unique<OFDLockHandle>(lock_fd);
  }

  gpfs_lwe_token_t token;
  ret = fn_lwe_request_right(lwe_session, hanp, hlen,
                             GPFS_LWE_RIGHT_EXCL,
                             GPFS_LWE_FLAG_WAIT, &token);
  if (ret < 0) {
    int err = errno;
    ldpp_dout(dpp, 5) << "gpfs version_lock: lwe_request_right failed: "
      << cpp_strerror(err)
      << ", falling back to OFD" << dendl;
    fn_dm_handle_free(hanp, hlen);
    return std::make_unique<OFDLockHandle>(lock_fd);
  }

  ldpp_dout(dpp, 20) << "gpfs version_lock: acquired LWE exclusive right"
    << dendl;

  /* LWE handle takes ownership of hanp and lock_fd */
  return std::make_unique<LWELockHandle>(
    fn_lwe_release_right, fn_dm_handle_free,
    lwe_session, hanp, hlen, token, lock_fd);
}

} } } // namespace rgw::sal::nsfs
