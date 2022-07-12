// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Intel <jianpeng.ma@intel.com>
 *
 * Author: Jianpeng Ma <jianpeng.ma@intel.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <filesystem>
#include <fstream>

#include <fmt/format.h>

#include "PMEMDevice.h"
#include "libpmem.h"
#include "include/types.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/blkdev.h"

#if defined(HAVE_LIBDML)
#include <dml/dml.hpp>
using execution_path = dml::automatic;
#endif

#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev-PMEM("  << path << ") "

PMEMDevice::PMEMDevice(CephContext *cct, aio_callback_t cb, void *cbpriv)
  : BlockDevice(cct, cb, cbpriv),
    fd(-1), addr(0),
    injecting_crash(0)
{
}

int PMEMDevice::_lock()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fd, F_SETLK, &l);
  if (r < 0)
    return -errno;
  return 0;
}

static int pmem_check_file_type(int fd, const char *pmem_file, uint64_t *total_size)
{
  namespace fs = std::filesystem;
  if (!fs::is_character_file(pmem_file)) {
    return -EINVAL;
  }
  struct stat file_stat;
  if (::fstat(fd, &file_stat)) {
    return -EINVAL;
  }
  fs::path char_dir = fmt::format("/sys/dev/char/{}:{}",
				  major(file_stat.st_rdev),
				  minor(file_stat.st_rdev));
  // Need to check if it is a DAX device
  if (auto subsys_path = char_dir / "subsystem";
      fs::read_symlink(subsys_path).filename().string() != "dax") {
    return -EINVAL;
  }
  if (total_size == nullptr) {
    return 0;
  }
  if (std::ifstream size_file(char_dir / "size"); size_file) {
    size_file >> *total_size;
    return size_file ? 0 : -EINVAL;
  } else {
    return -EINVAL;
  }
}

int PMEMDevice::open(const std::string& p)
{
  path = p;
  int r = 0;
  dout(1) << __func__ << " path " << path << dendl;

  fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    r = -errno;
    derr << __func__ << " open got: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = pmem_check_file_type(fd, path.c_str(), &size);
  if (!r) {
    dout(1) << __func__ << " This path " << path << " is a devdax dev " << dendl;
    devdax_device = true;
    // If using devdax char device, set it to not rotational device.
    rotational = false;
  }

  r = _lock();
  if (r < 0) {
    derr << __func__ << " failed to lock " << path << ": " << cpp_strerror(r)
	 << dendl;
    goto out_fail;
  }

  struct stat st;
  r = ::fstat(fd, &st);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fstat got " << cpp_strerror(r) << dendl;
    goto out_fail;
  }

  size_t map_len;
  addr = (char *)pmem_map_file(path.c_str(), 0,
                               devdax_device ? 0: PMEM_FILE_EXCL, O_RDWR,
			       &map_len, NULL);
  if (addr == NULL) {
    derr << __func__ << " pmem_map_file failed: " << pmem_errormsg() << dendl;
    goto out_fail;
  }
  size = map_len;

  // Operate as though the block size is 4 KB.  The backing file
  // blksize doesn't strictly matter except that some file systems may
  // require a read/modify/write if we write something smaller than
  // it.
  block_size = g_conf()->bdev_block_size;
  if (block_size != (unsigned)st.st_blksize) {
    dout(1) << __func__ << " backing device/file reports st_blksize "
      << st.st_blksize << ", using bdev_block_size "
      << block_size << " anyway" << dendl;
  }

  dout(1) << __func__
    << " size " << size
    << " (" << byte_u_t(size) << ")"
    << " block_size " << block_size
    << " (" << byte_u_t(block_size) << ")"
    << dendl;
  return 0;

 out_fail:
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  fd = -1;
  return r;
}

void PMEMDevice::close()
{
  dout(1) << __func__ << dendl;

  ceph_assert(addr != NULL);
  if (devdax_device) {
    devdax_device = false;
  }
  pmem_unmap(addr, size);

  ceph_assert(fd >= 0);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  fd = -1;

  path.clear();
}

int PMEMDevice::collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm) const
{
  (*pm)[prefix + "rotational"] = stringify((int)(bool)rotational);
  (*pm)[prefix + "size"] = stringify(get_size());
  (*pm)[prefix + "block_size"] = stringify(get_block_size());
  (*pm)[prefix + "driver"] = "PMEMDevice";
  (*pm)[prefix + "type"] = "ssd";

  struct stat st;
  int r = ::fstat(fd, &st);
  if (r < 0)
    return -errno;
  if (S_ISBLK(st.st_mode)) {
    (*pm)[prefix + "access_mode"] = "blk";
    char buffer[1024] = {0};
    BlkDev blkdev(fd);

    blkdev.model(buffer, sizeof(buffer));
    (*pm)[prefix + "model"] = buffer;

    buffer[0] = '\0';
    blkdev.dev(buffer, sizeof(buffer));
    (*pm)[prefix + "dev"] = buffer;

    // nvme exposes a serial number
    buffer[0] = '\0';
    blkdev.serial(buffer, sizeof(buffer));
    (*pm)[prefix + "serial"] = buffer;

  } else if (S_ISCHR(st.st_mode)) {
    (*pm)[prefix + "access_mode"] = "chardevice";
    (*pm)[prefix + "path"] = path;

  } else {
    (*pm)[prefix + "access_mode"] = "file";
    (*pm)[prefix + "path"] = path;
  }
  return 0;
}

bool PMEMDevice::support(const std::string &path)
{
  int is_pmem = 0;
  size_t map_len = 0;
  int r = 0;
  int local_fd;

  local_fd = ::open(path.c_str(), O_RDWR);
  if (local_fd < 0) {
    return false;
  }

  r = pmem_check_file_type(local_fd, path.c_str(), NULL);
  VOID_TEMP_FAILURE_RETRY(::close(local_fd));
  int flags = PMEM_FILE_EXCL;
  if (r == 0) {
    flags = 0;
  }

  void *addr = pmem_map_file(path.c_str(), 0, flags, O_RDONLY, &map_len, &is_pmem);
  if (addr != NULL) {
    pmem_unmap(addr, map_len);
    if (is_pmem) {
      return true;
    }
  }

  return false;
}

int PMEMDevice::flush()
{
  //Because all write is persist. So no need
  return 0;
}


void PMEMDevice::aio_submit(IOContext *ioc)
{
  if (ioc->priv) {
    ceph_assert(ioc->num_running == 0);
    aio_callback(aio_callback_priv, ioc->priv);
  } else {
    ioc->try_aio_wake();
  }
  return;
}

int PMEMDevice::write(uint64_t off, bufferlist& bl, bool buffered, int write_hint)
{
  uint64_t len = bl.length();
  dout(20) << __func__ << " " << off << "~" << len  << dendl;
  ceph_assert(is_valid_io(off, len));

  dout(40) << "data:\n";
  bl.hexdump(*_dout);
  *_dout << dendl;

  if (g_conf()->bdev_inject_crash &&
      rand() % g_conf()->bdev_inject_crash == 0) {
    derr << __func__ << " bdev_inject_crash: dropping io " << off << "~" << len
      << dendl;
    ++injecting_crash;
    return 0;
  }

  bufferlist::iterator p = bl.begin();
  uint64_t off1 = off;
  while (len) {
    const char *data;
    uint32_t l = p.get_ptr_and_advance(len, &data);

#if defined(HAVE_LIBDML)
    // Take care of the persistency issue
    auto result = dml::execute<execution_path>(dml::mem_move, dml::make_view(data, l), dml::make_view(addr + off1, l));
    ceph_assert(result.status == dml::status_code::ok);
#else
    pmem_memcpy_persist(addr + off1, data, l);
#endif
    len -= l;
    off1 += l;
  }
  return 0;
}

int PMEMDevice::aio_write(
  uint64_t off,
  bufferlist &bl,
  IOContext *ioc,
  bool buffered,
  int write_hint)
{
  return write(off, bl, buffered);
}


int PMEMDevice::read(uint64_t off, uint64_t len, bufferlist *pbl,
		      IOContext *ioc,
		      bool buffered)
{
  dout(5) << __func__ << " " << off << "~" << len  << dendl;
  ceph_assert(is_valid_io(off, len));

  bufferptr p = buffer::create_small_page_aligned(len);

#if defined(HAVE_LIBDML)
  auto result = dml::execute<execution_path>(dml::mem_move, dml::make_view(addr + off, len), dml::make_view(p.c_str(), len));
  ceph_assert(result.status == dml::status_code::ok);
#else
  memcpy(p.c_str(), addr + off, len);
#endif

  pbl->clear();
  pbl->push_back(std::move(p));

  dout(40) << "data:\n";
  pbl->hexdump(*_dout);
  *_dout << dendl;

  return 0;
}

int PMEMDevice::aio_read(uint64_t off, uint64_t len, bufferlist *pbl,
		      IOContext *ioc)
{
  return read(off, len, pbl, ioc, false);
}

int PMEMDevice::read_random(uint64_t off, uint64_t len, char *buf, bool buffered)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  ceph_assert(is_valid_io(off, len));


#if defined(HAVE_LIBDML)
  auto result = dml::execute<execution_path>(dml::mem_move, dml::make_view(addr + off, len), dml::make_view(buf, len));
  ceph_assert(result.status == dml::status_code::ok);
#else
  memcpy(buf, addr + off, len);
#endif
  return 0;
}


int PMEMDevice::invalidate_cache(uint64_t off, uint64_t len)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  return 0;
}


