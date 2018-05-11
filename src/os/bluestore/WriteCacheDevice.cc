// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
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
#include <fcntl.h>

#include "WriteCacheDevice.h"
#include "include/types.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/blkdev.h"
#include "common/align.h"
#include "include/scope_guard.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << this << " " << path << ") "

/**
 * Cache organization
 *
 * Cache area split for 2 separate rows
 * ROW1: [0 ... size/2]
 * [header] [data] [header] [data] ....
 * ROW2: [size/2 ... size]
 * [header] [data] [header] [data] ....
 *
 * header: 4K
 * - cons.id (8bytes)
 * - size
 * - destination position
 */



WriteCacheDevice::WriteCacheDevice(CephContext* cct, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv)
  : KernelDevice(cct, cb, cbpriv, d_cb, d_cbpriv),
    flushing(nullptr),
    current(nullptr),
    empty(nullptr),
    last_id(0)
{
}

WriteCacheDevice::~WriteCacheDevice()
{
  if (current)
    delete current;
  if (empty)
    delete empty;
  if (flushing)
    delete flushing;
}

/*
 * true -  stored to cache
 * false - failed to store
 */
bool WriteCacheDevice::store_in_cache(uint64_t disk_off, bufferlist& bl)
{
  std::lock_guard<std::mutex> l(lock);
  size_t length_align_up = align_up((size_t)bl.length(), block_size);
  string path;
  dout(20) << __func__ <<
      " disk_off=" << disk_off <<
      " bl.length()=" << bl.length() <<
      " length_align_up=" << length_align_up << dendl;

  if (current->pos + (block_size + length_align_up) > current->size)
  {
    /* must switch cache row */
    if (flushing != nullptr) {
      /* must flush, no space to write */
      flush_main();
    }
    assert(flushing == nullptr);
    assert(empty != nullptr);
    flushing = current;
    current = empty;
    flushing->pos = 0;
    empty = flushing;
    flushing = nullptr;
    KernelDevice::flush();
  }

  if (current->pos + (block_size + length_align_up) > current->size)
  {
    /* not able to cache present request */
    current->unflushed = true;
    return false;
  }

  dout(20) << __func__ <<
      " current->pos=" << current->pos <<
      " current->size=" << current->size <<
      " current->disk_offset=" << current->disk_offset << dendl;

  bufferlist header_bl;
  buffer::raw* header_data = buffer::create(block_size);
  header_bl.append(header_data);
  header* h = reinterpret_cast<header*>(header_bl.c_str());
  h->id = last_id;
  last_id++;
  h->dest = disk_off;
  h->size = bl.length();

  write_cache->write(current->disk_offset + current->pos, header_bl, false);
  current->pos += block_size;
  write_cache->write(current->disk_offset + current->pos, bl, false);
  current->pos += length_align_up;
  return true;
}

static void aio_cb(void *priv, void *priv2)
{
  //BlueStore *store = static_cast<BlueStore*>(priv);
  //BlueStore::AioContext *c = static_cast<BlueStore::AioContext*>(priv2);
  //c->aio_finish(store);
}

static void discard_cb(void *priv, void *priv2)
{
  //BlueStore *store = static_cast<BlueStore*>(priv);
  //interval_set<uint64_t> *tmp = static_cast<interval_set<uint64_t>*>(priv2);
  //store->handle_discard(*tmp);
}

int WriteCacheDevice::open_write_cache(CephContext* cct, const std::string& path)
{
  int r;
  int fd;
  fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    derr << __func__ << " cannot open '" << path << "'"  << dendl;
    return -1;
  }
  auto sg = make_scope_guard([=] { ::close(fd); });
  struct stat st;
  r = ::fstat(fd, &st);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fstat '" << path << "' failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  size_t size;
  if (S_ISBLK(st.st_mode)) {
    int64_t s;
    r = get_block_device_size(fd, &s);
    if (r < 0) {
      return -1;
    }
    size = s;
  } else {
    size = st.st_size;
  }
  if (size < cct->_conf->bluestore_block_writecache_size) {
    dout(5) << __func__ << " size of '" << path << "' is " << size << ", writecache NOT ENABLED" << dendl;
    return -1;
  }
  size_t row_size = align_down(cct->_conf->bluestore_block_writecache_size / 2, block_size);

  write_cache = std::make_unique<KernelDevice>(cct, aio_cb, static_cast<void*>(this), discard_cb, static_cast<void*>(this));
  r = write_cache->open(path);
  if (r < 0) {
    dout(20) << __func__ << " cannot open write cache '" << path << "'" << dendl;
    return r;
  }

  current = new row{0, row_size, 0, false};
  empty = new row{row_size, row_size, 0, false};
  return r;
}

static string get_dev_property(const char *dev, const char *property)
{
  char val[1024] = {0};
  get_block_device_string_property(dev, property, val, sizeof(val));
  return val;
}

int WriteCacheDevice::flush_main()
{
  int res = 0;
  bool flush_main_storage = false;
  if (current->unflushed) {
    /* we were unable to store data in cache, so we must flush main storage */
    flush_main_storage = true;
    current->unflushed = false;
  }
  if (flushing != nullptr) {
    /* wait for flushing to finish */
    flush_main_storage = true;
    flushing = current;
    current = empty;
    flushing->pos = 0;
    empty = flushing;
    flushing = nullptr;
  }
  if (flush_main_storage) {
    res = KernelDevice::flush();
  }
  return res;
}


int WriteCacheDevice::flush()
{
  std::lock_guard<std::mutex> l(lock);
  int res = 0, r;
  if (write_cache) {
    res = flush_main();
    r = write_cache->flush();
    if (res == 0) {
      res = r;
    }
  } else {
    res = KernelDevice::flush();
  }
  return res;
}

void WriteCacheDevice::aio_submit(IOContext *ioc)
{
  KernelDevice::aio_submit(ioc);
}

int WriteCacheDevice::write(
  uint64_t off,
  bufferlist &bl,
  bool buffered)
{
  int res;
  if (write_cache) {
    store_in_cache(off, bl);
  }
  res = KernelDevice::write(off, bl, buffered);
  return res;
}

int WriteCacheDevice::aio_write(
  uint64_t off,
  bufferlist &bl,
  IOContext *ioc,
  bool buffered)
{
  int res;
  if (write_cache) {
     store_in_cache(off, bl);
  }
  res = KernelDevice::aio_write(off, bl, ioc, buffered);
  return res;
}
