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

void WriteCacheDevice::header_t::setup()
{
  fixed_begin = 0x01A2B3C4D5E6F708LL;
  id_reversed = -id;
  size_reversed = -size;
  dest_reversed = -dest;
  fixed_end =   0x4C5D6E7F8091A2B3LL;
}

bool WriteCacheDevice::header_t::check()
{
  if (fixed_begin != 0x01A2B3C4D5E6F708LL)
    return false;
  if (id + id_reversed != 0)
    return false;
  if (size + size_reversed != 0)
    return false;
  if (dest + dest_reversed != 0)
    return false;
  if (fixed_end != 0x4C5D6E7F8091A2B3LL)
    return false;
  return true;
}

WriteCacheDevice::WriteCacheDevice(CephContext* cct, aio_callback_t cb, void *cbpriv, aio_callback_t d_cb, void *d_cbpriv)
  : KernelDevice(cct, cb, cbpriv, d_cb, d_cbpriv),
    last_used_id(0),
    current(nullptr),
    empty(nullptr),
    flushing(nullptr)
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

uint64_t WriteCacheDevice::next_id(uint64_t v)
{
  v++;
  if(v==0)
    v++;
  return v;
}

/*
 * true -  stored to cache
 * false - failed to store
 */
bool WriteCacheDevice::store_in_cache(uint64_t disk_off, bufferlist& bl)
{
  std::lock_guard<std::mutex> l(lock);
  size_t length_align_up = align_up((size_t)bl.length(), block_size);
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
    } else {
      assert(flushing == nullptr);
      assert(empty != nullptr);
      flushing = current;
      current = empty;
      dout(10) << __func__ << " flush_main switched row, current=" << current <<
              " current->disk_offset=" << current->disk_offset << dendl;
      flushing->pos = 0;
      empty = flushing;
      flushing = nullptr;
    }
    //flushing = nullptr;
    //KernelDevice::flush();
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
  header_t* h = reinterpret_cast<header_t*>(header_bl.c_str());
  last_used_id ++;
  h->id = last_used_id;
  h->dest = disk_off;
  h->size = bl.length();
  h->setup();

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
  this->path = path;
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
  bool b;
  b = replay(row_size);
  if (!b) {
    derr << __func__ << " unable to replay cache to main storage '" << path << "'" << dendl;
    assert(b && "failed to replay cache");
  }
  //current = new row_t{0, row_size, 0, false};
  //empty = new row_t{row_size, row_size, 0, false};
  return r;
}

bool WriteCacheDevice::replay(size_t row_size)
{
  row_t a_row{0, row_size, 0, false};
  row_t b_row{row_size, row_size, 0, false};
  dout(5) << __func__ << " entering" << dendl;
  uint64_t a_id;
  uint64_t b_id;
  bool a_ok;
  bool b_ok;
  bool r;
  uint64_t a_id_last;
  uint64_t b_id_last;
  a_ok = peek(a_row, a_id);
  b_ok = peek(b_row, b_id);
  dout(10) << __func__  << "row A " << (a_ok?"OK":"UNAVAIL") << " id=" << a_id << dendl;
  dout(10) << __func__  << "row B " << (b_ok?"OK":"UNAVAIL") << " id=" << b_id << dendl;
  if (a_ok && b_ok) {
    if (a_id < b_id) {
      r = replay_row(a_row, a_id_last);
      if (!r) {
        return false;
      }
      if (next_id(a_id_last) != b_id) {
        return false;
      }
      r = replay_row(b_row, b_id_last);
      if (r) {
        last_used_id = b_id_last;
        current = new row_t{0, row_size, 0, false};
        empty = new row_t{row_size, row_size, 0, false};
        return true;
      }
      return false;
    }
    if (a_id > b_id) {
      r = replay_row(b_row, b_id_last);
      if (!r) {
        return false;
      }
      if (next_id(b_id_last) != a_id) {
        return false;
      }
      r = replay_row(a_row, a_id_last);
      if (r) {
        last_used_id = a_id_last;
        current = new row_t{row_size, row_size, 0, false};
        empty = new row_t{0, row_size, 0, false};
        return true;
      }
      return false;
    }
    //a_id == b_id
    return false;
  }
  if (a_ok) {
    r = replay_row(a_row, a_id_last);
    if (!r) {
      return false;
    }
    last_used_id = a_id_last;
    current = new row_t{row_size, row_size, 0, false};
    empty = new row_t{0, row_size, 0, false};
    return true;
  }
  if (b_ok) {
    r = replay_row(b_row, b_id_last);
    if (!r) {
      return false;
    }
    last_used_id = b_id_last;
    current = new row_t{0, row_size, 0, false};
    empty = new row_t{row_size, row_size, 0, false};
    return true;
  }

  current = new row_t{0, row_size, 0, false};
  empty = new row_t{row_size, row_size, 0, false};
  dout(5) << __func__ << " done" << dendl;
  return true;
}

bool WriteCacheDevice::peek(row_t& row, uint64_t& id)
{
  bool r;
  header_t h;
  r = read_header(row, h);

  if (r) {
    id = h.id;
  }
  return r;
}

bool WriteCacheDevice::replay_row(row_t& row, uint64_t& last_id)
{
  bool result = false;
  bool r;
  header_t h;
  last_id = 0;
  do {
    r = read_header(row, h);
    if (!r) {
      dout(20) << __func__ << " subsequent entry read failed" << dendl;
      //this is possible and ok
      result = true;
      break;
    }
    row.pos += block_size;
    dout(30) << __func__ << " properly read entry id=" << h.id << dendl;
    if (row.pos + h.size > row.size) {
      dout(20) << __func__ << " entry id=" << h.id << " ok, but exceeds row size, failure" << dendl;
      result = false;
      break;
    }
    if (last_id != 0) {
      if (next_id(last_id) != h.id) {
        if ((int64_t)(h.id - next_id(last_id)) > 0) {
          dout(20) << __func__ << " entry id=" << h.id << " jumps to future, failure" << dendl;
          result = false;
        }
        dout(20) << __func__ << " entry id=" << h.id << " ok, but from past" << dendl;
        result = true;
        break;
      }
    }
    last_id = h.id;

    bufferptr p = buffer::create_page_aligned(h.size);
    int rr;
    rr = write_cache->read_random(row.disk_offset + row.pos, h.size, p.c_str(), false);
    if (rr != 0) {
      dout(20) << __func__ << " entry id=" << h.id << " ok, but cannot read content size=" <<
          h.size << " error: " << cpp_strerror(rr) << dendl;
      break;
    }
    bufferlist bl;
    bl.append(p);
    KernelDevice::write(h.dest, bl, false);
    row.pos += h.size;
  } while (row.pos + block_size <= row.size);
  return result;
}

bool WriteCacheDevice::read_header(const row_t& row, header_t& h)
{
  bufferlist header_bl;
  buffer::raw* header_data = buffer::create(block_size);
  header_bl.append(header_data);
  int r;
  r = write_cache->read_random(row.disk_offset + row.pos, block_size, header_bl.c_str(), false);
  if (r != 0) {
    dout(10) << __func__  << " failed to read" << dendl;
    return false;
  }
  h = *reinterpret_cast<header_t*>(header_bl.c_str());
  if (!h.check()) {
    dout(10) << __func__  << " check failed" << dendl;
    return false;
  }
  return true;
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
    dout(10) << __func__ << " last_used_id=" << last_used_id << dendl;
    dout(10) << __func__ << " switched row, current=" << current <<
        " current->disk_offset=" << current->disk_offset << dendl;
  }
  if (flush_main_storage) {
    res = KernelDevice::flush();
    dout(10) << __func__ << " flushed main, res=" << res << dendl;
  }
  return res;
}


int WriteCacheDevice::flush()
{
  std::lock_guard<std::mutex> l(lock);
  int res, r;
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
