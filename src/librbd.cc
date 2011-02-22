// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#define __STDC_FORMAT_MACROS
#include "config.h"

#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include "common/Cond.h"
#include "include/rbd/librbd.hpp"
#include "include/byteorder.h"

#include "include/intarith.h"

#include <errno.h>
#include <inttypes.h>
#include <iostream>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>

#include <sys/ioctl.h>

#include "include/rbd_types.h"

#include <linux/fs.h>

#include "include/fiemap.h"

#define DOUT_SUBSYS rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd: "

namespace librbd {

  using ceph::bufferlist;
  using librados::snap_t;

  class WatchCtx;

  struct SnapInfo {
    librados::snap_t id;
    uint64_t size;
    SnapInfo(librados::snap_t _id, uint64_t _size) : id(_id), size(_size) {};
  };

  struct ImageCtx {
    struct rbd_obj_header_ondisk header;
    ::SnapContext snapc;
    vector<librados::snap_t> snaps;
    std::map<std::string, struct SnapInfo> snaps_by_name;
    uint64_t snapid;
    std::string name;
    struct PoolCtx *pctx;
    pool_t data;
    WatchCtx *wctx;
    bool needs_refresh;
    Mutex lock;

    ImageCtx(std::string imgname, PoolCtx *pp) : snapid(0),
						 name(imgname),
						 pctx(pp),
						 needs_refresh(true),
						 lock("librbd::ImageCtx::lock") {}
    int set_snap(std::string snap_name)
    {
      std::map<std::string, struct SnapInfo>::iterator it = snaps_by_name.find(snap_name);
      if (it != snaps_by_name.end()) {
	snapid = it->second.id;
	return 0;
      }
      snapid = 0;
      return -ENOENT;
    }

    void add_snap(std::string snap_name, librados::snap_t id, uint64_t size)
    {
      snapc.snaps.push_back(id);
      snaps.push_back(id);
      struct SnapInfo info(id, size);
      snaps_by_name.insert(std::pair<std::string, struct SnapInfo>(snap_name, info));
    }
  };

  class WatchCtx : public librados::Rados::WatchCtx {
    ImageCtx *ictx;
    bool valid;
    Mutex lock;
  public:
    uint64_t cookie;
    WatchCtx(ImageCtx *ctx) : ictx(ctx),
			      valid(true),
			      lock("librbd::RBDClient::WatchCtx") {}
    virtual ~WatchCtx() {}
    void invalidate();
    virtual void notify(uint8_t opcode, uint64_t ver);
  };

class RBDClient
{
  librados::Rados rados;

public:
  struct AioCompletion;

  struct AioBlockCompletion {
    struct AioCompletion *completion;
    off_t ofs;
    size_t len;
    char *buf;
    map<off_t, size_t> m;
    bufferlist data_bl;

    AioBlockCompletion(AioCompletion *aio_completion, off_t _ofs, size_t _len, char *_buf) :
                                            completion(aio_completion), ofs(_ofs), len(_len), buf(_buf) {}
    void complete(int r);
  };



  struct AioCompletion {
    Mutex lock;
    Cond cond;
    bool done;
    int rval;
    callback_t complete_cb;
    void *complete_arg;
    rbd_completion_t rbd_comp;
    int pending_count;
    int ref;
    bool released;

    AioCompletion() : lock("RBDClient::AioCompletion::lock", true), done(false), rval(0), complete_cb(NULL), complete_arg(NULL),
		      rbd_comp(NULL), pending_count(0), ref(1), released(false) {
      dout(10) << "AioCompletion::AioCompletion() this=" << (void *)this << dendl;
    }
    ~AioCompletion() {
      dout(10) << "AioCompletion::~AioCompletion()" << dendl;
    }
    int wait_for_complete() {
      lock.Lock();
      while (!done)
	cond.Wait(lock);
      lock.Unlock();
      return 0;
    }

    void add_block_completion(AioBlockCompletion *aio_completion) {
      dout(10) << "add_block_completion this=" << (void *)this << dendl;
      lock.Lock();
      pending_count++;
      lock.Unlock();
      get();
    }

    void set_complete_cb(void *cb_arg, callback_t cb) {
      complete_cb = cb;
      complete_arg = cb_arg;
    }

    void complete_block(AioBlockCompletion *block_completion, int r);

    int get_return_value() {
      lock.Lock();
      int r = rval;
      lock.Unlock();
      return r;
    }

    void get() {
      lock.Lock();
      assert(ref > 0);
      ref++;
      lock.Unlock();
    }
    void release() {
      lock.Lock();
      assert(!released);
      released = true;
      put_unlock();
    }
    void put() {
      lock.Lock();
      put_unlock();
    }
    void put_unlock() {
      assert(ref > 0);
      int n = --ref;
      lock.Unlock();
      if (!n)
	delete this;
    }
  };
  static void rados_cb(rados_completion_t cb, void *arg);
  static void rados_aio_sparse_read_cb(rados_completion_t cb, void *arg);

  int initialize(int argc, const char *argv[]);
  void shutdown();

  int set_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name);
  int list(PoolCtx *pp, std::vector<string>& names);
  int create_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name);
  int create(pool_t& pool, string& md_oid, const char *imgname, uint64_t size, int *order);
  int rename(PoolCtx *pp, const char *srcname, const char *dstname);
  int info(PoolCtx *pp, ImageCtx *ictx, image_info_t& info);
  int remove(PoolCtx *pp, const char *imgname);
  int resize(PoolCtx *pp, ImageCtx *ictx, uint64_t size);
  int list_snaps(PoolCtx *pp, ImageCtx *ictx, std::vector<snap_info_t>& snaps);
  int add_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name);
  int rm_snap(PoolCtx *pp, string& md_oid, const char *snap_name);
  int check_ictx(ImageCtx *ictx);
  int refresh_ictx(ImageCtx *ictx, const char *snap_name);
  int rollback_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name);
  int remove_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name);
  int copy(PoolCtx *pp, const char *srcname, PoolCtx *pp_dest, const char *destname);

  int open_pools(const char *poolname, PoolCtx *pp);
  void close_pools(PoolCtx *pp);
  int open_image(PoolCtx *pctx, ImageCtx *ictx, const char *name, const char *snap_name);
  void close_image(ImageCtx *ictx);

  void trim_image(PoolCtx *pp, rbd_obj_header_ondisk *header, uint64_t newsize);
  int read_rbd_info(PoolCtx *pp, string& info_oid, struct rbd_info *info);

  int touch_rbd_info(librados::pool_t pool, string& info_oid);
  int rbd_assign_bid(librados::pool_t pool, string& info_oid, uint64_t *id);
  int read_header_bl(librados::pool_t pool, string& md_oid, bufferlist& header, uint64_t *ver);
  int notify_change(librados::pool_t pool, string& oid, uint64_t *pver, ImageCtx *ictx);
  int read_header(librados::pool_t pool, string& md_oid, struct rbd_obj_header_ondisk *header, uint64_t *ver);
  int write_header(PoolCtx *pp, string& md_oid, bufferlist& header);
  int tmap_set(PoolCtx *pp, string& imgname);
  int tmap_rm(PoolCtx *pp, string& imgname);
  int rollback_image(PoolCtx *pp, ImageCtx *ictx, uint64_t snapid);
  static void image_info(rbd_obj_header_ondisk& header, librbd::image_info_t& info);
  static string get_block_oid(rbd_obj_header_ondisk *header, uint64_t num);
  static uint64_t get_max_block(rbd_obj_header_ondisk *header);
  static uint64_t get_block_size(rbd_obj_header_ondisk *header);
  static uint64_t get_block_num(rbd_obj_header_ondisk *header, uint64_t ofs);
  static uint64_t get_block_ofs(rbd_obj_header_ondisk *header, uint64_t ofs);
  static int init_rbd_info(struct rbd_info *info);
  static void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
			      size_t size, int *order, uint64_t bid);

  int read_iterate(PoolCtx *ctx, ImageCtx *ictx, off_t off, size_t len,
                   int (*cb)(off_t, size_t, const char *, void *),
                   void *arg);
  int read(PoolCtx *ctx, ImageCtx *ictx, off_t off, size_t len, char *buf);
  int write(PoolCtx *ctx, ImageCtx *ictx, off_t off, size_t len, const char *buf);
  int aio_write(PoolCtx *pool, ImageCtx *ictx, off_t off, size_t len, const char *buf,
                AioCompletion *c);
  int aio_read(PoolCtx *ctx, ImageCtx *ictx, off_t off, size_t len,
               char *buf, AioCompletion *c);

  AioCompletion *aio_create_completion() {
    AioCompletion *c= new AioCompletion;
    return c;
  }
  AioCompletion *aio_create_completion(void *cb_arg, callback_t cb_complete) {
    AioCompletion *c = new AioCompletion;
    c->set_complete_cb(cb_arg, cb_complete);
    return c;
  }
  librados::Rados& get_rados() { return rados; }
};


void librbd::WatchCtx::invalidate()
{
  Mutex::Locker l(lock);
  valid = false;
}

void librbd::WatchCtx::notify(uint8_t opcode, uint64_t ver)
{
  Mutex::Locker l(lock);
  cout <<  " got notification opcode=" << (int)opcode << " ver=" << ver << " cookie=" << cookie << std::endl;
  if (valid) {
    Mutex::Locker lictx(ictx->lock);
    ictx->needs_refresh = true;
  }
}

void librbd::RBDClient::init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
					size_t size, int *order, uint64_t bid)
{
  uint32_t hi = bid >> 32;
  uint32_t lo = bid & 0xFFFFFFFF;
  memset(&ondisk, 0, sizeof(ondisk));

  memcpy(&ondisk.text, RBD_HEADER_TEXT, sizeof(RBD_HEADER_TEXT));
  memcpy(&ondisk.signature, RBD_HEADER_SIGNATURE, sizeof(RBD_HEADER_SIGNATURE));
  memcpy(&ondisk.version, RBD_HEADER_VERSION, sizeof(RBD_HEADER_VERSION));

  snprintf(ondisk.block_name, sizeof(ondisk.block_name), "rb.%x.%x", hi, lo);

  if (!*order)
    *order = RBD_DEFAULT_OBJ_ORDER;

  ondisk.image_size = size;
  ondisk.options.order = *order;
  ondisk.options.crypt_type = RBD_CRYPT_NONE;
  ondisk.options.comp_type = RBD_COMP_NONE;
  ondisk.snap_seq = 0;
  ondisk.snap_count = 0;
  ondisk.reserved = 0;
  ondisk.snap_names_len = 0;
}

void librbd::RBDClient::image_info(rbd_obj_header_ondisk& header, librbd::image_info_t& info)
{
  int obj_order = header.options.order;
  info.size = header.image_size;
  info.obj_size = 1 << obj_order;
  info.num_objs = header.image_size >> obj_order;
  info.order = obj_order;
}

string librbd::RBDClient::get_block_oid(rbd_obj_header_ondisk *header, uint64_t num)
{
  char o[RBD_MAX_SEG_NAME_SIZE];
  snprintf(o, RBD_MAX_SEG_NAME_SIZE,
       "%s.%012" PRIx64, header->block_name, num);
  return o;
}

uint64_t librbd::RBDClient::get_max_block(rbd_obj_header_ondisk *header)
{
  uint64_t size = header->image_size;
  int obj_order = header->options.order;
  uint64_t block_size = 1 << obj_order;
  uint64_t numseg = (size + block_size - 1) >> obj_order;

  return numseg;
}

uint64_t librbd::RBDClient::get_block_ofs(rbd_obj_header_ondisk *header, uint64_t ofs)
{
  int obj_order = header->options.order;
  uint64_t block_size = 1 << obj_order;
  return ofs & (block_size - 1);
}

uint64_t librbd::RBDClient::get_block_size(rbd_obj_header_ondisk *header)
{
  return 1 << header->options.order;
}

uint64_t librbd::RBDClient::get_block_num(rbd_obj_header_ondisk *header, uint64_t ofs)
{
  int obj_order = header->options.order;
  uint64_t num = ofs >> obj_order;

  return num;
}

int librbd::RBDClient::init_rbd_info(struct rbd_info *info)
{
  memset(info, 0, sizeof(*info));
  return 0;
}

void librbd::RBDClient::trim_image(PoolCtx *pp, rbd_obj_header_ondisk *header, uint64_t newsize)
{
  uint64_t numseg = get_max_block(header);
  uint64_t start = get_block_num(header, newsize);

  dout(2) << "trimming image data from " << numseg << " to " << start << " objects..." << dendl;
  for (uint64_t i=start; i<numseg; i++) {
    string oid = get_block_oid(header, i);
    rados.remove(pp->data, oid);
    if ((i & 127) == 0) {
      dout(2) << "\r\t" << i << "/" << numseg << dendl;
    }
  }
}

int librbd::RBDClient::read_rbd_info(PoolCtx *pp, string& info_oid, struct rbd_info *info)
{
  int r;
  bufferlist bl;

  r = rados.read(pp->md, info_oid, 0, bl, sizeof(*info));
  if (r < 0)
    return r;
  if (r == 0) {
    return init_rbd_info(info);
  }

  if (r < (int)sizeof(*info))
    return -EIO;

  memcpy(info, bl.c_str(), r);
  return 0;
}

int librbd::RBDClient::touch_rbd_info(librados::pool_t pool, string& info_oid)
{
  bufferlist bl;
  int r = rados.write(pool, info_oid, 0, bl, 0);
  if (r < 0)
    return r;
  return 0;
}

int librbd::RBDClient::rbd_assign_bid(librados::pool_t pool, string& info_oid, uint64_t *id)
{
  bufferlist bl, out;

  *id = 0;

  int r = touch_rbd_info(pool, info_oid);
  if (r < 0)
    return r;

  r = rados.exec(pool, info_oid, "rbd", "assign_bid", bl, out);
  if (r < 0)
    return r;

  bufferlist::iterator iter = out.begin();
  ::decode(*id, iter);

  return 0;
}


int librbd::RBDClient::read_header_bl(librados::pool_t pool, string& md_oid, bufferlist& header, uint64_t *ver)
{
  int r;
#define READ_SIZE 4096
  do {
    bufferlist bl;
    r = rados.read(pool, md_oid, 0, bl, READ_SIZE);
    if (r < 0)
      return r;
    header.claim_append(bl);
   } while (r == READ_SIZE);

  if (ver)
    *ver = rados.get_last_version(pool);

  return 0;
}

int librbd::RBDClient::notify_change(librados::pool_t pool, string& oid, uint64_t *pver, ImageCtx *ictx)
{
  uint64_t ver;

  if (ictx) {
    ictx->lock.Lock();
    ictx->needs_refresh = true;
    ictx->lock.Unlock();
  }

  if (pver)
    ver = *pver;
  else
    ver = rados.get_last_version(pool);
  rados.notify(pool, oid, ver);
  return 0;
}

int librbd::RBDClient::read_header(librados::pool_t pool, string& md_oid, struct rbd_obj_header_ondisk *header, uint64_t *ver)
{
  bufferlist header_bl;
  int r = read_header_bl(pool, md_oid, header_bl, ver);
  if (r < 0)
    return r;
  if (header_bl.length() < (int)sizeof(*header))
    return -EIO;
  memcpy(header, header_bl.c_str(), sizeof(*header));

  return 0;
}

int librbd::RBDClient::write_header(PoolCtx *pp, string& md_oid, bufferlist& header)
{
  bufferlist bl;
  int r = rados.write(pp->md, md_oid, 0, header, header.length());

  notify_change(pp->md, md_oid, NULL, NULL);

  return r;
}

int librbd::RBDClient::tmap_set(PoolCtx *pp, string& imgname)
{
  bufferlist cmdbl, emptybl;
  __u8 c = CEPH_OSD_TMAP_SET;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  ::encode(emptybl, cmdbl);
  return rados.tmap_update(pp->md, RBD_DIRECTORY, cmdbl);
}

int librbd::RBDClient::tmap_rm(PoolCtx *pp, string& imgname)
{
  bufferlist cmdbl;
  __u8 c = CEPH_OSD_TMAP_RM;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  return rados.tmap_update(pp->md, RBD_DIRECTORY, cmdbl);
}

int librbd::RBDClient::rollback_image(PoolCtx *pp, ImageCtx *ictx, uint64_t snapid)
{
  uint64_t numseg = get_max_block(&(ictx->header));

  for (uint64_t i = 0; i < numseg; i++) {
    int r;
    string oid = get_block_oid(&(ictx->header), i);
    librados::SnapContext sn;
    sn.seq = ictx->snapc.seq;
    sn.snaps.clear();
    vector<snapid_t>::iterator iter = ictx->snapc.snaps.begin();
    for (; iter != ictx->snapc.snaps.end(); ++iter) {
      sn.snaps.push_back(*iter);
    }
    r = rados.selfmanaged_snap_rollback_object(pp->data, oid, sn, snapid);
    if (r < 0 && r != -ENOENT)
      return r;
  }
  return 0;
}

int librbd::RBDClient::list(PoolCtx *pp, std::vector<std::string>& names)
{
  bufferlist bl;
  int r = rados.read(pp->md, RBD_DIRECTORY, 0, bl, 0);
  if (r < 0)
    return r;

  bufferlist::iterator p = bl.begin();
  bufferlist header;
  map<string,bufferlist> m;
  ::decode(header, p);
  ::decode(m, p);
  for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); q++)
    names.push_back(q->first);
  return 0;
}

int librbd::RBDClient::create_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name)
{
  int r = check_ictx(ictx);
  if (r < 0)
    return r;

  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  r = rados.set_snap_context(pp->data, ictx->snapc.seq, ictx->snaps);
  if (r < 0)
    return r;

  rados.set_snap(pp->data, 0);
  r = add_snap(pp, ictx, snap_name);
  notify_change(pp->md, md_oid, NULL, ictx);
  return r;
}

int librbd::RBDClient::remove_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name)
{
  int r = check_ictx(ictx);
  if (r < 0)
    return r;

  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  r = ictx->set_snap(snap_name);
  if (r < 0)
    return r;

  r = rados.set_snap_context(pp->data, ictx->snapc.seq, ictx->snaps);
  if (r < 0)
    return r;

  rados.set_snap(pp->data, ictx->snapid);

  r = rm_snap(pp, md_oid, snap_name);
  r = rados.selfmanaged_snap_remove(pp->data, ictx->snapid);
  notify_change(pp->md, md_oid, NULL, ictx);

  return r;
}

int librbd::RBDClient::create(pool_t& pool, string& md_oid, const char *imgname,
			      uint64_t size, int *order)
{

  // make sure it doesn't already exist
  int r = rados.stat(pool, md_oid, NULL, NULL);
  if (r == 0) {
    derr << "rbd image header " << md_oid << " already exists" << dendl;
    return -EEXIST;
  }

  uint64_t bid;
  string dir_info = RBD_INFO;
  r = rbd_assign_bid(pool, dir_info, &bid);
  if (r < 0) {
    derr << "failed to assign a block name for image" << dendl;
    return r;
  }

  struct rbd_obj_header_ondisk header;
  init_rbd_header(header, size, order, bid);

  bufferlist bl;
  bl.append((const char *)&header, sizeof(header));

  dout(2) << "adding rbd image to directory..." << dendl;
  bufferlist cmdbl, emptybl;
  __u8 c = CEPH_OSD_TMAP_SET;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  ::encode(emptybl, cmdbl);
  r = rados.tmap_update(pool, RBD_DIRECTORY, cmdbl);
  if (r < 0) {
    derr << "error adding img to directory: " << strerror(-r)<< dendl;
    return r;
  }

  dout(2) << "creating rbd image..." << dendl;
  r = rados.write(pool, md_oid, 0, bl, bl.length());
  if (r < 0) {
    derr << "error writing header: " << strerror(-r) << dendl;
    return r;
  }

  dout(2) << "done." << dendl;
  return 0;
}

int librbd::RBDClient::rename(PoolCtx *pp, const char *srcname, const char *dstname)
{
  string md_oid = srcname;
  md_oid += RBD_SUFFIX;
  string dst_md_oid = dstname;
  dst_md_oid += RBD_SUFFIX;
  string dstname_str = dstname;
  string imgname_str = srcname;
  uint64_t ver;
  bufferlist header;
  int r = read_header_bl(pp->md, md_oid, header, &ver);
  if (r < 0) {
    derr << "error reading header: " << md_oid << ": " << strerror(-r) << dendl;
    return r;
  }
  r = rados.stat(pp->md, dst_md_oid, NULL, NULL);
  if (r == 0) {
    derr << "rbd image header " << dst_md_oid << " already exists" << dendl;
    return -EEXIST;
  }
  r = write_header(pp, dst_md_oid, header);
  if (r < 0) {
    derr << "error writing header: " << dst_md_oid << ": " << strerror(-r) << dendl;
    return r;
  }
  r = tmap_set(pp, dstname_str);
  if (r < 0) {
    rados.remove(pp->md, dst_md_oid);
    derr << "can't add " << dst_md_oid << " to directory" << dendl;
    return r;
  }
  r = tmap_rm(pp, imgname_str);
  if (r < 0)
    derr << "warning: couldn't remove old entry from directory (" << imgname_str << ")" << dendl;

  r = rados.remove(pp->md, md_oid);
  if (r < 0)
    derr << "warning: couldn't remove old metadata" << dendl;
  notify_change(pp->md, md_oid, NULL, NULL);

  return 0;
}


int librbd::RBDClient::info(PoolCtx *pp, ImageCtx *ictx, librbd::image_info_t& info)
{
  int r = check_ictx(ictx);
  if (r < 0)
    return r;
  image_info(ictx->header, info);
  return 0;
}

int librbd::RBDClient::remove(PoolCtx *pp, const char *imgname)
{
  string md_oid = imgname;
  md_oid += RBD_SUFFIX;

  struct rbd_obj_header_ondisk header;
  int r = read_header(pp->md, md_oid, &header, NULL);
  if (r >= 0) {
    trim_image(pp, &header, 0);
    dout(2) << "\rremoving header..." << dendl;
    rados.remove(pp->md, md_oid);
  }

  dout(2) << "removing rbd image to directory..." << dendl;
  bufferlist cmdbl;
  __u8 c = CEPH_OSD_TMAP_RM;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  r = rados.tmap_update(pp->md, RBD_DIRECTORY, cmdbl);
  if (r < 0) {
    derr << "error removing img from directory: " << strerror(-r)<< dendl;
    return r;
  }

  dout(2) << "done." << dendl;
  return 0;
}

int librbd::RBDClient::resize(PoolCtx *pp, ImageCtx *ictx, uint64_t size)
{
  int r = check_ictx(ictx);
  if (r < 0)
    return r;

  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  // trim
  if (size == ictx->header.image_size) {
    dout(2) << "no change in size (" << size << " -> " << ictx->header.image_size << ")" << dendl;
    return 0;
  }

  if (size > ictx->header.image_size) {
    dout(2) << "expanding image " << size << " -> " << ictx->header.image_size << " objects" << dendl;
    ictx->header.image_size = size;
  } else {
    dout(2) << "shrinking image " << size << " -> " << ictx->header.image_size << " objects" << dendl;
    trim_image(pp, &(ictx->header), size);
    ictx->header.image_size = size;
  }

  // rewrite header
  bufferlist bl;
  bl.append((const char *)&(ictx->header), sizeof(ictx->header));
  r = rados.write(pp->md, md_oid, 0, bl, bl.length());
  if (r == -ERANGE)
    derr << "operation might have conflicted with another client!" << dendl;
  if (r < 0) {
    derr << "error writing header: " << strerror(-r) << dendl;
    return r;
  } else {
    notify_change(pp->md, md_oid, NULL, ictx);
  }

  dout(2) << "done." << dendl;
  return 0;
}

int librbd::RBDClient::list_snaps(PoolCtx *pp, ImageCtx *ictx, std::vector<librbd::snap_info_t>& snaps)
{
  int r = check_ictx(ictx);
  if (r < 0)
    return r;
  bufferlist bl, bl2;
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  for (std::map<std::string, struct SnapInfo>::iterator it = ictx->snaps_by_name.begin();
       it != ictx->snaps_by_name.end(); ++it) {
    librbd::snap_info_t info;
    info.name = it->first;
    info.id = it->second.id;
    info.size = it->second.size;
    snaps.push_back(info);
  }

  return 0;
}

int librbd::RBDClient::add_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name)
{
  bufferlist bl, bl2;
  uint64_t snap_id;
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  int r = rados.selfmanaged_snap_create(pp->md, &snap_id);
  if (r < 0) {
    derr << "failed to create snap id: " << strerror(-r) << dendl;
    return r;
  }

  ::encode(snap_name, bl);
  ::encode(snap_id, bl);

  r = rados.exec(pp->md, md_oid, "rbd", "snap_add", bl, bl2);
  if (r < 0) {
    derr << "rbd.snap_add execution failed failed: " << strerror(-r) << dendl;
    return r;
  }
  notify_change(pp->md, md_oid, NULL, ictx);

  return 0;
}

int librbd::RBDClient::rm_snap(PoolCtx *pp, string& md_oid, const char *snap_name)
{
  bufferlist bl, bl2;

  ::encode(snap_name, bl);

  int r = rados.exec(pp->md, md_oid, "rbd", "snap_remove", bl, bl2);
  if (r < 0) {
    derr << "rbd.snap_remove execution failed failed: " << strerror(-r) << dendl;
    return r;
  }

  return 0;
}

int librbd::RBDClient::check_ictx(ImageCtx *ictx)
{
  ictx->lock.Lock();
  bool needs_refresh = ictx->needs_refresh;
  ictx->lock.Unlock();

  int r = 0;
  if (needs_refresh)
    r = refresh_ictx(ictx, NULL);
  return r;
}

int librbd::RBDClient::refresh_ictx(ImageCtx *ictx, const char *snap_name)
{
  bufferlist bl, bl2;
  PoolCtx *pp = ictx->pctx;
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  int r = read_header(pp->md, md_oid, &(ictx->header), NULL);
  if (r < 0)
    return r;
  r = rados.exec(pp->md, md_oid, "rbd", "snap_list", bl, bl2);
  if (r < 0)
    return r;

  ictx->snaps.clear();
  ictx->snapc.snaps.clear();
  ictx->snaps_by_name.clear();

  uint32_t num_snaps;
  bufferlist::iterator iter = bl2.begin();
  ::decode(ictx->snapc.seq, iter);
  ::decode(num_snaps, iter);
  ictx->snapid = 0;
  for (uint32_t i=0; i < num_snaps; i++) {
    uint64_t id, image_size;
    string s;
    ::decode(id, iter);
    ::decode(image_size, iter);
    ::decode(s, iter);
    ictx->add_snap(s, id, image_size);
  }

  if (!ictx->snapc.is_valid()) {
    derr << "image snap context is invalid!" << dendl;
    return -EIO;
  }

  if (snap_name) {
    ictx->set_snap(snap_name);
    if (!ictx->snapid) {
      return -ENOENT;
    }
  }

  ictx->lock.Lock();
  ictx->needs_refresh = false;
  ictx->lock.Unlock();

  return 0;
}

int librbd::RBDClient::rollback_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name)
{
  int r = check_ictx(ictx);
  if (r < 0)
    return r;
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  r = ictx->set_snap(snap_name);
  if (r < 0)
    return r;

  r = rados.set_snap_context(pp->data, ictx->snapc.seq, ictx->snaps);
  if (r < 0)
    return r;

  rados.set_snap(pp->data, ictx->snapid);
  r = rollback_image(pp, ictx, ictx->snapid);
  if (r < 0)
    return r;

  return 0;
}

int librbd::RBDClient::copy(PoolCtx *pp, const char *srcname, PoolCtx *pp_dest, const char *destname)
{
  struct rbd_obj_header_ondisk header, dest_header;
  int64_t ret;
  int r;
  string md_oid, dest_md_oid;

  md_oid = srcname;
  md_oid += RBD_SUFFIX;

  dest_md_oid = destname;
  dest_md_oid += RBD_SUFFIX;

  ret = read_header(pp->md, md_oid, &header, NULL);
  if (ret < 0)
    return ret;

  uint64_t numseg = get_max_block(&header);
  uint64_t block_size = get_block_size(&header);
  int order = header.options.order;

  r = create(pp_dest->md, dest_md_oid, destname, header.image_size, &order);
  if (r < 0) {
    derr << "header creation failed" << dendl;
    return r;
  }

  ret = read_header(pp_dest->md, dest_md_oid, &dest_header, NULL);
  if (ret < 0) {
    derr << "failed to read newly created header" << dendl;
    return ret;
  }

  for (uint64_t i = 0; i < numseg; i++) {
    bufferlist bl;
    string oid = get_block_oid(&header, i);
    string dest_oid = get_block_oid(&dest_header, i);
    map<off_t, size_t> m;
    map<off_t, size_t>::iterator iter;
    r = rados.sparse_read(pp->data, oid, 0, block_size, m, bl);
    if (r < 0 && r == -ENOENT)
      r = 0;
    if (r < 0)
      return r;


    for (iter = m.begin(); iter != m.end(); ++iter) {
      off_t extent_ofs = iter->first;
      size_t extent_len = iter->second;
      bufferlist wrbl;
      if (extent_ofs + extent_len > bl.length()) {
	derr << "data error!" << dendl;
	return -EIO;
      }
      bl.copy(extent_ofs, extent_len, wrbl);
      r = rados.write(pp_dest->data, dest_oid, extent_ofs, wrbl, extent_len);
      if (r < 0)
	goto done;
    }
  }
  r = 0;

done:
  return r;
}

void librbd::RBDClient::close_pools(PoolCtx *pp)
{
  if (pp->data)
    rados.close_pool(pp->data);
  if (pp->md)
    rados.close_pool(pp->md);
}

int librbd::RBDClient::initialize(int argc, const char *argv[])
{
  vector<const char*> args;

  if (argc && argv) {
    argv_to_vec(argc, argv, args);
    env_to_vec(args);
  }

  common_set_defaults(false);
  common_init(args, "rbd", true);

  if (rados.initialize(argc, argv) < 0) {
    return -1;
  }
  return 0;
}

void librbd::RBDClient::shutdown()
{
  rados.shutdown();
}

int librbd::RBDClient::set_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name)
{
  int r = check_ictx(ictx);
  if (r < 0)
    return r;
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  r = ictx->set_snap(snap_name);
  if (r < 0)
    return r;

  r = rados.set_snap_context(pp->data, ictx->snapc.seq, ictx->snaps);
  if (r < 0)
    return r;

  rados.set_snap(pp->data, ictx->snapid);

  return 0;
}

int librbd::RBDClient::open_pools(const char *pool_name, PoolCtx *ctx)
{
  librados::pool_t pool, md_pool;

  int r = rados.open_pool(pool_name, &md_pool);
  if (r < 0) {
    derr << "error opening pool " << pool_name << " (err=" << r << ")" << dendl;
    return -1;
  }
  ctx->md = md_pool;

  r = rados.open_pool(pool_name, &pool);
  if (r < 0) {
    derr << "error opening pool " << pool_name << " (err=" << r << ")" << dendl;
    close_pools(ctx);
    return -1;
  }
  ctx->data = pool;
  return 0;
}

int librbd::RBDClient::open_image(PoolCtx *pctx, ImageCtx *ictx, const char *name, const char *snap_name)
{
  int r = refresh_ictx(ictx, snap_name);
  if (r < 0)
    return r;

  WatchCtx *wctx = new WatchCtx(ictx);
  if (!wctx)
    return -ENOMEM;
  ictx->wctx = wctx;
  string md_oid = name;
  md_oid += RBD_SUFFIX;

  r = rados.watch(pctx->md, md_oid, 0, &(wctx->cookie), wctx);
  return r;
}

void librbd::RBDClient::close_image(ImageCtx *ictx)
{
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  ictx->wctx->invalidate();
  rados.unwatch(ictx->pctx->md, md_oid, ictx->wctx->cookie);
  delete ictx->wctx;
  delete ictx;
  ictx = NULL;
}

int librbd::RBDClient::read_iterate(PoolCtx *ctx, ImageCtx *ictx, off_t off, size_t len,
                                    int (*cb)(off_t, size_t, const char *, void *),
                                    void *arg)
{
  int r = check_ictx(ictx);
  if (r < 0)
    return r;
  int64_t ret;
  int total_read = 0;
  uint64_t start_block = get_block_num(&ictx->header, off);
  uint64_t end_block = get_block_num(&ictx->header, off + len);
  uint64_t block_size = get_block_size(&ictx->header);
  uint64_t left = len;

  for (uint64_t i = start_block; i <= end_block; i++) {
    bufferlist bl;
    string oid = get_block_oid(&ictx->header, i);
    uint64_t block_ofs = get_block_ofs(&ictx->header, off + total_read);
    uint64_t read_len = min(block_size - block_ofs, left);

    map<off_t, size_t> m;
    map<off_t, size_t>::iterator iter;
    off_t bl_ofs = 0, buf_bl_pos = 0;
    r = rados.sparse_read(ctx->data, oid, block_ofs, read_len, m, bl);
    if (r < 0 && r == -ENOENT)
      r = 0;
    if (r < 0) {
      ret = r;
      goto done;
    }
    for (iter = m.begin(); iter != m.end(); ++iter) {
      off_t extent_ofs = iter->first;
      size_t extent_len = iter->second;
      /* a hole? */
      if (extent_ofs - block_ofs) {
        r = cb(total_read + buf_bl_pos, extent_ofs - block_ofs, NULL, arg);
        if (r < 0)
          return r;
      }

      if (bl_ofs + extent_len > bl.length())
        return -EIO;
      buf_bl_pos += extent_ofs - block_ofs;

      /* data */
      r = cb(total_read + buf_bl_pos, extent_len, bl.c_str() + bl_ofs, arg);
      if (r < 0)
        return r;
      bl_ofs += extent_len;
      buf_bl_pos += extent_len;
    }

    /* last hole */
    if (read_len - buf_bl_pos) {
      r = cb(total_read + buf_bl_pos, read_len - buf_bl_pos, NULL, arg);
      if (r < 0)
        return r;
    }

    total_read += read_len;
    left -= read_len;
  }
  ret = total_read;
done:
  return ret;
}

static int simple_read_cb(off_t ofs, size_t len, const char *buf, void *arg)
{
  char *dest_buf = (char *)arg;
  if (buf)
    memcpy(dest_buf + ofs, buf, len);
  else
    memset(dest_buf + ofs, 0, len);

  return 0;
}


int librbd::RBDClient::read(PoolCtx *ctx, ImageCtx *ictx, off_t ofs, size_t len, char *buf)
{
  return read_iterate(ctx, ictx, ofs, len, simple_read_cb, buf);
}

int librbd::RBDClient::write(PoolCtx *ctx, ImageCtx *ictx, off_t off, size_t len, const char *buf)
{
  if (!len)
    return 0;

  int r = check_ictx(ictx);
  if (r < 0)
    return r;

  int total_write = 0;
  uint64_t start_block = get_block_num(&ictx->header, off);
  uint64_t end_block = get_block_num(&ictx->header, off + len - 1);
  uint64_t block_size = get_block_size(&ictx->header);
  uint64_t left = len;

  for (uint64_t i = start_block; i <= end_block; i++) {
    bufferlist bl;
    string oid = get_block_oid(&ictx->header, i);
    uint64_t block_ofs = get_block_ofs(&ictx->header, off + total_write);
    uint64_t write_len = min(block_size - block_ofs, left);
    bl.append(buf + total_write, write_len);
    r = rados.write(ctx->data, oid, block_ofs, bl, write_len);
    if (r < 0)
      return r;
    if ((uint64_t)r != write_len)
      return -EIO;
    total_write += write_len;
    left -= write_len;
  }
  return total_write;
}

void librbd::RBDClient::AioBlockCompletion::complete(int r)
{
  dout(10) << "AioBlockCompletion::complete()" << dendl;
  if ((r >= 0 || r == -ENOENT) && buf) { // this was a sparse_read operation
    map<off_t, size_t>::iterator iter;
    off_t bl_ofs = 0, buf_bl_pos = 0;
    dout(10) << "ofs=" << ofs << " len=" << len << dendl;
    for (iter = m.begin(); iter != m.end(); ++iter) {
      off_t extent_ofs = iter->first;
      size_t extent_len = iter->second;

      dout(10) << "extent_ofs=" << extent_ofs << " extent_len=" << extent_len << dendl;

      /* a hole? */
      if (extent_ofs - ofs) {
	dout(10) << "<1>zeroing " << buf_bl_pos << "~" << extent_ofs << dendl;
        dout(10) << "buf=" << (void *)(buf + buf_bl_pos) << "~" << (void *)(buf + extent_ofs - ofs - 1) << dendl;
        memset(buf + buf_bl_pos, 0, extent_ofs - ofs);
      }

      if (bl_ofs + extent_len > len) {
        r = -EIO;
	break;
      }
      buf_bl_pos += extent_ofs - ofs;

      /* data */
      dout(10) << "<2>copying " << buf_bl_pos << "~" << extent_len << " from ofs=" << bl_ofs << dendl;
      dout(10) << "buf=" << (void *)(buf + buf_bl_pos) << "~" << (void *)(buf + buf_bl_pos + extent_len -1) << dendl;
      memcpy(buf + buf_bl_pos, data_bl.c_str() + bl_ofs, extent_len);
      bl_ofs += extent_len;
      buf_bl_pos += extent_len;
    }

    /* last hole */
    if (len - buf_bl_pos) {
      dout(10) << "<3>zeroing " << buf_bl_pos << "~" << len - buf_bl_pos << dendl;
      dout(10) << "buf=" << (void *)(buf + buf_bl_pos) << "~" << (void *)(buf + len -1) << dendl;
      memset(buf + buf_bl_pos, 0, len - buf_bl_pos);
    }

    r = len;
  }
  completion->complete_block(this, r);
}

void librbd::RBDClient::AioCompletion::complete_block(AioBlockCompletion *block_completion, int r)
{
  dout(10) << "RBDClient::AioCompletion::complete_block this=" << (void *)this << " complete_cb=" << (void *)complete_cb << dendl;
  lock.Lock();
  if (rval >= 0) {
    if (r < 0 && r != -EEXIST)
      rval = r;
    else if (r > 0)
      rval += r;
  }
  assert(pending_count);
  int count = --pending_count;
  if (!count) {
    if (complete_cb)
      complete_cb(rbd_comp, complete_arg);
    done = true;
    cond.Signal();
  }
  put_unlock();
}

void librbd::RBDClient::rados_cb(rados_completion_t c, void *arg)
{
  dout(10) << "librbd::RBDClient::rados_cb" << dendl;
  AioBlockCompletion *block_completion = (AioBlockCompletion *)arg;
  block_completion->complete(rados_aio_get_return_value(c));
}

int librbd::RBDClient::aio_write(PoolCtx *pool, ImageCtx *ictx, off_t off, size_t len, const char *buf,
			         AioCompletion *c)
{
  if (!len)
    return 0;

  int r = check_ictx(ictx);
  if (r < 0)
    return r;

  int total_write = 0;
  uint64_t start_block = get_block_num(&ictx->header, off);
  uint64_t end_block = get_block_num(&ictx->header, off + len - 1);
  uint64_t block_size = get_block_size(&ictx->header);
  uint64_t left = len;

  if (off + len > ictx->header.image_size)
    return -EINVAL;

  for (uint64_t i = start_block; i <= end_block; i++) {
    bufferlist bl;
    string oid = get_block_oid(&ictx->header, i);
    uint64_t block_ofs = get_block_ofs(&ictx->header, off + total_write);
    uint64_t write_len = min(block_size - block_ofs, left);
    bl.append(buf + total_write, write_len);
    AioBlockCompletion *block_completion = new AioBlockCompletion(c, off, len, NULL);
    c->add_block_completion(block_completion);
    librados::Rados::AioCompletion *rados_completion = rados.aio_create_completion(block_completion, NULL, rados_cb);
    r = rados.aio_write(pool->data, oid, block_ofs, bl, write_len, rados_completion);
    if (r < 0)
      goto done;
    total_write += write_len;
    left -= write_len;
  }
  return 0;
done:
  /* FIXME: cleanup all the allocated stuff */
  return r;
}

void librbd::RBDClient::rados_aio_sparse_read_cb(rados_completion_t c, void *arg)
{
  dout(10) << "librbd::RBDClient::rados_aio_sparse_read_cb" << dendl;
  AioBlockCompletion *block_completion = (AioBlockCompletion *)arg;
  block_completion->complete(rados_aio_get_return_value(c));
}

int librbd::RBDClient::aio_read(PoolCtx *ctx, ImageCtx *ictx, off_t off, size_t len,
				char *buf,
                                AioCompletion *c)
{
  int r = check_ictx(ictx);
  if (r < 0)
    return r;

  int64_t ret;
  int total_read = 0;
  uint64_t start_block = get_block_num(&ictx->header, off);
  uint64_t end_block = get_block_num(&ictx->header, off + len);
  uint64_t block_size = get_block_size(&ictx->header);
  uint64_t left = len;

  for (uint64_t i = start_block; i <= end_block; i++) {
    bufferlist bl;
    string oid = get_block_oid(&ictx->header, i);
    uint64_t block_ofs = get_block_ofs(&ictx->header, off + total_read);
    uint64_t read_len = min(block_size - block_ofs, left);

    map<off_t, size_t> m;
    map<off_t, size_t>::iterator iter;

    AioBlockCompletion *block_completion = new AioBlockCompletion(c, block_ofs, read_len, buf + total_read);
    c->add_block_completion(block_completion);

    librados::Rados::AioCompletion *rados_completion = rados.aio_create_completion(block_completion, rados_aio_sparse_read_cb, rados_cb);
    r = rados.aio_sparse_read(ctx->data, oid, block_ofs,
			      &block_completion->m, &block_completion->data_bl,
			      read_len, rados_completion);
    if (r < 0 && r == -ENOENT)
      r = 0;
    if (r < 0) {
      ret = r;
      goto done;
    }
    total_read += read_len;
    left -= read_len;
  }
  ret = total_read;
done:
  return ret;
}

/*
   RBD
*/

void librbd::RBD::version(int *major, int *minor, int *extra)
{
  librbd_version(major, minor, extra);
}

int librbd::RBD::initialize(int argc, const char *argv[])
{
  client = new RBDClient();
  return client->initialize(argc, argv);
}

void librbd::RBD::shutdown()
{
  client->shutdown();
  delete client;
}

int librbd::RBD::open_pool(const char *pool_name, pool_t *pool)
{
  librbd::PoolCtx *ctx = new librbd::PoolCtx;
  if (!ctx)
    return -ENOMEM;

  int ret = client->open_pools(pool_name, ctx);
  if (ret < 0)
    return ret;

  *pool = (pool_t)ctx;
  
  return 0; 
}

int librbd::RBD::close_pool(pool_t pool)
{
  PoolCtx *ctx = (PoolCtx *)pool;

  client->close_pools(ctx);
  delete ctx;
  
  return 0; 
}

int librbd::RBD::open_image(pool_t pool, const char *name, image_t *image, const char *snap_name)
{
  PoolCtx *pctx = (PoolCtx *)pool;
  ImageCtx *ictx = new ImageCtx(name, pctx);
  if (!ictx)
    return -ENOMEM;
  int r = client->open_image(pctx, ictx, name, snap_name);
  *image = (image_t)ictx;
  return r;
}

int librbd::RBD::close_image(image_t image)
{
  ImageCtx *ctx = (ImageCtx *)image;
  client->close_image(ctx);
  return 0; 
}

int librbd::RBD::create(pool_t pool, const char *name, size_t size, int *order)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  string md_oid = name;
  md_oid += RBD_SUFFIX;
  int r = client->create(ctx->md, md_oid, name, size, order);
  return r;
}

int librbd::RBD::remove(pool_t pool, const char *name)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  int r = client->remove(ctx, name);
  return r;
}

int librbd::RBD::resize(image_t image, size_t size)
{
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->resize(ictx->pctx, ictx, size);
  return r;
}

int librbd::RBD::stat(image_t image, image_info_t& info)
{
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->info(ictx->pctx, ictx, info);
  return r;
}

int librbd::RBD::list(pool_t pool, std::vector<std::string>& names)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  int r = client->list(ctx, names);
  return r;
}

int librbd::RBD::copy(pool_t src_pool, const char *srcname, pool_t dest_pool, const char *destname)
{
  PoolCtx *src_ctx = (PoolCtx *)src_pool;
  PoolCtx *dest_ctx = (PoolCtx *)dest_pool;
  int r = client->copy(src_ctx, srcname, dest_ctx, destname);
  return r;
}

int librbd::RBD::rename(pool_t src_pool, const char *srcname, const char *destname)
{
  PoolCtx *ctx = (PoolCtx *)src_pool;
  int r = client->rename(ctx, srcname, destname);
  return r;
}

int librbd::RBD::create_snap(image_t image, const char *snap_name)
{
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->create_snap(ictx->pctx, ictx, snap_name);
  return r;
}

int librbd::RBD::remove_snap(image_t image, const char *snap_name)
{
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->remove_snap(ictx->pctx, ictx, snap_name);
  return r;
}

int librbd::RBD::rollback_snap(image_t image, const char *snap_name)
{
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->rollback_snap(ictx->pctx, ictx, snap_name);
  return r;
}

int librbd::RBD::list_snaps(image_t image, std::vector<librbd::snap_info_t>& snaps)
{
  ImageCtx *ictx = (ImageCtx *)image;
  return client->list_snaps(ictx->pctx, ictx, snaps);
}

int librbd::RBD::set_snap(image_t image, const char *snap_name)
{
  ImageCtx *ictx = (ImageCtx *)image;
  return client->set_snap(ictx->pctx, ictx, snap_name);
}

void librbd::RBD::get_rados_pools(pool_t pool, librados::pool_t *md_pool, librados::pool_t *data_pool)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  if (md_pool)
    *md_pool = ctx->md;
  if (data_pool)
    *data_pool = ctx->data;
}

int librbd::RBD::read(image_t image, off_t ofs, size_t len, bufferlist& bl)
{
  ImageCtx *ictx = (ImageCtx *)image;
  bufferptr ptr(len);
  bl.push_back(ptr);
  return client->read(ictx->pctx, ictx, ofs, len, bl.c_str());
}

int librbd::RBD::read_iterate(image_t image, off_t ofs, size_t len,
                   int (*cb)(off_t, size_t, const char *, void *), void *arg)
{
  ImageCtx *ictx = (ImageCtx *)image;
  return client->read_iterate(ictx->pctx, ictx, ofs, len, cb, arg);
}

int librbd::RBD::write(image_t image, off_t ofs, size_t len, bufferlist& bl)
{
  ImageCtx *ictx = (ImageCtx *)image;
  if (bl.length() < len)
    return -EINVAL;
  return client->write(ictx->pctx, ictx, ofs, len, bl.c_str());
}


librbd::RBD::AioCompletion *librbd::RBD::aio_create_completion(void *cb_arg, callback_t complete_cb)
{
  RBDClient::AioCompletion *c = client->aio_create_completion(cb_arg, complete_cb);
  RBD::AioCompletion *rbd_completion = new AioCompletion(c);
  c->rbd_comp = rbd_completion;
  return rbd_completion;
}

int librbd::RBD::aio_write(image_t image, off_t off, size_t len, bufferlist& bl,
		           AioCompletion *c)
{
  ImageCtx *ictx = (ImageCtx *)image;
  if (bl.length() < len)
    return -EINVAL;
  return client->aio_write(ictx->pctx, ictx, off, len, bl.c_str(), (RBDClient::AioCompletion *)c->pc);
}

int librbd::RBD::aio_read(image_t image, off_t off, size_t len, bufferlist& bl,
		          AioCompletion *c)
{
  ImageCtx *ictx = (ImageCtx *)image;
  bufferptr ptr(len);
  bl.push_back(ptr);
  dout(10) << "librbd::RBD::aio_read() buf=" << (void *)bl.c_str() << "~" << (void *)(bl.c_str() + len - 1) << dendl;
  return client->aio_read(ictx->pctx, ictx, off, len, bl.c_str(), (RBDClient::AioCompletion *)c->pc);
}

int librbd::RBD::AioCompletion::wait_for_complete()
{
  RBDClient::AioCompletion *c = (RBDClient::AioCompletion *)pc;
  return c->wait_for_complete();
}

int librbd::RBD::AioCompletion::get_return_value()
{
  RBDClient::AioCompletion *c = (RBDClient::AioCompletion *)pc;
  return c->get_return_value();
}

void librbd::RBD::AioCompletion::release()
{
  RBDClient::AioCompletion *c = (RBDClient::AioCompletion *)pc;
  c->release();
}

librados::Rados& librbd::RBD::get_rados()
{
  return client->get_rados();
}

} // namespace librbd

extern "C" void librbd_version(int *major, int *minor, int *extra)
{
  if (major)
    *major = LIBRBD_VER_MAJOR;
  if (minor)
    *minor = LIBRBD_VER_MINOR;
  if (extra)
    *extra = LIBRBD_VER_EXTRA;
}

static Mutex rbd_init_mutex("rbd_init");
static int rbd_initialized = 0;
static librbd::RBDClient *rbd_client = NULL;

extern "C" int rbd_initialize(int argc, const char **argv)
{
  int r = 0;
  rbd_init_mutex.Lock();
  if (!rbd_initialized) {
    rbd_client = new librbd::RBDClient;
    if (!rbd_client) {
      r = -ENOMEM;
      goto done;
    }
    r = rbd_client->initialize(argc, argv);
    if (r < 0)
      goto done;
  }
  ++rbd_initialized;
done:
  rbd_init_mutex.Unlock();
  return r;
}

extern "C" void rbd_shutdown()
{
  rbd_init_mutex.Lock();
  if (!rbd_initialized) {
    derr << "rbd_shutdown() called without rbd_initialize()" << dendl;
    rbd_init_mutex.Unlock();
    return;
  }
  --rbd_initialized;
  if (!rbd_initialized) {
    rbd_client->shutdown();
    delete rbd_client;
    rbd_client = NULL;
  }
  rbd_init_mutex.Unlock();
}

/* pools */
extern "C" int rbd_open_pool(const char *pool_name, rbd_pool_t *pool)
{
  librbd::PoolCtx *ctx = new librbd::PoolCtx;
  if (!ctx)
    return -ENOMEM;
  int ret = rbd_client->open_pools(pool_name, ctx);
  if (ret < 0)
    return ret;

  *pool = (rbd_pool_t)ctx;

  return 0;
}

extern "C" int rbd_close_pool(rbd_pool_t pool)
{
  librbd::PoolCtx *ctx = (librbd::PoolCtx *)pool;

  rbd_client->close_pools(ctx);
  delete ctx;
  
  return 0; 
}

/* images */
extern "C" int rbd_list(rbd_pool_t pool, char *names, size_t *size)
{
  std::vector<std::string> cpp_names;
  int r = rbd_client->list((librbd::PoolCtx *)pool, cpp_names);
  if (r == -ENOENT)
    return 0;

  if (r < 0)
    return r;

  size_t expected_size = 0;

  for (size_t i = 0; i < cpp_names.size(); i++) {
    expected_size += cpp_names[i].size() + 1;
  }
  if (*size < expected_size) {
    *size = expected_size;
    return -ERANGE;
  }

  for (int i = 0; i < (int)cpp_names.size(); i++) {
    strcpy(names, cpp_names[i].c_str());
    names += strlen(names) + 1;
  }
  return (int)cpp_names.size();
}

extern "C" int rbd_create(rbd_pool_t pool, const char *name, size_t size, int *order)
{
  librbd::PoolCtx *ctx = (librbd::PoolCtx *)pool;
  string md_oid = name;
  md_oid += RBD_SUFFIX;
  return rbd_client->create(ctx->md, md_oid, name, size, order);
}

extern "C" int rbd_remove(rbd_pool_t pool, const char *name)
{
  librbd::PoolCtx *ctx = (librbd::PoolCtx *)pool;
  return rbd_client->remove(ctx, name);
}

extern "C" int rbd_copy(rbd_pool_t src_pool, const char *srcname, rbd_pool_t dest_pool, const char *destname)
{
  librbd::PoolCtx *src_ctx = (librbd::PoolCtx *)src_pool;
  librbd::PoolCtx *dest_ctx = (librbd::PoolCtx *)dest_pool;
  return rbd_client->copy(src_ctx, srcname, dest_ctx, destname);
}

extern "C" int rbd_rename(rbd_pool_t src_pool, const char *srcname, const char *destname)
{
  librbd::PoolCtx *ctx = (librbd::PoolCtx *)src_pool;
  return rbd_client->rename(ctx, srcname, destname);
}

extern "C" int rbd_open_image(rbd_pool_t pool, const char *name, rbd_image_t *image, const char *snap_name)
{
  librbd::PoolCtx *pctx = (librbd::PoolCtx *)pool;
  librbd::ImageCtx *ictx = new librbd::ImageCtx(name, pctx);
  if (!ictx)
    return -ENOMEM;
  int r = rbd_client->open_image(pctx, ictx, name, snap_name);
  *image = (rbd_image_t)ictx;
  return r;
}

extern "C" int rbd_close_image(rbd_image_t image)
{
  librbd::ImageCtx *ctx = (librbd::ImageCtx *)image;
  rbd_client->close_image(ctx);
  return 0; 
}

extern "C" int rbd_resize(rbd_image_t image, size_t size)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return rbd_client->resize(ictx->pctx, ictx, size);
}

extern "C" int rbd_stat(rbd_image_t image, rbd_image_info_t *info)
{
  librbd::image_info_t cpp_info;
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  int r = rbd_client->info(ictx->pctx, ictx, cpp_info);
  if (r < 0)
    return r;

  info->size = cpp_info.size;
  info->obj_size = cpp_info.obj_size;
  info->num_objs = cpp_info.num_objs;
  info->order = cpp_info.order;
  return 0;
}

/* snapshots */
extern "C" int rbd_create_snap(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return rbd_client->create_snap(ictx->pctx, ictx, snap_name);
}

extern "C" int rbd_remove_snap(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return rbd_client->remove_snap(ictx->pctx, ictx, snap_name);
}

extern "C" int rbd_rollback_snap(rbd_image_t image, const char *snap_name)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return rbd_client->rollback_snap(ictx->pctx, ictx, snap_name);
}

extern "C" int rbd_list_snaps(rbd_image_t image, rbd_snap_info_t *snaps, int *max_snaps)
{
  std::vector<librbd::snap_info_t> cpp_snaps;
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  int r = rbd_client->list_snaps(ictx->pctx, ictx, cpp_snaps);
  if (r == -ENOENT)
    return 0;
  if (r < 0)
    return r;
  if (!max_snaps)
    return -EINVAL;
  if (*max_snaps < (int)cpp_snaps.size() + 1) {
    *max_snaps = (int)cpp_snaps.size() + 1;
    return -ERANGE;
  }

  int i;

  for (i = 0; i < (int)cpp_snaps.size(); i++) {
    snaps[i].id = cpp_snaps[i].id;
    snaps[i].size = cpp_snaps[i].size;
    snaps[i].name = strdup(cpp_snaps[i].name.c_str());
    if (!snaps[i].name) {
      for (int j = 0; j < i; j++)
	free((void *)snaps[j].name);
      return -ENOMEM;
    }
  }
  snaps[i].id = 0;
  snaps[i].size = 0;
  snaps[i].name = NULL;

  return (int)cpp_snaps.size();
}

extern "C" void rbd_list_snaps_end(rbd_snap_info_t *snaps)
{
  while (snaps->name) {
    free((void *)snaps->name);
    snaps++;
  }
}

extern "C" int rbd_set_snap(rbd_image_t image, const char *snapname)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return rbd_client->set_snap(ictx->pctx, ictx, snapname);
}

extern "C" void rbd_get_rados_pools(rbd_pool_t pool, rados_pool_t *md_pool, rados_pool_t *data_pool)
{
  librbd::PoolCtx *ctx = (librbd::PoolCtx *)pool;
  if (md_pool)
    *md_pool = (rados_pool_t) ctx->md;
  if (data_pool)
    *data_pool = (rados_pool_t) ctx->data;
}

/* I/O */
extern "C" int rbd_read(rbd_image_t image, off_t ofs, size_t len, char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return rbd_client->read(ictx->pctx, ictx, ofs, len, buf);
}

extern "C" int rbd_read_iterate(rbd_image_t image, off_t ofs, size_t len,
				int (*cb)(off_t, size_t, const char *, void *), void *arg)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return rbd_client->read_iterate(ictx->pctx, ictx, ofs, len, cb, arg);
}

extern "C" int rbd_write(rbd_image_t image, off_t ofs, size_t len, const char *buf)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  return rbd_client->write(ictx->pctx, ictx, ofs, len, buf);
}

extern "C" int rbd_aio_create_completion(void *cb_arg, rbd_callback_t complete_cb, rbd_completion_t *c)
{
  librbd::RBDClient::AioCompletion *comp = rbd_client->aio_create_completion(cb_arg, complete_cb);
  librbd::RBD::AioCompletion *rbd_comp = new librbd::RBD::AioCompletion(comp);
  comp->rbd_comp = rbd_comp;
  *c = (rbd_completion_t) rbd_comp;
  return 0;
}

extern "C" int rbd_aio_write(rbd_image_t image, off_t off, size_t len, const char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return rbd_client->aio_write(ictx->pctx, ictx, off, len, buf, (librbd::RBDClient::AioCompletion *)comp->pc);
}

extern "C" int rbd_aio_read(rbd_image_t image, off_t off, size_t len, char *buf, rbd_completion_t c)
{
  librbd::ImageCtx *ictx = (librbd::ImageCtx *)image;
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return rbd_client->aio_read(ictx->pctx, ictx, off, len, buf, (librbd::RBDClient::AioCompletion *)comp->pc);
}

extern "C" int rbd_aio_wait_for_complete(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return comp->wait_for_complete();
}

extern "C" int rbd_aio_get_return_value(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  return comp->get_return_value();
}

extern "C" void rbd_aio_release(rbd_completion_t c)
{
  librbd::RBD::AioCompletion *comp = (librbd::RBD::AioCompletion *)c;
  comp->release();
}
