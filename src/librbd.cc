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
#include "include/librbd.hpp"
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

  struct PoolCtx {
    pool_t md;
    pool_t data;
  };

  struct ImageCtx {
    struct rbd_obj_header_ondisk header;
    ::SnapContext snapc;
    vector<librados::snap_t> snaps;
    uint64_t snapid;
    std::string name;
  };

class RBDClient
{
  librados::Rados rados;
public:
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
  int get_snapc(PoolCtx *pp, string& md_oid, const char *snap_name,
		   ::SnapContext& snapc, vector<snap_t>& snaps, uint64_t& snapid);
  int rollback_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name);
  int remove_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name);
  int copy(PoolCtx *pp, const char *srcname, PoolCtx *pp_dest, const char *destname);

  int open_pools(const char *poolname, PoolCtx *pp);
  void close_pools(PoolCtx *pp);

  void trim_image(PoolCtx *pp, const char *imgname, rbd_obj_header_ondisk *header, uint64_t newsize);
  int read_rbd_info(PoolCtx *pp, string& info_oid, struct rbd_info *info);

  int touch_rbd_info(librados::pool_t pool, string& info_oid);
  int rbd_assign_bid(librados::pool_t pool, string& info_oid, uint64_t *id);
  int read_header_bl(librados::pool_t pool, string& md_oid, bufferlist& header, uint64_t *ver);
  int notify_change(librados::pool_t pool, string& oid, uint64_t *pver);
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
  static int init_rbd_info(struct rbd_info *info);
  static void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
			      size_t size, int *order, uint64_t bid);

};


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

void librbd::RBDClient::trim_image(PoolCtx *pp, const char *imgname, rbd_obj_header_ondisk *header, uint64_t newsize)
{
  uint64_t numseg = get_max_block(header);
  uint64_t start = get_block_num(header, newsize);

  cout << "trimming image data from " << numseg << " to " << start << " objects..." << std::endl;
  for (uint64_t i=start; i<numseg; i++) {
    string oid = get_block_oid(header, i);
    rados.remove(pp->data, oid);
    if ((i & 127) == 0) {
      cout << "\r\t" << i << "/" << numseg;
      cout.flush();
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

int librbd::RBDClient::notify_change(librados::pool_t pool, string& oid, uint64_t *pver)
{
  uint64_t ver;
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

  notify_change(pp->md, md_oid, NULL);

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

int librbd::RBDClient::list(PoolCtx *pp, std::vector<string>& names)
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
  librados::snap_t snapid = 0;
  vector<librados::snap_t> snaps;
  ::SnapContext snapc;
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  int r = get_snapc(pp, md_oid, snap_name, snapc, snaps, snapid);
  if (r != -ENOENT && r < 0)
    return r;

  r = rados.set_snap_context(pp->data, snapc.seq, snaps);
  if (r < 0)
    return r;

  rados.set_snap(pp->data, snapid);
  r = add_snap(pp, ictx, snap_name);
  return r;
}

int librbd::RBDClient::remove_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name)
{
  librados::snap_t snapid = 0;
  vector<librados::snap_t> snaps;
  ::SnapContext snapc;
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  int r = get_snapc(pp, md_oid, snap_name, snapc, snaps, snapid);
  if (r < 0)
    return r;

  r = rados.set_snap_context(pp->data, snapc.seq, snaps);
  if (r < 0)
    return r;

  rados.set_snap(pp->data, snapid);

  r = rm_snap(pp, md_oid, snap_name);
  r = rados.selfmanaged_snap_remove(pp->data, snapid);
  return r;
}

int librbd::RBDClient::create(pool_t& pool, string& md_oid, const char *imgname,
			      uint64_t size, int *order)
{

  // make sure it doesn't already exist
  int r = rados.stat(pool, md_oid, NULL, NULL);
  if (r == 0) {
    cerr << "rbd image header " << md_oid << " already exists" << std::endl;
    return -EEXIST;
  }

  uint64_t bid;
  string dir_info = RBD_INFO;
  r = rbd_assign_bid(pool, dir_info, &bid);
  if (r < 0) {
    cerr << "failed to assign a block name for image" << std::endl;
    return r;
  }

  struct rbd_obj_header_ondisk header;
  init_rbd_header(header, size, order, bid);

  bufferlist bl;
  bl.append((const char *)&header, sizeof(header));

  cout << "adding rbd image to directory..." << std::endl;
  bufferlist cmdbl, emptybl;
  __u8 c = CEPH_OSD_TMAP_SET;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  ::encode(emptybl, cmdbl);
  r = rados.tmap_update(pool, RBD_DIRECTORY, cmdbl);
  if (r < 0) {
    cerr << "error adding img to directory: " << strerror(-r)<< std::endl;
    return r;
  }

  cout << "creating rbd image..." << std::endl;
  r = rados.write(pool, md_oid, 0, bl, bl.length());
  if (r < 0) {
    cerr << "error writing header: " << strerror(-r) << std::endl;
    return r;
  }

  cout << "done." << std::endl;
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
    cerr << "error reading header: " << md_oid << ": " << strerror(-r) << std::endl;
    return r;
  }
  r = rados.stat(pp->md, dst_md_oid, NULL, NULL);
  if (r == 0) {
    cerr << "rbd image header " << dst_md_oid << " already exists" << std::endl;
    return -EEXIST;
  }
  r = write_header(pp, dst_md_oid, header);
  if (r < 0) {
    cerr << "error writing header: " << dst_md_oid << ": " << strerror(-r) << std::endl;
    return r;
  }
  r = tmap_set(pp, dstname_str);
  if (r < 0) {
    rados.remove(pp->md, dst_md_oid);
    cerr << "can't add " << dst_md_oid << " to directory" << std::endl;
    return r;
  }
  r = tmap_rm(pp, imgname_str);
  if (r < 0)
    cerr << "warning: couldn't remove old entry from directory (" << imgname_str << ")" << std::endl;

  r = rados.remove(pp->md, md_oid);
  if (r < 0)
    cerr << "warning: couldn't remove old metadata" << std::endl;

  return 0;
}


int librbd::RBDClient::info(PoolCtx *pp, ImageCtx *ictx, librbd::image_info_t& info)
{
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
    trim_image(pp, imgname, &header, 0);
    cout << "\rremoving header..." << std::endl;
    rados.remove(pp->md, md_oid);
  }

  cout << "removing rbd image to directory..." << std::endl;
  bufferlist cmdbl;
  __u8 c = CEPH_OSD_TMAP_RM;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  r = rados.tmap_update(pp->md, RBD_DIRECTORY, cmdbl);
  if (r < 0) {
    cerr << "error removing img from directory: " << strerror(-r)<< std::endl;
    return r;
  }

  cout << "done." << std::endl;
  return 0;
}

int librbd::RBDClient::resize(PoolCtx *pp, ImageCtx *ictx, uint64_t size)
{
  uint64_t ver;
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  // trim
  if (size == ictx->header.image_size) {
    cout << "no change in size (" << size << " -> " << ictx->header.image_size << ")" << std::endl;
    return 0;
  }

  if (size > ictx->header.image_size) {
    cout << "expanding image " << size << " -> " << ictx->header.image_size << " objects" << std::endl;
    ictx->header.image_size = size;
  } else {
    cout << "shrinking image " << size << " -> " << ictx->header.image_size << " objects" << std::endl;
    trim_image(pp, ictx->name.c_str(), &(ictx->header), size);
    ictx->header.image_size = size;
  }

  // rewrite header
  bufferlist bl;
  bl.append((const char *)&(ictx->header), sizeof(ictx->header));
  rados.set_assert_version(pp->md, ver);
  int r = rados.write(pp->md, md_oid, 0, bl, bl.length());
  if (r == -ERANGE)
    cerr << "operation might have conflicted with another client!" << std::endl;
  if (r < 0) {
    cerr << "error writing header: " << strerror(-r) << std::endl;
    return r;
  } else {
    notify_change(pp->md, md_oid, NULL);
  }

  cout << "done." << std::endl;
  return 0;
}

int librbd::RBDClient::list_snaps(PoolCtx *pp, ImageCtx *ictx, std::vector<librbd::snap_info_t>& snaps)
{
  bufferlist bl, bl2;
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  int r = rados.exec(pp->md, md_oid, "rbd", "snap_list", bl, bl2);
  if (r < 0) {
    return r;
  }

  uint32_t num_snaps;
  uint64_t snap_seq;
  bufferlist::iterator iter = bl2.begin();
  ::decode(snap_seq, iter);
  ::decode(num_snaps, iter);
  for (uint32_t i=0; i < num_snaps; i++) {
    uint64_t id, image_size;
    string s;
    librbd::snap_info_t info;
    ::decode(id, iter);
    ::decode(image_size, iter);
    ::decode(s, iter);
    info.name = s;
    info.id = id;
    info.size = image_size;
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
    cerr << "failed to create snap id: " << strerror(-r) << std::endl;
    return r;
  }

  ::encode(snap_name, bl);
  ::encode(snap_id, bl);

  r = rados.exec(pp->md, md_oid, "rbd", "snap_add", bl, bl2);
  if (r < 0) {
    cerr << "rbd.snap_add execution failed failed: " << strerror(-r) << std::endl;
    return r;
  }
  notify_change(pp->md, md_oid, NULL);

  return 0;
}

int librbd::RBDClient::rm_snap(PoolCtx *pp, string& md_oid, const char *snap_name)
{
  bufferlist bl, bl2;

  ::encode(snap_name, bl);

  int r = rados.exec(pp->md, md_oid, "rbd", "snap_remove", bl, bl2);
  if (r < 0) {
    cerr << "rbd.snap_remove execution failed failed: " << strerror(-r) << std::endl;
    return r;
  }

  return 0;
}

int librbd::RBDClient::get_snapc(PoolCtx *pp, string& md_oid, const char *snap_name,
			      ::SnapContext& snapc, vector<snap_t>& snaps, uint64_t& snapid)
{
  bufferlist bl, bl2;

  int r = rados.exec(pp->md, md_oid, "rbd", "snap_list", bl, bl2);
  if (r < 0) {
    cerr << "list_snaps failed: " << strerror(-r) << std::endl;
    return r;
  }

  snaps.clear();

  uint32_t num_snaps;
  bufferlist::iterator iter = bl2.begin();
  ::decode(snapc.seq, iter);
  ::decode(num_snaps, iter);
  snapid = 0;
  for (uint32_t i=0; i < num_snaps; i++) {
    uint64_t id, image_size;
    string s;
    ::decode(id, iter);
    ::decode(image_size, iter);
    ::decode(s, iter);
    if (s.compare(snap_name) == 0)
      snapid = id;
    snapc.snaps.push_back(id);
    snaps.push_back(id);
  }

  if (!snapc.is_valid()) {
    cerr << "image snap context is invalid! can't rollback" << std::endl;
    return -EIO;
  }

  if (!snapid) {
    return -ENOENT;
  }

  return 0;
}

int librbd::RBDClient::rollback_snap(PoolCtx *pp, ImageCtx *ictx, const char *snap_name)
{
  string md_oid = ictx->name;
  md_oid += RBD_SUFFIX;

  int r = get_snapc(pp, md_oid, snap_name, ictx->snapc, ictx->snaps, ictx->snapid);
  if (r < 0)
    return r;

  r = rados.set_snap_context(pp->data, ictx->snapc.seq, ictx->snaps);
  if (r < 0)
    return r;

  rados.set_snap(pp->data, ictx->snapid);
  r = read_header(pp->md, md_oid, &(ictx->header), NULL);
  if (r < 0) {
    cerr << "error reading header: " << md_oid << ": " << strerror(-r) << std::endl;
    return r;
  }
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
    cerr << "header creation failed" << std::endl;
    return r;
  }

  ret = read_header(pp_dest->md, dest_md_oid, &dest_header, NULL);
  if (ret < 0) {
    cerr << "failed to read newly created header" << std::endl;
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
	cerr << "data error!" << std::endl;
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
  string md_oid = ictx->name;
  vector<snap_t> snaps;
  md_oid += RBD_SUFFIX;

  int r = get_snapc(pp, md_oid, snap_name, ictx->snapc, ictx->snaps, ictx->snapid);
  if (r < 0)
    return r;

  r = rados.set_snap_context(pp->data, ictx->snapc.seq, ictx->snaps);
  if (r < 0)
    return r;

  rados.set_snap(pp->data, ictx->snapid);

  return 0;
}

void librbd::RBD::version(int *major, int *minor, int *extra)
{
  librbd_version(major, minor, extra);
}

int librbd::RBDClient::open_pools(const char *pool_name, PoolCtx *ctx)
{
  librados::pool_t pool, md_pool;

  int r = rados.open_pool(pool_name, &md_pool);
  if (r < 0) {
    cerr << "error opening pool " << pool_name << " (err=" << r << ")" << std::endl;
    return -1;
  }
  ctx->md = md_pool;

  r = rados.open_pool(pool_name, &pool);
  if (r < 0) {
    cerr << "error opening pool " << pool_name << " (err=" << r << ")" << std::endl;
    close_pools(ctx);
    return -1;
  }
  ctx->data = pool;
  return 0;
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

int librbd::RBD::open_image(pool_t pool, const char *name, image_t *image, const char *snap_name = NULL)
{
  PoolCtx *pctx = (PoolCtx *)pool;
  ImageCtx *ictx = new librbd::ImageCtx;
  if (!ictx)
    return -ENOMEM;

  ictx->name = name;
  string md_oid = name;
  md_oid += RBD_SUFFIX;

  int r = client->read_header(pctx->md, md_oid, &(ictx->header), NULL);
  if (r < 0)
    return r;

  ictx->snapid = 0;
  if (snap_name) {
    r = client->get_snapc(pctx, md_oid, snap_name, ictx->snapc, ictx->snaps, ictx->snapid);
    if (r < 0)
      return r;
  }

  *image = (image_t)ictx;

  return 0;
}

int librbd::RBD::close_image(image_t image)
{
  ImageCtx *ctx = (ImageCtx *)image;
  delete ctx;
  return 0; 
}

int librbd::RBD::create(pool_t pool, const char *name, size_t size)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  string md_oid = name;
  md_oid += RBD_SUFFIX;
  int order = 0;
  int r = client->create(ctx->md, md_oid, name, size, &order);
  return r;
}

int librbd::RBD::remove(pool_t pool, const char *name)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  int r = client->remove(ctx, name);
  return r;
}

int librbd::RBD::resize(pool_t pool, image_t image, size_t size)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->resize(ctx, ictx, size);
  return r;
}

int librbd::RBD::stat(pool_t pool, image_t image, image_info_t& info)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->info(ctx, ictx, info);
  return r;
}

int librbd::RBD::list(pool_t pool, std::vector<string>& names)
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

int librbd::RBD::create_snap(pool_t pool, image_t image, const char *snap_name)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->create_snap(ctx, ictx, snap_name);
  return r;
}

int librbd::RBD::remove_snap(pool_t pool, image_t image, const char *snap_name)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->remove_snap(ctx, ictx, snap_name);
  return r;
}

int librbd::RBD::rollback_snap(pool_t pool, image_t image, const char *snap_name)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->rollback_snap(ctx, ictx, snap_name);
  return r;
}

int librbd::RBD::list_snaps(pool_t pool, image_t image, std::vector<librbd::snap_info_t>& snaps)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  ImageCtx *ictx = (ImageCtx *)image;
  int r = client->list_snaps(ctx, ictx, snaps);
  return r;
}

int librbd::RBD::set_snap(pool_t pool, image_t image, const char *snap_name)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  ImageCtx *ictx = (ImageCtx *)image;
  return client->set_snap(ctx, ictx, snap_name);
}

void librbd::RBD::get_rados_pools(pool_t pool, librados::pool_t *md_pool, librados::pool_t *data_pool)
{
  PoolCtx *ctx = (PoolCtx *)pool;
  if (md_pool)
    *md_pool = ctx->md;
  if (data_pool)
    *data_pool = ctx->data;
}

}

extern "C" void librbd_version(int *major, int *minor, int *extra)
{
  if (major)
    *major = LIBRBD_VER_MAJOR;
  if (minor)
    *minor = LIBRBD_VER_MINOR;
  if (extra)
    *extra = LIBRBD_VER_EXTRA;
}

#if 0 /* waiting until C++ interface stabilizes */
static librbd::RBD rbd;

extern "C" int rbd_initialize(int argc, const char **argv)
{
  return rbd.initialize(argc, argv);
}

extern "C" void rbd_shutdown()
{
  rbd.shutdown();
}

extern "C" int rbd_open_pool(const char *pool_name, rbd_pool_t *pool)
{
  librbd::pool_t cpp_pool;
  int r = rbd.open_pool(pool_name, &cpp_pool);
  if (r < 0)
    return r;

  *pool = (rbd_pool_t) cpp_pool;
  return 0;
}

extern "C" int rbd_close_pool(rbd_pool_t pool)
{
  return rbd.close_pool((librbd::pool_t) pool);
}

/* images */
extern "C" int rbd_create(rbd_pool_t pool, const char *name, size_t size)
{
  return rbd.create(pool, name, size);
}

extern "C" int rbd_remove(rbd_pool_t pool, const char *name)
{
  return rbd.remove(pool, name);
}

extern "C" int rbd_resize(rbd_pool_t pool, const char *name, size_t size)
{
  return rbd.resize(pool, name, size);
}

extern "C" int rbd_stat(rbd_pool_t pool, const char *name, rbd_image_info_t *info)
{
  librbd::image_info_t cpp_info;
  int r = rbd.stat(pool, name, cpp_info);
  if (r < 0)
    return r;

  info->size = cpp_info.size;
  info->obj_size = cpp_info.obj_size;
  info->num_objs = cpp_info.num_objs;
  info->order = cpp_info.order;
  return 0;
}

extern "C" size_t rbd_list(rbd_pool_t pool, char **names, size_t max_names)
{
  std::vector<string> cpp_names;
  librbd::pool_t cpp_pool = (librbd::pool_t) pool;
  int r = rbd.list(cpp_pool, cpp_names);
  if (r == -ENOENT)
    return 0;
  if (r < 0)
    return r;
  if (max_names < cpp_names.size())
    return -ERANGE;

  for (size_t i = 0; i < cpp_names.size(); i++) {
    names[i] = strdup(cpp_names[i].c_str());
    if (!names[i])
      return -ENOMEM;
  }
  return cpp_names.size();
}

/* snapshots */
extern "C" int rbd_create_snap(rbd_pool_t pool, const char *image, const char *snap_name)
{
  return rbd.create_snap(pool, image, snap_name);
}

extern "C" int rbd_remove_snap(rbd_pool_t pool, const char *image, const char *snap_name)
{
  return rbd.remove_snap(pool, image, snap_name);
}

extern "C" int rbd_rollback_snap(rbd_pool_t pool, const char *image, const char *snap_name)
{
  return rbd.rollback_snap(pool, image, snap_name);
}

extern "C" size_t rbd_list_snaps(rbd_pool_t pool, const char *image, rbd_snap_info_t *snaps, size_t max_snaps)
{
  std::vector<librbd::snap_info_t> cpp_snaps;
  int r = rbd.list_snaps(pool, image, cpp_snaps);
  if (r == -ENOENT)
    return 0;
  if (r < 0)
    return r;
  if (max_snaps < cpp_snaps.size())
    return -ERANGE;

  for (size_t i = 0; i < cpp_snaps.size(); i++) {
    snaps[i].id = cpp_snaps[i].id;
    snaps[i].size = cpp_snaps[i].size;
    snaps[i].name = strdup(cpp_snaps[i].name.c_str());
    if (!snaps[i].name)
      return -ENOMEM;
  }
  return cpp_snaps.size();
}
#endif
