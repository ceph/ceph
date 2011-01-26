// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#define __STDC_FORMAT_MACROS
#include "config.h"

#include "common/errno.h"
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

static librbd::RBD rbd;
//static librados::Rados rados;
static string dir_oid = RBD_DIRECTORY;
static string dir_info_oid = RBD_INFO;

void usage()
{
  cout << "usage: rbd [-n <auth user>] [OPTIONS] <cmd> ...\n"
       << "where 'pool' is a rados pool name (default is 'rbd') and 'cmd' is one of:\n"
       << "  <ls | list> [pool-name]                   list rbd images\n"
       << "  info [image-name]                         show information about image size,\n"
       << "                                            striping, etc.\n"
       << "  create [image-name]                       create an empty image (requires size\n"
       << "                                            param)\n"
       << "  resize [image-name]                       resize (expand or contract) image\n"
       << "                                            (requires size param)\n"
       << "  rm [image-name]                           delete an image\n"
       << "  export [image-name] [dest-path]           export image to file\n"
       << "  import [path] [dst-image]                 import image from file (dest defaults\n"
       << "                                            as the filename part of file)\n"
       << "  <cp | copy> [src-image] [dest-image]      copy image to dest\n"
       << "  <mv | rename> [src-image] [dest-image]    copy image to dest\n"
       << "  snap ls [image-name]                      dump list of image snapshots\n"
       << "  snap create <--snap=name> [image-name]    create a snapshot\n"
       << "  snap rollback <--snap=name> [image-name]  rollback image head to snapshot\n"
       << "  snap rm <--snap=name> [image-name]        deletes a snapshot\n"
       << "  watch [image-name]                        watch events on image\n"
       << "\n"
       << "Other input options:\n"
       << "  -p, --pool <pool>            source pool name\n"
       << "  --image <image-name>         image name\n"
       << "  --dest <name>                destination [pool and] image name\n"
       << "  --snap <snapname>            specify snapshot name\n"
       << "  --dest-pool <name>           destination pool name\n"
       << "  --path <path-name>           path name for import/export (if not specified)\n"
       << "  --size <size in MB>          size parameter for create and resize commands\n";
}

void usage_exit()
{
  assert(1);
  usage();
  exit(1);
}

static void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
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

static void print_info(const char *imgname, librbd::image_info_t& info)
{
  cout << "rbd image '" << imgname << "':\n"
       << "\tsize " << prettybyte_t(info.size) << " in "
       << info.num_objs << " objects\n"
       << "\torder " << info.order
       << " (" << prettybyte_t(info.obj_size) << " objects)"
       << std::endl;
}

static string get_block_oid(rbd_obj_header_ondisk *header, uint64_t num)
{
  char o[RBD_MAX_SEG_NAME_SIZE];
  snprintf(o, RBD_MAX_SEG_NAME_SIZE,
	   "%s.%012" PRIx64, header->block_name, num);
  return o;
}

static uint64_t get_max_block(rbd_obj_header_ondisk *header)
{
  uint64_t size = header->image_size;
  int obj_order = header->options.order;
  uint64_t block_size = 1 << obj_order;
  uint64_t numseg = (size + block_size - 1) >> obj_order;

  return numseg;
}

static uint64_t get_block_size(rbd_obj_header_ondisk *header)
{
  return 1 << header->options.order;
}

static uint64_t get_block_num(rbd_obj_header_ondisk *header, uint64_t ofs)
{
  int obj_order = header->options.order;
  uint64_t num = ofs >> obj_order;

  return num;
}
static uint64_t get_pos_in_block(rbd_obj_header_ondisk *header, uint64_t ofs)
{
  int obj_order = header->options.order;
  uint64_t mask = (1 << obj_order) - 1;
  return ofs & mask;
}

static uint64_t bounded_pos_in_block(rbd_obj_header_ondisk *header, uint64_t end_ofs, unsigned int i)
{
  int obj_order = header->options.order;
  if (end_ofs >> obj_order > i)
    return (1 << obj_order);
  return get_pos_in_block(header, end_ofs);
}

static int init_rbd_info(struct rbd_info *info)
{
  memset(info, 0, sizeof(*info));
  return 0;
}
/*
int read_rbd_info(librbd::pools_t& pp, string& info_oid, struct rbd_info *info)
{
  int r;
  bufferlist bl;

  r = rados.read(pp.md, info_oid, 0, bl, sizeof(*info));
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

static int touch_rbd_info(librados::pool_t pool, string& info_oid)
{
  bufferlist bl;
  int r = rados.write(pool, info_oid, 0, bl, 0);
  if (r < 0)
    return r;
  return 0;
}

static int rbd_assign_bid(librados::pool_t pool, string& info_oid, uint64_t *id)
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


static int read_header_bl(librados::pool_t pool, string& md_oid, bufferlist& header, uint64_t *ver)
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

static int notify_change(librados::pool_t pool, string& oid, uint64_t *pver)
{
  uint64_t ver;
  if (pver)
    ver = *pver;
  else
    ver = rados.get_last_version(pool);
  rados.notify(pool, oid, ver);
  return 0;
}

static int read_header(librados::pool_t pool, string& md_oid, struct rbd_obj_header_ondisk *header, uint64_t *ver)
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

static int write_header(librbd::pools_t& pp, string& md_oid, bufferlist& header)
{
  bufferlist bl;
  int r = rados.write(pp.md, md_oid, 0, header, header.length());

  notify_change(pp.md, md_oid, NULL);

  return r;
}

static int tmap_set(librbd::pools_t& pp, string& imgname)
{
  bufferlist cmdbl, emptybl;
  __u8 c = CEPH_OSD_TMAP_SET;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  ::encode(emptybl, cmdbl);
  return rados.tmap_update(pp.md, dir_oid, cmdbl);
}

static int tmap_rm(librbd::pools_t& pp, string& imgname)
{
  bufferlist cmdbl;
  __u8 c = CEPH_OSD_TMAP_RM;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  return rados.tmap_update(pp.md, dir_oid, cmdbl);
}

static int rollback_image(librbd::pools_t& pp, struct rbd_obj_header_ondisk *header,
                          ::SnapContext& snapc, uint64_t snapid)
{
  uint64_t numseg = get_max_block(header);

  for (uint64_t i = 0; i < numseg; i++) {
    int r;
    string oid = get_block_oid(header, i);
    librados::SnapContext sn;
    sn.seq = snapc.seq;
    sn.snaps.clear();
    vector<snapid_t>::iterator iter = snapc.snaps.begin();
    for (; iter != snapc.snaps.end(); ++iter) {
      sn.snaps.push_back(*iter);
    }
    r = rados.selfmanaged_snap_rollback_object(pp.data, oid, sn, snapid);
    if (r < 0 && r != -ENOENT)
      return r;
  }
  return 0;
}
*/
static int do_list(const char* poolname)
{
  std::vector<string> names;
  int r = rbd.list_images(poolname, names);
  if (r < 0)
    return r;

  for (std::vector<string>::const_iterator i = names.begin(); i != names.end(); i++)
    cout << *i << std::endl;
  return 0;
}

static int do_create(const char *poolname, const char *imgname, size_t size)
{
  int r = rbd.create_image(poolname, imgname, size);
  if (r < 0)
    return r;
  return 0;
}
/*
static int do_rename(const char *poolname, const char *imgname, const char *destname)
{
  string dst_md_oid = dstname;
  dst_md_oid += RBD_SUFFIX;
  string dstname_str = dstname;
  string imgname_str = imgname;
  uint64_t ver;
  bufferlist header;
  int r = read_header_bl(pp.md, md_oid, header, &ver);
  if (r < 0) {
    cerr << "error reading header: " << md_oid << ": " << strerror(-r) << std::endl;
    return r;
  }
  r = rados.stat(pp.md, dst_md_oid, NULL, NULL);
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
    rados.remove(pp.md, dst_md_oid);
    cerr << "can't add " << dst_md_oid << " to directory" << std::endl;
    return r;
  }
  r = tmap_rm(pp, imgname_str);
  if (r < 0)
    cerr << "warning: couldn't remove old entry from directory (" << imgname_str << ")" << std::endl;

  r = rados.remove(pp.md, md_oid);
  if (r < 0)
    cerr << "warning: couldn't remove old metadata" << std::endl;
}
*/

static int do_show_info(const char *poolname, const char *imgname)
{
  librbd::image_info_t info;
  int r = rbd.stat_image(poolname, imgname, info);
  if (r < 0)
    return r;

  print_info(imgname, info);
  return 0;
}

 static int do_delete(const char *poolname, const char *imgname)
{
  int r = rbd.remove_image(poolname, imgname);
  if (r < 0)
    return r;

  return 0;
}

static int do_resize(const char *poolname, const char *imgname, size_t size)
{
  int r = rbd.resize_image(poolname, imgname, size);
  if (r < 0)
    return r;

  return 0;
}

static int do_list_snaps(const char *poolname, const char *imgname)
{
  std::vector<librbd::snap_info_t> snaps;
  int r = rbd.list_snaps(poolname, imgname, snaps);
  if (r < 0)
    return r;

  for (std::vector<librbd::snap_info_t>::iterator i = snaps.begin(); i != snaps.end(); ++i) {
    cout << i->id << '\t' << i->name << '\t' << i->size << std::endl;
  }
  return 0;
}

static int do_add_snap(const char *poolname, const char *imgname, const char *snapname)
{
  int r = rbd.create_snap(poolname, imgname, snapname);
  if (r < 0)
    return r;

  return 0;
}

static int do_remove_snap(const char *poolname, const char *imgname, const char *snapname)
{
  int r = rbd.remove_snap(poolname, imgname, snapname);
  if (r < 0)
    return r;

  return 0;
}

static int do_rollback_snap(const char *poolname, const char *imgname, const char *snapname)
{
  int r = rbd.rollback_snap(poolname, imgname, snapname);
  if (r < 0)
    return r;

  return 0;
}
/*
static int do_export(librbd::pools_t& pp, string& md_oid, const char *path)
{
  struct rbd_obj_header_ondisk header;
  int64_t ret;
  int r;

  ret = read_header(pp.md, md_oid, &header, NULL);
  if (ret < 0)
    return ret;

  uint64_t numseg = get_max_block(&header);
  uint64_t block_size = get_block_size(&header);
  int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
  uint64_t pos = 0;

  if (fd < 0)
    return -errno;

  for (uint64_t i = 0; i < numseg; i++) {
    bufferlist bl;
    string oid = get_block_oid(&header, i);
    map<off_t, size_t> m;
    map<off_t, size_t>::iterator iter;
    off_t bl_ofs = 0;
    r = rados.sparse_read(pp.data, oid, 0, block_size, m, bl);
    if (r < 0 && r == -ENOENT)
      r = 0;
    if (r < 0) {
      ret = r;
      goto done;
    }

    for (iter = m.begin(); iter != m.end(); ++iter) {
      off_t extent_ofs = iter->first;
      size_t extent_len = iter->second;
      ret = lseek64(fd, pos + extent_ofs, SEEK_SET);
      if (ret < 0) {
	ret = -errno;
	cerr << "could not seek to pos " << pos << std::endl;
	goto done;
      }
      if (bl_ofs + extent_len > bl.length()) {
        cerr << "data error!" << std::endl;
        return -EIO;
      }
      ret = write(fd, bl.c_str() + bl_ofs, extent_len);
      if (ret < 0)
        goto done;
      bl_ofs += extent_len;
    }

    pos += block_size;
  }
  r = ftruncate(fd, header.image_size);
  if (r < 0)
    ret = -errno;

  ret = 0;

done:
  close(fd);
  return ret;
}
*/
static const char *imgname_from_path(const char *path)
{
  const char *imgname;

  imgname = strrchr(path, '/');
  if (imgname)
    imgname++;
  else
    imgname = path;

  return imgname;
}

static void update_snap_name(char *imgname, char **snap)
{
  char *s;

  s = strrchr(imgname, '@');
  if (!s)
    return;

  *s = '\0';

  if (!snap)
    return;

  s++;
  if (*s)
    *snap = s;
}

static void set_pool_image_name(const char *orig_pool, const char *orig_img,
                                char **new_pool, char **new_img, char **snap)
{
  const char *sep;

  if (orig_pool)
    return;

  if (!orig_img) {
    *new_pool = strdup("rbd");
    return;
  }

  sep = strchr(orig_img, '/');
  if (!sep) {
    *new_pool= strdup("rbd");
    *new_img = strdup(orig_img);
    goto done_img;
  }

  *new_pool =  strdup(orig_img);
  sep = strchr(*new_pool, '/');
  assert (sep);

  *(char *)sep = '\0';
  *new_img = strdup(sep + 1);

done_img:
  update_snap_name(*new_img, snap);
}
#if 0
static int do_import(librados::pool_t pool, const char *imgname, int order, const char *path)
{
  int fd = open(path, O_RDONLY);
  int r;
  uint64_t size;
  uint64_t block_size;
  struct stat stat_buf;
  string md_oid;
  struct fiemap *fiemap;

  if (fd < 0) {
    r = -errno;
    cerr << "error opening " << path << std::endl;
    return r;
  }

  r = fstat(fd, &stat_buf);
  if (r < 0) {
    r = -errno;
    cerr << "stat error " << path << std::endl;
    return r;
  }
  size = (uint64_t)stat_buf.st_size;

  assert(imgname);

  md_oid = imgname;
  md_oid += RBD_SUFFIX;

  //  r = do_create(pool, md_oid, imgname, size, &order);
  if (r < 0) {
    cerr << "image creation failed" << std::endl;
    return r;
  }

  block_size = 1 << order;

  /* FIXME: use fiemap to read sparse files */
  struct rbd_obj_header_ondisk header;
  r = read_header(pool, md_oid, &header, NULL);
  if (r < 0) {
    cerr << "error reading header" << std::endl;
    return r;
  }

  fiemap = read_fiemap(fd);
  if (!fiemap) {
    fiemap = (struct fiemap *)malloc(sizeof(struct fiemap) +  sizeof(struct fiemap_extent));
    if (!fiemap) {
      cerr << "Failed to allocate fiemap, not enough memory." << std::endl;
      return -ENOMEM;
    }
    fiemap->fm_start = 0;
    fiemap->fm_length = size;
    fiemap->fm_flags = 0;
    fiemap->fm_extent_count = 1;
    fiemap->fm_mapped_extents = 1;
    fiemap->fm_extents[0].fe_logical = 0;
    fiemap->fm_extents[0].fe_physical = 0;
    fiemap->fm_extents[0].fe_length = size;
    fiemap->fm_extents[0].fe_flags = 0;
  }

  uint64_t extent = 0;

  while (extent < fiemap->fm_mapped_extents) {
    uint64_t start_block, end_block;
    off_t file_pos, end_ofs;
    size_t extent_len = 0;

    file_pos = fiemap->fm_extents[extent].fe_logical; /* position within the file we're reading */
    start_block = get_block_num(&header, file_pos); /* starting block */

    do { /* try to merge consecutive extents */
#define LARGE_ENOUGH_EXTENT (32 * 1024 * 1024)
      if (extent_len &&
          extent_len + fiemap->fm_extents[extent].fe_length > LARGE_ENOUGH_EXTENT)
        break; /* don't try to merge if we're big enough */

      extent_len += fiemap->fm_extents[extent].fe_length;  /* length of current extent */
      end_ofs = MIN(size, file_pos + extent_len);

      end_block = get_block_num(&header, end_ofs - 1); /* ending block */

      extent++;
      if (extent == fiemap->fm_mapped_extents)
        break;
      
    } while (end_ofs == (off_t)fiemap->fm_extents[extent].fe_logical);

    cerr << "rbd import file_pos=" << file_pos << " extent_len=" << extent_len << std::endl;
    for (uint64_t i = start_block; i <= end_block; i++) {
      uint64_t ofs_in_block = get_pos_in_block(&header, file_pos); /* the offset within the starting block */
      uint64_t seg_size = bounded_pos_in_block(&header, end_ofs, i) - ofs_in_block;
      uint64_t seg_left = seg_size;

      cerr << "i=" << i << " (" << start_block << "/" << end_block << ") "
           << "seg_size=" << seg_size << " seg_left=" << seg_left << std::endl;
      while (seg_left) {
        uint64_t len = seg_left;
        bufferptr p(len);
        cerr << "reading " << len << " bytes at offset " << file_pos << std::endl;
	{
	  ssize_t rval = TEMP_FAILURE_RETRY(::pread(fd, p.c_str(), len, file_pos));
	  if (rval < 0) {
	    r = -errno;
	    cerr << "failed to read file: " << cpp_strerror(r) << std::endl;
	    goto done;
	  }
	  len = rval;
	}
        bufferlist bl;
        bl.append(p);
        string oid = get_block_oid(&header, i);
        cerr << "writing " << len << " bytes at offset " << ofs_in_block << " at block " << oid << std::endl;
        r = rados.write(pool, oid, ofs_in_block, bl, len);
        if (r < 0) {
          cerr << "error writing to image block" << std::endl;
          goto done;
        }

        seg_left -= len;
        file_pos += len;
        ofs_in_block += len;
      }
    }
  }

  r = 0;

done:
  free(fiemap);

  return r;
}

static int do_copy(librbd::pools_t& pp, const char *imgname, const char *destname)
{
  struct rbd_obj_header_ondisk header, dest_header;
  int64_t ret;
  int r;
  string md_oid, dest_md_oid;

  md_oid = imgname;
  md_oid += RBD_SUFFIX;

  dest_md_oid = destname;
  dest_md_oid += RBD_SUFFIX;

  ret = read_header(pp.md, md_oid, &header, NULL);
  if (ret < 0)
    return ret;

  uint64_t numseg = get_max_block(&header);
  uint64_t block_size = get_block_size(&header);
  int order = header.options.order;

  r = do_create(pp.dest, dest_md_oid, destname, header.image_size, &order);
  if (r < 0) {
    cerr << "header creation failed" << std::endl;
    return r;
  }

  ret = read_header(pp.dest, dest_md_oid, &dest_header, NULL);
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
    r = rados.sparse_read(pp.data, oid, 0, block_size, m, bl);
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
      r = rados.write(pp.dest, dest_oid, extent_ofs, wrbl, extent_len);
      if (r < 0)
        goto done;
    }
  }
  r = 0;

done:
  return r;
}

class RbdWatchCtx : public librados::Rados::WatchCtx {
  string name;
public:
  RbdWatchCtx(const char *imgname) : name(imgname) {}
  virtual ~RbdWatchCtx() {}
  virtual void notify(uint8_t opcode, uint64_t ver) {
    cout << name << " got notification opcode=" << (int)opcode << " ver=" << ver << std::endl;
  }
};

static int do_watch(librbd::pools_t& pp, const char *imgname)
{
  string md_oid, dest_md_oid;
  uint64_t cookie;
  RbdWatchCtx ctx(imgname);

  md_oid = imgname;
  md_oid += RBD_SUFFIX;

  librados::Rados rados;
  int r = rados.watch(pp.md, md_oid, 0, &cookie, &ctx);
  if (r < 0) {
    cerr << "watch failed" << std::endl;
    return r;
  }

  cout << "press enter to exit..." << std::endl;
  getchar();
  return 0;
}
#endif
static void err_exit()
{
  rbd.shutdown();
  exit(1);
}

enum {
  OPT_NO_CMD = 0,
  OPT_LIST,
  OPT_INFO,
  OPT_CREATE,
  OPT_RESIZE,
  OPT_RM,
  OPT_EXPORT,
  OPT_IMPORT,
  OPT_COPY,
  OPT_RENAME,
  OPT_SNAP_CREATE,
  OPT_SNAP_ROLLBACK,
  OPT_SNAP_REMOVE,
  OPT_SNAP_LIST,
  OPT_WATCH,
};

static int get_cmd(const char *cmd, bool *snapcmd)
{
  if (strcmp(cmd, "snap") == 0) {
    if (*snapcmd)
      return -EINVAL;
    *snapcmd = true;
    return 0;
  }

  if (!*snapcmd) {
    if (strcmp(cmd, "ls") == 0 ||
        strcmp(cmd, "list") == 0)
      return OPT_LIST;
    if (strcmp(cmd, "info") == 0)
      return OPT_INFO;
    if (strcmp(cmd, "create") == 0)
      return OPT_CREATE;
    if (strcmp(cmd, "resize") == 0)
      return OPT_RESIZE;
    if (strcmp(cmd, "rm") == 0)
      return OPT_RM;
    if (strcmp(cmd, "export") == 0)
      return OPT_EXPORT;
    if (strcmp(cmd, "import") == 0)
      return OPT_IMPORT;
    if (strcmp(cmd, "copy") == 0 ||
        strcmp(cmd, "cp") == 0)
      return OPT_COPY;
    if (strcmp(cmd, "rename") == 0 ||
        strcmp(cmd, "mv") == 0)
      return OPT_RENAME;
    if (strcmp(cmd, "watch") == 0)
      return OPT_WATCH;
  } else {
    if (strcmp(cmd, "create") == 0||
        strcmp(cmd, "add") == 0)
      return OPT_SNAP_CREATE;
    if (strcmp(cmd, "rollback") == 0||
        strcmp(cmd, "revert") == 0)
      return OPT_SNAP_ROLLBACK;
    if (strcmp(cmd, "remove") == 0||
        strcmp(cmd, "rm") == 0)
      return OPT_SNAP_REMOVE;
    if (strcmp(cmd, "ls") == 0||
        strcmp(cmd, "list") == 0)
      return OPT_SNAP_LIST;
  }

  return -EINVAL;
}

static void set_conf_param(const char *param, const char **var1, const char **var2)
{
  if (!*var1)
    *var1 = param;
  else if (var2 && !*var2)
    *var2 = param;
  else
    usage_exit();
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  DEFINE_CONF_VARS(usage_exit);
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  librbd::pools_t pp = { 0, 0, 0 };
  vector<librados::snap_t> snaps;
  ::SnapContext snapc;

  int opt_cmd = OPT_NO_CMD;
  common_set_defaults(false);
  common_init(args, "rbd", true);
  set_foreground_logging();

  const char *poolname = NULL;
  uint64_t size = 0;
  int order = 0;
  const char *imgname = NULL, *snapname = NULL, *destname = NULL, *dest_poolname = NULL, *path = NULL;
  bool is_snap_cmd = false;
  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("pool", 'p')) {
      CONF_SAFE_SET_ARG_VAL(&poolname, OPT_STR);
    } else if (CONF_ARG_EQ("dest-pool", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&dest_poolname, OPT_STR);
    } else if (CONF_ARG_EQ("snap", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&snapname, OPT_STR);
    } else if (CONF_ARG_EQ("image", 'i')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
    } else if (CONF_ARG_EQ("size", 's')) {
      CONF_SAFE_SET_ARG_VAL(&size, OPT_LONGLONG);
      size <<= 20; // MB -> bytes
    } else if (CONF_ARG_EQ("order", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&order, OPT_INT);
    } else if (CONF_ARG_EQ("path", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&path, OPT_STR);
    } else if (CONF_ARG_EQ("dest", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&destname, OPT_STR);
    } else {
      if (!opt_cmd) {
        opt_cmd = get_cmd(CONF_VAL, &is_snap_cmd);
        if (opt_cmd < 0) {
          cerr << "invalid command: " << CONF_VAL << std::endl;
          usage_exit();
        }
      } else {
        switch (opt_cmd) {
          case OPT_LIST:
            set_conf_param(CONF_VAL, &poolname, NULL);
            break;
          case OPT_INFO:
          case OPT_CREATE:
          case OPT_RESIZE:
          case OPT_RM:
          case OPT_SNAP_CREATE:
          case OPT_SNAP_ROLLBACK:
          case OPT_SNAP_REMOVE:
          case OPT_SNAP_LIST:
          case OPT_WATCH:
            set_conf_param(CONF_VAL, &imgname, NULL);
            break;
          case OPT_EXPORT:
            set_conf_param(CONF_VAL, &imgname, &path);
            break;
          case OPT_IMPORT:
            set_conf_param(CONF_VAL, &path, &destname);
            break;
          case OPT_COPY:
          case OPT_RENAME:
            set_conf_param(CONF_VAL, &imgname, &destname);
            break;
        }
      }
    }
    //  usage_exit();
  }
  if (!opt_cmd)
    usage_exit();

  if (opt_cmd == OPT_EXPORT && !imgname) {
    cerr << "error: image name was not specified" << std::endl;
    usage_exit();
  }

  if (opt_cmd == OPT_IMPORT && !path) {
    cerr << "error: path was not specified" << std::endl;
    usage_exit();
  }

  if (opt_cmd == OPT_IMPORT && !destname)
    destname = imgname_from_path(path);

  if (opt_cmd != OPT_LIST && opt_cmd != OPT_IMPORT && !imgname) {
    cerr << "error: image name was not specified" << std::endl;
    usage_exit();
  }

  set_pool_image_name(poolname, imgname, (char **)&poolname, (char **)&imgname, (char **)&snapname);
  set_pool_image_name(dest_poolname, destname, (char **)&dest_poolname, (char **)&destname, NULL);

  if ((opt_cmd == OPT_SNAP_CREATE || opt_cmd == OPT_SNAP_ROLLBACK ||
       opt_cmd == OPT_SNAP_REMOVE) && !snapname) {
    cerr << "error: snap name was not specified" << std::endl;
    usage_exit();
  }

  if (!dest_poolname)
    dest_poolname = poolname;

  if (opt_cmd == OPT_EXPORT && !path)
    path = imgname;

  if (opt_cmd == OPT_COPY && !destname ) {
    cerr << "error: destination image name was not specified" << std::endl;
    usage_exit();
  }

  if (rbd.initialize(argc, argv) < 0) {
    cerr << "error: couldn't initialize rbd!" << std::endl;
    exit(1);
  }

  int r;
  /*
  if (snapname) {
    r = do_get_snapc(pp, md_oid, snapname, snapc, snaps, snapid);
    if (r == -ENOENT) {
      if (opt_cmd == OPT_SNAP_CREATE)
        r = 0;
      else
        cerr << "snapshot not found: " << snapname << std::endl;
    }
    if (r < 0) {
      cerr << "error searching for snapshot: " << strerror(-r) << std::endl;
      err_exit(pp);
    }

    r = rados.set_snap_context(pool, snapc.seq, snaps);
    if (r < 0) {
      cerr << "error setting snapshot context: " << strerror(-r) << std::endl;
      err_exit(pp);
    }

    rados.set_snap(pool, snapid);
  }

  if (opt_cmd == OPT_COPY || opt_cmd == OPT_IMPORT) {
    r = rados.open_pool(dest_poolname, &dest_pool);
    if (r < 0) {
      cerr << "error opening pool " << dest_poolname << " (err=" << r << ")" << std::endl;
      err_exit(pp);
    }
    pp.dest = dest_pool;
  }
  */
  switch (opt_cmd) {
  case OPT_LIST:
    r = do_list(poolname);
    if (r < 0) {
      switch (r) {
      case -ENOENT:
        cerr << "pool " << poolname << " doesn't contain rbd images" << std::endl;
        break;
      default:
        cerr << "error: " << strerror(-r) << std::endl;
      }
      err_exit();
    }
    break;

  case OPT_CREATE:
    if (!size) {
      cerr << "must specify size in MB to create an rbd image" << std::endl;
      usage();
      err_exit();
    }
    if (order && (order < 12 || order > 25)) {
      cerr << "order must be between 12 (4 KB) and 25 (32 MB)" << std::endl;
      usage();
      err_exit();
    }
    r = do_create(poolname, imgname, size);
    if (r < 0) {
      cerr << "create error: " << strerror(-r) << std::endl;
      err_exit();
    }
    break;
/*
  case OPT_RENAME:
    r = do_rename(poolname, imgname, destname);
    if (r < 0) {
      cerr << "rename error: " << strerror(-r) << std::endl;
      err_exit();
    }
    break;
*/
  case OPT_INFO:
    r = do_show_info(poolname, imgname);
    if (r < 0) {
      cerr << "error: " << strerror(-r) << std::endl;
      err_exit();
    }
    break;

  case OPT_RM:
    r = do_delete(poolname, imgname);
    if (r < 0) {
      cerr << "delete error: " << strerror(-r) << std::endl;
      err_exit();
    }
    break;

  case OPT_RESIZE:
    r = do_resize(poolname, imgname, size);
    if (r < 0) {
      cerr << "resize error: " << strerror(-r) << std::endl;
      err_exit();
    }
    break;

  case OPT_SNAP_LIST:
    if (!imgname) {
      usage();
      err_exit();
    }
    r = do_list_snaps(poolname, imgname);
    if (r < 0) {
      cerr << "failed to list snapshots: " << strerror(-r) << std::endl;
      err_exit();
    }
    break;

  case OPT_SNAP_CREATE:
    if (!imgname || !snapname) {
      usage();
      err_exit();
    }
    r = do_add_snap(poolname, imgname, snapname);
    if (r < 0) {
      cerr << "failed to create snapshot: " << strerror(-r) << std::endl;
      err_exit();
    }
    break;

  case OPT_SNAP_ROLLBACK:
    if (!imgname) {
      usage();
      err_exit();
    }
    r = do_rollback_snap(poolname, imgname, snapname);
    if (r < 0) {
      cerr << "rollback failed: " << strerror(-r) << std::endl;
      err_exit();
    }
    break;

  case OPT_SNAP_REMOVE:
    if (!imgname) {
      usage();
      err_exit();
    }
    r = do_remove_snap(poolname, imgname, snapname);
    if (r < 0) {
      cerr << "rollback failed: " << strerror(-r) << std::endl;
      err_exit();
    }
    break;
    /*
  case OPT_EXPORT:
    if (!path) {
      cerr << "pathname should be specified" << std::endl;
      err_exit(pp);
    }
    r = do_export(pp, md_oid, path);
    if (r < 0) {
      cerr << "export error: " << strerror(-r) << std::endl;
      err_exit(pp);
    }
    break;

  case OPT_IMPORT:
     if (!path) {
      cerr << "pathname should be specified" << std::endl;
      err_exit(pp);
    }
    r = do_import(pp.dest, destname, order, path);
    if (r < 0) {
      cerr << "import failed: " << strerror(-r) << std::endl;
      err_exit(pp);
    }
    break;

  case OPT_COPY:
    r = do_copy(pp, imgname, destname);
    if (r < 0) {
      cerr << "copy failed: " << strerror(-r) << std::endl;
      err_exit(pp);
    }
    break;

  case OPT_WATCH:
    r = do_watch(pp, imgname);
    if (r < 0) {
      cerr << "watch failed: " << strerror(-r) << std::endl;
      err_exit(pp);
    }
    break;
    */
  }

  /*  rados.close_pool(pool);
  rados.close_pool(md_pool);
  rados.shutdown();
  */
  rbd.shutdown();
  return 0;
}
