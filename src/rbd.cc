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
static int do_list(librbd::pool_t pool)
{
  std::vector<string> names;
  int r = rbd.list(pool, names);
  if (r < 0)
    return r;

  for (std::vector<string>::const_iterator i = names.begin(); i != names.end(); i++)
    cout << *i << std::endl;
  return 0;
}

static int do_create(librbd::pool_t pool, const char *imgname, size_t size, int *order)
{
  int r = rbd.create(pool, imgname, size, order);
  if (r < 0)
    return r;
  return 0;
}

static int do_rename(librbd::pool_t pool, const char *imgname, const char *destname)
{
  int r = rbd.rename(pool, imgname, destname);
  if (r < 0)
    return r;
  return 0;
}

static int do_show_info(const char *imgname, librbd::image_t image)
{
  librbd::image_info_t info;
  int r = rbd.stat(image, info);
  if (r < 0)
    return r;

  print_info(imgname, info);
  return 0;
}

 static int do_delete(librbd::pool_t pool, const char *imgname)
{
  int r = rbd.remove(pool, imgname);
  if (r < 0)
    return r;

  return 0;
}

static int do_resize(librbd::image_t image, size_t size)
{
  int r = rbd.resize(image, size);
  if (r < 0)
    return r;

  return 0;
}

static int do_list_snaps(librbd::image_t image)
{
  std::vector<librbd::snap_info_t> snaps;
  int r = rbd.list_snaps(image, snaps);
  if (r < 0)
    return r;

  for (std::vector<librbd::snap_info_t>::iterator i = snaps.begin(); i != snaps.end(); ++i) {
    cout << i->id << '\t' << i->name << '\t' << i->size << std::endl;
  }
  return 0;
}

static int do_add_snap(librbd::image_t image, const char *snapname)
{
  int r = rbd.create_snap(image, snapname);
  if (r < 0)
    return r;

  return 0;
}

static int do_remove_snap(librbd::image_t image, const char *snapname)
{
  int r = rbd.remove_snap(image, snapname);
  if (r < 0)
    return r;

  return 0;
}

static int do_rollback_snap(librbd::image_t image, const char *snapname)
{
  int r = rbd.rollback_snap(image, snapname);
  if (r < 0)
    return r;

  return 0;
}

static int export_read_cb(off_t ofs, size_t len, const char *buf, void *arg)
{
  int ret;
  int fd = *(int *)arg;

  if (!buf) /* a hole */
    return 0;

  ret = lseek64(fd, ofs, SEEK_SET);
  if (ret < 0)
    return -errno;
  ret = write(fd, buf, len);
  if (ret < 0)
    return -errno;
  return 0;
}

static int do_export(librbd::image_t image, const char *path)
{
  int r;
  librbd::image_info_t info;
  int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
  if (fd < 0)
    return -errno;

  r = rbd.stat(image, info);
  if (r < 0)
    return r;

  r = rbd.read_iterate(image, 0, info.size, export_read_cb, (void *)&fd);

  close(fd);
  if (r < 0)
    return r;

  r = ftruncate(fd, info.size);
  if (r < 0)
    return r;

  return 0;
}

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

static int do_import(librados::pool_t pool, const char *imgname, int *order, const char *path)
{
  int fd = open(path, O_RDONLY);
  int r;
  uint64_t size;
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

  r = do_create(pool, imgname, size, order);
  if (r < 0) {
    cerr << "image creation failed" << std::endl;
    return r;
  }
  librbd::image_t image;
  r = rbd.open_image(pool, imgname, &image, NULL);
  if (r < 0) {
    cerr << "failed to open image" << std::endl;
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
    off_t file_pos, end_ofs;
    size_t extent_len = 0;

    file_pos = fiemap->fm_extents[extent].fe_logical; /* position within the file we're reading */

    do { /* try to merge consecutive extents */
#define LARGE_ENOUGH_EXTENT (32 * 1024 * 1024)
      if (extent_len &&
          extent_len + fiemap->fm_extents[extent].fe_length > LARGE_ENOUGH_EXTENT)
        break; /* don't try to merge if we're big enough */

      extent_len += fiemap->fm_extents[extent].fe_length;  /* length of current extent */
      end_ofs = MIN(size, file_pos + extent_len);

      extent++;
      if (extent == fiemap->fm_mapped_extents)
        break;
      
    } while (end_ofs == (off_t)fiemap->fm_extents[extent].fe_logical);

    cerr << "rbd import file_pos=" << file_pos << " extent_len=" << extent_len << std::endl;
#define READ_BLOCK_LEN (4 * 1024 * 1024)
    uint64_t left = extent_len;
    while (left) {
      uint64_t cur_seg = (left < READ_BLOCK_LEN ? left : READ_BLOCK_LEN);
      while (cur_seg) {
        bufferptr p(cur_seg);
        cerr << "reading " << cur_seg << " bytes at offset " << file_pos << std::endl;
        ssize_t rval = TEMP_FAILURE_RETRY(::pread(fd, p.c_str(), cur_seg, file_pos));
        if (ret < 0) {
          r = -errno;
          cerr << "error reading file: " << cpp_strerror(r) << std::endl;
          goto done;
        }
	len = rval
        bufferlist bl;
        bl.append(p);

        r = rbd.write(image, file_pos, len, bl);
        if (r < 0) {
          cerr << "error writing to image block" << std::endl;
          goto done;
        }

        file_pos += len;
        cur_seg -= len;
        left -= len;
      }
    }
  }

  r = 0;

done:
  free(fiemap);

  return r;
}

static int do_copy(librbd::pool_t& pp, const char *imgname, librbd::pool_t& dest_pp, const char *destname)
{
  int r = rbd.copy(pp, imgname, dest_pp, destname);
  if (r < 0)
    return r;
  return 0;
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

static int do_watch(librbd::pool_t& pp, const char *imgname)
{
  string md_oid, dest_md_oid;
  uint64_t cookie;
  RbdWatchCtx ctx(imgname);
  librados::pool_t md_pool;

  rbd.get_rados_pools(pp, &md_pool, NULL);

  md_oid = imgname;
  md_oid += RBD_SUFFIX;

  librados::Rados rados;
  int r = rados.watch(md_pool, md_oid, 0, &cookie, &ctx);
  if (r < 0) {
    cerr << "watch failed" << std::endl;
    return r;
  }

  cout << "press enter to exit..." << std::endl;
  getchar();
  return 0;
}

static void err_exit(librbd::pool_t pool, librbd::image_t image = NULL)
{
  if (image)
    rbd.close_image(image);
  rbd.close_pool(pool);
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

  librbd::pool_t pool, dest_pool;
  librbd::image_t image = NULL;

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

  int r = rbd.open_pool(poolname, &pool);
  if (r < 0) {
      cerr << "error opening pool " << poolname << " (err=" << r << ")" << std::endl;
      err_exit(pool);
  }

  if (imgname &&
      (opt_cmd == OPT_RESIZE || opt_cmd == OPT_INFO || opt_cmd == OPT_SNAP_LIST ||
       opt_cmd == OPT_SNAP_CREATE || opt_cmd == OPT_SNAP_ROLLBACK ||
       opt_cmd == OPT_SNAP_REMOVE || opt_cmd == OPT_EXPORT)) {
    r = rbd.open_image(pool, imgname, &image, NULL);
    if (r < 0) {
      cerr << "error opening image " << imgname << " (err=" << r << ")" << std::endl;
      err_exit(pool);
    }
  }

  if (snapname) {
    r = rbd.set_snap(image, snapname);
    if (r < 0 && !(r == -ENOENT && opt_cmd == OPT_SNAP_CREATE)) {
      cerr << "error setting snapshot context: " << strerror(-r) << std::endl;
      err_exit(pool, image);
    }
  }

  if (opt_cmd == OPT_COPY || opt_cmd == OPT_IMPORT) {
    r = rbd.open_pool(dest_poolname, &dest_pool);
    if (r < 0) {
      cerr << "error opening pool " << dest_poolname << " (err=" << r << ")" << std::endl;
      err_exit(pool);
    }
  }

  switch (opt_cmd) {
  case OPT_LIST:
    r = do_list(pool);
    if (r < 0) {
      switch (r) {
      case -ENOENT:
        cerr << "pool " << poolname << " doesn't contain rbd images" << std::endl;
        break;
      default:
        cerr << "error: " << strerror(-r) << std::endl;
      }
      err_exit(pool);
    }
    break;

  case OPT_CREATE:
    if (!size) {
      cerr << "must specify size in MB to create an rbd image" << std::endl;
      usage();
      err_exit(pool);
    }
    if (order && (order < 12 || order > 25)) {
      cerr << "order must be between 12 (4 KB) and 25 (32 MB)" << std::endl;
      usage();
      err_exit(pool);
    }
    r = do_create(pool, imgname, size, &order);
    if (r < 0) {
      cerr << "create error: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    break;

  case OPT_RENAME:
    r = do_rename(pool, imgname, destname);
    if (r < 0) {
      cerr << "rename error: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    break;

  case OPT_INFO:
    r = do_show_info(imgname, image);
    if (r < 0) {
      cerr << "error: " << strerror(-r) << std::endl;
      err_exit(pool, image);
    }
    break;

  case OPT_RM:
    r = do_delete(pool, imgname);
    if (r < 0) {
      cerr << "delete error: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    break;

  case OPT_RESIZE:
    r = do_resize(image, size);
    if (r < 0) {
      cerr << "resize error: " << strerror(-r) << std::endl;
      err_exit(pool, image);
    }
    break;

  case OPT_SNAP_LIST:
    if (!imgname) {
      usage();
      err_exit(pool);
    }
    r = do_list_snaps(image);
    if (r < 0) {
      cerr << "failed to list snapshots: " << strerror(-r) << std::endl;
      err_exit(pool, image);
    }
    break;

  case OPT_SNAP_CREATE:
    if (!imgname || !snapname) {
      usage();
      err_exit(pool, image);
    }
    r = do_add_snap(image, snapname);
    if (r < 0) {
      cerr << "failed to create snapshot: " << strerror(-r) << std::endl;
      err_exit(pool, image);
    }
    break;

  case OPT_SNAP_ROLLBACK:
    if (!imgname) {
      usage();
      err_exit(pool);
    }
    r = do_rollback_snap(image, snapname);
    if (r < 0) {
      cerr << "rollback failed: " << strerror(-r) << std::endl;
      err_exit(pool, image);
    }
    break;

  case OPT_SNAP_REMOVE:
    if (!imgname) {
      usage();
      err_exit(pool);
    }
    r = do_remove_snap(image, snapname);
    if (r < 0) {
      cerr << "rollback failed: " << strerror(-r) << std::endl;
      err_exit(pool, image);
    }
    break;

  case OPT_EXPORT:
    if (!path) {
      cerr << "pathname should be specified" << std::endl;
      err_exit(pool);
    }
    r = do_export(image, path);
    if (r < 0) {
      cerr << "export error: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    break;

  case OPT_IMPORT:
     if (!path) {
      cerr << "pathname should be specified" << std::endl;
      err_exit(pool);
    }
    r = do_import(dest_pool, destname, &order, path);
    if (r < 0) {
      cerr << "import failed: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    break;

  case OPT_COPY:
    r = do_copy(pool, imgname, dest_pool, destname);
    if (r < 0) {
      cerr << "copy failed: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    break;

  case OPT_WATCH:
    r = do_watch(pool, imgname);
    if (r < 0) {
      cerr << "watch failed: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    break;
  }

  if (image)
    rbd.close_image(image);

  rbd.close_pool(pool);
  rbd.shutdown();
  return 0;
}
