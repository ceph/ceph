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
#include "common/config.h"

#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "include/byteorder.h"

#include "include/intarith.h"

#include <errno.h>
#include <inttypes.h>
#include <iostream>
#include <memory>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>

#include <sys/ioctl.h>

#include "include/rbd_types.h"

#include <linux/fs.h>

#include "include/fiemap.h"

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

static void print_info(const char *imgname, librbd::image_info_t& info)
{
  cout << "rbd image '" << imgname << "':\n"
       << "\tsize " << prettybyte_t(info.size) << " in "
       << info.num_objs << " objects"
       << std::endl
       << "\torder " << info.order
       << " (" << prettybyte_t(info.obj_size) << " objects)"
       << std::endl
       << "\tblock_name_prefix: " << info.block_name_prefix
       << std::endl
       << "\tparent: " << info.parent_name
       << " (pool " << info.parent_pool << ")"
       << std::endl;
}

static int do_list(librbd::RBD &rbd, librados::IoCtx& io_ctx)
{
  std::vector<string> names;
  int r = rbd.list(io_ctx, names);
  if (r < 0)
    return r;

  for (std::vector<string>::const_iterator i = names.begin(); i != names.end(); i++)
    cout << *i << std::endl;
  return 0;
}

static int do_create(librbd::RBD &rbd, librados::IoCtx& io_ctx,
		     const char *imgname, size_t size, int *order)
{
  int r = rbd.create(io_ctx, imgname, size, order);
  if (r < 0)
    return r;
  return 0;
}

static int do_rename(librbd::RBD &rbd, librados::IoCtx& io_ctx,
		     const char *imgname, const char *destname)
{
  int r = rbd.rename(io_ctx, imgname, destname);
  if (r < 0)
    return r;
  return 0;
}

static int do_show_info(const char *imgname, librbd::Image& image)
{
  librbd::image_info_t info;
  int r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  print_info(imgname, info);
  return 0;
}

 static int do_delete(librbd::RBD &rbd, librados::IoCtx& io_ctx, const char *imgname)
{
  int r = rbd.remove(io_ctx, imgname);
  if (r < 0)
    return r;

  return 0;
}

static int do_resize(librbd::Image& image, size_t size)
{
  int r = image.resize(size);
  if (r < 0)
    return r;

  return 0;
}

static int do_list_snaps(librbd::Image& image)
{
  std::vector<librbd::snap_info_t> snaps;
  int r = image.snap_list(snaps);
  if (r < 0)
    return r;

  for (std::vector<librbd::snap_info_t>::iterator i = snaps.begin(); i != snaps.end(); ++i) {
    cout << i->id << '\t' << i->name << '\t' << i->size << std::endl;
  }
  return 0;
}

static int do_add_snap(librbd::Image& image, const char *snapname)
{
  int r = image.snap_create(snapname);
  if (r < 0)
    return r;

  return 0;
}

static int do_remove_snap(librbd::Image& image, const char *snapname)
{
  int r = image.snap_remove(snapname);
  if (r < 0)
    return r;

  return 0;
}

static int do_rollback_snap(librbd::Image& image, const char *snapname)
{
  int r = image.snap_rollback(snapname);
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

  cerr << "writing " << len << " bytes at ofs " << ofs << std::endl;
  return 0;
}

static int do_export(librbd::Image& image, const char *path)
{
  int r;
  librbd::image_info_t info;
  bufferlist bl;
  int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
  if (fd < 0)
    return -errno;

  r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  r = image.read_iterate(0, info.size, export_read_cb, (void *)&fd);
  if (r < 0)
    return r;

  r = write(fd, bl.c_str(), bl.length());
  if (r < 0)
    return r;

  r = ftruncate(fd, info.size);
  if (r < 0)
    return r;

  close(fd);

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

static int do_import(librbd::RBD &rbd, librados::IoCtx& io_ctx,
		     const char *imgname, int *order, const char *path)
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

  r = do_create(rbd, io_ctx, imgname, size, order);
  if (r < 0) {
    cerr << "image creation failed" << std::endl;
    return r;
  }
  librbd::Image image;
  r = rbd.open(io_ctx, image, imgname);
  if (r < 0) {
    cerr << "failed to open image" << std::endl;
    return r;
  }
  fsync(fd); /* flush it first, otherwise extents information might not have been flushed yet */
  fiemap = read_fiemap(fd);
  if (fiemap && !fiemap->fm_mapped_extents) {
    cerr << "empty fiemap!" << std::endl;
    free(fiemap);
    fiemap = NULL;
  }
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
      end_ofs = MIN((off_t)size, file_pos + (off_t)extent_len);

      extent++;
      if (extent == fiemap->fm_mapped_extents)
        break;
      
    } while (end_ofs == (off_t)fiemap->fm_extents[extent].fe_logical);

    cerr << "rbd import file_pos=" << file_pos << " extent_len=" << extent_len << std::endl;
#define READ_BLOCK_LEN (4 * 1024 * 1024)
    uint64_t left = end_ofs - file_pos;
    while (left) {
      uint64_t cur_seg = (left < READ_BLOCK_LEN ? left : READ_BLOCK_LEN);
      while (cur_seg) {
        bufferptr p(cur_seg);
        cerr << "reading " << cur_seg << " bytes at offset " << file_pos << std::endl;
        ssize_t rval = TEMP_FAILURE_RETRY(::pread(fd, p.c_str(), cur_seg, file_pos));
        if (rval < 0) {
          r = -errno;
          cerr << "error reading file: " << cpp_strerror(r) << std::endl;
          goto done;
        }
	size_t len = rval;
        if (!len) {
          r = 0;
          goto done;
        }
        bufferlist bl;
        bl.append(p);
        librbd::RBD::AioCompletion *completion = new librbd::RBD::AioCompletion(NULL, NULL);
        if (!completion) {
          r = -ENOMEM;
          goto done;
        }
        r = image.aio_write(file_pos, len, bl, completion);
        if (r < 0)
          goto done;
	completion->wait_for_complete();
	r = completion->get_return_value();
	completion->release();
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

static int do_copy(librbd::RBD &rbd, librados::IoCtx& pp,
	   const char *imgname, librados::IoCtx& dest_pp,
	   const char *destname)
{
  int r = rbd.copy(pp, imgname, dest_pp, destname);
  if (r < 0)
    return r;
  return 0;
}

class RbdWatchCtx : public librados::WatchCtx {
  string name;
public:
  RbdWatchCtx(const char *imgname) : name(imgname) {}
  virtual ~RbdWatchCtx() {}
  virtual void notify(uint8_t opcode, uint64_t ver) {
    cout << name << " got notification opcode=" << (int)opcode << " ver=" << ver << std::endl;
  }
};

static int do_watch(librados::IoCtx& pp, const char *imgname)
{
  string md_oid, dest_md_oid;
  uint64_t cookie;
  RbdWatchCtx ctx(imgname);

  md_oid = imgname;
  md_oid += RBD_SUFFIX;

  int r = pp.watch(md_oid, 0, &cookie, &ctx);
  if (r < 0) {
    cerr << "watch failed" << std::endl;
    return r;
  }

  cout << "press enter to exit..." << std::endl;
  getchar();

  return 0;
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
  librados::Rados rados;
  librbd::RBD rbd;
  librados::IoCtx io_ctx, dest_io_ctx;
  librbd::Image image;

  vector<const char*> args;
  DEFINE_CONF_VARS(usage_exit);
  // TODO: use rados conf api
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  int opt_cmd = OPT_NO_CMD;
  common_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY);
  keyring_init(&g_conf);

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

  if (rados.init(NULL) < 0) {
    cerr << "error: couldn't initialize rados!" << std::endl;
    exit(1);
  }

  if (rados.connect() < 0) {
    cerr << "error: couldn't connect to the cluster!" << std::endl;
    exit(1);
  }

  // TODO: add conf
  int r = rados.ioctx_create(poolname, io_ctx);
  if (r < 0) {
      cerr << "error opening pool " << poolname << " (err=" << r << ")" << std::endl;
      exit(1);
  }

  if (imgname &&
      (opt_cmd == OPT_RESIZE || opt_cmd == OPT_INFO || opt_cmd == OPT_SNAP_LIST ||
       opt_cmd == OPT_SNAP_CREATE || opt_cmd == OPT_SNAP_ROLLBACK ||
       opt_cmd == OPT_SNAP_REMOVE || opt_cmd == OPT_EXPORT || opt_cmd == OPT_WATCH)) {
    r = rbd.open(io_ctx, image, imgname);
    if (r < 0) {
      cerr << "error opening image " << imgname << ": " << strerror(r) << std::endl;
      exit(1);
    }
  }

  if (snapname) {
    r = image.snap_set(snapname);
    if (r < 0 && !(r == -ENOENT && opt_cmd == OPT_SNAP_CREATE)) {
      cerr << "error setting snapshot context: " << strerror(-r) << std::endl;
      exit(1);
    }
  }

  if (opt_cmd == OPT_COPY || opt_cmd == OPT_IMPORT) {
    r = rados.ioctx_create(dest_poolname, dest_io_ctx);
    if (r < 0) {
      cerr << "error opening pool " << dest_poolname << " (err=" << r << ")" << std::endl;
      exit(1);
    }
  }

  switch (opt_cmd) {
  case OPT_LIST:
    r = do_list(rbd, io_ctx);
    if (r < 0) {
      switch (r) {
      case -ENOENT:
        cerr << "pool " << poolname << " doesn't contain rbd images" << std::endl;
        break;
      default:
        cerr << "error: " << strerror(-r) << std::endl;
      }
      exit(1);
    }
    break;

  case OPT_CREATE:
    if (!size) {
      cerr << "must specify size in MB to create an rbd image" << std::endl;
      usage();
      exit(1);
    }
    if (order && (order < 12 || order > 25)) {
      cerr << "order must be between 12 (4 KB) and 25 (32 MB)" << std::endl;
      usage();
      exit(1);
    }
    r = do_create(rbd, io_ctx, imgname, size, &order);
    if (r < 0) {
      cerr << "create error: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_RENAME:
    r = do_rename(rbd, io_ctx, imgname, destname);
    if (r < 0) {
      cerr << "rename error: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_INFO:
    r = do_show_info(imgname, image);
    if (r < 0) {
      cerr << "error: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_RM:
    r = do_delete(rbd, io_ctx, imgname);
    if (r < 0) {
      cerr << "delete error: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_RESIZE:
    r = do_resize(image, size);
    if (r < 0) {
      cerr << "resize error: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_SNAP_LIST:
    if (!imgname) {
      usage();
      exit(1);
    }
    r = do_list_snaps(image);
    if (r < 0) {
      cerr << "failed to list snapshots: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_SNAP_CREATE:
    if (!imgname || !snapname) {
      usage();
      exit(1);
    }
    r = do_add_snap(image, snapname);
    if (r < 0) {
      cerr << "failed to create snapshot: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_SNAP_ROLLBACK:
    if (!imgname) {
      usage();
      exit(1);
    }
    r = do_rollback_snap(image, snapname);
    if (r < 0) {
      cerr << "rollback failed: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_SNAP_REMOVE:
    if (!imgname) {
      usage();
      exit(1);
    }
    r = do_remove_snap(image, snapname);
    if (r < 0) {
      cerr << "rollback failed: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_EXPORT:
    if (!path) {
      cerr << "pathname should be specified" << std::endl;
      exit(1);
    }
    r = do_export(image, path);
    if (r < 0) {
      cerr << "export error: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_IMPORT:
     if (!path) {
      cerr << "pathname should be specified" << std::endl;
      exit(1);
    }
    r = do_import(rbd, dest_io_ctx, destname, &order, path);
    if (r < 0) {
      cerr << "import failed: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_COPY:
    r = do_copy(rbd, io_ctx, imgname, dest_io_ctx, destname);
    if (r < 0) {
      cerr << "copy failed: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_WATCH:
    r = do_watch(io_ctx, imgname);
    if (r < 0) {
      cerr << "watch failed: " << strerror(-r) << std::endl;
      exit(1);
    }
    break;
  }

  return 0;
}
