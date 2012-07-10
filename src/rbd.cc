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

#include "mon/MonClient.h"
#include "mon/MonMap.h"
#include "common/config.h"

#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/safe_io.h"
#include "common/secret.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "include/byteorder.h"

#include "include/intarith.h"

#include "include/compat.h"
#include "common/blkdev.h"

#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <tr1/memory>
#include <sys/ioctl.h>

#include "include/rbd_types.h"

#if defined(__linux__)
#include <linux/fs.h>
#endif

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include "include/fiemap.h"

#define MAX_SECRET_LEN 1000
#define MAX_POOL_NAME_SIZE 128

static string dir_oid = RBD_DIRECTORY;
static string dir_info_oid = RBD_INFO;

void usage()
{
  cout << 
"usage: rbd [-n <auth user>] [OPTIONS] <cmd> ...\n"
"where 'pool' is a rados pool name (default is 'rbd') and 'cmd' is one of:\n"
"  (ls | list) [pool-name]                     list rbd images\n"
"  info <image-name>                           show information about image size,\n"
"                                              striping, etc.\n"
"  create [--order <bits>] --size <MB> <name>  create an empty image\n"
"  clone [--order <bits>] <parentsnap> <clonename>\n"
"                                              clone a snapshot into a COW\n"
"                                               child image\n"
"  resize --size <MB> <image-name>             resize (expand or contract) image\n"
"  rm <image-name>                             delete an image\n"
"  export <image-name> <path>                  export image to file\n"
"  import <path> <image-name>                  import image from file\n"
"                                              (dest defaults)\n"
"                                              as the filename part of file)\n"
"  (cp | copy) <src> <dest>                    copy src image to dest\n"
"  (mv | rename) <src> <dest>                  rename src image to dest\n"
"  snap ls <image-name>                        dump list of image snapshots\n"
"  snap create <snap-name>                     create a snapshot\n"
"  snap rollback <snap-name>                   rollback image to snapshot\n"
"  snap rm <snap-name>                         deletes a snapshot\n"
"  snap purge <image-name>                     deletes all snapshots\n"
"  watch <image-name>                          watch events on image\n"
"  map <image-name>                            map image to a block device\n"
"                                              using the kernel\n"
"  unmap <device>                              unmap a rbd device that was\n"
"                                              mapped by the kernel\n"
"  showmapped                                  show the rbd images mapped\n"
"                                              by the kernel\n"
"\n"
"<image-name>, <snap-name> are [pool/]name[@snap], or you may specify\n"
"individual pieces of names with -p/--pool, --image, and/or --snap.\n"
"\n"
"Other input options:\n"
"  -p, --pool <pool>            source pool name\n"
"  --image <image-name>         image name\n"
"  --dest <image-name>          destination [pool and] image name\n"
"  --snap <snap-name>           snapshot name\n"
"  --dest-pool <name>           destination pool name\n"
"  --path <path-name>           path name for import/export\n"
"  --size <size in MB>          size of image for create and resize\n"
"  --order <bits>               the object size in bits; object size will be\n"
"                               (1 << order) bytes. Default is 22 (4 MB).\n"
"\n"
"For the map command:\n"
"  --user <username>            rados user to authenticate as\n"
"  --secret <path>              file containing secret key for use with cephx\n";
}

void usage_exit()
{
  assert(1);
  usage();
  exit(1);
}

static string feature_str(uint64_t features)
{
  string s = ""; 

  if (features & RBD_FEATURE_LAYERING)
    s += "layering";
  return s;
}

struct MyProgressContext : public librbd::ProgressContext {
  const char *operation;
  int last_pc;

  MyProgressContext(const char *o) : operation(o), last_pc(0) {
  }
  
  int update_progress(uint64_t offset, uint64_t total) {
    int pc = total ? (offset * 100ull / total) : 0;
    if (pc != last_pc) {
      cout << "\r" << operation << ": "
	//	   << offset << " / " << total << " "
	   << pc << "% complete...";
      cout.flush();
      last_pc = pc;
    }
    return 0;
  }
  void finish() {
    cout << "\r" << operation << ": 100% complete...done." << std::endl;
  }
  void fail() {
    cout << "\r" << operation << ": " << last_pc << "% complete...failed." << std::endl;
  }
};

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
		     const char *imgname, uint64_t size, int *order,
		     bool old_format, uint64_t features)
{
  int r;
  if (features == 0) 
    features = RBD_FEATURES_ALL;

  if (old_format)
    r = rbd.create(io_ctx, imgname, size, order);
  else
    r = rbd.create2(io_ctx, imgname, size, features, order);
  if (r < 0)
    return r;
  return 0;
}

static int do_clone(librbd::RBD &rbd, librados::IoCtx &p_ioctx,
		    const char *p_name, const char *p_snapname, 
		    librados::IoCtx &c_ioctx, const char *c_name,
		    uint64_t features, int *c_order)
{
  if (features == 0)
    features = RBD_FEATURES_ALL;
  else if ((features & RBD_FEATURE_LAYERING) != RBD_FEATURE_LAYERING)
    return -EINVAL;

  return rbd.clone(p_ioctx, p_name, p_snapname, c_ioctx, c_name, features,
		    c_order);
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
  string parent_pool, parent_name, parent_snapname;
  uint8_t old_format;
  uint64_t overlap, features;
  int r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  r = image.old_format(&old_format);
  if (r < 0)
    return r;

  r = image.overlap(&overlap);
  if (r < 0)
    return r;

  r = image.features(&features);
  if (r < 0)
    return r;

  cout << "rbd image '" << imgname << "':\n"
       << "\tsize " << prettybyte_t(info.size) << " in "
       << info.num_objs << " objects"
       << std::endl
       << "\torder " << info.order
       << " (" << prettybyte_t(info.obj_size) << " objects)"
       << std::endl
       << "\tblock_name_prefix: " << info.block_name_prefix
       << std::endl
       << "\told format: " << (old_format ? "True" : "False")
       << std::endl
       << "\tfeatures: " << feature_str(features)
       << std::endl;

  // parent info, if present 
  if ((image.parent_info(&parent_pool, &parent_name, &parent_snapname) == 0) &&
      parent_name.length() > 0) {

    cout << "\tparent: " << parent_pool << "/" << parent_name
	 << "@" << parent_snapname << std::endl;
    cout << "\toverlap: " << prettybyte_t(overlap) << std::endl;
  }
  return 0;
}

static int do_delete(librbd::RBD &rbd, librados::IoCtx& io_ctx, const char *imgname)
{
  MyProgressContext pc("Removing image");
  int r = rbd.remove_with_progress(io_ctx, imgname, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

static int do_resize(librbd::Image& image, uint64_t size)
{
  MyProgressContext pc("Resizing image");
  int r = image.resize_with_progress(size, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

static int do_list_snaps(librbd::Image& image)
{
  std::vector<librbd::snap_info_t> snaps;
  int r = image.snap_list(snaps);
  if (r < 0)
    return r;

  cout << "ID\tNAME\t\tSIZE" << std::endl;
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
  MyProgressContext pc("Rolling back to snapshot");
  int r = image.snap_rollback_with_progress(snapname, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

static int do_purge_snaps(librbd::Image& image)
{
  MyProgressContext pc("Removing all snapshots");
  std::vector<librbd::snap_info_t> snaps;
  int r = image.snap_list(snaps);
  if (r < 0) {
    pc.fail();
    return r;
  }

  for (size_t i = 0; i < snaps.size(); ++i) {
    image.snap_remove(snaps[i].name.c_str());
    pc.update_progress(i + 1, snaps.size());
  }

  pc.finish();
  return 0;
}

struct ExportContext {
  int fd;
  MyProgressContext pc;

  ExportContext(int f) : fd(f), pc("Exporting image") {}
};

static int export_read_cb(uint64_t ofs, size_t len, const char *buf, void *arg)
{
  ssize_t ret;
  ExportContext *ec = (ExportContext *)arg;
  int fd = ec->fd;

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
  int64_t r;
  librbd::image_info_t info;
  int fd;

  r = image.stat(info, sizeof(info));
  if (r < 0)
    return r;

  fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
  if (fd < 0)
    return -errno;

  ExportContext ec(fd);
  r = image.read_iterate(0, info.size, export_read_cb, (void *)&ec);
  if (r < 0)
    goto out;

  r = ftruncate(fd, info.size);
  if (r < 0)
    goto out;

 out:
  close(fd);
  if (r < 0)
    ec.pc.fail();
  else
    ec.pc.finish();
  return r;
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
		     const char *imgname, int *order, const char *path,
		     bool old_format, uint64_t features, int64_t size)
{
  int fd, r;
  struct stat stat_buf;
  struct fiemap *fiemap;
  MyProgressContext pc("Importing image");

  if (! strcmp(path, "-")) {
    fd = 0;
  } else {
    fd = open(path, O_RDONLY);
  }

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
  if (stat_buf.st_size)
    size = (uint64_t)stat_buf.st_size;

  if (!size) {
    r = get_block_device_size(fd, &size);
    if (r < 0) {
      cerr << "unable to get size of file/block device: " << cpp_strerror(r) << std::endl;
      return r;
    }
  }

  assert(imgname);

  r = do_create(rbd, io_ctx, imgname, size, order, old_format, features);
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

    //cerr << "rbd import file_pos=" << file_pos << " extent_len=" << extent_len << std::endl;
#define READ_BLOCK_LEN (4 * 1024 * 1024)
    uint64_t left = end_ofs - file_pos;
    while (left) {
      pc.update_progress(file_pos, size);
      uint64_t cur_seg = (left < READ_BLOCK_LEN ? left : READ_BLOCK_LEN);
      while (cur_seg) {
        bufferptr p(cur_seg);
        //cerr << "reading " << cur_seg << " bytes at offset " << file_pos << std::endl;
	ssize_t rval;
	if(extent == 0 && fiemap->fm_extents[extent].fe_logical == 0) {
	  rval = TEMP_FAILURE_RETRY(::read(fd, p.c_str(), cur_seg));
	} else {
	  rval = TEMP_FAILURE_RETRY(::pread(fd, p.c_str(), cur_seg, file_pos));
	}
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
  if (r < 0)
    pc.fail();
  else
    pc.finish();
  free(fiemap);

  return r;
}

static int do_copy(librbd::Image &src, librados::IoCtx& dest_pp,
		   const char *destname)
{
  MyProgressContext pc("Image copy");
  int r = src.copy_with_progress(dest_pp, destname, pc);
  pc.finish();
  if (r < 0)
    return r;
  return 0;
}

class RbdWatchCtx : public librados::WatchCtx {
  string name;
public:
  RbdWatchCtx(const char *imgname) : name(imgname) {}
  virtual ~RbdWatchCtx() {}
  virtual void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) {
    cout << name << " got notification opcode=" << (int)opcode << " ver=" << ver << " bl.length=" << bl.length() << std::endl;
  }
};

static int do_watch(librados::IoCtx& pp, const char *imgname)
{
  string md_oid, dest_md_oid;
  uint64_t cookie;
  RbdWatchCtx ctx(imgname);

  string old_header_oid = imgname;
  old_header_oid += RBD_SUFFIX;
  string new_header_oid = RBD_HEADER_PREFIX;
  new_header_oid += imgname;
  bool old_format = true;

  int r = pp.stat(old_header_oid, NULL, NULL);
  if (r < 0) {
    r = pp.stat(new_header_oid, NULL, NULL);
    if (r < 0)
      return r;
    old_format = false;
  }

  r = pp.watch(old_format ? old_header_oid : new_header_oid,
	       0, &cookie, &ctx);
  if (r < 0) {
    cerr << "watch failed" << std::endl;
    return r;
  }

  cout << "press enter to exit..." << std::endl;
  getchar();

  return 0;
}

static int do_kernel_add(const char *poolname, const char *imgname,
			 const char *snapname, const char *secretfile,
			 const char *user)
{
  MonMap monmap;
  int r = monmap.build_initial(g_ceph_context, cerr);
  if (r < 0)
    return r;

  map<string, entity_addr_t>::const_iterator it = monmap.mon_addr.begin();
  ostringstream oss;
  for (size_t i = 0; i < monmap.mon_addr.size(); ++i, ++it) {
    oss << it->second.addr;
    if (i + 1 < monmap.mon_addr.size())
      oss << ",";
  }

  oss << " name=" << user;

  char key_name[strlen(user) + strlen("client.")];
  snprintf(key_name, sizeof(key_name), "client.%s", user);
  char secret_buf[MAX_SECRET_LEN];
  char *secret = NULL;
  if (secretfile) {
    r = read_secret_from_file(secretfile, secret_buf, sizeof(secret_buf));
    if (r < 0)
      return r;
    secret = secret_buf;
  }

  if (secret || is_kernel_secret(key_name)) {
    char option[MAX_SECRET_LEN + 7];
    r = get_secret_option(secret, key_name, option, sizeof(option));
    if (r < 0) {
      return r;
    }
    oss << "," << option;
  }

  oss << " " << poolname << " " << imgname;

  if (snapname) {
    oss << " " << snapname;
  }

  // write to /sys/bus/rbd/add
  int fd = open("/sys/bus/rbd/add", O_WRONLY);
  if (fd < 0) {
    r = -errno;
    if (r == -ENOENT) {
      cerr << "/sys/bus/rbd/add does not exist!" << std::endl
	   << "Did you run 'modprobe rbd' or is your rbd module too old?" << std::endl;
    }
    return r;
  }

  string add = oss.str();
  r = safe_write(fd, add.c_str(), add.size());
  close(fd);

  return r;
}

static int read_file(const char *filename, char *buf, size_t bufsize)
{
    int fd = open(filename, O_RDONLY);
    if (fd < 0)
      return -errno;

    int r = safe_read(fd, buf, bufsize);
    if (r < 0) {
      cerr << "Warning: could not read " << filename << ": " << cpp_strerror(-r) << std::endl;
      return r;
    }

    char *end = buf;
    while (end < buf + bufsize && *end && *end != '\n') {
      end++;
    }
    *end = '\0';

    close(fd);
    return r;
}

void do_closedir(DIR *dp)
{
  if (dp)
    closedir(dp);
}

static int do_kernel_showmapped()
{
  int r;
  const char *devices_path = "/sys/bus/rbd/devices";
  std::tr1::shared_ptr<DIR> device_dir(opendir(devices_path), do_closedir);
  if (!device_dir.get()) {
    r = -errno;
    cerr << "Could not open " << devices_path << ": " << cpp_strerror(-r) << std::endl;
    return r;
  }

  struct dirent *dent;
  dent = readdir(device_dir.get());
  if (!dent) {
    r = -errno;
    cerr << "Error reading " << devices_path << ": " << cpp_strerror(-r) << std::endl;
    return r;
  }

  cout << "id\tpool\timage\tsnap\tdevice" << std::endl;

  do {
    if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
      continue;

    char fn[PATH_MAX];

    char dev[PATH_MAX];
    snprintf(dev, sizeof(dev), "/dev/rbd%s", dent->d_name);

    char name[RBD_MAX_IMAGE_NAME_SIZE];
    snprintf(fn, sizeof(fn), "%s/%s/name", devices_path, dent->d_name);
    r = read_file(fn, name, sizeof(name));
    if (r < 0) {
      cerr << "could not read name from " << fn << ": " << cpp_strerror(-r) << std::endl;
      continue;
    }

    char pool[4096];
    snprintf(fn, sizeof(fn), "%s/%s/pool", devices_path, dent->d_name);
    r = read_file(fn, pool, sizeof(pool));
    if (r < 0) {
      cerr << "could not read name from " << fn << ": " << cpp_strerror(-r) << std::endl;
      continue;
    }

    char snap[4096];
    snprintf(fn, sizeof(fn), "%s/%s/current_snap", devices_path, dent->d_name);
    r = read_file(fn, snap, sizeof(snap));
    if (r < 0) {
      cerr << "could not read name from " << fn << ": " << cpp_strerror(-r) << std::endl;
      continue;
    }

    cout << dent->d_name << "\t" << pool << "\t" << name << "\t" << snap << "\t" << dev << std::endl;

  } while ((dent = readdir(device_dir.get())));

  return 0;
}

static int get_rbd_seq(int major_num, string &seq)
{
  int r;
  const char *devices_path = "/sys/bus/rbd/devices";
  DIR *device_dir = opendir(devices_path);
  if (!device_dir) {
    r = -errno;
    cerr << "Could not open " << devices_path << ": " << cpp_strerror(-r) << std::endl;
    return r;
  }

  struct dirent *dent;
  dent = readdir(device_dir);
  if (!dent) {
    r = -errno;
    cerr << "Error reading " << devices_path << ": " << cpp_strerror(-r) << std::endl;
    return r;
  }

  char major[32];
  do {
    if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
      continue;

    char fn[strlen(devices_path) + strlen(dent->d_name) + strlen("//major") + 1];

    snprintf(fn, sizeof(fn), "%s/%s/major", devices_path, dent->d_name);
    r = read_file(fn, major, sizeof(major));
    if (r < 0) {
      cerr << "could not read major number from " << fn << ": " << cpp_strerror(-r) << std::endl;
      continue;
    }

    int cur_major = atoi(major);
    if (cur_major == major_num) {
      seq = string(dent->d_name);
      return 0;
    }

  } while ((dent = readdir(device_dir)));

  return -ENOENT;
}

static int do_kernel_rm(const char *dev)
{
  struct stat dev_stat;
  int r = stat(dev, &dev_stat);
  if (!S_ISBLK(dev_stat.st_mode)) {
    cerr << dev << " is not a block device" << std::endl;
    return -EINVAL;
  }

  int major = major(dev_stat.st_rdev);
  string seq_num;
  r = get_rbd_seq(major, seq_num);
  if (r == -ENOENT) {
    cerr << dev << " is not an rbd device" << std::endl;
    return -EINVAL;
  }
  if (r < 0)
    return r;

  int fd = open("/sys/bus/rbd/remove", O_WRONLY);
  if (fd < 0) {
    return -errno;
  }

  r = safe_write(fd, seq_num.c_str(), seq_num.size());
  if (r < 0) {
    cerr << "Failed to remove rbd device" << ": " << cpp_strerror(-r) << std::endl;
    close(fd);
    return r;
  }

  r = close(fd);
  if (r < 0)
    r = -errno;
  return r;
}

enum {
  OPT_NO_CMD = 0,
  OPT_LIST,
  OPT_INFO,
  OPT_CREATE,
  OPT_CLONE,
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
  OPT_SNAP_PURGE,
  OPT_WATCH,
  OPT_MAP,
  OPT_UNMAP,
  OPT_SHOWMAPPED,
};

static int get_cmd(const char *cmd, bool snapcmd)
{
  if (!snapcmd) {
    if (strcmp(cmd, "ls") == 0 ||
        strcmp(cmd, "list") == 0)
      return OPT_LIST;
    if (strcmp(cmd, "info") == 0)
      return OPT_INFO;
    if (strcmp(cmd, "create") == 0)
      return OPT_CREATE;
    if (strcmp(cmd, "clone") == 0)
      return OPT_CLONE;
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
    if (strcmp(cmd, "map") == 0)
      return OPT_MAP;
    if (strcmp(cmd, "showmapped") == 0)
      return OPT_SHOWMAPPED;
    if (strcmp(cmd, "unmap") == 0)
      return OPT_UNMAP;
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
    if (strcmp(cmd, "purge") == 0)
      return OPT_SNAP_PURGE;
  }

  return OPT_NO_CMD;
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

  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  int opt_cmd = OPT_NO_CMD;
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char *poolname = NULL;
  uint64_t size = 0;  // in bytes
  int order = 0;
  bool old_format = true;
  const char *imgname = NULL, *snapname = NULL, *destname = NULL, *dest_poolname = NULL, *dest_snapname = NULL, *path = NULL, *secretfile = NULL, *user = NULL, *devpath = NULL;

  std::string val;
  std::ostringstream err;
  long long sizell = 0;
  std::vector<const char*>::iterator i;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      exit(0);
    } else if (ceph_argparse_flag(args, i, "--new-format", (char*)NULL)) {
      old_format = false;
    } else if (ceph_argparse_witharg(args, i, &val, "-p", "--pool", (char*)NULL)) {
      poolname = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-pool", (char*)NULL)) {
      dest_poolname = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--snap", (char*)NULL)) {
      snapname = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--image", (char*)NULL)) {
      imgname = strdup(val.c_str());
    } else if (ceph_argparse_withlonglong(args, i, &sizell, &err, "-s", "--size", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      size = sizell << 20;   // bytes to MB
    } else if (ceph_argparse_withint(args, i, &order, &err, "--order", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--path", (char*)NULL)) {
      path = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--dest", (char*)NULL)) {
      destname = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--secret", (char*)NULL)) {
      secretfile = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--user", (char*)NULL)) {
      user = strdup(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--parent", (char *)NULL)) {
      imgname = strdup(val.c_str());
    } else {
      ++i;
    }
  }

  i = args.begin();
  if (i == args.end()) {
    cerr << "you must specify a command." << std::endl;
    usage_exit();
  }
  else if (strcmp(*i, "snap") == 0) {
    i = args.erase(i);
    if (i == args.end()) {
      cerr << "which snap command do you want?" << std::endl;
      usage_exit();
    }
    opt_cmd = get_cmd(*i, true);
  }
  else {
    opt_cmd = get_cmd(*i, false);
  }
  if (opt_cmd == OPT_NO_CMD) {
    cerr << "error parsing command '" << *i << "'" << std::endl;
    usage_exit();
  }

  for (i = args.erase(i); i != args.end(); ++i) {
    const char *v = *i;
    switch (opt_cmd) {
      case OPT_LIST:
	set_conf_param(v, &poolname, NULL);
	break;
      case OPT_INFO:
      case OPT_CREATE:
      case OPT_RESIZE:
      case OPT_RM:
      case OPT_SNAP_CREATE:
      case OPT_SNAP_ROLLBACK:
      case OPT_SNAP_REMOVE:
      case OPT_SNAP_LIST:
      case OPT_SNAP_PURGE:
      case OPT_WATCH:
      case OPT_MAP:
	set_conf_param(v, &imgname, NULL);
	break;
      case OPT_UNMAP:
	set_conf_param(v, &devpath, NULL);
	break;
      case OPT_EXPORT:
	set_conf_param(v, &imgname, &path);
	break;
      case OPT_IMPORT:
	set_conf_param(v, &path, &destname);
	break;
      case OPT_COPY:
      case OPT_RENAME:
	set_conf_param(v, &imgname, &destname);
	break;
      case OPT_CLONE:
	if (imgname == NULL) {
	  set_conf_param(v, &imgname, NULL);
        } else {
	  set_conf_param(v, &destname, NULL);
	}
	break;
      case OPT_SHOWMAPPED:
	usage_exit();
	break;
      default:
	assert(0);
	break;
    }
  }
  if (!user)
    user = "admin";

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

  if (opt_cmd != OPT_LIST && opt_cmd != OPT_IMPORT && opt_cmd != OPT_UNMAP && opt_cmd != OPT_SHOWMAPPED &&
      !imgname) {
    cerr << "error: image name was not specified" << std::endl;
    usage_exit();
  }

  // do this unconditionally so we can parse pool/image@snapshot into
  // the relevant parts
  set_pool_image_name(poolname, imgname, (char **)&poolname,
		      (char **)&imgname, (char **)&snapname);
  if (snapname && opt_cmd != OPT_SNAP_CREATE && opt_cmd != OPT_SNAP_ROLLBACK &&
      opt_cmd != OPT_SNAP_REMOVE && opt_cmd != OPT_INFO &&
      opt_cmd != OPT_EXPORT && opt_cmd != OPT_COPY &&
      opt_cmd != OPT_MAP && opt_cmd != OPT_CLONE) {
    cerr << "error: snapname specified for a command that doesn't use it" << std::endl;
    usage_exit();
  }
  if ((opt_cmd == OPT_SNAP_CREATE || opt_cmd == OPT_SNAP_ROLLBACK ||
       opt_cmd == OPT_SNAP_REMOVE || opt_cmd == OPT_CLONE) && !snapname) {
    cerr << "error: snap name was not specified" << std::endl;
    usage_exit();
  }

  set_pool_image_name(dest_poolname, destname, (char **)&dest_poolname, (char **)&destname, (char **)&dest_snapname);

  if (!poolname)
    poolname = "rbd";
  if (!dest_poolname)
    dest_poolname = poolname;

  if (opt_cmd == OPT_EXPORT && !path)
    path = imgname;

  if ((opt_cmd == OPT_COPY || opt_cmd == OPT_CLONE) && !destname ) {
    cerr << "error: destination image name was not specified" << std::endl;
    usage_exit();
  }

  if ((opt_cmd == OPT_CLONE) && dest_snapname) {
    cerr << "error: cannot clone to a snapshot" << std::endl;
    usage_exit();
  }

  if ((opt_cmd == OPT_CLONE) && size) {
    cerr << "error: clone must begin at size of parent" << std::endl;
    usage_exit();
  }

  if ((opt_cmd == OPT_RENAME) && (strcmp(poolname, dest_poolname) != 0)) {
    cerr << "error: mv/rename across pools not supported" << std::endl;
    cerr << "source pool: " << poolname << " dest pool: " << dest_poolname
      << std::endl;
    exit(EXIT_FAILURE);
  }

  bool talk_to_cluster = (opt_cmd != OPT_MAP &&
			  opt_cmd != OPT_UNMAP &&
			  opt_cmd != OPT_SHOWMAPPED);
  if (talk_to_cluster && rados.init_with_context(g_ceph_context) < 0) {
    cerr << "error: couldn't initialize rados!" << std::endl;
    exit(1);
  }

  if (talk_to_cluster && rados.connect() < 0) {
    cerr << "error: couldn't connect to the cluster!" << std::endl;
    exit(1);
  }

  int r;
  if (talk_to_cluster) {
    r = rados.ioctx_create(poolname, io_ctx);
    if (r < 0) {
      cerr << "error opening pool " << poolname << ": " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
  }

  if (imgname && talk_to_cluster &&
      (opt_cmd == OPT_RESIZE || opt_cmd == OPT_INFO || opt_cmd == OPT_SNAP_LIST ||
       opt_cmd == OPT_SNAP_CREATE || opt_cmd == OPT_SNAP_ROLLBACK ||
       opt_cmd == OPT_SNAP_REMOVE || opt_cmd == OPT_SNAP_PURGE ||
       opt_cmd == OPT_EXPORT || opt_cmd == OPT_WATCH || opt_cmd == OPT_COPY)) {
    r = rbd.open(io_ctx, image, imgname);
    if (r < 0) {
      cerr << "error opening image " << imgname << ": " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
  }

  if (snapname && talk_to_cluster &&
      (opt_cmd == OPT_INFO || opt_cmd == OPT_EXPORT || opt_cmd == OPT_COPY)) {
    r = image.snap_set(snapname);
    if (r < 0) {
      cerr << "error setting snapshot context: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
  }

  if (opt_cmd == OPT_COPY || opt_cmd == OPT_IMPORT || opt_cmd == OPT_CLONE) {
    r = rados.ioctx_create(dest_poolname, dest_io_ctx);
    if (r < 0) {
      cerr << "error opening pool " << dest_poolname << ": " << cpp_strerror(-r) << std::endl;
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
        cerr << "error: " << cpp_strerror(-r) << std::endl;
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
    r = do_create(rbd, io_ctx, imgname, size, &order, old_format, 0);
    if (r < 0) {
      cerr << "create error: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_CLONE:
    if (order && (order < 12 || order > 25)) {
      cerr << "order must be between 12 (4 KB) and 25 (32 MB)" << std::endl;
      usage();
      exit(1);
    }

    r = do_clone(rbd, io_ctx, imgname, snapname, dest_io_ctx, destname,
		 RBD_FEATURE_LAYERING, &order);
    if (r < 0) {
      cerr << "clone error: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_RENAME:
    r = do_rename(rbd, io_ctx, imgname, destname);
    if (r < 0) {
      cerr << "rename error: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_INFO:
    r = do_show_info(imgname, image);
    if (r < 0) {
      cerr << "error: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_RM:
    r = do_delete(rbd, io_ctx, imgname);
    if (r < 0) {
      if (r == -ENOTEMPTY) {
	cerr << "delete error: image has snapshots - these must be deleted"
	     << " with 'rbd snap purge' before the image can be removed."
	     << std::endl;
      } else if (r == -EBUSY) {
	cerr << "delete error: image still has watchers"
	     << std::endl
	     << "This means the image is still open or the client using "
	     << "it crashed. Try again after closing/unmapping it or "
	     << "waiting 30s for the crashed client to timeout."
	     << std::endl;
      } else {
	cerr << "delete error: " << cpp_strerror(-r) << std::endl;
      }
      exit(-r);
    }
    break;

  case OPT_RESIZE:
    r = do_resize(image, size);
    if (r < 0) {
      cerr << "resize error: " << cpp_strerror(-r) << std::endl;
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
      cerr << "failed to list snapshots: " << cpp_strerror(-r) << std::endl;
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
      cerr << "failed to create snapshot: " << cpp_strerror(-r) << std::endl;
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
      cerr << "rollback failed: " << cpp_strerror(-r) << std::endl;
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
      cerr << "rollback failed: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_SNAP_PURGE:
    if (!imgname) {
      usage();
      exit(1);
    }
    r = do_purge_snaps(image);
    if (r < 0) {
      cerr << "removing snaps failed: " << cpp_strerror(-r) << std::endl;
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
      cerr << "export error: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_IMPORT:
    if (!path) {
      cerr << "pathname should be specified" << std::endl;
      exit(1);
    }
    r = do_import(rbd, dest_io_ctx, destname, &order, path,
		  old_format, 0, size);
    if (r < 0) {
      cerr << "import failed: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_COPY:
    r = do_copy(image, dest_io_ctx, destname);
    if (r < 0) {
      cerr << "copy failed: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_WATCH:
    r = do_watch(io_ctx, imgname);
    if (r < 0) {
      cerr << "watch failed: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_MAP:
    r = do_kernel_add(poolname, imgname, snapname, secretfile, user);
    if (r < 0) {
      cerr << "add failed: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_UNMAP:
    r = do_kernel_rm(devpath);
    if (r < 0) {
      cerr << "remove failed: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;

  case OPT_SHOWMAPPED:
    r = do_kernel_showmapped();
    if (r < 0) {
      cerr << "showmapped failed: " << cpp_strerror(-r) << std::endl;
      exit(1);
    }
    break;
  }

  return 0;
}
