// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "config.h"

#include "common/common_init.h"
#include "include/librados.hpp"
using namespace librados;
#include "include/byteorder.h"


#include <iostream>

#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <errno.h>

#include "include/rbd_types.h"

Rados rados;
pool_t pool;

void usage()
{
  cout << "usage: rbdtool [-n <auth user>] [-p|--pool <name>] [-o|--object <imagename>] <cmd>\n"
       << "where 'pool' is a rados pool name (default is 'rbd') and 'cmd' is one of:\n"
       << "\t--list    list rbd images\n"
       << "\t--info    show information about image size, striping, etc.\n"
       << "\t--create <image name> --size <size in MB>\n"
       << "\t          create an image\n"
       << "\t--resize <image name> --size <new size in MB>\n"
       << "\t          resize (expand or contract) image\n"
       << "\t--delete <image name>\n"
       << "\t          delete an image\n"
       << "\t--list-snaps <image name>\n"
       << "\t          dump list of specific image snapshots\n"
       << "\t--add-snap <snap name>\n"
       << "\t          create a snapshot for the specified image\n"
       << "\t--rollback-snap <snap name>\n"
       << "\t          rollback image head to specified snapshot\n"
       << "\t--rename <dst name>\n"
       << "\t          rename rbd image\n"
       << "\t--export <path>\n"
       << "\t          export image to path\n";
}

void usage_exit()
{
  usage();
  exit(1);
}


static void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
			    size_t size, int order, uint64_t bid)
{
  uint32_t hi = bid >> 32;
  uint32_t lo = bid & 0xFFFFFFFF;
  memset(&ondisk, 0, sizeof(ondisk));

  memcpy(&ondisk.text, RBD_HEADER_TEXT, sizeof(RBD_HEADER_TEXT));
  memcpy(&ondisk.signature, RBD_HEADER_SIGNATURE, sizeof(RBD_HEADER_SIGNATURE));
  memcpy(&ondisk.version, RBD_HEADER_VERSION, sizeof(RBD_HEADER_VERSION));

  snprintf(ondisk.block_name, sizeof(ondisk.block_name), "rb.%x.%x", hi, lo);

  ondisk.image_size = size;
  if (order)
    ondisk.options.order = order;
  else
    ondisk.options.order = RBD_DEFAULT_OBJ_ORDER;
  ondisk.options.crypt_type = RBD_CRYPT_NONE;
  ondisk.options.comp_type = RBD_COMP_NONE;
  ondisk.snap_seq = 0;
  ondisk.snap_count = 0;
  ondisk.reserved = 0;
  ondisk.snap_names_len = 0;
}

void print_header(char *imgname, rbd_obj_header_ondisk *header)
{
  int obj_order = header->options.order;
  cout << "rbd image '" << imgname << "':\n"
       << "\tsize " << prettybyte_t(header->image_size) << " in "
       << (header->image_size >> obj_order) << " objects\n"
       << "\torder " << obj_order
       << " (" << prettybyte_t(1 << obj_order) << " objects)"
       << std::endl;
}

static string get_block_oid(rbd_obj_header_ondisk *header, uint64_t num)
{
  char o[RBD_MAX_SEG_NAME_SIZE];
  sprintf(o, "%s.%012llx", header->block_name, (unsigned long long)num);
  return o;
}

static uint64_t get_max_block(rbd_obj_header_ondisk *header)
{
  uint64_t size = header->image_size;
  int obj_order = header->options.order;
  uint64_t numseg = size >> obj_order;

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

void trim_image(const char *imgname, rbd_obj_header_ondisk *header, uint64_t newsize)
{
  uint64_t numseg = get_max_block(header);
  uint64_t start = get_block_num(header, newsize);

  cout << "trimming image data from " << numseg << " to " << start << " objects..." << std::endl;
  for (uint64_t i=start; i<numseg; i++) {
    string oid = get_block_oid(header, i);
    rados.remove(pool, oid);
    if ((i & 127) == 0) {
      cout << "\r\t" << i << "/" << numseg;
      cout.flush();
    }
  }
}

static int init_rbd_info(struct rbd_info *info)
{
  memset(info, 0, sizeof(*info));
  return 0;
}

int read_rbd_info(pool_t pool, string& info_oid, struct rbd_info *info)
{
  int r;
  bufferlist bl;

  r = rados.read(pool, info_oid, 0, bl, sizeof(*info));
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

static int touch_rbd_info(pool_t pool, string& info_oid)
{
  bufferlist bl;
  int r = rados.write(pool, info_oid, 0, bl, 0);
  if (r < 0)
    return r;
  return 0;
}

static int rbd_assign_bid(pool_t pool, string& info_oid, uint64_t *id)
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


static int read_header_bl(pool_t pool, string& md_oid, bufferlist& header)
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

  return 0;
}

static int read_header(pool_t pool, string& md_oid, struct rbd_obj_header_ondisk *header)
{
  bufferlist header_bl;
  int r = read_header_bl(pool, md_oid, header_bl);
  if (r < 0)
    return r;
  if (header_bl.length() < (int)sizeof(*header))
    return -EIO;
  memcpy(header, header_bl.c_str(), sizeof(*header));

  return 0;
}

static int write_header(pool_t pool, string& md_oid, bufferlist& header)
{
  bufferlist bl;
  int r = rados.write(pool, md_oid, 0, header, header.length());

  return r;
}

static int tmap_set(pool_t pool, string& dir_oid, string& imgname)
{
  bufferlist cmdbl, emptybl;
  __u8 c = CEPH_OSD_TMAP_SET;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  ::encode(emptybl, cmdbl);
  return rados.tmap_update(pool, dir_oid, cmdbl);
}

static int tmap_rm(pool_t pool, string& dir_oid, string& imgname)
{
  bufferlist cmdbl;
  __u8 c = CEPH_OSD_TMAP_RM;
  ::encode(c, cmdbl);
  ::encode(imgname, cmdbl);
  return rados.tmap_update(pool, dir_oid, cmdbl);
}

static int rollback_image(pool_t pool, struct rbd_obj_header_ondisk *header,
                          SnapContext& snapc, uint64_t snapid)
{
  uint64_t numseg = get_max_block(header);

  for (uint64_t i = 0; i < numseg; i++) {
    int r;
    string oid = get_block_oid(header, i);
    r = rados.selfmanaged_snap_rollback_object(pool, oid, snapc, snapid);
    if (r < 0 && r != -ENOENT)
      return r;
  }
  return 0;
}

static int do_export(pool_t pool, string& md_oid, const char *path)
{
  struct rbd_obj_header_ondisk header;
  int ret, r;

  ret = read_header(pool, md_oid, &header);
  if (ret < 0)
    return ret;

  uint64_t numseg = get_max_block(&header);
  uint64_t block_size = get_block_size(&header);
  int fd = open(path, O_WRONLY | O_CREAT, 0644);
  uint64_t pos = 0;

  if (fd < 0)
    return -errno;

  for (uint64_t i = 0; i < numseg; i++) {
    bufferlist bl;
    string oid = get_block_oid(&header, i);
    r = rados.read(pool, oid, 0, bl, block_size);
    if (r < 0 && r == -ENOENT)
      r = 0;
    if (r < 0) {
      ret = r;
      goto done;
    }

    if (bl.length()) {
      ret = write(fd, bl.c_str(), bl.length());
      if (ret < 0)
        goto done;
    }

    pos += block_size;

    if (bl.length() < block_size) {
      ret = lseek(fd, pos, SEEK_SET);
      if (ret < 0) {
        ret = -errno;
        cerr << "could not seek to pos " << pos << std::endl;
        goto done;
      }
    }
  }
  r = ftruncate(fd, pos);
  if (r < 0)
    ret = -errno;

  ret = 0;

done:
  close(fd);
  return ret;
}


static void err_exit(pool_t pool)
{
  rados.close_pool(pool);
  rados.shutdown();
  exit(1);
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  DEFINE_CONF_VARS(usage_exit);
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_set_defaults(false);
  common_init(args, "rbdtool", true);

  bool opt_create = false, opt_delete = false, opt_list = false, opt_info = false, opt_resize = false,
       opt_list_snaps = false, opt_add_snap = false, opt_rollback_snap = false, opt_rename = false,
       opt_export = false;
  char *poolname = (char *)"rbd";
  uint64_t size = 0;
  int order = 0;
  char *imgname = NULL, *snapname = NULL, *dstname = NULL, *path = NULL;
  string md_oid;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("list", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&opt_list, OPT_BOOL);
    } else if (CONF_ARG_EQ("create", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
      opt_create = true;
    } else if (CONF_ARG_EQ("delete", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
      opt_delete = true;
    } else if (CONF_ARG_EQ("resize", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
      opt_resize = true;
    } else if (CONF_ARG_EQ("info", 'i')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
      opt_info = true;
    } else if (CONF_ARG_EQ("list-snaps", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
      opt_list_snaps = true;
    } else if (CONF_ARG_EQ("add-snap", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&snapname, OPT_STR);
      opt_add_snap = true;
    } else if (CONF_ARG_EQ("rollback-snap", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&snapname, OPT_STR);
      opt_rollback_snap = true;
    } else if (CONF_ARG_EQ("pool", 'p')) {
      CONF_SAFE_SET_ARG_VAL(&poolname, OPT_STR);
    } else if (CONF_ARG_EQ("object", 'o')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
    } else if (CONF_ARG_EQ("size", 's')) {
      CONF_SAFE_SET_ARG_VAL(&size, OPT_LONGLONG);
      size <<= 20; // MB -> bytes
    } else if (CONF_ARG_EQ("order", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&order, OPT_INT);
    } else if (CONF_ARG_EQ("rename", '\0')) {
      opt_rename = true;
      CONF_SAFE_SET_ARG_VAL(&dstname, OPT_STR);
    } else if (CONF_ARG_EQ("export", '\0')) {
      opt_export = true;
      CONF_SAFE_SET_ARG_VAL(&path, OPT_STR);
    } else 
      usage_exit();
  }

  if (!opt_create && !opt_delete && !opt_list && !opt_info && !opt_resize &&
      !opt_list_snaps && !opt_add_snap && !opt_rollback_snap && !opt_rename &&
      !opt_export) {
    usage_exit();
  }

  if (!opt_list && !imgname) {
    usage_exit();
  }

  if (rados.initialize(argc, argv) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  if (!opt_list) {
    md_oid = imgname;
    md_oid += RBD_SUFFIX;
  }
  string dir_oid = RBD_DIRECTORY;
  string dir_info_oid= RBD_INFO;

  int r = rados.open_pool(poolname, &pool);
  if (r < 0) {
    cerr << "error opening pool (err=" << r << ")" << std::endl;
    err_exit(pool);
  }

  if (opt_list) {
    bufferlist bl;
    r = rados.read(pool, dir_oid, 0, bl, 0);
    if (r < 0) {
      cerr << "pool " << poolname << " doesn't contain rbd images" << std::endl;
      err_exit(pool);
    }
    bufferlist::iterator p = bl.begin();
    bufferlist header;
    map<string,bufferlist> m;
    ::decode(header, p);
    ::decode(m, p);
    for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); q++)
      cout << q->first << std::endl;
  } else if (opt_create) {
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

    // make sure it doesn't already exist
    r = rados.stat(pool, md_oid, NULL, NULL);
    if (r == 0) {
      cerr << "rbd image header " << md_oid << " already exists" << std::endl;
      err_exit(pool);
    }

    uint64_t bid;
    r = rbd_assign_bid(pool, dir_info_oid, &bid);
    if (r < 0) {
      err_exit(pool);
    }

    struct rbd_obj_header_ondisk header;
    init_rbd_header(header, size, order, bid);
    
    bufferlist bl;
    bl.append((const char *)&header, sizeof(header));
    
    print_header(imgname, &header);

    cout << "adding rbd image to directory..." << std::endl;
    bufferlist cmdbl, emptybl;
    __u8 c = CEPH_OSD_TMAP_SET;
    ::encode(c, cmdbl);
    ::encode(imgname, cmdbl);
    ::encode(emptybl, cmdbl);
    r = rados.tmap_update(pool, dir_oid, cmdbl);
    if (r < 0) {
      cerr << "error adding img to directory: " << strerror(-r)<< std::endl;
      err_exit(pool);
    }

    cout << "creating rbd image..." << std::endl;
    r = rados.write(pool, md_oid, 0, bl, bl.length());
    if (r < 0) {
      cerr << "error writing header: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    cout << "done." << std::endl;
  } else if (opt_rename) {
    string dst_md_oid = dstname;
    dst_md_oid += RBD_SUFFIX;
    string dstname_str = dstname;
    string imgname_str = imgname;
    r = rados.stat(pool, dst_md_oid, NULL, NULL);
    if (r == 0) {
      cerr << "rbd image header " << dst_md_oid << " already exists" << std::endl;
      err_exit(pool);
    }
    bufferlist header;
    r = read_header_bl(pool, md_oid, header);
    if (r < 0) {
      cerr << "error reading header: " << md_oid << ": " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    r = write_header(pool, dst_md_oid, header);
    if (r < 0) {
      cerr << "error writing header: " << dst_md_oid << ": " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    r = tmap_set(pool, dir_oid, dstname_str);
    if (r < 0) {
      rados.remove(pool, dst_md_oid);
      cerr << "can't add " << dst_md_oid << " to directory" << std::endl;
      err_exit(pool);
    }
    r = tmap_rm(pool, dir_oid, imgname_str);
    if (r < 0)
      cerr << "warning: couldn't remove old entry from directory (" << imgname_str << ")" << std::endl;

    r = rados.remove(pool, md_oid);
    if (r < 0)
      cerr << "warning: couldn't remove old metadata" << std::endl;
  } else if (opt_info || opt_delete || opt_resize) {
    struct rbd_obj_header_ondisk header;
    r = read_header(pool, md_oid, &header);

    if (opt_delete) {
      if (r >= 0) {
	print_header(imgname, &header);
	trim_image(imgname, &header, 0);
	cout << "\rremoving header..." << std::endl;
	rados.remove(pool, md_oid);
      }
	
      cout << "removing rbd image to directory..." << std::endl;
      bufferlist cmdbl;
      __u8 c = CEPH_OSD_TMAP_RM;
      ::encode(c, cmdbl);
      ::encode(imgname, cmdbl);
      r = rados.tmap_update(pool, dir_oid, cmdbl);
      if (r < 0) {
	cerr << "error removing img from directory: " << strerror(-r)<< std::endl;
	err_exit(pool);
      }
    } else { /* opt_info || opt_resize */
      if (r < 0)
	err_exit(pool);
      
      if (opt_info) {
	print_header(imgname, &header);
      }

      if (opt_resize) {
	// trim
	if (size == header.image_size) {
	  cout << "no change in size (" << size << " -> " << header.image_size << ")" << std::endl;
	  print_header(imgname, &header);
	} else {
	  if (size > header.image_size) {
	    cout << "expanding image " << size << " -> " << header.image_size << " objects" << std::endl;
	    header.image_size = size;
	  } else {
	    cout << "shrinking image " << size << " -> " << header.image_size << " objects" << std::endl;
	    trim_image(imgname, &header, size);
	    header.image_size = size;
	  }
	  print_header(imgname, &header);

	  // rewrite header
	  bufferlist bl;
	  bl.append((const char *)&header, sizeof(header));
	  r = rados.write(pool, md_oid, 0, bl, bl.length());
	  if (r < 0) {
	    cerr << "error writing header: " << strerror(-r) << std::endl;
	    err_exit(pool);
	  }
	}
      }
    }
      
    cout << "done." << std::endl;
  } else if (opt_list_snaps) {
    bufferlist bl, bl2;
    if (!imgname) {
      usage();
      err_exit(pool);
    }

    r = rados.exec(pool, md_oid, "rbd", "snap_list", bl, bl2);
    if (r < 0) {
      cerr << "list_snaps failed: " << strerror(-r) << std::endl;
      err_exit(pool);
    }

    uint32_t num_snaps;
    uint64_t snap_seq;
    bufferlist::iterator iter = bl2.begin();
    ::decode(snap_seq, iter);
    ::decode(num_snaps, iter);
    for (uint32_t i=0; i < num_snaps; i++) {
      uint64_t id, image_size;
      string s;
      ::decode(id, iter);
      ::decode(image_size, iter);
      ::decode(s, iter);
      cout << id << "\t" << s << "\t" << image_size << std::endl;
    }
  } else if (opt_add_snap) {
    bufferlist bl, bl2;
    uint64_t snap_id;
    if (!imgname || !snapname) {
      usage();
      err_exit(pool);
    }
    r = rados.selfmanaged_snap_create(pool, &snap_id);
    if (r < 0) {
      cerr << "failed to create snap id: " << strerror(-r) << std::endl;
      err_exit(pool);
    }

    ::encode(snapname, bl);
    ::encode(snap_id, bl);

    r = rados.exec(pool, md_oid, "rbd", "snap_add", bl, bl2);
    if (r < 0) {
      cerr << "list_snaps failed: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
  } else if (opt_rollback_snap) {
    bufferlist bl, bl2;
    if (!imgname) {
      usage();
      err_exit(pool);
    }

    r = rados.exec(pool, md_oid, "rbd", "snap_list", bl, bl2);
    if (r < 0) {
      cerr << "list_snaps failed: " << strerror(-r) << std::endl;
      err_exit(pool);
    }

    uint32_t num_snaps;
    bufferlist::iterator iter = bl2.begin();
    SnapContext snapc;
    ::decode(snapc.seq, iter);
    ::decode(num_snaps, iter);
    uint64_t snapid = 0;
    for (uint32_t i=0; i < num_snaps; i++) {
      uint64_t id, image_size;
      string s;
      ::decode(id, iter);
      ::decode(image_size, iter);
      ::decode(s, iter);
      if (s.compare(snapname) == 0)
        snapid = id;
      snapc.snaps.push_back(id);
    }
    if (!snapid) {
      cerr << "snapshot not found: " << snapname << std::endl;
      err_exit(pool);
    }

    if (!snapc.is_valid()) {
      cerr << "image snap context is invalid! can't rollback" << std::endl;
      err_exit(pool);
    }

    struct rbd_obj_header_ondisk header;
    r = read_header(pool, md_oid, &header);
    if (r < 0) {
      cerr << "error reading header: " << md_oid << ": " << strerror(-r) << std::endl;
      err_exit(pool);
    }
    r = rollback_image(pool, &header, snapc, snapid);
    if (r < 0) {
      cerr << "error during rollback: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
  } else if (opt_export) {
    r = do_export(pool, md_oid, path);
    if (r < 0) {
      cerr << "export error: " << strerror(-r) << std::endl;
      err_exit(pool);
    }
  }

  rados.close_pool(pool);
  rados.shutdown(); 
  return 0;
}

