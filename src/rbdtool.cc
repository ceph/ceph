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

#include "include/rbd_types.h"

Rados rados;
pool_t pool;

void usage()
{
  cout << "usage: rbdtool [-n <auth user>] [-p|--pool <name>] <cmd>\n"
       << "where 'pool' is a rados pool name (default is 'rbd') and 'cmd' is one of:\n"
       << "\t--list    list rbd images\n"
       << "\t--info    show information about image size, striping, etc.\n"
       << "\t--create <image name> --size <size in MB>\n"
       << "\t          create an image\n"
       << "\t--resize <image name> --size <new size in MB>\n"
       << "\t          resize (expand or contract) image\n"
       << "\t--delete <image name>\n"
       << "\t          delete an image" << std::endl;
  exit(1);
}


static void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
			    size_t size, int order)
{
  memset(&ondisk, 0, sizeof(ondisk));

  memcpy(&ondisk.text, rbd_text, sizeof(rbd_text));
  memcpy(&ondisk.signature, rbd_signature, sizeof(rbd_signature));
  memcpy(&ondisk.version, rbd_version, sizeof(rbd_version));

  ondisk.image_size = size;
  if (order)
    ondisk.obj_order = order;
  else
    ondisk.obj_order = RBD_DEFAULT_OBJ_ORDER;
  ondisk.crypt_type = RBD_CRYPT_NONE;
  ondisk.comp_type = RBD_COMP_NONE;
  ondisk.snap_seq = 0;
  ondisk.snap_count = 0;
  ondisk.snap_names_len = 0;
}

void print_header(char *imgname, rbd_obj_header_ondisk *header)
{
  cout << "rbd image '" << imgname << "':\n"
       << "\tsize " << prettybyte_t(header->image_size) << " in "
       << (header->image_size >> header->obj_order) << " objects\n"
       << "\torder " << (int)header->obj_order << " (" << prettybyte_t(1 << (header->obj_order)) << " objects)"
       << std::endl;
}

void trim_image(const char *imgname, rbd_obj_header_ondisk *header, __u64 newsize)
{
  __u64 size = header->image_size;
  __u64 numseg = size >> header->obj_order;
  __u64 start = newsize >> header->obj_order;

  cout << "trimming image data from " << numseg << " to " << start << " objects..." << std::endl;
  for (__u64 i=start; i<numseg; i++) {
    char o[RBD_MAX_SEG_NAME_SIZE];
    sprintf(o, "%s.%012llx", imgname, (unsigned long long)i);
    string oid = o;
    rados.remove(pool, oid);
    if ((i & 127) == 0) {
      cout << "\r\t" << i << "/" << numseg;
      cout.flush();
    }
  }
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  DEFINE_CONF_VARS(usage);
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, "rbdtool", false, true);

  bool opt_create = false, opt_delete = false, opt_list = false, opt_info = false, opt_resize = false;
  char *poolname = (char *)"rbd";
  __u64 size = 0;
  int order = 0;
  char *imgname;

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
    } else if (CONF_ARG_EQ("pool", 'p')) {
      CONF_SAFE_SET_ARG_VAL(&poolname, OPT_STR);
    } else if (CONF_ARG_EQ("object", 'n')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
    } else if (CONF_ARG_EQ("size", 's')) {
      CONF_SAFE_SET_ARG_VAL(&size, OPT_LONGLONG);
      size <<= 20; // MB -> bytes
    } else if (CONF_ARG_EQ("order", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&order, OPT_INT);
    } else 
      usage();
  }

  if (!opt_create && !opt_delete && !opt_list && !opt_info && !opt_resize)
    usage();


  if (rados.initialize(argc, argv) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  string md_oid = imgname;
  md_oid += RBD_SUFFIX;
  string dir_oid = RBD_DIRECTORY;

  int r = rados.open_pool(poolname, &pool);
  if (r < 0) {
    cerr << "error opening pool (err=" << r << ")" << std::endl;
    exit(1);
  }

  if (opt_list) {
    bufferlist bl;
    r = rados.read(pool, dir_oid, 0, bl, 0);
    if (r < 0) {
      cerr << "error reading rbd directory object " << dir_oid << ": " << strerror(-r) << std::endl;
      exit(1);
    }
    bufferlist::iterator p = bl.begin();
    bufferlist header;
    map<nstring,bufferlist> m;
    ::decode(header, p);
    ::decode(m, p);
    for (map<nstring,bufferlist>::iterator q = m.begin(); q != m.end(); q++)
      cout << q->first << std::endl;
  }
  else if (opt_create) {
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

    // make sure it doesn't already exist
    r = rados.stat(pool, md_oid, NULL, NULL);
    if (r == 0) {
      cerr << "rbd image header " << md_oid << " already exists" << std::endl;
      exit(1);
    }

    struct rbd_obj_header_ondisk header;
    init_rbd_header(header, size, order);
    
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
      exit(1);
    }

    cout << "creating rbd image..." << std::endl;
    r = rados.write(pool, md_oid, 0, bl, bl.length());
    if (r < 0) {
      cerr << "error writing header: " << strerror(-r) << std::endl;
      exit(1);
    }
    cout << "done." << std::endl;
  }
  else if (opt_info || opt_delete || opt_resize) {
    // read header
    struct rbd_obj_header_ondisk header;
    bufferlist bl;
    r = rados.read(pool, md_oid, 0, bl, sizeof(header));
    if (r < 0) {
      cerr << "error reading header " << md_oid << ": " << strerror(-r) << std::endl;
    } else {
      bufferlist::iterator p = bl.begin();
      p.copy(sizeof(header), (char *)&header);
    }

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
	exit(1);
      }
    } else {
      if (r < 0)
	exit(1);
      
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
	    exit(1);
	  }
	}
      }
    }
      
    cout << "done." << std::endl;
  }

  rados.close_pool(pool);
  rados.shutdown(); 
  return 0;
}

