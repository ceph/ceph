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
#include "osdc/librados.h"
#include "include/byteorder.h"


#include <iostream>

#include <stdlib.h>
#include <time.h>
#include <sys/types.h>

#include "include/rbd_types.h"


void usage()
{
  cout << " usage: [--list] [--create <image name> --size <size in MB>] [--delete <img name>] [-p|--pool=<name>]" << std::endl;
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
}

void print_header(char *imgname, rbd_obj_header_ondisk *header)
{
  cout << "rbd image '" << imgname << "':\n"
       << "\tsize " << kb_t(header->image_size >> 10) << " in "
       << (header->image_size >> header->obj_order) << " objects\n"
       << "\torder " << (int)header->obj_order << " (" << kb_t(1 << (header->obj_order-10)) << " objects)"
       << std::endl;
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  DEFINE_CONF_VARS(usage);
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, "rbdtool", false, true);

  bool opt_create = false, opt_delete = false, opt_list = false;
  char *poolname;
  __u64 size = 0;
  int order = 0;
  char *imgname;


  Rados rados;
  if (rados.initialize(argc, argv) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("list", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&opt_list, OPT_BOOL);
    } else if (CONF_ARG_EQ("create", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
      opt_create = true;
    } else if (CONF_ARG_EQ("delete", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
      opt_delete = true;
    } else if (CONF_ARG_EQ("pool", 'p')) {
      CONF_SAFE_SET_ARG_VAL(&poolname, OPT_STR);
    } else if (CONF_ARG_EQ("object", 'n')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
    } else if (CONF_ARG_EQ("size", 's')) {
      CONF_SAFE_SET_ARG_VAL(&size, OPT_LONGLONG);
    } else if (CONF_ARG_EQ("order", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&order, OPT_INT);
    } else 
      usage();
  }

  if (!opt_create && !opt_delete && !opt_list)
    usage();

  string imgmd_str = imgname;
  imgmd_str += RBD_SUFFIX;
  object_t md_oid(imgmd_str.c_str());
  object_t dir_oid(RBD_DIRECTORY);

  rados_pool_t pool;

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
  if (opt_create) {
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
    size <<= 20; // MB -> bytes

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
  if (opt_delete) {
    struct rbd_obj_header_ondisk header;
    bufferlist bl;
    r = rados.read(pool, md_oid, 0, bl, sizeof(header));
    if (r < 0) {
      cerr << "error reading header " << md_oid << ": " << strerror(-r) << std::endl;
      exit(1);
    } else {
      bufferlist::iterator p = bl.begin();
      p.copy(sizeof(header), (char *)&header);
      __u64 size = header.image_size;
      __u64 numseg = size >> header.obj_order;
      print_header(imgname, &header);
      
      cout << "removing data..." << std::endl;
      for (__u64 i=0; i<numseg; i++) {
	char o[RBD_MAX_SEG_NAME_SIZE];
	sprintf(o, "%s.%012llx", imgname, (unsigned long long)i);
	object_t oid(o);
	rados.remove(pool, oid);
	if ((i & 127) == 0) {
	  cout << "\r\t" << i << "/" << numseg;
	  cout.flush();
	}
      }
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

    cout << "done." << std::endl;
  }

  rados.close_pool(pool);
  rados.shutdown(); 
  return 0;
}

