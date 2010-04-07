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
  cout << " usage: [--create <image name> --pool=<name> --size=size]" << std::endl;
  exit(1);
}


static void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
			    size_t size)
{
  memset(&ondisk, 0, sizeof(ondisk));

  memcpy(&ondisk.text, rbd_text, sizeof(rbd_text));
  memcpy(&ondisk.signature, rbd_signature, sizeof(rbd_signature));
  memcpy(&ondisk.version, rbd_version, sizeof(rbd_version));

  ondisk.image_size = size;
  ondisk.obj_order = RBD_DEFAULT_OBJ_ORDER;
  ondisk.crypt_type = RBD_CRYPT_NONE;
  ondisk.comp_type = RBD_COMP_NONE;
  ondisk.snap_seq = 0;
  ondisk.snap_count = 0;
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  DEFINE_CONF_VARS(usage);
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, "rbdtool", false, true);

  bool opt_create = false;
  char *poolname;
  size_t size;
  char *imgname;


  Rados rados;
  if (rados.initialize(argc, argv) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("create", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
      opt_create = true;
    } else if (CONF_ARG_EQ("pool", 'p')) {
      CONF_SAFE_SET_ARG_VAL(&poolname, OPT_STR);
    } else if (CONF_ARG_EQ("object", 'n')) {
      CONF_SAFE_SET_ARG_VAL(&imgname, OPT_STR);
    } else if (CONF_ARG_EQ("size", 's')) {
      CONF_SAFE_SET_ARG_VAL(&size, OPT_LONGLONG);
    } else 
      usage();
  }

  if (!opt_create)
    usage();

  bufferlist bl;

  string imgmd_str = imgname;
  imgmd_str += RBD_SUFFIX;
  object_t md_oid(imgmd_str.c_str());

  rados_pool_t pool;

  int r = rados.open_pool(poolname, &pool);
  if (r < 0) {
    cerr << "error opening pool (err=" << r << ")" << std::endl;
    exit(1);
  }
  struct rbd_obj_header_ondisk header;
  init_rbd_header(header, size);

  bl.append((const char *)&header, sizeof(header));

  r = rados.write(pool, md_oid, 0, bl, bl.length());
  if (r < 0) {
    cerr << "error writing header (err=" << r << ")" << std::endl;
    exit(1);
  }

  rados.close_pool(pool);
  rados.shutdown(); 

  cout << "created rbd object" << std::endl;

  return 0;
}

