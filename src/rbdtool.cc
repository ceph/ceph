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

void usage()
{
  cout << " usage: [--create --pool=<name> --object=<name> --size=size]" << std::endl;
  exit(1);
}

struct rbd_obj_header_ondisk {
	char text[64];
	char signature[4];
	char version[8];
	uint64_t image_size;
	uint8_t obj_order;
	uint8_t crypt_type;
	uint8_t comp_type;
	uint64_t snap_seq;
	uint16_t snap_count;
	uint64_t snap_id[0];
} __attribute__((packed));

static const char rbd_text[] = "<<< Rados Block Device Image >>>\n";
static const char rbd_signature[] = "RBD";
static const char rbd_version[] = "001.000";

static void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
			    size_t size)
{
	memset(&ondisk, 0, sizeof(ondisk));

	memcpy(&ondisk.text, rbd_text, sizeof(rbd_text));
	memcpy(&ondisk.signature, rbd_signature, sizeof(rbd_signature));
	memcpy(&ondisk.version, rbd_version, sizeof(rbd_version));

	ondisk.image_size = mswab64(size);
	ondisk.obj_order = 22;
	ondisk.crypt_type = 0;
	ondisk.comp_type = 0;
	ondisk.snap_count = mswab16(0);
}

int main(int argc, const char **argv) 
{
  bool opt_create = false;
  char *poolname;
  size_t size;
  char *objname;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  DEFINE_CONF_VARS(usage);
  common_init(args, "rbdtool", false, false);

  Rados rados;
  if (rados.initialize(argc, argv) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("create", 'c')) {
      CONF_SAFE_SET_ARG_VAL(&opt_create, OPT_BOOL);
    } else if (CONF_ARG_EQ("pool", 'p')) {
      CONF_SAFE_SET_ARG_VAL(&poolname, OPT_STR);
    } else if (CONF_ARG_EQ("object", 'n')) {
      CONF_SAFE_SET_ARG_VAL(&objname, OPT_STR);
    } else if (CONF_ARG_EQ("size", 's')) {
      CONF_SAFE_SET_ARG_VAL(&size, OPT_LONGLONG);
    } else 
      usage();
  }

  if (!opt_create)
    usage();

  bufferlist bl;

  string obj_str(objname);
  string objmd_str = obj_str;
  objmd_str.append(".rbd");

  object_t oid(obj_str.c_str());
  object_t md_oid(objmd_str.c_str());

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

