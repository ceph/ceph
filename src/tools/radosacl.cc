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

#include <stdlib.h>
#include <time.h>
#include <errno.h>

#include <iostream> // for std::cerr

#include "include/types.h"
#include "include/rados/librados.hpp"

using namespace std;
using namespace librados;

void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  str[0] = '\0';
  for (int i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}


#define ID_SIZE 8

#define ACL_RD	0x1
#define ACL_WR	0x2

struct ACLID {
  char id[ID_SIZE + 1];

  void encode(bufferlist& bl) const {
    bl.append((const char *)id, ID_SIZE);
  }
  void decode(bufferlist::const_iterator& iter) {
    iter.copy(ID_SIZE, (char *)id);
  }
};
WRITE_CLASS_ENCODER(ACLID)

typedef __u32 ACLFlags;


inline bool operator<(const ACLID& l, const ACLID& r)
{
  return (memcmp(&l, &r, ID_SIZE) < 0);
}

struct ACLPair {
  ACLID id;
  ACLFlags flags;
};

class ObjectACLs {
  map<ACLID, ACLFlags> acls_map;

public:

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(acls_map, bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    using ceph::decode;
    decode(acls_map, bl);
  }

  int read_acl(ACLID& id, ACLFlags *flags);
  void set_acl(ACLID& id, ACLFlags flags);
};
WRITE_CLASS_ENCODER(ObjectACLs)

int ObjectACLs::read_acl(ACLID& id, ACLFlags *flags)
{
  if (!flags)
    return -EINVAL;

  map<ACLID, ACLFlags>::iterator iter = acls_map.find(id);

  if (iter == acls_map.end())
    return -ENOENT;

  *flags = iter->second;

  return 0;
}

void ObjectACLs::set_acl(ACLID& id, ACLFlags flags)
{
  acls_map[id] = flags;
}



class ACLEntity
{
  string name;
  map<ACLID, ACLEntity> groups;
};

typedef map<ACLID, ACLEntity> tACLIDEntityMap;

static map<ACLID, ACLEntity> users;
static map<ACLID, ACLEntity> groups;

void get_user(ACLID& aclid, ACLEntity *entity)
{
  //users.find(aclid);
}





int main(int argc, const char **argv) 
{
  Rados rados;
  if (rados.init(NULL) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }
  if (rados.conf_read_file(NULL)) {
     cerr << "couldn't read Ceph configuration file!" << std::endl;
     exit(1);
  }
  if (rados.connect() < 0) {
     cerr << "couldn't connect to cluster!" << std::endl;
     exit(1);
  }

  time_t tm;
  bufferlist bl, bl2;
  char buf[128];

  time(&tm);
  snprintf(buf, 128, "%s", ctime(&tm));
  bl.append(buf, strlen(buf));

  const char *oid = "bar";

  IoCtx io_ctx;
  int r = rados.ioctx_create("data", io_ctx);
  cout << "open io_ctx result = " << r << " pool = " << io_ctx.get_pool_name() << std::endl;

  ACLID id;

  snprintf(id.id, sizeof(id.id), "%.8x", 0x1234);
  cout << "id=" << id.id << std::endl;

  r = io_ctx.exec(oid, "acl", "get", bl, bl2);
  cout << "exec(acl get) returned " << r
       << " len=" << bl2.length() << std::endl;
  ObjectACLs oa;
  if (r >= 0) {
    auto iter = bl2.cbegin();
    oa.decode(iter);
  }

  oa.set_acl(id, ACL_RD);
  bl.clear();
  oa.encode(bl);
  r = io_ctx.exec(oid, "acl", "set", bl, bl2);
  cout << "exec(acl set) returned " << r
       << " len=" << bl2.length() << std::endl;

  const unsigned char *md5 = (const unsigned char *)bl2.c_str();
  char md5_str[bl2.length()*2 + 1];
  buf_to_hex(md5, bl2.length(), md5_str);
  cout << "md5 result=" << md5_str << std::endl;

  int size = io_ctx.read(oid, bl2, 128, 0);
  cout << "read result=" << bl2.c_str() << std::endl;
  cout << "size=" << size << std::endl;

  return 0;
}

