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

#include "osdc/librados.h"

#include <iostream>

#include <stdlib.h>
#include <time.h>
#include <errno.h>

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
};

typedef __u32 ACLFlags;

void encode(const ACLID& id, bufferlist& bl)
{
  bl.append((const char *)id.id, ID_SIZE);
}

void decode(ACLID& id, bufferlist::iterator& iter)
{
  iter.copy(ID_SIZE, (char *)id.id);
}

inline bool operator<(const ACLID& l, const ACLID& r)
{
  return (memcmp(&l, &r, ID_SIZE) > 0);
}

struct ACLPair {
  ACLID id;
  ACLFlags flags;
};

class ObjectACLs {
  map<ACLID, ACLFlags> acls_map;

public:

  void encode(bufferlist& bl) const {
    ::encode(acls_map, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(acls_map, bl);
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
  nstring name;
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
  if (rados.initialize(0, NULL) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  time_t tm;
  bufferlist bl, bl2;
  char buf[128];

  time(&tm);
  snprintf(buf, 128, "%s", ctime(&tm));
  bl.append(buf, strlen(buf));

  object_t oid("bar");

  rados_pool_t pool;
  int r = rados.open_pool("data", &pool);
  cout << "open pool result = " << r << " pool = " << pool << std::endl;

  ACLID id;

  sprintf(id.id, "%.16x", 0x1234);
  cout << "id=" << id.id << std::endl;

  r = rados.exec(pool, oid, "acl", "get", bl, bl2);
  cout << "exec returned " << r << " len=" << bl2.length() << std::endl;
  ObjectACLs oa;
  if (r >= 0) {
    bufferlist::iterator iter = bl2.begin();
    oa.decode(iter);
  }

  oa.set_acl(id, ACL_RD);
  bl.clear();
  oa.encode(bl);
  r = rados.exec(pool, oid, "acl", "set", bl, bl2);

  const unsigned char *md5 = (const unsigned char *)bl2.c_str();
  char md5_str[bl2.length()*2 + 1];
  buf_to_hex(md5, bl2.length(), md5_str);
  cout << "md5 result=" << md5_str << std::endl;

  int size = rados.read(pool, oid, 0, bl2, 128);
  cout << "read result=" << bl2.c_str() << std::endl;
  cout << "size=" << size << std::endl;

#if 0
  Rados::ListCtx ctx;
  int entries;
  do {
    list<object_t> vec;
    r = rados.list(pool, 2, vec, ctx);
    entries = vec.size();
    cout << "list result=" << r << " entries=" << entries << std::endl;
    list<object_t>::iterator iter;
    for (iter = vec.begin(); iter != vec.end(); ++iter) {
      cout << *iter << std::endl;
    }
  } while (entries);
#endif
#if 0
  r = rados.remove(pool, oid);
  cout << "remove result=" << r << std::endl;
  rados.close_pool(pool);
#endif

  return 0;
}

