// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "KeyValueDB.h"
#include "LevelDBStore.h"

KeyValueDB *KeyValueDB::create(CephContext *cct, const string& type,
			       const string& dir)
{
  if (type == "leveldb") {
    return new LevelDBStore(cct, dir);
  }
#ifdef HAVE_KINETIC
  if (kv_type == KV_TYPE_KINETIC) {
    store = new KineticStore(g_ceph_context);
  }
#endif
  return NULL;
}

int KeyValueDB::test_init(const string& type, const string& dir)
{
  if (type == "leveldb"){
    return LevelDBStore::_test_init(dir);
  }
#ifdef HAVE_KINETIC
  if (kv_type == KV_TYPE_KINETIC) {
    return 0;
  }
#endif
  return -EINVAL;
}
