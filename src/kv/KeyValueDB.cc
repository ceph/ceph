// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "KeyValueDB.h"
#include "LevelDBStore.h"
#ifdef HAVE_LIBROCKSDB
#include "RocksDBStore.h"
#endif
#ifdef HAVE_KINETIC
#include "KineticStore.h"
#endif

KeyValueDB *KeyValueDB::create(CephContext *cct, const string& type,
			       const string& dir,
			       void *p)
{
  if (type == "leveldb") {
    return new LevelDBStore(cct, dir);
  }
#ifdef HAVE_KINETIC
  if (type == "kinetic" &&
      cct->check_experimental_feature_enabled("kinetic")) {
    return new KineticStore(cct);
  }
#endif
#ifdef HAVE_LIBROCKSDB
  if (type == "rocksdb" &&
      cct->check_experimental_feature_enabled("rocksdb")) {
    return new RocksDBStore(cct, dir, p);
  }
#endif
  return NULL;
}

int KeyValueDB::test_init(const string& type, const string& dir)
{
  if (type == "leveldb") {
    return LevelDBStore::_test_init(dir);
  }
#ifdef HAVE_KINETIC
  if (type == "kinetic") {
    return KineticStore::_test_init(g_ceph_context);
  }
#endif
#ifdef HAVE_LIBROCKSDB
  if (type == "rocksdb") {
    return RocksDBStore::_test_init(dir);
  }
#endif
  return -EINVAL;
}
