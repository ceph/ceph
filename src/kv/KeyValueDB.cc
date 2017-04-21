// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "KeyValueDB.h"
#ifdef WITH_LEVELDB
#include "LevelDBStore.h"
#endif
#include "MemDB.h"
#ifdef HAVE_LIBROCKSDB
#include "RocksDBStore.h"
#endif
#ifdef HAVE_KINETIC
#include "KineticStore.h"
#endif
#ifdef HAVE_LIBAIO
#include "os/bluestore/BlueStore.h"
#endif

KeyValueDB *KeyValueDB::create(CephContext *cct, const string& type,
			       const string& dir,
			       void *p)
{
#ifdef WITH_LEVELDB
  if (type == "leveldb") {
    return new LevelDBStore(cct, dir);
  }
#endif
#ifdef HAVE_KINETIC
  if (type == "kinetic" &&
      cct->check_experimental_feature_enabled("kinetic")) {
    return new KineticStore(cct);
  }
#endif
#ifdef HAVE_LIBROCKSDB
  if (type == "rocksdb") {
    return new RocksDBStore(cct, dir, p);
  }
#endif

#ifdef HAVE_LIBAIO
  if (type == "bluestore-kv") {
    // note: we'll leak this!  the only user is ceph-kvstore-tool and
    // we don't care.
    BlueStore *bluestore = new BlueStore(cct, dir);
    KeyValueDB *db = nullptr;
    int r = bluestore->start_kv_only(&db);
    if (r < 0)
      return nullptr;  // yes, we leak.
    return db;
  }
#endif
  if ((type == "memdb") && 
    cct->check_experimental_feature_enabled("memdb")) {
    return new MemDB(cct, dir, p);
  }
  return NULL;
}

int KeyValueDB::test_init(const string& type, const string& dir)
{
#ifdef WITH_LEVELDB
  if (type == "leveldb") {
    return LevelDBStore::_test_init(dir);
  }
#endif
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

  if (type == "memdb") {
    return MemDB::_test_init(dir);
  }
  return -EINVAL;
}
