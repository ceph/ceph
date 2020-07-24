// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "KeyValueDB.h"
#ifdef WITH_LEVELDB
#include "LevelDBStore.h"
#endif
#ifdef HAVE_LIBROCKSDB
#include "RocksDBStore.h"
#endif

using std::map;
using std::string;

KeyValueDB *KeyValueDB::create(CephContext *cct, const string& type,
			       const string& dir,
			       map<string,string> options,
			       void *p)
{
#ifdef WITH_LEVELDB
  if (type == "leveldb") {
    return new LevelDBStore(cct, dir);
  }
#endif
#ifdef HAVE_LIBROCKSDB
  if (type == "rocksdb") {
    return new RocksDBStore(cct, dir, options, p);
  }
#endif
  return NULL;
}

int KeyValueDB::test_init(const string& type, const string& dir)
{
#ifdef WITH_LEVELDB
  if (type == "leveldb") {
    return LevelDBStore::_test_init(dir);
  }
#endif
#ifdef HAVE_LIBROCKSDB
  if (type == "rocksdb") {
    return RocksDBStore::_test_init(dir);
  }
#endif
  return -EINVAL;
}
