// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "KeyValueDB.h"
#ifdef WITH_LEVELDB
#include "LevelDBStore.h"
#endif
#include "MemDB.h"
#include "RocksDBStore.h"

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
  if (type == "rocksdb") {
    return new RocksDBStore(cct, dir, options, p);
  }
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
  if (type == "rocksdb") {
    return RocksDBStore::_test_init(dir);
  }
  if (type == "memdb") {
    return MemDB::_test_init(dir);
  }
  return -EINVAL;
}
