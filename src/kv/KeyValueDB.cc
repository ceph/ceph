// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "KeyValueDB.h"
#include "RocksDBStore.h"

using std::map;
using std::string;

KeyValueDB *KeyValueDB::create(CephContext *cct, const string& type,
			       const string& dir,
			       map<string,string> options,
			       void *p)
{
  if (type == "rocksdb") {
    return new RocksDBStore(cct, dir, options, p);
  }
  return NULL;
}

int KeyValueDB::test_init(const string& type, const string& dir)
{
  if (type == "rocksdb") {
    return RocksDBStore::_test_init(dir);
  }
  return -EINVAL;
}
