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

bool KeyValueDB::restore_backup(CephContext *cct,
                                const std::string &type,
                                const std::string &path,
                                const std::string &backup_location,
                                const std::string &version)
{
  if (type == "rocksdb") {
    return RocksDBStore::restore_backup(cct, path, backup_location, version);
  }
  return false;
}

int KeyValueDB::test_init(const string& type, const string& dir)
{
  if (type == "rocksdb") {
    return RocksDBStore::_test_init(dir);
  }
  return -EINVAL;
}
