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
  return NULL;
}

bool KeyValueDB::test_init(const string& type, const string& dir)
{
  if (type == "leveldb"){
    return LevelDBStore::_test_init(dir);
  }
  return false;
}
