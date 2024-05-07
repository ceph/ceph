// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FreelistManager.h"
#include "BitmapFreelistManager.h"

FreelistManager *FreelistManager::create(
  CephContext* cct,
  std::string type,
  std::string prefix)
{
  // a bit of a hack... we hard-code the prefixes here.  we need to
  // put the freelistmanagers in different prefixes because the merge
  // op is per prefix, has to done pre-db-open, and we don't know the
  // freelist type until after we open the db.
  ceph_assert(prefix == "B");
  if (type == "bitmap") {
    return new BitmapFreelistManager(cct, "B", "b");
  }
  if (type == "null") {
    // use BitmapFreelistManager with the null option to stop allocations from going to RocksDB
    auto *fm = new BitmapFreelistManager(cct, "B", "b");
    fm->set_null_manager();
    return fm;
  }

  return NULL;
}

void FreelistManager::setup_merge_operators(KeyValueDB *db,
					    const std::string& type)
{
  BitmapFreelistManager::setup_merge_operator(db, "b");
}
