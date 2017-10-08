// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FreelistManager.h"
#include "BitmapFreelistManager.h"

FreelistManager *FreelistManager::create(
  CephContext* cct,
  string type,
  KeyValueDB *kvdb,
  string prefix_alloc,
  string prefix_alloc_bitmap)
{
  // a bit of a hack... we hard-code the prefixes here.  we need to
  // put the freelistmanagers in different prefixes because the merge
  // op is per prefix, has to done pre-db-open, and we don't know the
  // freelist type until after we open the db.
  if (type == "bitmap")
    return new BitmapFreelistManager(cct, kvdb, prefix_alloc, prefix_alloc_bitmap);
  return NULL;
}

void FreelistManager::setup_merge_operators(KeyValueDB *db)
{
  BitmapFreelistManager::setup_merge_operator(db, "b");
  BitmapFreelistManager::setup_merge_operator(db, "f");
}
