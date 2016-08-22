// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FreelistManager.h"
#include "ExtentFreelistManager.h"
#include "BitmapFreelistManager.h"
#include "SMRFreelistManager.h"

#include "common/debug.h"

#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "smr_freelist "

FreelistManager *FreelistManager::create(
  string type,
  KeyValueDB *kvdb,
  string prefix,
  string bdev)
{
  // a bit of a hack... we hard-code the prefixes here.  we need to
  // put the freelistmanagers in different prefixes because the merge
  // op is per prefix, has to done pre-db-open, and we don't know the
  // freelist type until after we open the db.
  assert(prefix == "B");
  if (type == "extent")
    return new ExtentFreelistManager(kvdb, "B");
  if (type == "bitmap")
    return new BitmapFreelistManager(kvdb, "B", "b");
  if (type == "smr"){
    dout(10) << __func__ << "Path sent to Freelist Manager = " << bdev << dendl;
    return new SMRFreelistManager(kvdb, "B", bdev);
  }
  return NULL;
}

void FreelistManager::setup_merge_operators(KeyValueDB *db)
{
  BitmapFreelistManager::setup_merge_operator(db, "b");
}
