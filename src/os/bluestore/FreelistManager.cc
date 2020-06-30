// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FreelistManager.h"
#include "BitmapFreelistManager.h"
#ifdef HAVE_LIBZBC
#include "ZonedFreelistManager.h"
#endif

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
  if (type == "bitmap")
    return new BitmapFreelistManager(cct, "B", "b");

#ifdef HAVE_LIBZBC
  // With zoned drives there is only one FreelistManager implementation that we
  // can use, and we also know if a drive is zoned right after opening it
  // (BlueStore::_open_bdev).  Hence, we set freelist_type to "zoned" whenever
  // we open the device and it turns out to be is zoned.  We ignore |prefix|
  // passed to create and use the prefixes defined for zoned devices at the top
  // of BlueStore.cc.
  if (type == "zoned")
    return new ZonedFreelistManager(cct, "Z", "z");
#endif

  return NULL;
}

void FreelistManager::setup_merge_operators(KeyValueDB *db,
					    const std::string& type)
{
#ifdef HAVE_LIBZBC
  if (type == "zoned")
    ZonedFreelistManager::setup_merge_operator(db, "z");
  else
#endif
    BitmapFreelistManager::setup_merge_operator(db, "b");
}
