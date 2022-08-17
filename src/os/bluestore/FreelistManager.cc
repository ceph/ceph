// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FreelistManager.h"
#include "BitmapFreelistManager.h"
#ifdef HAVE_LIBZBD
#include "ZonedFreelistManager.h"
#endif
#include "FileFreelistManager.h"

FreelistManager *FreelistManager::create(
  ObjectStore* store,
  std::string type,
  std::string prefix)
{
  // a bit of a hack... we hard-code the prefixes here.  we need to
  // put the freelistmanagers in different prefixes because the merge
  // op is per prefix, has to done pre-db-open, and we don't know the
  // freelist type until after we open the db.
  ceph_assert(prefix == "B");
  if (type == "bitmap") {
    return new BitmapFreelistManager(store, "B", "b");
  }
  if (type == "null") {
    return new FileFreelistManager(store);
  }
#ifdef HAVE_LIBZBD
  // With zoned drives there is only one FreelistManager implementation that we
  // can use, and we also know if a drive is zoned right after opening it
  // (BlueStore::_open_bdev).  Hence, we set freelist_type to "zoned" whenever
  // we open the device and it turns out to be is zoned.  We ignore |prefix|
  // passed to create and use the prefixes defined for zoned devices at the top
  // of BlueStore.cc.
  if (type == "zoned")
    return new ZonedFreelistManager(store, "Z", "z");
#endif
  return NULL;
}

void FreelistManager::setup_merge_operators(KeyValueDB *db,
					    const std::string& type)
{
#ifdef HAVE_LIBZBD
  if (type == "zoned")
    ZonedFreelistManager::setup_merge_operator(db, "z");
  else
#endif
    BitmapFreelistManager::setup_merge_operator(db, "b");
}

bool FreelistManager::enumerate(KeyValueDB *kvdb,
				std::function<bool(uint64_t offset, uint64_t length)> next_extent)
{
  bool result = true;
  enumerate_reset();
  uint64_t offset;
  uint64_t length;
  while (enumerate_next(kvdb, &offset, &length) == true) {
    if (next_extent(offset, length) == false) {
      result = false;
      break;
    }
  }
  enumerate_reset();
  return result;
}
