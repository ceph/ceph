// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FreelistManager.h"
#include "ExtentFreelistManager.h"
#include "BitmapFreelistManager.h"

FreelistManager *FreelistManager::create(
  string type,
  KeyValueDB *kvdb,
  string prefix)
{
  if (type == "extent")
    return new ExtentFreelistManager(kvdb, prefix);
  if (type == "bitmap")
    return new BitmapFreelistManager(kvdb, prefix);
  return NULL;
}
