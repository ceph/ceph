// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FreelistManager.h"
#include "ExtentFreelistManager.h"

FreelistManager *FreelistManager::create(string type)
{
  if (type == "extent")
    return new ExtentFreelistManager;
  return NULL;
}
