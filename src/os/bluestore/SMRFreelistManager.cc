// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SMRFreelistManager.h"
#include "kv/KeyValueDB.h"
#include "kv.h"
#include <string>
#include "libzbc/zbc.h"
#include "libzbc/lib/zbc_tools.h"

#include "common/debug.h"

#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "freelist "

SMRFreelistManager:: SMRFreelistManager(KeyValueDB *kvdb, string prefix, string bdev)
 : kvdb(kvdb),
meta_prefix(prefix)
{
  m_num_zones = zbc_get_zones(bdev.c_str());
  m_num_random = zbc_get_random_zones(bdev.c_str());
  assert(m_num_zones);
}

int SMRFreelistManager::init()
{
  dout(1) << __func__ << dendl;

}
