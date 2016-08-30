// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SMRAllocator.h"
#include "bluestore_types.h"
#include "BlueStore.h"

#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "smralloc "
  
SMRAllocator::SMRAllocator(int64_t size, string bdev_path)
{
  int64_t block_size = g_conf->bdev_block_size;
  dout(10) << __func__ << "size of block " << block_size << dendl;  
  dout(10) << __func__ << "size sent " << size << dendl;  
  
  size_t numZones = zbc_get_zones(bdev_path);
    
}

SMRAllocator::~SMRAllocator()
{
}

void SMRAllocator::_insert_free(uint64_t offset, uint64_t len)
{
  std::lock_guard<std::mutex> l(lock);
  num_free += len;
}
