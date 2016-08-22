// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SMRAllocator.h"
#include "bluestore_types.h"
#include "BlueStore.h"

#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "smralloc "
  
SMRAllocator::SMRAllocator()
  : num_free(0),
    num_uncommitted(0),
    num_committing(0),
    num_reserved(0),
    free(10),
    last_alloc(0) 
{
  int64_t block_size = g_conf->bdev_block_size;
  dout(10) << __func__ << "size of block " << block_size << dendl;  
}

SMRAllocator::SMRAllocator(int64_t size)
  : num_free(0),
    num_uncommitted(0),
    num_committing(0),
    num_reserved(0),
    free(10),
    last_alloc(0) 
{
  int64_t block_size = g_conf->bdev_block_size;
  dout(10) << __func__ << "size of block " << block_size << dendl;  
  dout(10) << __func__ << "size sent " << size << dendl;  
}

SMRAllocator::~SMRAllocator()
{
}

void SMRAllocator::_insert_free(uint64_t offset, uint64_t len)
{
  // TODO logic to add SMR extent information here
  std::lock_guard<std::mutex> l(lock);
  
  num_free += len;
}



