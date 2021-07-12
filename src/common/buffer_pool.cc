// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "buffer_pool.h"

#include "include/ceph_assert.h"

std::array<BufferPool<void>, PooledObject::POOLED_OBJECT_MAX_SIZE> \
        PooledObject::object_pools;

void* PooledObject::operator new(std::size_t sz) {
  if (sz >= POOLED_OBJECT_MAX_SIZE) {
    return ::operator new(sz);
  }
  auto ptr = object_pools[sz].get();
  return ptr ? ptr : ::operator new(sz);
}

void PooledObject::operator delete(void* ptr, std::size_t sz) {
  if (sz >= POOLED_OBJECT_MAX_SIZE) {
    ::operator delete(ptr);
    return;
  }
  object_pools[sz].put(ptr);
}
