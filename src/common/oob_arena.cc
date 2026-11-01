// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/oob_arena.h"
#include "common/Formatter.h"
#include "common/deleter.h"

#include <algorithm>
#include <bit>
#include <cerrno>
#include <cstring>

#include <sys/mman.h>

namespace ceph::oob {

std::shared_ptr<Arena> Arena::create(uint64_t size, std::string* err)
{
  size = std::max<uint64_t>(MAX, size / MAX * MAX);
  // mapped, not taken from the heap: the heap allocator does not count
  // it, so the OSD's cache autotuner, which holds the heap to
  // osd_memory_target, does not shrink the caches by it. Aligned to
  // 2 MiB, so that the region can take huge pages where the daemon
  // allows them (thp; Ceph daemons turn them off by default): map 2 MiB
  // more than asked, and trim both ends.
  constexpr uint64_t align = 2 << 20;
  void* p = ::mmap(nullptr, size + align, PROT_READ | PROT_WRITE,
		   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (p == MAP_FAILED) {
    *err = "cannot map " + std::to_string(size) +
      " bytes for out-of-band transfers: " + std::strerror(errno);
    return nullptr;
  }
  const auto start = reinterpret_cast<uintptr_t>(p);
  const uintptr_t base = (start + align - 1) & ~uintptr_t(align - 1);
  if (base > start) {
    ::munmap(p, base - start);
  }
  ::munmap(reinterpret_cast<void*>(base + size), start + align - base);
#ifdef MADV_DONTFORK
  // the transports register the region with the NIC: a child of fork(),
  // such as the smartctl that device health runs, must not share its
  // pages, or the parent's next write copies a page that the NIC still
  // writes to the old one
  if (::madvise(reinterpret_cast<void*>(base), size, MADV_DONTFORK) < 0) {
    *err = "cannot keep the out-of-band memory from children: " +
      std::string(std::strerror(errno));
    ::munmap(reinterpret_cast<void*>(base), size);
    return nullptr;
  }
#endif
  return std::shared_ptr<Arena>(new Arena(reinterpret_cast<char*>(base),
					  size));
}

Arena::Arena(char* base, uint64_t size)
  : base_(base), size_(size)
{
  for (uint64_t ofs = 0; ofs < size; ofs += MAX) {
    free_[ORDERS - 1].insert(ofs);
  }
  st.size = size;
}

Arena::~Arena()
{
  ::munmap(base_, size_);
}

uint64_t Arena::round(uint64_t len)
{
  if (len > MAX) {
    return 0;
  }
  return std::max(MIN, std::bit_ceil(len));
}

unsigned Arena::order_of(uint64_t len)
{
  return static_cast<unsigned>(std::countr_zero(round(len) / MIN));
}

Arena::slice_t Arena::alloc(uint64_t len)
{
  const uint64_t need = round(len);
  std::lock_guard l(mtx);
  if (need == 0) {
    st.failed++;
    return {};
  }
  const unsigned k = order_of(need);
  unsigned j = k;
  while (j < ORDERS && free_[j].empty()) {
    j++;
  }
  if (j == ORDERS) {
    st.failed++;
    return {};
  }
  const uint64_t ofs = *free_[j].begin();
  free_[j].erase(free_[j].begin());
  // split down to the order asked for, keeping the lower halves
  while (j > k) {
    j--;
    free_[j].insert(ofs + (MIN << j));
  }
  st.in_use += need;
  st.allocs++;
  return {ofs, need};
}

void Arena::free(uint64_t offset, uint64_t len)
{
  uint64_t size = round(len);
  std::lock_guard l(mtx);
  if (auto it = shared_.find(offset); it != shared_.end()) {
    if (--it->second.refs > 0) {
      return;
    }
    size = it->second.len;
    shared_.erase(it);
  }
  if (size == 0) {
    return;
  }
  free_locked(offset, size);
}

Arena::slice_t Arena::alloc_shared(uint64_t len)
{
  const auto s = alloc(len);
  if (s) {
    std::lock_guard l(mtx);
    shared_[s.offset] = shared_t{s.len, 1};
  }
  return s;
}

ceph::buffer::ptr Arena::alloc_shared_ptr(uint64_t len)
{
  const auto s = alloc_shared(len);
  if (!s) {
    return {};
  }
  return ceph::buffer::ptr(ceph::buffer::claim_buffer(
    len, ptr(s), make_deleter([self = shared_from_this(), s] {
      self->free(s);
    })));
}

bool Arena::hold(const char* p, uint64_t n, slice_t* s)
{
  if (p < base_ || p >= base_ + size_) {
    return false;
  }
  const uint64_t ofs = static_cast<uint64_t>(p - base_);
  std::lock_guard l(mtx);
  auto it = shared_.upper_bound(ofs);
  if (it == shared_.begin()) {
    return false;
  }
  --it;
  if (ofs + n > it->first + it->second.len) {
    return false;
  }
  it->second.refs++;
  *s = slice_t{it->first, it->second.len};
  return true;
}

void Arena::free_locked(uint64_t offset, uint64_t size)
{
  st.in_use -= size;
  unsigned k = order_of(size);
  uint64_t ofs = offset;
  // join with the buddy while it is free too
  while (k + 1 < ORDERS) {
    const uint64_t buddy = ofs ^ (MIN << k);
    if (free_[k].erase(buddy) == 0) {
      break;
    }
    ofs = std::min(ofs, buddy);
    k++;
  }
  free_[k].insert(ofs);
}

char* Arena::reserve(uint64_t len)
{
  const uint64_t blocks = (len + MAX - 1) / MAX;
  if (blocks == 0) {
    return nullptr;
  }
  std::lock_guard l(mtx);
  // at least a block stays out of every run, for buffers: a reservation
  // that took the whole arena would leave every transfer to stage
  // nowhere, and go inline
  if (st.reserved + blocks * MAX >= size_) {
    return nullptr;
  }
  auto& top = free_[ORDERS - 1];
  // the highest run, so that buffers stay at the start
  for (auto it = top.rbegin(); it != top.rend(); ++it) {
    const uint64_t last = *it;
    if (last + MAX < blocks * MAX) {
      break;
    }
    const uint64_t first = last + MAX - blocks * MAX;
    uint64_t n = 0;
    while (n < blocks && top.count(first + n * MAX)) {
      n++;
    }
    if (n == blocks) {
      for (uint64_t i = 0; i < blocks; i++) {
        top.erase(first + i * MAX);
      }
      st.reserved += blocks * MAX;
      return base_ + first;
    }
  }
  return nullptr;
}

void Arena::unreserve(char* p, uint64_t len)
{
  const uint64_t blocks = (len + MAX - 1) / MAX;
  if (!p || blocks == 0) {
    return;
  }
  const uint64_t first = static_cast<uint64_t>(p - base_);
  std::lock_guard l(mtx);
  for (uint64_t i = 0; i < blocks; i++) {
    free_[ORDERS - 1].insert(first + i * MAX);
  }
  st.reserved -= blocks * MAX;
}

Arena::stats_t Arena::stats() const
{
  std::lock_guard l(mtx);
  return st;
}

void Arena::dump(ceph::Formatter* f) const
{
  const auto s = stats();
  f->dump_unsigned("arena_size", s.size);
  f->dump_unsigned("arena_in_use", s.in_use);
  f->dump_unsigned("arena_reserved", s.reserved);
  f->dump_unsigned("arena_allocs", s.allocs);
  f->dump_unsigned("arena_failed", s.failed);
}

} // namespace ceph::oob
