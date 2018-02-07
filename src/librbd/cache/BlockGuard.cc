// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/BlockGuard.h"
#include "common/dout.h"
#include <algorithm>
#include <unordered_map>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::BlockGuard: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

BlockGuard::BlockGuard(CephContext *cct, uint64_t cache_size,
                       uint32_t block_size)
  : m_cct(cct), m_max_blocks(cache_size%block_size?cache_size/block_size+1:cache_size/block_size),
    m_block_size(block_size),
    m_lock("librbd::cache::BlockGuard::m_lock"){
}

void BlockGuard::create_block_ios(IOType io_type,
                                  const ImageExtents &image_extents,
                                  BlockIOs *block_ios,
                                  C_BlockRequest *block_request) {
  typedef std::unordered_map<uint64_t, BlockIO> BlockToBlockIOs;

  BlockToBlockIOs block_to_block_ios;
  Block *block_info;
  uint64_t buffer_offset = 0;
  for (auto &extent : image_extents) {
    uint64_t image_offset = extent.first;
    uint64_t image_length = extent.second;
    while (image_length > 0) {
      uint64_t block = image_offset / m_block_size;
      uint32_t block_offset = image_offset % m_block_size;
      uint32_t block_end_offset = std::min<uint64_t>(
        block_offset + image_length, m_block_size);
      uint32_t block_length = block_end_offset - block_offset;

      // TODO block extent merging(?)
      block_info = m_block_map->find_block(block);
      assert(block_info != nullptr);
      auto &block_io = block_to_block_ios[block];
      block_io.block_info = block_info;
      
      block_io.extents.emplace_back(buffer_offset, block_offset, block_length);

      buffer_offset += block_length;
      image_offset += block_length;
      image_length -= block_length;
    }
  }

  for (auto &pair : block_to_block_ios) {
    block_request->add_request();
    pair.second.io_type = io_type;
    pair.second.partial_block = (
      pair.second.extents.size() != 1 ||
      pair.second.extents.front().block_offset != 0 ||
      pair.second.extents.front().block_length != m_block_size);
    pair.second.block_request = block_request;

    ldout(m_cct, 20) << "block_io=[" << pair.second << "]" << dendl;
    block_ios->emplace_back(std::move(pair.second));
  }
}

BlockGuard::C_BlockRequest::C_BlockRequest(Context *on_finish)
  : lock("librbd::cache::BlockGuard::C_BlockRequest::lock"),
    on_finish(on_finish) {
}

void BlockGuard::C_BlockRequest::activate() {
  int r;
  {
    Mutex::Locker locker(lock);
    activated = true;
    if (pending_requests > 0) {
      return;
    }
    r = ret_val;
  }
  complete(r);
}

void BlockGuard::C_BlockRequest::fail(int r) {
  {
    Mutex::Locker locker(lock);
    if (ret_val >= 0 && r < 0) {
      ret_val = r;
    }
  }
  activate();
}

void BlockGuard::C_BlockRequest::add_request() {
  Mutex::Locker locker(lock);
  ++pending_requests;
}

void BlockGuard::C_BlockRequest::complete_request(int r) {
  {
    Mutex::Locker locker(lock);
    if (ret_val >= 0 && r < 0) {
      ret_val = r;
    }

    if (--pending_requests > 0 || !activated) {
      return;
    }
    r = ret_val;
  }
  complete(r);
}

void BlockGuard::C_BlockRequest::finish(int r) {
  on_finish->complete(r);
}

} // namespace cache
} // namespace librbd
