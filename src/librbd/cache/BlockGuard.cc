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
  : m_cct(cct), m_max_blocks(cache_size%block_size?cache_size/block_size+1:cache_size/block_size), m_block_size(block_size),
    m_lock("librbd::cache::BlockGuard::m_lock"),
    m_detained_block_pool(m_max_blocks),
    m_detained_blocks_buckets(m_max_blocks),
    m_detained_blocks(BlockToDetainedBlocks::bucket_traits(
      &m_detained_blocks_buckets.front(), m_max_blocks)) {
  for (auto &detained_block : m_detained_block_pool) {
    m_free_detained_blocks.push_back(detained_block);
  }
  for(auto block = 0; block < m_max_blocks; block++) {
    //chendi: initiate universal BlockIOMap
    Block* block_info = new Block(block);
    m_Block_map.insert(std::make_pair(block, block_info));
  }
}

void BlockGuard::create_block_ios(IOType io_type,
                                  const ImageExtents &image_extents,
                                  BlockIOs *block_ios,
                                  C_BlockRequest *block_request) {
  typedef std::unordered_map<uint64_t, BlockIO> BlockToBlockIOs;
  //ldout(m_cct, 20) << "image_extents=" << image_extents << dendl;

  BlockToBlockIOs block_to_block_ios;
  BlockToBlocksMap::iterator block_it;
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
      // chendi: find block_io from map
      block_it = m_Block_map.find(block);
      assert(block_it != m_Block_map.end());
      auto &block_io = block_to_block_ios[block];
      block_io.block_info = block_it->second;
      
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
    pair.second.block = pair.first;
    pair.second.block_request = block_request;
    pair.second.tid = 0;

    ldout(m_cct, 20) << "block_io=[" << pair.second << "]" << dendl;
    block_ios->emplace_back(std::move(pair.second));
  }
}

int BlockGuard::detain(uint64_t block, BlockIO *block_io) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << "block=" << block << ", "
                   << "free_slots=" << m_free_detained_blocks.size() << dendl;
  assert(block_io == nullptr || block == block_io->block);

  DetainedBlock *detained_block;
  auto detained_block_it = m_detained_blocks.find(block);
  if (detained_block_it != m_detained_blocks.end()) {
    // request against an already detained block
    detained_block = &(*detained_block_it);
    if (block_io == nullptr) {
      // policy is attempting to release this (busy) block
      return -EBUSY;
    }

    if (block_io != nullptr) {
      detained_block->block_ios.emplace_back(*block_io);
    }

    // alert the caller that the IO was detained
    return 1;
  } else {
    if (m_free_detained_blocks.empty()) {
      ldout(m_cct, 20) << "no free detained block cells" << dendl;
      return -ENOMEM;
    }

    detained_block = &m_free_detained_blocks.front();
    m_free_detained_blocks.pop_front();

    detained_block->block = block;
    m_detained_blocks.insert(*detained_block);
    if (block_io != nullptr) {
      detained_block->block_ios.emplace_back(*block_io);
    }
    return 0;
  }
}

int BlockGuard::release(uint64_t block, BlockIOs *block_ios) {
  Mutex::Locker locker(m_lock);
  auto detained_block_it = m_detained_blocks.find(block);
  assert(detained_block_it != m_detained_blocks.end());

  auto &detained_block = *detained_block_it;
  ldout(m_cct, 20) << "block=" << block << ", "
                   << "pending_ios="
                   << (detained_block.block_ios.empty() ?
                        0 : detained_block.block_ios.size()) << ", "
                   << "free_slots=" << m_free_detained_blocks.size() << dendl;

  if (!detained_block.block_ios.empty()) {
    block_ios->push_back(std::move(detained_block.block_ios.front()));
    detained_block.block_ios.pop_front();
  } else {
    m_detained_blocks.erase(detained_block_it);
    m_free_detained_blocks.push_back(detained_block);
  }
  return 0;
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
