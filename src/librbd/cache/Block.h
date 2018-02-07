// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_BLOCK_MAP
#define CEPH_LIBRBD_CACHE_BLOCK_MAP

#include "librbd/cache/Types.h" 
#include "include/lru.h"
#include <mutex>
#include <unordered_map>

namespace librbd {
namespace cache {
 
struct Block {
  std::mutex lock;

  uint64_t block;
  Entry* entry;
  uint8_t status;

  void *tail_block_io_request; // used to track ios on this block
  bool in_process;        // true if this block has been in processing by thread
  Block( uint64_t block ) : block(block),
      entry(nullptr), status(NOT_IN_CACHE),
      tail_block_io_request(nullptr),
      in_process(false) {}
};

class BlockMap {
public:
  //An universal map to pre-allocate in memory data for each block
  typedef std::unordered_map<uint64_t, Block*> BlockToBlocksMap;
  BlockToBlocksMap m_block_map;
  uint64_t m_block_count = 0;

  BlockMap(uint64_t block_count): m_block_count(block_count) {
    for(uint64_t block = 0; block < m_block_count; block++) {
      Block* block_info = new Block(block);
      m_block_map.insert(std::make_pair(block, block_info));
    }
  }

  Block* find_block(uint64_t block_id) {
    auto block_it = m_block_map.find(block_id);
    if(block_it == m_block_map.end())
      return nullptr;
    else
      return block_it->second;
  }

  BlockToBlocksMap::iterator begin() {
    return m_block_map.begin();
  }

  BlockToBlocksMap::iterator end() {
    return m_block_map.end();
  }

};
} //end of namespace cache
} //end of namespace librbd
#endif
