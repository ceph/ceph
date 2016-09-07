// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_BLOCK_GUARD
#define CEPH_LIBRBD_CACHE_BLOCK_GUARD

#include "include/buffer.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include "librbd/cache/Types.h"
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/variant.hpp>
#include <iosfwd>
#include <list>
#include <string>
#include <vector>
#include "include/assert.h"

struct CephContext;

namespace librbd {
namespace cache {

class BlockGuard {
public:
  struct BlockIO;

  struct C_BlockRequest : public Context {
    Mutex lock;
    uint32_t pending_requests = 0;
    bool activated = false;
    int ret_val = 0;
    Context *on_finish;

    C_BlockRequest(Context *on_finish);

    void activate();
    void fail(int r);

    void add_request();
    void complete_request(int r);

    virtual void finish(int r) override;

    virtual void remap(PolicyMapResult policy_map_result,
                       BlockIO &&block_io) = 0;
  };

  struct BlockIOExtent {
    BlockIOExtent(uint64_t buffer_offset, uint32_t block_offset,
                  uint32_t block_length)
      : buffer_offset(buffer_offset), block_offset(block_offset),
        block_length(block_length) {
    }
    uint64_t buffer_offset;
    uint32_t block_offset;
    uint32_t block_length;
  };
  typedef std::vector<BlockIOExtent> BlockIOExtents;

  struct BlockIO {
    // TODO intrusive_list

    BlockIO() {
    }
    BlockIO(uint64_t block, BlockIOExtents &&extents)
      : block(block), extents(extents) {
    }

    uint64_t tid;
    uint64_t block;
    BlockIOExtents extents;

    IOType io_type : 2;     ///< IO type for deferred IO request
    bool partial_block : 1; ///< true if not full block request
    C_BlockRequest *block_request;
  };
  typedef std::list<BlockIO> BlockIOs;

  BlockGuard(CephContext *cct, uint32_t max_blocks, uint32_t block_size);
  BlockGuard(const BlockGuard&) = delete;
  BlockGuard &operator=(const BlockGuard&) = delete;

  // convert a image-extent scatter/gather request to block-extent
  void create_block_ios(IOType io_type, const ImageExtents &image_extents,
                        BlockIOs *block_ios, C_BlockRequest *block_request);

  // detain future IO against a specified block
  int detain(uint64_t block, BlockIO *block_io);

  // release detained IO against a specified block
  int release(uint64_t block, BlockIOs *block_ios);

  inline friend std::ostream &operator<<(std::ostream &os,
                                         const BlockIOExtent& rhs) {
    os << "buffer_offset=" << rhs.buffer_offset << ", "
       << "block_offset=" << rhs.block_offset << ", "
       << "block_length=" << rhs.block_length;
    return os;
  }
  inline friend std::ostream &operator<<(std::ostream &os,
                                         const BlockIO& rhs) {
    os << "block=" << rhs.block << ", "
       << "io_type=" << rhs.io_type << ", "
       << "[";
    std::string delim;
    for (auto &extent : rhs.extents) {
      os << "[" << extent << "]" << delim;
      delim = ", ";
    }
    os << "]";
    return os;
  }

private:

  struct DetainedBlock : public boost::intrusive::list_base_hook<>,
                         public boost::intrusive::unordered_set_base_hook<> {
    uint64_t block;
    BlockIOs block_ios;
  };
  struct DetainedBlockKey {
    typedef uint64_t type;
    type operator()(const DetainedBlock &detained_block) const {
      return detained_block.block;
    }
  };

  typedef std::vector<DetainedBlock> DetainedBlockPool;
  typedef boost::intrusive::list<DetainedBlock> DetainedBlocks;
  typedef boost::intrusive::unordered_set<
    DetainedBlock,
    boost::intrusive::key_of_value<DetainedBlockKey> > BlockToDetainedBlocks;
  typedef BlockToDetainedBlocks::bucket_type BlockToDetainedBlocksBucket;
  typedef std::vector<BlockToDetainedBlocksBucket> BlockToDetainedBlocksBuckets;

  CephContext *m_cct;
  uint32_t m_max_blocks;
  uint32_t m_block_size;

  Mutex m_lock;
  DetainedBlockPool m_detained_block_pool;
  DetainedBlocks m_free_detained_blocks;
  BlockToDetainedBlocksBuckets m_detained_blocks_buckets;
  BlockToDetainedBlocks m_detained_blocks;
};

} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_BLOCK_GUARD
