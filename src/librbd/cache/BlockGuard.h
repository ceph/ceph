// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_BLOCK_GUARD
#define CEPH_LIBRBD_CACHE_BLOCK_GUARD

#include "include/buffer.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include "librbd/cache/Types.h"
#include "librbd/cache/Block.h"
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/variant.hpp>
#include <iosfwd>
#include <list>
#include <string>
#include <vector>
#include <mutex>
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

  struct C_BlockIORequest : public Context {
    CephContext *cct;
    C_BlockIORequest *next_block_request;
    uint16_t inflight_requests_count;
  
    C_BlockIORequest(CephContext *cct, C_BlockIORequest *next_block_request)
      : cct(cct), next_block_request(next_block_request) {
    }
  
    virtual void finish(int r) override {
      if (next_block_request) {
        if (r < 0) {
          // abort the chain of requests upon failure
          next_block_request->complete(r);
        } else {
          // execute next request in chain
          next_block_request->send();
        }
      }
    }
  
    virtual void complete(int r) {
      finish(r);
      delete this;
    }
    virtual void send() = 0;
    virtual const char *get_name() const = 0;
    virtual void* get_buffer_ptr(){ return nullptr; }
  };


  struct BlockIO {
    // TODO intrusive_list
    Block *block_info;
    BlockIOExtents extents;
    IOType io_type : 2;     // IO type for deferred IO request
    bool partial_block : 1; // true if not full block request

    C_BlockRequest *block_request;
  };
  typedef std::list<BlockIO> BlockIOs;

  BlockGuard(CephContext *cct, uint64_t cache_size, uint32_t block_size);
  BlockGuard(const BlockGuard&) = delete;
  BlockGuard &operator=(const BlockGuard&) = delete;

  // convert a image-extent scatter/gather request to block-extent
  void create_block_ios(IOType io_type, const ImageExtents &image_extents,
                        BlockIOs *block_ios, C_BlockRequest *block_request);

  inline void set_block_map(void* block_map) {
    m_block_map = (BlockMap*)block_map;
  }
  inline friend std::ostream &operator<<(std::ostream &os,
                                         const BlockIOExtent& rhs) {
    os << "buffer_offset=" << rhs.buffer_offset << ", "
       << "block_offset=" << rhs.block_offset << ", "
       << "block_length=" << rhs.block_length;
    return os;
  }
  inline friend std::ostream &operator<<(std::ostream &os,
                                         const BlockIO& rhs) {
    os << "block=" << rhs.block_info->block << ", "
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
  CephContext *m_cct;
  uint32_t m_max_blocks;
  uint32_t m_block_size;
  BlockMap* m_block_map;

  Mutex m_lock;

};

} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_BLOCK_GUARD
