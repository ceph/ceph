// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_BLOCK_GUARD_H
#define CEPH_LIBRBD_IO_BLOCK_GUARD_H

#include "include/int_types.h"
#include "common/dout.h"
#include "common/ceph_mutex.h"
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <deque>
#include <list>
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::BlockGuard: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {

struct BlockExtent {
  // [block_start, block_end)
  uint64_t block_start = 0;
  uint64_t block_end = 0;

  BlockExtent() {
  }
  BlockExtent(uint64_t block_start, uint64_t block_end)
    : block_start(block_start), block_end(block_end) {
  }

  friend std::ostream& operator<< (std::ostream& os, const BlockExtent& block_extent) {
    os << "[block_start=" << block_extent.block_start
       << ", block_end=" << block_extent.block_end << "]";
    return os;
  }
};

struct BlockGuardCell {
};

/**
 * Helper class to restrict and order concurrent IO to the same block. The
 * definition of a block is dependent upon the user of this class. It might
 * represent a backing object, 512 byte sectors, etc.
 */
template <typename BlockOperation>
class BlockGuard {
private:
  struct DetainedBlockExtent;

public:
  typedef std::list<BlockOperation> BlockOperations;

  BlockGuard(CephContext *cct)
    : m_cct(cct) {
  }

  BlockGuard(const BlockGuard&) = delete;
  BlockGuard &operator=(const BlockGuard&) = delete;

  /**
   * Detain future IO for a range of blocks. the guard will keep
   * ownership of the provided operation if the operation is blocked.
   * @return 0 upon success and IO can be issued
   *         >0 if the IO is blocked,
   *         <0 upon error
   */
  int detain(const BlockExtent &block_extent, BlockOperation *block_operation,
             BlockGuardCell **cell) {
    std::lock_guard locker{m_lock};
    ldout(m_cct, 20) << block_extent
                     << ", free_slots="
                     << m_free_detained_block_extents.size()
                     << dendl;

    DetainedBlockExtent *detained_block_extent;
    auto it = m_detained_block_extents.find(block_extent);
    if (it != m_detained_block_extents.end()) {
      // request against an already detained block
      detained_block_extent = &(*it);
      if (block_operation != nullptr) {
        detained_block_extent->block_operations.emplace_back(
          std::move(*block_operation));
      }

      // alert the caller that the IO was detained
      *cell = nullptr;
      return detained_block_extent->block_operations.size();
    } else {
      if (!m_free_detained_block_extents.empty()) {
        detained_block_extent = &m_free_detained_block_extents.front();
        detained_block_extent->block_operations.clear();
        m_free_detained_block_extents.pop_front();
      } else {
        ldout(m_cct, 20) << "no free detained block cells" << dendl;
        m_detained_block_extent_pool.emplace_back();
        detained_block_extent = &m_detained_block_extent_pool.back();
      }

      detained_block_extent->block_extent = block_extent;
      m_detained_block_extents.insert(*detained_block_extent);
      *cell = reinterpret_cast<BlockGuardCell*>(detained_block_extent);
      return 0;
    }
  }

  /**
   * Release any detained IO operations from the provided cell.
   */
  void release(BlockGuardCell *cell, BlockOperations *block_operations) {
    std::lock_guard locker{m_lock};

    ceph_assert(cell != nullptr);
    auto &detained_block_extent = reinterpret_cast<DetainedBlockExtent &>(
      *cell);
    ldout(m_cct, 20) << detained_block_extent.block_extent
                     << ", pending_ops="
                     << detained_block_extent.block_operations.size()
                     << dendl;

    *block_operations = std::move(detained_block_extent.block_operations);
    m_detained_block_extents.erase(detained_block_extent.block_extent);
    m_free_detained_block_extents.push_back(detained_block_extent);
  }

private:
  struct DetainedBlockExtent : public boost::intrusive::list_base_hook<>,
                               public boost::intrusive::set_base_hook<> {
    BlockExtent block_extent;
    BlockOperations block_operations;
  };

  struct DetainedBlockExtentKey {
    typedef BlockExtent type;
    const BlockExtent &operator()(const DetainedBlockExtent &value) {
      return value.block_extent;
    }
  };

  struct DetainedBlockExtentCompare {
    bool operator()(const BlockExtent &lhs,
                    const BlockExtent &rhs) const {
      // check for range overlap (lhs < rhs)
      if (lhs.block_end <= rhs.block_start) {
        return true;
      }
      return false;
    }
  };

  typedef std::deque<DetainedBlockExtent> DetainedBlockExtentsPool;
  typedef boost::intrusive::list<DetainedBlockExtent> DetainedBlockExtents;
  typedef boost::intrusive::set<
    DetainedBlockExtent,
    boost::intrusive::compare<DetainedBlockExtentCompare>,
    boost::intrusive::key_of_value<DetainedBlockExtentKey> >
      BlockExtentToDetainedBlockExtents;

  CephContext *m_cct;

  ceph::mutex m_lock = ceph::make_mutex("librbd::BlockGuard::m_lock");
  DetainedBlockExtentsPool m_detained_block_extent_pool;
  DetainedBlockExtents m_free_detained_block_extents;
  BlockExtentToDetainedBlockExtents m_detained_block_extents;

};

} // namespace librbd

#undef dout_subsys
#undef dout_prefix
#define dout_prefix *_dout

#endif // CEPH_LIBRBD_IO_BLOCK_GUARD_H
