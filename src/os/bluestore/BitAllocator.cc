// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 *
 * BitMap Tree Design:
 * Storage is divided into bitmap of blocks. Each bitmap has size of
 * unsigned long. Group of bitmap creates a Zone. Zone is a unit where
 * at a time single thread can be active as well as single biggest
 * contiguous allocation that can be requested.
 *
 * Rest of the nodes are classified into three categories:
 *   root node or Allocator
 *   internal nodes or BitMapAreaIN
 *   final nodes that contains Zones called BitMapAreaLeaf
 * This classification is according to their own implmentation of some
 * of the interfaces defined in BitMapArea.
 */

#include "BitAllocator.h"
#include <assert.h>
#include "bluestore_types.h"
#include "common/debug.h"
#include <math.h>

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "bitalloc:"

MEMPOOL_DEFINE_OBJECT_FACTORY(BitMapArea, BitMapArea, bluestore_alloc);
MEMPOOL_DEFINE_OBJECT_FACTORY(BitMapAreaIN, BitMapAreaIN, bluestore_alloc);
MEMPOOL_DEFINE_OBJECT_FACTORY(BitMapAreaLeaf, BitMapAreaLeaf, bluestore_alloc);
MEMPOOL_DEFINE_OBJECT_FACTORY(BitMapZone, BitMapZone, bluestore_alloc);
MEMPOOL_DEFINE_OBJECT_FACTORY(BmapEntry, BmapEntry, bluestore_alloc);
MEMPOOL_DEFINE_OBJECT_FACTORY(BitAllocator, BitAllocator, bluestore_alloc);

int64_t BitMapAreaLeaf::count = 0;
int64_t BitMapZone::count = 0;
int64_t BitMapZone::total_blocks = 0;

void AllocatorExtentList::add_extents(int64_t start, int64_t count)
{
  bluestore_pextent_t *last_extent = NULL;
  bool can_merge = false;

  if (!m_extents->empty()) {
    last_extent = &(m_extents->back());
    uint64_t last_offset = last_extent->end() / m_block_size;
    uint32_t last_length = last_extent->length / m_block_size;
    if ((last_offset == (uint64_t) start) &&
        (!m_max_blocks || (last_length + count) <= m_max_blocks)) {
      can_merge = true;
    }
  }

  if (can_merge) {
    last_extent->length += (count * m_block_size);
  } else {
    m_extents->emplace_back(bluestore_pextent_t(start * m_block_size,
					count * m_block_size));
  }
}

int64_t BmapEntityListIter::index()
{
  return m_cur_idx;
}

BmapEntry::BmapEntry(CephContext*, const bool full)
{
  if (full) {
    m_bits = BmapEntry::full_bmask();
  } else {
    m_bits = BmapEntry::empty_bmask();
  }
}

BmapEntry::~BmapEntry()
{

}

bool BmapEntry::check_bit(int bit)
{
  return (atomic_fetch() & bit_mask(bit));
}

bool BmapEntry::is_allocated(int64_t offset, int64_t num_bits)
{
  bmap_t bmask = BmapEntry::align_mask(num_bits) >> offset;
  return ((m_bits & bmask) == bmask);
}

void BmapEntry::clear_bit(int bit)
{
  bmap_t bmask = bit_mask(bit);
  m_bits &= ~(bmask);
}

void BmapEntry::clear_bits(int offset, int num_bits)
{
  if (num_bits == 0) {
    return;
  }

  bmap_t bmask = BmapEntry::align_mask(num_bits) >> offset;
  m_bits &= ~(bmask);
}

void BmapEntry::set_bits(int offset, int num_bits)
{
  if (num_bits == 0) {
    return;
  }

  bmap_t bmask = BmapEntry::align_mask(num_bits) >> offset;
  m_bits |= bmask;
}

/*
 * Allocate a bit if it was free.
 * Retruns true if it was free.
 */
bool BmapEntry::check_n_set_bit(int bit)
{
  bmap_t bmask = bit_mask(bit);
  bool res = !(m_bits & bmask);
  m_bits |= bmask;
  return res;
}

/*
 * Find N cont free bits in BitMap starting from an offset.
 *
 * Returns number of continuous bits found.
 */
int BmapEntry::find_n_cont_bits(int start_offset, int64_t num_bits)
{
  int count = 0;
  int i = 0;

  if (num_bits == 0) {
    return 0;
  }

  if (start_offset >= BmapEntry::size()) {
    return 0;
  }

  for (i = start_offset; i < BmapEntry::size() && count < num_bits; i++) {
    if (!check_n_set_bit(i)) {
      break;
    }
    count++;
  }

  return count;
}

/*
 * Find N free bits starting search from a given offset.
 *
 * Returns number of bits found, start bit and end of
 * index next to bit where our search ended + 1.
 */
int BmapEntry::find_n_free_bits(int start_idx, int64_t max_bits,
         int *free_bit, int *end_idx)
{
  int i = 0;
  int count = 0;

  *free_bit = 0;
  alloc_assert(max_bits > 0);

  /*
   * Find free bit aligned to bit_align return the bit_num in free_bit.
   */
  if (atomic_fetch() == BmapEntry::full_bmask()) {
    /*
     * All bits full, return fail.
     */
    *end_idx = BmapEntry::size();
    return 0;
  }

  /*
   * Do a serial scan on bitmap.
   */
  for (i = start_idx; i < BmapEntry::size(); i++) {
    if (check_n_set_bit(i)) {
      /*
       * Found first free bit
       */
      *free_bit = i;
      count++;
      break;
    }
  }
  count += find_n_cont_bits(i + 1, max_bits - 1);

  (*end_idx) = i + count;
  return count;
}

/*
 * Find first series of contiguous bits free in bitmap starting
 * from start offset that either
 * satisfy our need or are touching right edge of bitmap.
 *
 * Returns allocated bits, start bit of allocated, number of bits
 * scanned from start offset.
 */
int
BmapEntry::find_first_set_bits(int64_t required_blocks,
          int bit_offset, int *start_offset,
          int64_t *scanned)
{
  int allocated = 0;
  int conti = 0;
  int end_idx = 0;

  *scanned = 0;

  while (bit_offset < BmapEntry::size()) {
    conti = find_n_free_bits(bit_offset, required_blocks,
           start_offset, &end_idx);

    *scanned += end_idx - bit_offset;
    /*
     * Either end of bitmap or got required.
     */
    if (conti == required_blocks ||
        (conti + *start_offset == BmapEntry::size())) {
      allocated += conti;
      break;
    }

    /*
     * Did not get expected, search from next index again.
     */
    clear_bits(*start_offset, conti);
    allocated = 0;

    bit_offset = end_idx;
  }

  return allocated;
}

void BmapEntry::dump_state(CephContext* const cct, const int& count)
{
  dout(0) << count << ":: 0x" << std::hex << m_bits << std::dec << dendl;
}

/*
 * Zone related functions.
 */
void BitMapZone::init(CephContext* const cct,
                      const int64_t zone_num,
                      const int64_t total_blocks,
                      const bool def)
{
  m_area_index = zone_num;
  BitMapZone::total_blocks = total_blocks;
  alloc_assert(size() > 0);

  m_used_blocks = def? total_blocks: 0;

  int64_t num_bmaps = total_blocks / BmapEntry::size();
  alloc_assert(num_bmaps < std::numeric_limits<int16_t>::max());
  alloc_assert(total_blocks < std::numeric_limits<int32_t>::max());
  alloc_assert(!(total_blocks % BmapEntry::size()));

  m_bmap_vec.resize(num_bmaps, BmapEntry(cct, def));
  incr_count();
}

int64_t BitMapZone::sub_used_blocks(int64_t num_blocks)
{
  return std::atomic_fetch_sub(&m_used_blocks, (int32_t) num_blocks);
}

int64_t BitMapZone::add_used_blocks(int64_t num_blocks)
{
  return std::atomic_fetch_add(&m_used_blocks, (int32_t)num_blocks) + num_blocks;
}

/* Intensionally hinted because BitMapAreaLeaf::child_check_n_lock. */
inline int64_t BitMapZone::get_used_blocks()
{
  return std::atomic_load(&m_used_blocks);
}

bool BitMapZone::reserve_blocks(int64_t num_blocks)
{
  ceph_abort();
  return false;
}

void BitMapZone::unreserve(int64_t num_blocks, int64_t allocated)
{
  ceph_abort();
}

int64_t BitMapZone::get_reserved_blocks()
{
  ceph_abort();
  return 0;
}

BitMapZone::BitMapZone(CephContext* cct, int64_t total_blocks,
		       int64_t zone_num)
  : BitMapArea(cct)
{
  init(cct, zone_num, total_blocks, false);
}

BitMapZone::BitMapZone(CephContext* cct, int64_t total_blocks,
		       int64_t zone_num, bool def)
  : BitMapArea(cct)
{
  init(cct, zone_num, total_blocks, def);
}

void BitMapZone::shutdown()
{
}

BitMapZone::~BitMapZone()
{
}

/*
 * Check if some search took zone marker to end.
 *
 * The inline hint has been added intensionally because of importance of this
 * method for BitMapAreaLeaf::child_check_n_lock, and thus for the overall
 * allocator's performance. Examination of disassemblies coming from GCC 5.4.0
 * showed that the compiler really needs that hint.
 */
inline bool BitMapZone::is_exhausted()
{
  /* BitMapZone::get_used_blocks operates atomically. No need for lock. */
  return BitMapZone::get_used_blocks() == BitMapZone::size();
}

bool BitMapZone::is_allocated(int64_t start_block, int64_t num_blocks)
{
  BmapEntry *bmap = NULL;
  int bit = 0;
  int64_t falling_in_bmap = 0;

  while (num_blocks) {
    bit = start_block % BmapEntry::size();
    bmap = &m_bmap_vec[start_block / BmapEntry::size()];
    falling_in_bmap = std::min(num_blocks, BmapEntry::size() - bit);

    if (!bmap->is_allocated(bit, falling_in_bmap)) {
      return false;
    }

    start_block += falling_in_bmap;
    num_blocks -= falling_in_bmap;
  }

  return true;
}

void BitMapZone::set_blocks_used(int64_t start_block, int64_t num_blocks)
{
  BmapEntry *bmap = NULL;
  int bit = 0;
  int64_t falling_in_bmap = 0;
  int64_t blks = num_blocks;

  while (blks) {
    bit = start_block % BmapEntry::size();
    bmap = &m_bmap_vec[start_block / BmapEntry::size()];
    falling_in_bmap = std::min(blks, BmapEntry::size() - bit);

    bmap->set_bits(bit, falling_in_bmap);

    start_block += falling_in_bmap;
    blks -= falling_in_bmap;
  }
  add_used_blocks(num_blocks);
}

void BitMapZone::free_blocks_int(int64_t start_block, int64_t num_blocks)
{
  BmapEntry *bmap = NULL;
  int bit = 0;
  int64_t falling_in_bmap = 0;
  int64_t count = num_blocks;
  int64_t first_blk = start_block;
  
  if (num_blocks == 0) {
    return; 
  }
  alloc_dbg_assert(is_allocated(start_block, num_blocks));

  while (count) {
    bit = first_blk % BmapEntry::size();
    bmap = &m_bmap_vec[first_blk / BmapEntry::size()];
    falling_in_bmap = std::min(count, BmapEntry::size() - bit);

    bmap->clear_bits(bit, falling_in_bmap);

    first_blk += falling_in_bmap;
    count -= falling_in_bmap;
  }
  alloc_dbg_assert(!is_allocated(start_block, num_blocks));
}

void BitMapZone::lock_excl()
{
  m_lock.lock();
}

bool BitMapZone::lock_excl_try()
{
  return m_lock.try_lock();
}

void BitMapZone::unlock()
{
  m_lock.unlock();
}

bool BitMapZone::check_locked()
{
  return !lock_excl_try();
}

void BitMapZone::free_blocks(int64_t start_block, int64_t num_blocks)
{
  free_blocks_int(start_block, num_blocks);
  sub_used_blocks(num_blocks);
  alloc_assert(get_used_blocks() >= 0);
}

int64_t BitMapZone::alloc_blocks_dis(int64_t num_blocks,
           int64_t min_alloc,
     int64_t hint,
     int64_t zone_blk_off, 
     AllocatorExtentList *alloc_blocks)
{
  int64_t bmap_idx = hint / BmapEntry::size();
  int bit = hint % BmapEntry::size();
  BmapEntry *bmap = NULL;
  int64_t allocated = 0;
  int64_t blk_off = 0;
  int64_t alloc_cont = 0;
  int64_t last_cont = 0;
  int64_t last_running_ext = 0;
  int search_idx = bit;
  int64_t scanned = 0;
  int start_off = 0;
  

  alloc_assert(check_locked());

  BitMapEntityIter <BmapEntry> iter = BitMapEntityIter<BmapEntry>(
          &m_bmap_vec, bmap_idx);
  bmap = iter.next();
  if (!bmap) {
    return 0;
  }

  while (allocated < num_blocks) {
    blk_off = zone_blk_off + bmap_idx * bmap->size();
    if (last_cont) {
      /*
       * We had bits free at end of last bitmap, try to complete required
       * min alloc size using that.
       */
      alloc_cont = bmap->find_n_cont_bits(0, min_alloc - last_cont);
      allocated += alloc_cont;
      last_cont += alloc_cont;
      
      if (!alloc_cont) {
        if (last_cont) {
          this->free_blocks_int(last_running_ext - zone_blk_off, last_cont);
        }
        allocated -= last_cont;
        last_cont = 0;
      } else if (last_cont / min_alloc) {
          /*
           * Got contiguous min_alloc_size across bitmaps.
           */
          alloc_blocks->add_extents(last_running_ext, last_cont);
          last_cont = 0;
          last_running_ext = 0;
      }
      search_idx = alloc_cont;
    } else {
      /*
       * Try to allocate  min_alloc_size bits from given bmap.
       */
      alloc_cont = bmap->find_first_set_bits(min_alloc, search_idx, &start_off, &scanned);
      search_idx = search_idx + scanned;
      allocated += alloc_cont;
      if (alloc_cont / min_alloc) {
        /*
         * Got contiguous min_alloc_size within a bitmap.
         */
        alloc_blocks->add_extents(blk_off + start_off, min_alloc);
      }
      
      if (alloc_cont % min_alloc) {
        /*
         * Got some bits at end of bitmap, carry them to try match with
         * start bits from next bitmap.
         */
        if (!last_cont) {
          last_running_ext = blk_off + start_off;
        } 
        last_cont += alloc_cont % min_alloc;
      }
    }
  
   
    if (search_idx == BmapEntry::size()) {
      search_idx = 0;
      bmap_idx = iter.index();
      if ((bmap = iter.next()) == NULL) {
        if (last_cont) {
          this->free_blocks_int(last_running_ext - zone_blk_off, last_cont);
        }
        allocated -= last_cont;
        break;
      }
    }
  }

  add_used_blocks(allocated);
  return allocated;
}



void BitMapZone::dump_state(CephContext* const cct, int& count)
{
  BmapEntry *bmap = NULL;
  int bmap_idx = 0;
  BitMapEntityIter <BmapEntry> iter = BitMapEntityIter<BmapEntry>(
          &m_bmap_vec, 0);
  dout(0) << __func__ << " zone " << count << " dump start " << dendl;
  while ((bmap = static_cast<BmapEntry *>(iter.next()))) {
    bmap->dump_state(cct, bmap_idx);
    bmap_idx++;
  }
  dout(0) << __func__ << " zone " << count << " dump end " << dendl;
  count++;
}


/*
 * BitMapArea Leaf and non-Leaf functions.
 */
int64_t BitMapArea::get_zone_size(CephContext* cct)
{
  return cct->_conf->bluestore_bitmapallocator_blocks_per_zone;
}

int64_t BitMapArea::get_span_size(CephContext* cct)
{
  return cct->_conf->bluestore_bitmapallocator_span_size;
}

int BitMapArea::get_level(CephContext* cct, int64_t total_blocks)
{
  int level = 1;
  int64_t zone_size_block = get_zone_size(cct);
  int64_t span_size = get_span_size(cct);
  int64_t spans = zone_size_block * span_size;
  while (spans < total_blocks) {
    spans *= span_size;
    level++;
  }
  return level;
}

int64_t BitMapArea::get_level_factor(CephContext* cct, int level)
{
  alloc_assert(level > 0);

  int64_t zone_size = get_zone_size(cct);
  if (level == 1) {
    return zone_size;
  }

  int64_t level_factor = zone_size;
  int64_t span_size = get_span_size(cct);
  while (--level) {
    level_factor *= span_size;
  }

  return level_factor;
}

int64_t BitMapArea::get_index()
{
  return m_area_index;
}

/*
 * BitMapArea Leaf and Internal
 */
BitMapAreaIN::BitMapAreaIN(CephContext* cct)
  : BitMapArea(cct)
{
  // nothing
}

void BitMapAreaIN::init_common(CephContext* const cct,
                               const int64_t total_blocks,
                               const int64_t area_idx,
                               const bool def)
{
  m_area_index = area_idx;
  m_total_blocks = total_blocks;
  m_level = BitMapArea::get_level(cct, total_blocks);
  m_reserved_blocks = 0;

  m_used_blocks = def? total_blocks: 0;
}

void BitMapAreaIN::init(CephContext* const cct,
                        int64_t total_blocks,
                        const int64_t area_idx,
                        const bool def)
{
  int64_t num_child = 0;
  alloc_assert(!(total_blocks % BmapEntry::size()));

  init_common(cct, total_blocks, area_idx, def);
  int64_t level_factor = BitMapArea::get_level_factor(cct, m_level);

  num_child = (total_blocks + level_factor - 1) / level_factor;
  alloc_assert(num_child < std::numeric_limits<int16_t>::max());

  m_child_size_blocks = level_factor;

  std::vector<BitMapArea*> children;
  children.reserve(num_child);
  int i = 0;
  for (i = 0; i < num_child - 1; i++) {
    if (m_level <= 2) {
      children.push_back(new BitMapAreaLeaf(cct, m_child_size_blocks, i, def));
    } else {
      children.push_back(new BitMapAreaIN(cct, m_child_size_blocks, i, def));
    }
    total_blocks -= m_child_size_blocks;
  }

  int last_level = BitMapArea::get_level(cct, total_blocks);
  if (last_level == 1) {
    children.push_back(new BitMapAreaLeaf(cct, total_blocks, i, def));
  } else {
    children.push_back(new BitMapAreaIN(cct, total_blocks, i, def));
  }
  m_child_list = BitMapAreaList(std::move(children));
}

BitMapAreaIN::BitMapAreaIN(CephContext* cct,int64_t total_blocks,
			   int64_t area_idx)
  : BitMapArea(cct)
{
  init(cct, total_blocks, area_idx, false);
}

BitMapAreaIN::BitMapAreaIN(CephContext* cct, int64_t total_blocks,
			   int64_t area_idx, bool def)
  : BitMapArea(cct)
{
  init(cct, total_blocks, area_idx, def);
}

BitMapAreaIN::~BitMapAreaIN()
{
}

void BitMapAreaIN::shutdown()
{
  lock_excl();
  m_total_blocks = -1;
  m_area_index = -2;
  unlock();
}

bool BitMapAreaIN::child_check_n_lock(BitMapArea *child, int64_t required)
{
  child->lock_shared();

  if (child->is_exhausted()) {
    child->unlock();
    return false;
  }

  int64_t child_used_blocks = child->get_used_blocks();
  int64_t child_total_blocks = child->size();
  if ((child_total_blocks - child_used_blocks) < required) {
    child->unlock();
    return false;
  }

  return true;
}

void BitMapAreaIN::child_unlock(BitMapArea *child)
{
  child->unlock();
}

bool BitMapAreaIN::is_exhausted()
{
  return get_used_blocks() == size();
}

int64_t BitMapAreaIN::add_used_blocks(int64_t blks)
{
  std::lock_guard<std::mutex> l(m_blocks_lock);
  m_used_blocks += blks;
  return m_used_blocks;
}

int64_t BitMapAreaIN::sub_used_blocks(int64_t num_blocks)
{
  std::lock_guard<std::mutex> l(m_blocks_lock);

  int64_t used_blks = m_used_blocks;
  m_used_blocks -= num_blocks;
  alloc_assert(m_used_blocks >= 0);
  return used_blks;
}

int64_t BitMapAreaIN::get_used_blocks()
{
  std::lock_guard<std::mutex> l(m_blocks_lock);
  return m_used_blocks;
}

int64_t BitMapAreaIN::get_used_blocks_adj()
{
  std::lock_guard<std::mutex> l(m_blocks_lock);
  return m_used_blocks - m_reserved_blocks;
}

bool BitMapAreaIN::reserve_blocks(int64_t num)
{
  bool res = false;
  std::lock_guard<std::mutex> u_l(m_blocks_lock);
  if (m_used_blocks + num <= size()) {
    m_used_blocks += num;
    m_reserved_blocks += num;
    res = true;
  }
  alloc_assert(m_used_blocks <= size());
  return res;
}

void BitMapAreaIN::unreserve(int64_t needed, int64_t allocated)
{
  std::lock_guard<std::mutex> l(m_blocks_lock);
  m_used_blocks -= (needed - allocated);
  m_reserved_blocks -= needed;
  alloc_assert(m_used_blocks >= 0);
  alloc_assert(m_reserved_blocks >= 0);
}
int64_t BitMapAreaIN::get_reserved_blocks()
{
  std::lock_guard<std::mutex> l(m_blocks_lock); 
  return m_reserved_blocks;
}

bool BitMapAreaIN::is_allocated(int64_t start_block, int64_t num_blocks)
{
  BitMapArea *area = NULL;
  int64_t area_block_offset = 0;
  int64_t falling_in_area = 0;

  alloc_assert(start_block >= 0 &&
      (start_block + num_blocks <= size()));

  if (num_blocks == 0) {
    return true;
  }

  while (num_blocks) {
    area = static_cast<BitMapArea *>(m_child_list.get_nth_item(
                    start_block / m_child_size_blocks));

    area_block_offset = start_block % m_child_size_blocks;
    falling_in_area = std::min(m_child_size_blocks - area_block_offset,
              num_blocks);
    if (!area->is_allocated(area_block_offset, falling_in_area)) {
      return false;
    }
    start_block += falling_in_area;
    num_blocks -= falling_in_area;
  }
  return true;
}

int64_t BitMapAreaIN::alloc_blocks_dis_int_work(bool wrap, int64_t num_blocks, int64_t min_alloc, 
           int64_t hint, int64_t area_blk_off, AllocatorExtentList *block_list)
{
  BitMapArea *child = NULL;
  int64_t allocated = 0;
  int64_t blk_off = 0;

  BmapEntityListIter iter = BmapEntityListIter(
        &m_child_list, hint / m_child_size_blocks, wrap);

  while ((child = static_cast<BitMapArea *>(iter.next()))) {
    if (!child_check_n_lock(child, 1)) {
      hint = 0;
      continue;
    }

    blk_off = child->get_index() * m_child_size_blocks + area_blk_off;
    allocated += child->alloc_blocks_dis(num_blocks - allocated, min_alloc,
                            hint % m_child_size_blocks, blk_off, block_list);
    hint = 0;
    child_unlock(child);
    if (allocated == num_blocks) {
      break;
    }
  }

  return allocated;
}

int64_t BitMapAreaIN::alloc_blocks_dis_int(int64_t num_blocks, int64_t min_alloc,
                       int64_t hint, int64_t area_blk_off, AllocatorExtentList *block_list)
{
  return alloc_blocks_dis_int_work(false, num_blocks, min_alloc, hint,
                     area_blk_off, block_list);
}

int64_t BitMapAreaIN::alloc_blocks_dis(int64_t num_blocks, int64_t min_alloc,
           int64_t hint, int64_t blk_off, AllocatorExtentList *block_list)
{
  int64_t allocated = 0;

  lock_shared();
  allocated += alloc_blocks_dis_int(num_blocks, min_alloc, hint, blk_off, block_list);
  add_used_blocks(allocated);

  unlock();
  return allocated;
}


void BitMapAreaIN::set_blocks_used_int(int64_t start_block, int64_t num_blocks)
{
  BitMapArea *child = NULL;
  int64_t child_block_offset = 0;
  int64_t falling_in_child = 0;
  int64_t blks = num_blocks;
  int64_t start_blk = start_block;

  alloc_assert(start_block >= 0);

  while (blks) {
    child = static_cast<BitMapArea *>(m_child_list.get_nth_item(
                  start_blk / m_child_size_blocks));

    child_block_offset = start_blk % child->size();
    falling_in_child = std::min(m_child_size_blocks - child_block_offset,
              blks);
    child->set_blocks_used(child_block_offset, falling_in_child);
    start_blk += falling_in_child;
    blks -= falling_in_child;
  }

  add_used_blocks(num_blocks);
  alloc_dbg_assert(is_allocated(start_block, num_blocks));
}

void BitMapAreaIN::set_blocks_used(int64_t start_block, int64_t num_blocks)
{
  if (num_blocks == 0) {
    return;
  }

  lock_shared();
  set_blocks_used_int(start_block, num_blocks);
  unlock();
}

void BitMapAreaIN::free_blocks_int(int64_t start_block, int64_t num_blocks)
{
  BitMapArea *child = NULL;
  int64_t child_block_offset = 0;
  int64_t falling_in_child = 0;

  alloc_assert(start_block >= 0 &&
    (start_block + num_blocks) <= size());

  if (num_blocks == 0) {
    return;
  }

  while (num_blocks) {
    child = static_cast<BitMapArea *>(m_child_list.get_nth_item(
          start_block / m_child_size_blocks));

    child_block_offset = start_block % m_child_size_blocks;

    falling_in_child = std::min(m_child_size_blocks - child_block_offset,
              num_blocks);
    child->free_blocks(child_block_offset, falling_in_child);
    start_block += falling_in_child;
    num_blocks -= falling_in_child;
  }

}
void BitMapAreaIN::free_blocks(int64_t start_block, int64_t num_blocks)
{
  if (num_blocks == 0) {
    return;
  }
  lock_shared();
  alloc_dbg_assert(is_allocated(start_block, num_blocks));

  free_blocks_int(start_block, num_blocks);
  (void) sub_used_blocks(num_blocks);

  unlock();
}

void BitMapAreaIN::dump_state(CephContext* const cct, int& count)
{
  BitMapArea *child = NULL;

  BmapEntityListIter iter = BmapEntityListIter(
        &m_child_list, 0, false);

  while ((child = static_cast<BitMapArea *>(iter.next()))) {
    child->dump_state(cct, count);
  }
}

/*
 * BitMapArea Leaf
 */
BitMapAreaLeaf::BitMapAreaLeaf(CephContext* cct, int64_t total_blocks,
			       int64_t area_idx)
  : BitMapAreaIN(cct)
{
  init(cct, total_blocks, area_idx, false);
}

BitMapAreaLeaf::BitMapAreaLeaf(CephContext* cct, int64_t total_blocks,
			       int64_t area_idx, bool def)
  : BitMapAreaIN(cct)
{
  init(cct, total_blocks, area_idx, def);
}

void BitMapAreaLeaf::init(CephContext* const cct,
                          const int64_t total_blocks,
                          const int64_t area_idx,
                          const bool def)
{
  int64_t num_child = 0;
  alloc_assert(!(total_blocks % BmapEntry::size()));

  init_common(cct, total_blocks, area_idx, def);
  alloc_assert(m_level == 1);
  int zone_size_block = get_zone_size(cct);
  alloc_assert(zone_size_block > 0);
  num_child = (total_blocks + zone_size_block - 1) / zone_size_block;
  alloc_assert(num_child);
  m_child_size_blocks = total_blocks / num_child;

  std::vector<BitMapArea*> children;
  children.reserve(num_child);
  for (int i = 0; i < num_child; i++) {
    children.emplace_back(new BitMapZone(cct, m_child_size_blocks, i, def));
  }

  m_child_list = BitMapAreaList(std::move(children));

  BitMapAreaLeaf::incr_count();
}

BitMapAreaLeaf::~BitMapAreaLeaf()
{
  lock_excl();

  for (int64_t i = 0; i < m_child_list.size(); i++) {
    auto child = static_cast<BitMapArea *>(m_child_list.get_nth_item(i));
    delete child;
  }

  unlock();
}

/* Intensionally hinted because BitMapAreaLeaf::alloc_blocks_dis_int. */
inline bool BitMapAreaLeaf::child_check_n_lock(BitMapZone* const child,
                                               const int64_t required,
                                               const bool lock)
{
  /* The exhausted check can be performed without acquiring the lock. This
   * is because 1) BitMapZone::is_exhausted() actually operates atomically
   * and 2) it's followed by the exclusive, required-aware re-verification. */
  if (child->BitMapZone::is_exhausted()) {
    return false;
  }

  if (lock) {
    child->lock_excl();
  } else if (!child->lock_excl_try()) {
    return false;
  }

  int64_t child_used_blocks = child->get_used_blocks();
  int64_t child_total_blocks = child->size();
  if ((child_total_blocks - child_used_blocks) < required) {
    child->unlock();
    return false;
  }

  return true;
}

int64_t BitMapAreaLeaf::alloc_blocks_dis_int(int64_t num_blocks, int64_t min_alloc, 
                                 int64_t hint, int64_t area_blk_off, AllocatorExtentList *block_list)
{
  BitMapZone* child = nullptr;
  int64_t allocated = 0;
  int64_t blk_off = 0;

  BmapEntityListIter iter = BmapEntityListIter(
        &m_child_list, hint / m_child_size_blocks, false);

  /* We're sure the only element type we aggregate is BitMapZone,
   * so there is no business to go through vptr and thus prohibit
   * compiler to inline the stuff. Consult BitMapAreaLeaf::init. */
  while ((child = static_cast<BitMapZone*>(iter.next()))) {
    if (!child_check_n_lock(child, 1, false)) {
      hint = 0;
      continue;
    }

    blk_off = child->get_index() * m_child_size_blocks + area_blk_off;
    allocated += child->alloc_blocks_dis(num_blocks - allocated, min_alloc,
                                         hint % m_child_size_blocks, blk_off, block_list);
    child->unlock();
    if (allocated == num_blocks) {
      break;
    }
    hint = 0;
  }
  return allocated;
}

void BitMapAreaLeaf::free_blocks_int(int64_t start_block, int64_t num_blocks)
{
  BitMapArea *child = NULL;
  int64_t child_block_offset = 0;
  int64_t falling_in_child = 0;

  alloc_assert(start_block >= 0 &&
    (start_block + num_blocks) <= size());

  if (num_blocks == 0) {
    return;
  }

  while (num_blocks) {
    child = static_cast<BitMapArea *>(m_child_list.get_nth_item(
          start_block / m_child_size_blocks));

    child_block_offset = start_block % m_child_size_blocks;

    falling_in_child = std::min(m_child_size_blocks - child_block_offset,
              num_blocks);

    child->lock_excl();
    child->free_blocks(child_block_offset, falling_in_child);
    child->unlock();
    start_block += falling_in_child;
    num_blocks -= falling_in_child;
  }
}

/*
 * Main allocator functions.
 */
BitAllocator::BitAllocator(CephContext* cct, int64_t total_blocks,
			   int64_t zone_size_block, bmap_alloc_mode_t mode)
  : BitMapAreaIN(cct),
    cct(cct)
{
  init_check(total_blocks, zone_size_block, mode, false, false);
}

BitAllocator::BitAllocator(CephContext* cct, int64_t total_blocks,
			   int64_t zone_size_block, bmap_alloc_mode_t mode,
			   bool def)
  : BitMapAreaIN(cct),
    cct(cct)
{
  init_check(total_blocks, zone_size_block, mode, def, false);
}

BitAllocator::BitAllocator(CephContext* cct, int64_t total_blocks,
			   int64_t zone_size_block, bmap_alloc_mode_t mode,
			   bool def, bool stats_on)
  : BitMapAreaIN(cct),
    cct(cct)
{
  init_check(total_blocks, zone_size_block, mode, def, stats_on);
}

void BitAllocator::init_check(int64_t total_blocks, int64_t zone_size_block,
       bmap_alloc_mode_t mode, bool def, bool stats_on)
{
  int64_t unaligned_blocks = 0;

  if (mode != SERIAL && mode != CONCURRENT) {
    ceph_abort();
  }

  if (total_blocks <= 0) {
    ceph_abort();
  }

  if (zone_size_block == 0 ||
    zone_size_block < BmapEntry::size()) {
    ceph_abort();
  }

  zone_size_block = (zone_size_block / BmapEntry::size()) *
        BmapEntry::size();

  unaligned_blocks = total_blocks % zone_size_block;
  m_extra_blocks = unaligned_blocks? zone_size_block - unaligned_blocks: 0;
  total_blocks = round_up_to(total_blocks, zone_size_block);

  m_alloc_mode = mode;
  m_is_stats_on = stats_on;
  if (m_is_stats_on) {
    m_stats = new BitAllocatorStats();
  }

  pthread_rwlock_init(&m_rw_lock, NULL);
  init(cct, total_blocks, 0, def);
  if (!def && unaligned_blocks) {
    /*
     * Mark extra padded blocks used from beginning.
     */
    set_blocks_used(total_blocks - m_extra_blocks, m_extra_blocks);
  }
}

void BitAllocator::lock_excl()
{
  pthread_rwlock_wrlock(&m_rw_lock);
}

void BitAllocator::lock_shared()
{
  pthread_rwlock_rdlock(&m_rw_lock);
}

bool BitAllocator::try_lock()
{
  bool get_lock = false;
  if (pthread_rwlock_trywrlock(&m_rw_lock) == 0) {
    get_lock = true;
  }

  return get_lock;
}

void BitAllocator::unlock()
{
  pthread_rwlock_unlock(&m_rw_lock);
}

BitAllocator::~BitAllocator()
{
  lock_excl();

  for (int64_t i = 0; i < m_child_list.size(); i++) {
    auto child = static_cast<BitMapArea *>(m_child_list.get_nth_item(i));
    delete child;
  }

  unlock();
  pthread_rwlock_destroy(&m_rw_lock);
}

void
BitAllocator::shutdown()
{
  bool get_lock = try_lock();
  assert(get_lock);
  bool get_serial_lock = try_serial_lock();
  assert(get_serial_lock);
  serial_unlock();
  unlock();
}

void BitAllocator::unreserve_blocks(int64_t unused)
{
  unreserve(unused, 0);
}

void BitAllocator::serial_lock()
{
  if (m_alloc_mode == SERIAL) {
    m_serial_mutex.lock();
  }
}

void BitAllocator::serial_unlock()
{
  if (m_alloc_mode == SERIAL) {
    m_serial_mutex.unlock();
  }
}

bool BitAllocator::try_serial_lock()
{
  bool get_lock = false;
  if (m_alloc_mode == SERIAL) {
    if (m_serial_mutex.try_lock() == 0) {
      get_lock = true;
    }
  } else {
    get_lock = true;
  }
  return get_lock;
}

bool BitAllocator::child_check_n_lock(BitMapArea *child, int64_t required)
{
  child->lock_shared();

  if (child->is_exhausted()) {
    child->unlock();
    return false;
  }

  int64_t child_used_blocks = child->get_used_blocks();
  int64_t child_total_blocks = child->size();
  if ((child_total_blocks - child_used_blocks) < required) {
    child->unlock();
    return false;
  }

  return true;
}

void BitAllocator::child_unlock(BitMapArea *child)
{
  child->unlock();
}

bool BitAllocator::check_input_dis(int64_t num_blocks)
{
  if (num_blocks == 0 || num_blocks > size()) {
    return false;
  }
  return true;
}

bool BitAllocator::check_input(int64_t num_blocks)
{
  if (num_blocks == 0 || num_blocks > get_zone_size(cct)) {
    return false;
  }
  return true;
}

void BitAllocator::free_blocks(int64_t start_block, int64_t num_blocks)
{
  if (num_blocks == 0) {
    return;
  }

  alloc_assert(start_block + num_blocks <= size());
  if (is_stats_on()) {
    m_stats->add_free_calls(1);
    m_stats->add_freed(num_blocks);
  }

  lock_shared();
  alloc_dbg_assert(is_allocated(start_block, num_blocks));

  free_blocks_int(start_block, num_blocks);
  (void) sub_used_blocks(num_blocks);

  unlock();
}


void BitAllocator::set_blocks_used(int64_t start_block, int64_t num_blocks)
{
  if (num_blocks == 0) {
    return;
  }

  alloc_assert(start_block + num_blocks <= size());
  lock_shared();
  serial_lock();
  set_blocks_used_int(start_block, num_blocks);

  serial_unlock();
  unlock();
}

/*
 * Allocate N dis-contiguous blocks.
 */
int64_t BitAllocator::alloc_blocks_dis_int(int64_t num_blocks, int64_t min_alloc,
                       int64_t hint, int64_t area_blk_off, AllocatorExtentList *block_list)
{
  return alloc_blocks_dis_int_work(true, num_blocks, min_alloc, hint,
                     area_blk_off, block_list);
}

int64_t BitAllocator::alloc_blocks_dis_res(int64_t num_blocks, int64_t min_alloc,
                                           int64_t hint, AllocatorExtentList *block_list)
{
  return alloc_blocks_dis_work(num_blocks, min_alloc, hint, block_list, true);
}

int64_t BitAllocator::alloc_blocks_dis_work(int64_t num_blocks, int64_t min_alloc,
                                            int64_t hint, AllocatorExtentList *block_list, bool reserved)
{
  int scans = 1;
  int64_t allocated = 0;
  /*
   * This is root so offset is 0 yet.
   */
  int64_t blk_off = 0;

  if (!check_input_dis(num_blocks)) {
    return 0;
  }

  if (is_stats_on()) {
    m_stats->add_alloc_calls(1);
    m_stats->add_allocated(num_blocks);
  }

  lock_shared();
  serial_lock();
  if (!reserved && !reserve_blocks(num_blocks)) {
    goto exit;
  }

  if (is_stats_on()) {
    m_stats->add_concurrent_scans(scans);
  }

  while (scans && allocated < num_blocks) {
    allocated += alloc_blocks_dis_int(num_blocks - allocated, min_alloc, hint + allocated, blk_off, block_list);
    scans--;
  }

  if (allocated < num_blocks) {
    /*
     * Could not find anything in concurrent scan.
     * Go in serial manner to get something for sure
     * if available.
     */
    serial_unlock();
    unlock();
    lock_excl();
    serial_lock();
    allocated += alloc_blocks_dis_int(num_blocks - allocated, min_alloc, hint + allocated,
                                      blk_off, block_list);
    if (is_stats_on()) {
      m_stats->add_serial_scans(1);
    }
  }

  unreserve(num_blocks, allocated);
  alloc_dbg_assert(is_allocated_dis(block_list, allocated));

exit:
  serial_unlock();
  unlock();

  return allocated;
}

bool BitAllocator::is_allocated_dis(AllocatorExtentList *blocks, int64_t num_blocks)
{
  int64_t count = 0;
  for (int64_t j = 0; j < blocks->get_extent_count(); j++) {
    auto p = blocks->get_nth_extent(j);
    count += p.second;
    if (!is_allocated(p.first, p.second)) {
      return false;
    }
  }

  alloc_assert(count == num_blocks);
  return true;
}

void BitAllocator::free_blocks_dis(int64_t num_blocks, AllocatorExtentList *block_list)
{
  int64_t freed = 0;
  lock_shared();
  if (is_stats_on()) {
    m_stats->add_free_calls(1);
    m_stats->add_freed(num_blocks);
  }

  for (int64_t i = 0; i < block_list->get_extent_count(); i++) {
    free_blocks_int(block_list->get_nth_extent(i).first,
                    block_list->get_nth_extent(i).second);
    freed += block_list->get_nth_extent(i).second;
  }

  alloc_assert(num_blocks == freed);
  sub_used_blocks(num_blocks);
  alloc_assert(get_used_blocks() >= 0);
  unlock();
}

void BitAllocator::dump()
{
  int count = 0;
  serial_lock(); 
  dump_state(cct, count);
  serial_unlock(); 
}
