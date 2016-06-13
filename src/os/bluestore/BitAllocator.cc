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
 * Rest of the nodes are classified in to three catagories:
 *   root note or Allocator
 *   internal nodes or BitMapAreaIN
 *   finally nodes that contains Zones called BitMapAreaLeaf
 * This classification is according to their own implmentation of some
 * of the interfaces defined in BitMapArea.
 */

#include "BitAllocator.h"
#include <assert.h>
#include <math.h>

#define debug_assert assert
#define MAX_INT16 ((uint16_t) -1 >> 1)
#define MAX_INT32 ((uint32_t) -1 >> 1)

int64_t BitMapAreaLeaf::count = 0;
int64_t BitMapZone::count = 0;
int64_t BitMapZone::total_blocks = BITMAP_SPAN_SIZE;

/*
 * BmapEntityList functions.
 */
void BmapEntityListIter::init(BitMapAreaList *list, int64_t start_idx, bool wrap)
{
  m_list = list;
  m_start_idx = start_idx;
  m_cur_idx = m_start_idx;
  m_wrap = wrap;
  m_wrapped = false;
  m_end = false;
}

BmapEntityListIter::BmapEntityListIter(BitMapAreaList *list, int64_t start_idx)
{
  init(list, start_idx, false);
}

BmapEntityListIter::BmapEntityListIter(BitMapAreaList *list, int64_t start_idx, bool wrap)
{
  init(list, start_idx, wrap);
}

BitMapArea* BmapEntityListIter::next()
{
  int64_t cur_idx = m_cur_idx;

  if (m_wrapped &&
    cur_idx == m_start_idx) {
    /*
     * End of wrap cycle + 1
     */
    if (!m_end) {
      m_end = true;
      return m_list->get_nth_item(cur_idx);
    }
    return NULL;
  }
  m_cur_idx++;

  if (m_cur_idx == m_list->size() &&
      m_wrap) {
    m_cur_idx = 0;
    m_wrapped = true;
  }
  if (cur_idx == m_list->size()) {
    /*
     * End of list
     */
    return NULL;
  }
  debug_assert(cur_idx < m_list->size());
  return m_list->get_nth_item(cur_idx);
}

int64_t BmapEntityListIter::index()
{
  return m_cur_idx;
}

void BmapEntityListIter::decr_idx()
{
  m_cur_idx--;
  debug_assert(m_cur_idx >= 0);
}

/*
 * Bitmap Entry functions.
 */
BmapEntry::BmapEntry(bool full)
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

bmap_t BmapEntry::full_bmask() {
  return (bmap_t) -1;
}

int64_t BmapEntry::size() {
  return (sizeof(bmap_t) * 8);
}

bmap_t BmapEntry::empty_bmask() {
  return (bmap_t) 0;
}

bmap_t BmapEntry::align_mask(int x)
{
  return ((x) >= BmapEntry::size()? (bmap_t) -1 : (~(((bmap_t) -1) >> (x))));
}

bmap_t BmapEntry::bit_mask(int bit)
{
  return ((bmap_t) 0x1 << ((BmapEntry::size() - 1) - bit));
}
bool BmapEntry::check_bit(int bit)
{
  return (atomic_fetch() & bit_mask(bit));
}

bmap_t BmapEntry::atomic_fetch()
{
  return m_bits;
}

bool BmapEntry::is_allocated(int64_t start_bit, int64_t num_bits)
{
  for (int i = start_bit; i < num_bits + start_bit; i++) {
    if (!check_bit(i)) {
      return false;
    }
  }
  return true;
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
 * Find N free bits starting search from an given offset.
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
  debug_assert(max_bits > 0);

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

/*
 * Find N number of free bits in bitmap. Need not be contiguous.
 */
int BmapEntry::find_any_free_bits(int start_offset, int64_t num_blocks,
            int64_t *allocated_blocks, int64_t block_offset,
            int64_t *scanned)
{
  int allocated = 0;
  int required = num_blocks;
  int i = 0;

  *scanned = 0;

  if (atomic_fetch() == BmapEntry::full_bmask()) {
    return 0;
  }

  /*
   * Do a serial scan on bitmap.
   */
  for (i = start_offset; i < BmapEntry::size() &&
        allocated < required; i++) {
    if (check_n_set_bit(i)) {
      allocated_blocks[allocated] = i + block_offset;
      allocated++;
    }
  }

  *scanned = i - start_offset;
  return allocated;
}

/*
 * Zone related functions.
 */
void BitMapZone::init(int64_t zone_num, int64_t total_blocks, bool def)
{
  m_area_index = zone_num;
  debug_assert(size() > 0);
  m_type = ZONE;

  m_used_blocks = def? total_blocks: 0;

  int64_t num_bmaps = total_blocks / BmapEntry::size();
  debug_assert(num_bmaps < MAX_INT16);
  debug_assert(total_blocks < MAX_INT32);
  debug_assert(!(total_blocks % BmapEntry::size()));

  std::vector<BmapEntry> *bmaps = new std::vector<BmapEntry> (num_bmaps, BmapEntry(def));
  m_bmap_list = bmaps;
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

int64_t BitMapZone::get_used_blocks()
{
  return std::atomic_load(&m_used_blocks);
}

bool BitMapZone::reserve_blocks(int64_t num_blocks)
{
  debug_assert(0);
  return false;
}

void BitMapZone::unreserve(int64_t num_blocks, int64_t allocated)
{
  debug_assert(0);
}

int64_t BitMapZone::get_reserved_blocks()
{
  debug_assert(0);
  return 0;
}

BitMapZone::BitMapZone(int64_t total_blocks, int64_t zone_num)
{
  init(zone_num, total_blocks, false);
}

BitMapZone::BitMapZone(int64_t total_blocks, int64_t zone_num, bool def)
{
  init(zone_num, total_blocks, def);
}

void BitMapZone::shutdown()
{
}

BitMapZone::~BitMapZone()
{
  delete m_bmap_list;
}

/*
 * Check if some search took zone marker to end.
 */
bool BitMapZone::is_exhausted()
{
  debug_assert(check_locked());
  if (get_used_blocks() == size()) {
    return true;
  } else {
    return false;
  }
}

bool BitMapZone::is_allocated(int64_t start_block, int64_t num_blocks)
{
  BmapEntry *bmap = NULL;
  int bit = 0;
  int64_t falling_in_bmap = 0;

  while (num_blocks) {
    bit = start_block % BmapEntry::size();
    bmap = &(*m_bmap_list)[start_block / BmapEntry::size()];
    falling_in_bmap = MIN(num_blocks, BmapEntry::size() - bit);

    if (!bmap->is_allocated(bit, falling_in_bmap)) {
      return false;
    }

    start_block += falling_in_bmap;
    num_blocks -= falling_in_bmap;
  }

  return true;
}

/*
 * Allocate N continuous bits in a zone starting from
 * marker provided in iter.
 */
int64_t BitMapZone::alloc_cont_bits(int64_t num_blocks,
         BitMapEntityIter<BmapEntry> *iter,
         int64_t *scanned)
{
  BmapEntry *bmap = NULL;
  int64_t required = num_blocks;
  debug_assert(check_locked());
  while ((bmap = (BmapEntry *) iter->next())) {
    int64_t found = 0;
    int64_t max_expected = MIN(required, BmapEntry::size());
    found = bmap->find_n_cont_bits(0, max_expected);

    required -= found;

    if (found < max_expected) {
      break;
    }
  }

  /*
   * scanned == allocated
   */
  *scanned = num_blocks - required;
  return (num_blocks - required);
}

void BitMapZone::set_blocks_used(int64_t start_block, int64_t num_blocks)
{
  BmapEntry *bmap = NULL;
  int bit = 0;
  int64_t falling_in_bmap = 0;
  int64_t blks = num_blocks;

  while (blks) {
    bit = start_block % BmapEntry::size();
    bmap = &(*m_bmap_list)[start_block / BmapEntry::size()];
    falling_in_bmap = MIN(blks, BmapEntry::size() - bit);

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

  while (num_blocks) {
    bit = start_block % BmapEntry::size();
    bmap = &(*m_bmap_list)[start_block / BmapEntry::size()];
    falling_in_bmap = MIN(num_blocks, BmapEntry::size() - bit);

    bmap->clear_bits(bit, falling_in_bmap);

    start_block += falling_in_bmap;
    num_blocks -= falling_in_bmap;
  }
}

void BitMapZone::lock_excl()
{
  m_lock.lock();
}

bool BitMapZone::lock_excl_try()
{
  if (m_lock.try_lock()) {
    return true;
  }
  return false;
}

void BitMapZone::unlock()
{
  m_lock.unlock();
}

bool BitMapZone::check_locked()
{
  return !lock_excl_try();
}

/*
 * Try find N contiguous blocks in a Zone.
 * Nothing less than N is good and considered failure.
 *
 * Caller must take exclusive lock on Zone.
 */
int64_t BitMapZone::alloc_blocks(int64_t num_blocks, int64_t *start_block)
{
  int64_t bmap_idx = 0;
  int bit_idx = 0;
  BmapEntry *bmap = NULL;
  int64_t allocated = 0;

  debug_assert(check_locked());

  bit_idx = 0;
  bmap_idx = 0;
  BitMapEntityIter <BmapEntry> iter = BitMapEntityIter<BmapEntry>(
          m_bmap_list, bmap_idx);

  while ((bmap = (BmapEntry *) iter.next())) {
    int64_t scanned = 0;
    int start_offset = -1;

    allocated = bmap->find_first_set_bits(num_blocks,
          bit_idx, &start_offset, &scanned);

    bit_idx = 0;

    if (allocated > 0) {
      (*start_block) = start_offset +
               (iter.index() - 1) * bmap->size();

      allocated += alloc_cont_bits(num_blocks - allocated,
                &iter, &scanned);
      /*
       * Iter need to go one step back for case when allocation
       * is not enough and start from last bitmap again.
       */
      iter.decr_idx();
      bit_idx = scanned % BmapEntry::size();
    }

    if (allocated < num_blocks) {
      free_blocks_int(*start_block, allocated);
      allocated = 0;
      *start_block = 0;
    } else {
      /*
       * Got required.
       */
      break;
    }
  }

  add_used_blocks(allocated);
  return allocated;
}

void BitMapZone::free_blocks(int64_t start_block, int64_t num_blocks)
{
  free_blocks_int(start_block, num_blocks);
  sub_used_blocks(num_blocks);
  debug_assert(get_used_blocks() >= 0);
}

/*
 * Allocate N blocks, dis-contiguous are fine
 */
int64_t BitMapZone::alloc_blocks_dis(int64_t num_blocks, int64_t zone_blk_off, int64_t *alloc_blocks)
{
  int64_t bmap_idx = 0;
  int bit = 0;
  BmapEntry *bmap = NULL;
  int64_t allocated = 0;
  int64_t blk_off = 0;

  debug_assert(check_locked());

  BitMapEntityIter <BmapEntry> iter = BitMapEntityIter<BmapEntry>(
          m_bmap_list, bmap_idx);
  while ((bmap = (BmapEntry *) iter.next())) {
    int64_t scanned = 0;
    blk_off = (iter.index() - 1) * BmapEntry::size() + zone_blk_off;
    allocated += bmap->find_any_free_bits(bit, num_blocks - allocated,
            &alloc_blocks[allocated], blk_off, &scanned);

  }

  add_used_blocks(allocated);

  return allocated;
}

/*
 * BitMapArea Leaf and non-Leaf functions.
 */
int64_t BitMapArea::get_span_size()
{
  return BITMAP_SPAN_SIZE;
}

bmap_area_type_t BitMapArea::level_to_type(int level)
{
  if (level == 0) {
    return ZONE;
  } else if (level == 1) {
    return LEAF;
  } else {
    return NON_LEAF;
  }
}

int BitMapArea::get_level(int64_t total_blocks)
{
  int level = 1;
  int64_t span_size = BitMapArea::get_span_size();
  int64_t spans = span_size * span_size;
  while (spans < total_blocks) {
    spans *= span_size;
    level++;
  }
  return level;
}

int64_t BitMapArea::get_index()
{
  return m_area_index;
}

bmap_area_type_t BitMapArea::get_type()
{
  return m_type;
}

/*
 * BitMapArea Leaf and Internal
 */
BitMapAreaIN::BitMapAreaIN()
{
  // nothing
}

void BitMapAreaIN::init_common(int64_t total_blocks, int64_t area_idx, bool def)
{
  m_area_index = area_idx;
  m_total_blocks = total_blocks;
  m_level = BitMapArea::get_level(total_blocks);
  m_type = BitMapArea::level_to_type(m_level);
  m_reserved_blocks = 0;

  m_used_blocks = def? total_blocks: 0;
}

void BitMapAreaIN::init(int64_t total_blocks, int64_t area_idx, bool def)
{
  int64_t num_child = 0;
  debug_assert(!(total_blocks % BmapEntry::size()));

  init_common(total_blocks, area_idx, def);
  int64_t level_factor = pow(BitMapArea::get_span_size(), m_level);

  num_child = (total_blocks + level_factor - 1) / level_factor;
  debug_assert(num_child < MAX_INT16);

  m_child_size_blocks = level_factor;

  BitMapArea **children = new BitMapArea*[num_child];
  int i = 0;
  for (i = 0; i < num_child - 1; i++) {
    if (m_level <= 2) {
      children[i] = new BitMapAreaLeaf(m_child_size_blocks, i, def);
    } else {
      children[i] = new BitMapAreaIN(m_child_size_blocks, i, def);
    }
    total_blocks -= m_child_size_blocks;
  }

  int last_level = BitMapArea::get_level(total_blocks);
  if (last_level == 1) {
    children[i] = new BitMapAreaLeaf(total_blocks, i, def);
  } else {
    children[i] = new BitMapAreaIN(total_blocks, i, def);
  }
  BitMapAreaList *list = new BitMapAreaList(children, num_child);
  m_child_list = list;
  m_num_child = num_child;
}

BitMapAreaIN::BitMapAreaIN(int64_t total_blocks, int64_t area_idx)
{
  init(total_blocks, area_idx, false);
}

BitMapAreaIN::BitMapAreaIN(int64_t total_blocks, int64_t area_idx, bool def)
{
  init(total_blocks, area_idx, def);
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

  return true;
}

void BitMapAreaIN::child_unlock(BitMapArea *child)
{
  child->unlock();
}

bool BitMapAreaIN::is_exhausted()
{
  if (get_used_blocks() == size()) {
    return true;
  }
  return false;
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
  debug_assert(m_used_blocks >= 0);
  return used_blks;
}

int64_t BitMapAreaIN::get_used_blocks()
{
  std::lock_guard<std::mutex> l(m_blocks_lock);
  return m_used_blocks;
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
  debug_assert(m_used_blocks <= size());
  return res;
}

void BitMapAreaIN::unreserve(int64_t needed, int64_t allocated)
{
  std::lock_guard<std::mutex> l(m_blocks_lock);
  m_used_blocks -= (needed - allocated);
  m_reserved_blocks -= needed;
  debug_assert(m_used_blocks >= 0);
  debug_assert(m_reserved_blocks >= 0);
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

  debug_assert(start_block >= 0 &&
      (start_block + num_blocks <= size()));

  if (num_blocks == 0) {
    return true;
  }

  while (num_blocks) {
    area = (BitMapArea *) m_child_list->get_nth_item(
                    start_block / m_child_size_blocks);

    area_block_offset = start_block % m_child_size_blocks;
    falling_in_area = MIN(m_child_size_blocks - area_block_offset,
              num_blocks);
    if (!area->is_allocated(area_block_offset, falling_in_area)) {
      return false;
    }
    start_block += falling_in_area;
    num_blocks -= falling_in_area;
  }
  return true;
}

bool BitMapAreaIN::is_allocated(int64_t *alloc_blocks, int64_t num_blocks, int64_t blk_off)
{
  for (int64_t i = 0; i < num_blocks; i++) {
    if (!is_allocated(alloc_blocks[i] - blk_off, 1)) {
      return false;
    }
  }

  return true;
}

int64_t BitMapAreaIN::alloc_blocks_int(bool wait, bool wrap,
                         int64_t num_blocks, int64_t *start_block)
{
  BitMapArea *child = NULL;
  int64_t allocated = 0;

  *start_block = 0;
  BmapEntityListIter iter = BmapEntityListIter(
                                m_child_list, 0, wrap);

  while ((child = (BitMapArea *) iter.next())) {
    if (!child_check_n_lock(child, num_blocks - allocated)) {
      continue;
    }

    allocated = child->alloc_blocks(wait, num_blocks, start_block);
    child_unlock(child);
    if (allocated == num_blocks) {
      (*start_block) += child->get_index() * m_child_size_blocks;
      break;
    }

    child->free_blocks(*start_block, allocated);
    *start_block = 0;
    allocated = 0;
  }
  return allocated;
}

int64_t BitMapAreaIN::alloc_blocks(bool wait, int64_t num_blocks,
                      int64_t *start_block)
{
  int64_t allocated = 0;

  lock_shared();

  if (!reserve_blocks(num_blocks)) {
    goto exit;
  }

  allocated = alloc_blocks_int(wait, false, num_blocks, start_block);

  unreserve(num_blocks, allocated);
  debug_assert((get_used_blocks() <= m_total_blocks));
  debug_assert(is_allocated(*start_block, allocated));

exit:
  unlock();
  return allocated;
}

int64_t BitMapAreaIN::alloc_blocks_dis_int(bool wait, int64_t num_blocks,
           int64_t area_blk_off, int64_t *block_list)
{
  BitMapArea *child = NULL;
  int64_t allocated = 0;
  int64_t blk_off = 0;

  BmapEntityListIter iter = BmapEntityListIter(
        m_child_list, 0, true);

  while ((child = (BitMapArea *) iter.next())) {
    if (!child_check_n_lock(child, 1)) {
      continue;
    }

    blk_off = child->get_index() * m_child_size_blocks + area_blk_off;
    allocated += child->alloc_blocks_dis(wait, num_blocks,
                            blk_off, &block_list[allocated]);
    child_unlock(child);
    if (allocated == num_blocks) {
      break;
    }
  }

  return allocated;
}

int64_t BitMapAreaIN::alloc_blocks_dis(bool wait, int64_t num_blocks,
           int64_t blk_off, int64_t *block_list)
{
  int64_t allocated = 0;

  lock_shared();
  allocated += alloc_blocks_dis_int(wait, num_blocks, blk_off, &block_list[allocated]);
  add_used_blocks(allocated);
  debug_assert(is_allocated(block_list, allocated, blk_off));

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

  debug_assert(start_block >= 0);

  while (blks) {
    child = (BitMapArea *) m_child_list->get_nth_item(
                  start_blk / m_child_size_blocks);

    child_block_offset = start_blk % child->size();
    falling_in_child = MIN(m_child_size_blocks - child_block_offset,
              blks);
    child->set_blocks_used(child_block_offset, falling_in_child);
    start_blk += falling_in_child;
    blks -= falling_in_child;
  }

  add_used_blocks(num_blocks);
  debug_assert(is_allocated(start_block, num_blocks));
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

  debug_assert(start_block >= 0 &&
    (start_block + num_blocks) <= size());

  if (num_blocks == 0) {
    return;
  }

  while (num_blocks) {
    child = (BitMapArea *) m_child_list->get_nth_item(
          start_block / m_child_size_blocks);

    child_block_offset = start_block % m_child_size_blocks;

    falling_in_child = MIN(m_child_size_blocks - child_block_offset,
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
  debug_assert(is_allocated(start_block, num_blocks));

  free_blocks_int(start_block, num_blocks);
  (void) sub_used_blocks(num_blocks);

  unlock();
}

/*
 * BitMapArea Leaf
 */
BitMapAreaLeaf::BitMapAreaLeaf(int64_t total_blocks, int64_t area_idx)
{
  init(total_blocks, area_idx, false);
}

BitMapAreaLeaf::BitMapAreaLeaf(int64_t total_blocks, int64_t area_idx, bool def)
{
  init(total_blocks, area_idx, def);
}

void BitMapAreaLeaf::init(int64_t total_blocks, int64_t area_idx,
          bool def)
{
  int64_t num_child = 0;
  debug_assert(!(total_blocks % BmapEntry::size()));

  init_common(total_blocks, area_idx, def);
  num_child = total_blocks / pow(BitMapArea::get_span_size(), m_level);
  m_child_size_blocks = total_blocks / num_child;

  debug_assert(m_level == 1);
   BitMapArea **children = new BitMapArea*[num_child];
  for (int i = 0; i < num_child; i++) {
      children[i] = new BitMapZone(m_child_size_blocks, i, def);
  }

  BitMapAreaList *list = new BitMapAreaList(children, num_child);

  m_child_list = list;
  m_num_child = num_child;

  BitMapAreaLeaf::incr_count();
}

BitMapAreaLeaf::~BitMapAreaLeaf()
{
  lock_excl();

  BitMapAreaList *list = m_child_list;
  for (int64_t i = 0; i < list->size(); i++) {
    BitMapArea *child = (BitMapArea *) list->get_nth_item(i);
    delete child;
  }

  delete [] list->get_item_list();
  delete list;

  unlock();
}

bool BitMapAreaLeaf::child_check_n_lock(BitMapArea *child, int64_t required, bool lock)
{
  if (lock) {
    child->lock_excl();
  } else if (!child->lock_excl_try()) {
    return false;
  }

  if (child->is_exhausted()) {
    child->unlock();
    return false;
  }
  return true;
}

void BitMapAreaLeaf::child_unlock(BitMapArea *child)
{
  child->unlock();
}

int64_t BitMapAreaLeaf::alloc_blocks_int(bool wait, bool wrap,
                         int64_t num_blocks, int64_t *start_block)
{
  BitMapArea *child = NULL;
  int64_t allocated = 0;

  *start_block = 0;

  BmapEntityListIter iter = BmapEntityListIter(
                                m_child_list, 0, false);

  while ((child = iter.next())) {
    if (!child_check_n_lock(child, num_blocks - allocated, false)) {
      continue;
    }
    debug_assert(child->get_type() == ZONE);

    allocated = child->alloc_blocks(num_blocks, start_block);
    child_unlock(child);
    if (allocated == num_blocks) {
      (*start_block) += child->get_index() * m_child_size_blocks;
      break;
    }

    child->free_blocks(*start_block, allocated);
    *start_block = 0;
    allocated = 0;
  }
  return allocated;
}

int64_t BitMapAreaLeaf::alloc_blocks_dis_int(bool wait, int64_t num_blocks,
                                 int64_t area_blk_off, int64_t *block_list)
{
  BitMapArea *child = NULL;
  int64_t allocated = 0;
  int64_t blk_off = 0;

  BmapEntityListIter iter = BmapEntityListIter(
        m_child_list, 0, false);

  while ((child = (BitMapArea *) iter.next())) {
    if (!child_check_n_lock(child, 1, false)) {
      continue;
    }

    blk_off = child->get_index() * m_child_size_blocks + area_blk_off;
    allocated += child->alloc_blocks_dis(num_blocks, blk_off, &block_list[allocated]);
    child_unlock(child);
    if (allocated == num_blocks) {
      break;
    }
  }
  return allocated;
}

void BitMapAreaLeaf::free_blocks_int(int64_t start_block, int64_t num_blocks)
{
  BitMapArea *child = NULL;
  int64_t child_block_offset = 0;
  int64_t falling_in_child = 0;

  debug_assert(start_block >= 0 &&
    (start_block + num_blocks) <= size());

  if (num_blocks == 0) {
    return;
  }

  while (num_blocks) {
    child = (BitMapArea *) m_child_list->get_nth_item(
          start_block / m_child_size_blocks);

    child_block_offset = start_block % m_child_size_blocks;

    falling_in_child = MIN(m_child_size_blocks - child_block_offset,
              num_blocks);

    child->lock_excl();
    child->free_blocks(child_block_offset, falling_in_child);
    child->unlock();
    start_block += falling_in_child;
    num_blocks -= falling_in_child;
  }
}

/*
 * BitMapArea List related functions
 */
BitMapAreaList::BitMapAreaList(BitMapArea **list, int64_t len)
{
  m_items = list;
  m_num_items = len;
  return;
}

/*
 * Main allocator functions.
 */
BitAllocator::BitAllocator(int64_t total_blocks, int64_t zone_size_block, bmap_alloc_mode_t mode)
{
  init_check(total_blocks, zone_size_block, mode, false, false);
}

BitAllocator::BitAllocator(int64_t total_blocks, int64_t zone_size_block,
         bmap_alloc_mode_t mode, bool def)
{
  init_check(total_blocks, zone_size_block, mode, def, false);
}

BitAllocator::BitAllocator(int64_t total_blocks, int64_t zone_size_block,
         bmap_alloc_mode_t mode, bool def, bool stats_on)
{
  init_check(total_blocks, zone_size_block, mode, def, stats_on);
}

void BitAllocator::init_check(int64_t total_blocks, int64_t zone_size_block,
       bmap_alloc_mode_t mode, bool def, bool stats_on)
{
  int64_t unaligned_blocks = 0;

  if (mode != SERIAL && mode != CONCURRENT) {
    debug_assert(0);
  }

  if (total_blocks <= 0) {
    debug_assert(0);
  }

  if (zone_size_block == 0 ||
    zone_size_block < BmapEntry::size()) {
    debug_assert(0);
  }

  zone_size_block = (zone_size_block / BmapEntry::size()) *
        BmapEntry::size();

  if (total_blocks < zone_size_block) {
    debug_assert(0);
  }

  unaligned_blocks = total_blocks % zone_size_block;
  total_blocks = ROUND_UP_TO(total_blocks, zone_size_block);

  m_alloc_mode = mode;
  m_is_stats_on = stats_on;
  if (m_is_stats_on) {
    m_stats = new BitAllocatorStats();
  }

  pthread_rwlock_init(&m_rw_lock, NULL);
  init(total_blocks, 0, def);
  if (!def && unaligned_blocks) {
    /*
     * Mark extra padded blocks used from begning.
     */
    set_blocks_used(total_blocks - (zone_size_block - unaligned_blocks),
                 (zone_size_block - unaligned_blocks));
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

void BitAllocator::unlock()
{
  pthread_rwlock_unlock(&m_rw_lock);
}

BitAllocator::~BitAllocator()
{
  lock_excl();

  BitMapAreaList *list = m_child_list;
  for (int64_t i = 0; i < list->size(); i++) {
    BitMapArea *child = (BitMapArea *) list->get_nth_item(i);
    delete child;
  }

  delete [] list->get_item_list();
  delete list;

  unlock();
  pthread_rwlock_destroy(&m_rw_lock);
}

void
BitAllocator::shutdown()
{
  lock_excl();
  serial_lock();
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

bool BitAllocator::child_check_n_lock(BitMapArea *child, int64_t required)
{
  child->lock_shared();

  if (child->is_exhausted()) {
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
  if (num_blocks == 0) {
    return false;
  }

  if (num_blocks > size()) {
    return false;
  }
  return true;
}

bool BitAllocator::check_input(int64_t num_blocks)
{
  if (num_blocks == 0) {
    return false;
  }

  if (num_blocks > BitMapArea::get_span_size()) {
    return false;
  }
  return true;
}

/*
 * Interface to allocate blocks after reserve.
 */
int64_t BitAllocator::alloc_blocks_res(int64_t num_blocks, int64_t *start_block)
{
  int scans = 1;
  int64_t allocated = 0;

  *start_block = 0;
  if (!check_input(num_blocks)) {
    return 0;
  }

  lock_shared();
  serial_lock();

  if (is_stats_on()) {
    m_stats->add_concurrent_scans(scans);
  }

  while (scans && !allocated) {
    allocated = alloc_blocks_int(false, true, num_blocks, start_block);
    scans--;
  }

  if (!allocated) {
    /*
     * Could not find anything in two scans.
     * Go in serial manner.
     */
    serial_unlock();
    unlock();
    lock_excl();
    serial_lock();
    allocated = alloc_blocks_int(false, true, num_blocks, start_block);
    if (is_stats_on()) {
      m_stats->add_serial_scans(1);
    }
  }

  debug_assert(is_allocated(*start_block, allocated));
  unreserve(num_blocks, allocated);

  serial_unlock();
  unlock();

  return allocated;
}

int64_t BitAllocator::alloc_blocks(int64_t num_blocks, int64_t *start_block)
{
  int scans = 1;
  int64_t allocated = 0;

  *start_block = 0;
  if (!check_input(num_blocks)) {
    debug_assert(0);
    return 0;
  }

  lock_shared();
  serial_lock();

  if (!reserve_blocks(num_blocks)) {
    goto exit;
  }
  if (is_stats_on()) {
    m_stats->add_alloc_calls(1);
    m_stats->add_allocated(num_blocks);
  }

  if (is_stats_on()) {
    m_stats->add_concurrent_scans(scans);
  }

  while (scans && !allocated) {
    allocated = alloc_blocks_int(false, true,  num_blocks, start_block);
    scans--;
  }

  if (!allocated) {
    /*
     * Could not find anything in two scans.
     * Go in serial manner.
     */
    serial_unlock();
    unlock();
    lock_excl();
    serial_lock();
    allocated = alloc_blocks_int(false, true, num_blocks, start_block);
    if (!allocated) {
      allocated = alloc_blocks_int(false, true, num_blocks, start_block);
      debug_assert(allocated);
    }
    if (is_stats_on()) {
      m_stats->add_serial_scans(1);
    }
  }

  unreserve(num_blocks, allocated);
  debug_assert((get_used_blocks() <= m_total_blocks));
  debug_assert(is_allocated(*start_block, allocated));

exit:
  serial_unlock();
  unlock();

  return allocated;
}

void BitAllocator::free_blocks(int64_t start_block, int64_t num_blocks)
{
  if (num_blocks == 0) {
    return;
  }

  debug_assert(start_block + num_blocks <= size());
  if (is_stats_on()) {
    m_stats->add_free_calls(1);
    m_stats->add_freed(num_blocks);
  }

  lock_shared();
  debug_assert(is_allocated(start_block, num_blocks));

  free_blocks_int(start_block, num_blocks);
  (void) sub_used_blocks(num_blocks);

  unlock();
}


void BitAllocator::set_blocks_used(int64_t start_block, int64_t num_blocks)
{
  if (num_blocks == 0) {
    return;
  }

  debug_assert(start_block + num_blocks <= size());
  lock_shared();
  serial_lock();
  set_blocks_used_int(start_block, num_blocks);

  serial_unlock();
  unlock();
}

/*
 * Allocate N dis-contiguous blocks.
 */
int64_t BitAllocator::alloc_blocks_dis(int64_t num_blocks, int64_t *block_list)
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
  if (!reserve_blocks(num_blocks)) {
    goto exit;
  }

  if (is_stats_on()) {
    m_stats->add_concurrent_scans(scans);
  }

  while (scans && allocated < num_blocks) {
    allocated += alloc_blocks_dis_int(false, num_blocks, blk_off, &block_list[allocated]);
    scans--;
  }

  if (allocated < num_blocks) {
    /*
     * Could not find anything in two scans.
     * Go in serial manner to get something for sure
     * if available.
     */
    serial_unlock();
    unlock();
    lock_excl();
    serial_lock();
    allocated += alloc_blocks_dis_int(false, num_blocks, blk_off, &block_list[allocated]);
    if (is_stats_on()) {
      m_stats->add_serial_scans(1);
    }
  }

  unreserve(num_blocks, allocated);
  debug_assert(is_allocated(block_list, allocated, 0));

exit:
  serial_unlock();
  unlock();

  return allocated;
}

void BitAllocator::free_blocks_dis(int64_t num_blocks, int64_t *block_list)
{
  lock_shared();
  if (is_stats_on()) {
    m_stats->add_free_calls(1);
    m_stats->add_freed(num_blocks);
  }

  for (int64_t i = 0; i < num_blocks; i++) {
    free_blocks_int(block_list[i], 1);
  }

  sub_used_blocks(num_blocks);
  debug_assert(get_used_blocks() >= 0);
  unlock();
}
