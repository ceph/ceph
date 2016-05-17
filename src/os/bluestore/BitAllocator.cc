// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */

#include "BitAllocator.h"
#include <assert.h>

#define debug_assert assert
#define MIN(x, y) ((x) > (y) ? (y) : (x))

/*
 * BmapEntityList functions.
 */
BmapEntityListIter::BmapEntityListIter(BmapEntityList *list)
{
  m_list = list;
  m_start_idx = list->get_marker();
  m_cur_idx = m_start_idx;
  m_wrap = false;
}

BmapEntityListIter::BmapEntityListIter(BmapEntityList *list, bool wrap)
{
  m_list = list;
  m_start_idx = list->get_marker();
  m_cur_idx = m_start_idx;
  m_wrap = wrap;
}

BmapEntityListIter::BmapEntityListIter(BmapEntityList *list, int64_t start_idx)
{
  m_list = list;
  m_wrap = false;
  m_start_idx = start_idx;
  m_cur_idx = m_start_idx;
  m_wrapped = false;
}

BmapEntityListIter::BmapEntityListIter(BmapEntityList *list, int64_t start_idx, bool wrap)
{
  m_list = list;
  m_wrap = wrap;
  m_start_idx = start_idx;
  m_cur_idx = m_start_idx;
  m_wrapped = false;
}

BmapEntity * BmapEntityListIter::next()
{
  int64_t cur_idx = m_cur_idx;

  if (m_wrapped &&
    cur_idx == m_start_idx) {
    /*
     * End of wrap cycle
     */
    return NULL;
  }
  m_cur_idx++;

  if (m_cur_idx == m_list->size()
      &&m_wrap) {
    m_cur_idx %= m_list->size();
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
  debug_assert(m_cur_idx > 0);
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
  return (m_bits & bit_mask(bit));
}

bmap_t BmapEntry::atomic_fetch()
{
 return std::atomic_load(&m_bits);
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
  (void) std::atomic_fetch_and(&m_bits, ~(bmask));
}

void BmapEntry::clear_bits(int offset, int num_bits)
{
  if (num_bits == 0) {
    return;
  }
  bmap_t bmask = BmapEntry::align_mask(num_bits) >> offset;
  (void) std::atomic_fetch_and(&m_bits, ~(bmask));
}

void BmapEntry::set_bits(int offset, int num_bits)
{
  for (int i = 0; i < num_bits; i++) {
    bmap_t bmask = bit_mask(i + offset);
    (void) std::atomic_fetch_or(&m_bits, bmask);
  }
}

/*
 * Allocate a bit if it was free.
 * Retruns true if it was free.
 */
bool BmapEntry::check_n_set_bit(int bit)
{
  bmap_t bmask = bit_mask(bit);
  return !(atomic_fetch_or(&m_bits, bmask) & bmask);
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
 * Bitmap List related functions.
 */
int64_t BmapList::incr_marker(int64_t add)
{
  return std::atomic_fetch_add(&m_marker, add);
}

void BmapList::set_marker(int64_t val)
{
 atomic_store(&m_marker, val);
}

int64_t BmapList::get_marker()
{
 return std::atomic_load(&m_marker);
}

/*
 * Zone related functions.
 */
void BitMapZone::init(int64_t zone_num, int64_t total_blocks, bool def)
{
  m_zone_num = zone_num;
  m_total_blocks = total_blocks;

  m_used_blocks = def? total_blocks: 0;

  int64_t num_bmaps = total_blocks / BmapEntry::size();
  debug_assert(!(total_blocks % BmapEntry::size()));

  BmapEntity **bmaps = new BmapEntity *[num_bmaps];
  for (int i = 0; i < num_bmaps; i++) {
    bmaps[i] = new BmapEntry(def);
  }

  BmapEntityList *list = new BmapList(bmaps, num_bmaps, 0);

  m_bmap_list = list;
  m_state = ZONE_ACTIVE;
}

BitMapZone::BitMapZone(int64_t zone_num, int64_t total_blocks)
{
  init(zone_num, total_blocks, false);
}

BitMapZone::BitMapZone(int64_t zone_num, int64_t total_blocks, bool def)
{
  init(zone_num, total_blocks, def);
}

BitMapZone::~BitMapZone()
{
  lock_zone(true);

  m_state = ZONE_FREE;
  BmapEntityList *list = m_bmap_list;

  for (int64_t i = 0; i < list->size(); i++) {
    BmapEntry *bmap = (BmapEntry *) list->get_nth_item(i);
    delete bmap;
  }

  delete [] list->get_item_list();
  delete list;

  unlock_zone();
}

/*
 * Check if some search took zone marker to end.
 */
bool BitMapZone::is_exhausted()
{
  if (m_bmap_list->get_marker() >=
    m_total_blocks) {
    m_bmap_list->set_marker(0);
    return true;
  }
  return false;
}

void BitMapZone::reset_marker()
{
  m_bmap_list->set_marker(0);
}

bool BitMapZone::is_allocated(int64_t start_block, int64_t num_blocks)
{
  BmapEntry *bmap = NULL;
  int bit = 0;
  int64_t falling_in_bmap = 0;

  while (num_blocks) {
    bit = start_block % BmapEntry::size();
    bmap = (BmapEntry *) m_bmap_list->get_nth_item(start_block /
               BmapEntry::size());
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
         BmapEntityListIter *iter,
         int64_t *scanned)
{
  BmapEntry *bmap = NULL;
  int64_t required = num_blocks;
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
    bmap = (BmapEntry *) m_bmap_list->get_nth_item(start_block /
               BmapEntry::size());
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
    bmap = (BmapEntry *) m_bmap_list->get_nth_item(start_block /
               BmapEntry::size());
    falling_in_bmap = MIN(num_blocks, BmapEntry::size() - bit);

    bmap->clear_bits(bit, falling_in_bmap);

    start_block += falling_in_bmap;
    num_blocks -= falling_in_bmap;
  }

}

int64_t BitMapZone::get_index()
{
  return m_zone_num;
}

bool BitMapZone::lock_zone(bool wait)
{
  if (wait) {
    m_lock.lock();
    return true;
  }

  if (m_lock.try_lock()) {
    return true;
  } else {
    return false;
  }
}

void BitMapZone::unlock_zone()
{
  m_lock.unlock();
}

int64_t BitMapZone::get_used_blocks()
{
  return std::atomic_load(&m_used_blocks);
}

int64_t BitMapZone::size() {
  return m_total_blocks;
}

int64_t BitMapZone::add_used_blocks(int64_t blks)
{
  return std::atomic_fetch_add(&m_used_blocks, blks) + blks;
}

int64_t BitMapZone::sub_used_blocks(int64_t blks)
{
  int64_t used_blks = std::atomic_fetch_sub(&m_used_blocks, blks) - blks;
  debug_assert(used_blks >= 0);
  return used_blks;
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
  int64_t marker = m_bmap_list->get_marker();
  int64_t marker_add = 0;
  BmapEntry *bmap = NULL;
  int64_t allocated = 0;
  int64_t zone_block_offset = get_index() * size();

  bmap_idx = marker / BmapEntry::size();
  bit_idx = marker % BmapEntry::size();

  BmapEntityListIter iter = BmapEntityListIter(
          m_bmap_list, bmap_idx);

  while ((bmap = (BmapEntry *) iter.next())) {
    int64_t scanned = 0;
    int start_offset = -1;

    allocated = bmap->find_first_set_bits(num_blocks,
          bit_idx, &start_offset, &scanned);

    marker_add += scanned;
    bit_idx = 0;

    if (allocated > 0) {
      (*start_block) = start_offset +
               (iter.index() - 1) * bmap->size() +
          m_zone_num * size();

      allocated += alloc_cont_bits(num_blocks - allocated,
                &iter, &scanned);
      marker_add += scanned;
      /*
       * Iter need to go one step back for case when allocation
       * is not enough and start from last bitmap again.
       */
      iter.decr_idx();
      bit_idx = scanned % BmapEntry::size();
    }

    if (allocated < num_blocks) {
      free_blocks_int((*start_block - zone_block_offset), allocated);
      allocated = 0;
    } else {
      /*
       * Got required.
       */
      break;
    }
  }

  m_bmap_list->incr_marker(marker_add);
  add_used_blocks(allocated);

  return allocated;
}

void BitMapZone::free_blocks(int64_t start_block, int64_t num_blocks)
{
  free_blocks_int(start_block, num_blocks);
  debug_assert(get_used_blocks() > 0);
  sub_used_blocks(num_blocks);
}

/*
 * Allocate N blocks, dis-contiguous are fine
 */
int64_t BitMapZone::alloc_blocks_dis(int64_t num_blocks, int64_t *alloc_blocks)
{
  int64_t bmap_idx = 0;
  int bit = 0;
  int64_t marker_add = 0;
  BmapEntry *bmap = NULL;
  int64_t allocated = 0;
  int64_t blk_off = 0;

  bmap_idx = m_bmap_list->get_marker() / BmapEntry::size();
  bit = m_bmap_list->get_marker() % BmapEntry::size();

  BmapEntityListIter iter = BmapEntityListIter(
        m_bmap_list, bmap_idx);
  while ((bmap = (BmapEntry *) iter.next())) {
    int64_t scanned = 0;
    blk_off = (iter.index() - 1) * BmapEntry::size() +
        m_zone_num * size();
    allocated += bmap->find_any_free_bits(bit, num_blocks - allocated,
            &alloc_blocks[allocated], blk_off, &scanned);

    marker_add += scanned;
  }

  m_bmap_list->incr_marker(marker_add);
  add_used_blocks(allocated);

  return allocated;
}

/*
 * Zone List related functions
 */

ZoneList::ZoneList(BmapEntity **list, int64_t len) :
      BmapEntityList(list, len)
{
  m_marker = 0;
  return;
}

ZoneList::ZoneList(BmapEntity **list, int64_t len, int64_t marker) :
    BmapEntityList(list, len)
{
  m_marker = marker;
  return;
}

int64_t ZoneList::incr_marker(int64_t add) {
  std::lock_guard<std::mutex> l (m_marker_mutex);
  m_marker += add;
  m_marker %= size();

  return m_marker;
}

int64_t ZoneList::get_marker()
{
  std::lock_guard<std::mutex> l (m_marker_mutex);
  return m_marker;
}

void ZoneList::set_marker(int64_t val)
{
  std::lock_guard<std::mutex> l (m_marker_mutex);
  m_marker = val;
}

/*
 * Main allocator functions.
 */
BitAllocator::BitAllocator(int64_t total_blocks, int64_t zone_size_block, bmap_alloc_mode_t mode)
{
  init(total_blocks, zone_size_block, mode, false);
}

BitAllocator::BitAllocator(int64_t total_blocks, int64_t zone_size_block,
         bmap_alloc_mode_t mode, bool def)
{
  init(total_blocks, zone_size_block, mode, def);
}

void BitAllocator::init(int64_t total_blocks, int64_t zone_size_block,
       bmap_alloc_mode_t mode, bool def)
{
  int64_t total_zones = 0;

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

  total_blocks = (total_blocks / zone_size_block) * zone_size_block;
  total_zones = total_blocks / zone_size_block;

  debug_assert(total_blocks > 0);
  debug_assert(total_zones > 0);

  pthread_rwlock_init(&m_alloc_slow_lock, NULL);

  m_total_blocks = total_blocks;
  m_total_zones = total_zones;
  m_zone_size_blocks = zone_size_block;
  m_alloc_mode = mode;
  m_allocated_blocks = def? total_blocks: 0;
  m_reserved_blocks = 0;

  BmapEntity **zonesp = new BmapEntity* [total_zones];
  for (int i = 0; i < total_zones; i++) {
    zonesp[i] = new BitMapZone(i, zone_size_block, def);
  }

  BmapEntityList *list = new ZoneList(zonesp, total_zones, 0);

  m_zone_list = list;

  m_state = ALLOC_ACTIVE;
}

BitAllocator::~BitAllocator()
{
  alloc_lock(true);

  m_state = ALLOC_DESTROY;
  BmapEntityList *list = m_zone_list;

  for (int64_t i = 0; i < list->size(); i++) {
    BitMapZone *zone = (BitMapZone *) list->get_nth_item(i);
    delete zone;
  }

  delete [] list->get_item_list();
  delete list;

  alloc_unlock();
  pthread_rwlock_destroy(&m_alloc_slow_lock);
}

void
BitAllocator::shutdown()
{
  alloc_lock(true);
  serial_lock();
}

int64_t BitAllocator::sub_used_blocks(int64_t num_blocks)
{
  int64_t used_blks =
    std::atomic_fetch_sub(&m_allocated_blocks, num_blocks);
  debug_assert(used_blks > 0);
  return used_blks;
}

int64_t BitAllocator::add_used_blocks(int64_t num_blocks)
{
  return std::atomic_fetch_add(&m_allocated_blocks, num_blocks) + num_blocks;
}

int64_t BitAllocator::get_used_blocks()
{
  return std::atomic_load(&m_allocated_blocks);
}

int64_t BitAllocator::get_reserved_blocks()
{
  return m_reserved_blocks;
}

int64_t BitAllocator::size()
{
  return m_total_blocks;
}

bool BitAllocator::reserve_blocks(int64_t num)
{
  bool res = false;
  std::lock_guard<std::mutex> l(m_res_blocks_lock);
  if (add_used_blocks(num) <= size()) {
    res = true;
    m_reserved_blocks += num;
  } else {
    sub_used_blocks(num);
    res = false;
  }

  debug_assert(m_allocated_blocks <= size());
  return res;
}

void BitAllocator::unreserve(int64_t needed, int64_t allocated)
{
  std::lock_guard<std::mutex> l(m_res_blocks_lock);
  sub_used_blocks(needed - allocated);
  m_reserved_blocks -= needed;
  debug_assert(m_allocated_blocks >= 0);
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

void BitAllocator::alloc_lock(bool write) {
  if (write) {
    pthread_rwlock_wrlock(&m_alloc_slow_lock);
  } else {
    pthread_rwlock_rdlock(&m_alloc_slow_lock);
  }
}

void BitAllocator::alloc_unlock()
{
  pthread_rwlock_unlock(&m_alloc_slow_lock);
}

bool BitAllocator::zone_free_to_alloc(BmapEntity *item,
        int64_t required, bool wait)
{
  BitMapZone *zone = (BitMapZone *) item;
  if (!zone->lock_zone(wait)) {
    return false;
  }

  if (zone->is_exhausted()) {
    if (zone->get_index() ==
        m_zone_list->get_marker()) {
      m_zone_list->incr_marker(1);
    }
    zone->unlock_zone();
    return false;
  }

  if (zone->get_used_blocks() + required >
    zone->size()) {
    zone->unlock_zone();
    return false;
  }

  return true;
}

bool BitAllocator::is_allocated(int64_t start_block, int64_t num_blocks)
{
  BitMapZone *zone = NULL;
  int64_t zone_block_offset = 0;
  int64_t falling_in_zone = 0;

  debug_assert(start_block >= 0 &&
      (start_block + num_blocks <= size()));

  if (num_blocks == 0) {
    return true;
  }

  assert(start_block >= 0);

  while (num_blocks) {
    zone = (BitMapZone *) m_zone_list->get_nth_item(
          start_block / m_zone_size_blocks);

    zone_block_offset = start_block % m_zone_size_blocks;
    falling_in_zone = MIN(m_zone_size_blocks - zone_block_offset,
              num_blocks);
    if (!zone->is_allocated(zone_block_offset, falling_in_zone)) {
      return false;
    }
    start_block += falling_in_zone;
    num_blocks -= falling_in_zone;
  }
  return true;
}

bool BitAllocator::is_allocated(int64_t *alloc_blocks, int64_t num_blocks)
{
  for (int64_t i = 0; i < num_blocks; i++) {
    return is_allocated(alloc_blocks[i], 1);
  }

  return true;
}

/*
 * Allocate N contiguous blocks.
 */
int64_t BitAllocator::alloc_blocks_int(int64_t num_blocks,
       int64_t *start_block, bool wait)
{

  int64_t zone_mark = m_zone_list->get_marker();
  BitMapZone *zone = NULL;
  int64_t allocated = 0;

  BmapEntityListIter iter = BmapEntityListIter(
                                m_zone_list, zone_mark, true);

  while ((zone = (BitMapZone *) iter.next())) {

    if (!zone_free_to_alloc(zone, num_blocks, wait)) {
      continue;
    }

    allocated = zone->alloc_blocks(num_blocks, start_block);

    zone->unlock_zone();
    if (allocated == num_blocks) {
      break;
    }

    zone->free_blocks(*start_block, allocated);
    allocated = 0;
  }

  return allocated;
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

  if (num_blocks > m_zone_size_blocks) {
    return false;
  }
  return true;
}

/*
 * Interface to allocate blocks after reserve.
 */
int64_t BitAllocator::alloc_blocks_res(int64_t num_blocks, int64_t *start_block)
{
  int scans = 2;
  int64_t allocated = 0;

  if (!check_input(num_blocks)) {
    return 0;
  }

  alloc_lock(false);
  serial_lock();

  while (scans && !allocated) {
    allocated = alloc_blocks_int(num_blocks, start_block, false);
    scans --;
  }

  if (!allocated) {
    /*
     * Could not find anything in two scans.
     * Go in serial manner.
     */
    allocated = alloc_blocks_int(num_blocks, start_block, true);
  }

  debug_assert(is_allocated(*start_block, allocated));

  serial_unlock();
  alloc_unlock();

  return allocated;
}

int64_t BitAllocator::alloc_blocks(int64_t num_blocks, int64_t *start_block)
{
  int scans = 2;
  int64_t allocated = 0;

  if (!check_input(num_blocks)) {
    return 0;
  }

  alloc_lock(false);
  serial_lock();

  if (!reserve_blocks(num_blocks)) {
    goto exit;
  }

  while (scans && !allocated) {
    allocated = alloc_blocks_int(num_blocks, start_block, false);
    scans --;
  }

  if (!allocated) {
    /*
     * Could not find anything in two scans.
     * Go in serial manner.
     */
    allocated = alloc_blocks_int(num_blocks, start_block, true);
  }
  if (allocated != num_blocks)
    unreserve(num_blocks, allocated);
  debug_assert((m_allocated_blocks <= m_total_blocks));
  debug_assert(is_allocated(*start_block, allocated));

exit:
  serial_unlock();
  alloc_unlock();

  return allocated;
}

void BitAllocator::free_blocks_int(int64_t start_block, int64_t num_blocks)
{
  BitMapZone *zone = NULL;
  int64_t zone_block_offset = 0;
  int64_t falling_in_zone = 0;

  debug_assert(start_block >= 0 &&
    (start_block + num_blocks) <= size());

  if (num_blocks == 0) {
    return;
  }

  assert(start_block >= 0);

  while (num_blocks) {
    zone = (BitMapZone *) m_zone_list->get_nth_item(
          start_block / m_zone_size_blocks);

    zone_block_offset = start_block % m_zone_size_blocks;

    falling_in_zone = MIN(m_zone_size_blocks - zone_block_offset,
              num_blocks);
    zone->free_blocks(zone_block_offset, falling_in_zone);
    start_block += falling_in_zone;
    num_blocks -= falling_in_zone;
  }

}

void BitAllocator::free_blocks(int64_t start_block, int64_t num_blocks)
{
  alloc_lock(false);
  debug_assert(is_allocated(start_block, num_blocks));

  free_blocks_int(start_block, num_blocks);
  (void) sub_used_blocks(num_blocks);

  alloc_unlock();
}

void BitAllocator::set_blocks_used(int64_t start_block, int64_t num_blocks)
{
  BitMapZone *zone = NULL;
  int64_t zone_block_offset = 0;
  int64_t falling_in_zone = 0;
  int64_t blks = num_blocks;
  int64_t start_blk = start_block;

  if (num_blocks == 0) {
    return;
  }

  alloc_lock(false);
  serial_lock();

  assert(start_block >= 0);

  while (blks) {
    zone = (BitMapZone *) m_zone_list->get_nth_item(
          start_blk / m_zone_size_blocks);

    zone_block_offset = start_blk % m_zone_size_blocks;
    falling_in_zone = MIN(m_zone_size_blocks - zone_block_offset,
              blks);
    zone->set_blocks_used(zone_block_offset, falling_in_zone);
    start_blk += falling_in_zone;
    blks -= falling_in_zone;
  }

  add_used_blocks(num_blocks);
  debug_assert(is_allocated(start_block, num_blocks));

  serial_unlock();
  alloc_unlock();
}

int64_t BitAllocator::alloc_blocks_dis_int(int64_t num_blocks,
         int64_t *block_list, bool lock)
{
  int64_t zone_mark = m_zone_list->get_marker();
  BitMapZone *zone = NULL;
  int64_t allocated = 0;

  alloc_lock(false);
  serial_lock();

  BmapEntityListIter iter = BmapEntityListIter(
        m_zone_list, zone_mark, true);

  while ((zone = (BitMapZone *) iter.next())) {

    if (!zone_free_to_alloc(zone, 1, lock)) {
      continue;
    }

    allocated += zone->alloc_blocks_dis(num_blocks, &block_list[allocated]);
    zone->unlock_zone();
    if (allocated == num_blocks) {
      break;
    }
  }

  alloc_unlock();
  serial_unlock();

  return allocated;
}

/*
 * Allocate N dis-contiguous blocks.
 */
int64_t BitAllocator::alloc_blocks_dis(int64_t num_blocks, int64_t *block_list)
{
  int scans = 2;
  int64_t allocated = 0;

  if (!check_input_dis(num_blocks)) {
    return 0;
  }

  if (!reserve_blocks(num_blocks)) {
    return 0;
  }

  while (scans && allocated < num_blocks) {
    allocated += alloc_blocks_dis_int(num_blocks, &block_list[allocated], false);
    scans --;
  }

  if (allocated < num_blocks) {
    /*
     * Could not find anything in two scans.
     * Go in serial manner to get something for sure
     * if available.
     */
    allocated += alloc_blocks_dis_int(num_blocks, &block_list[allocated], true);
  }

  unreserve(num_blocks, allocated);
  debug_assert(is_allocated(block_list, allocated));

  return allocated;
}

void BitAllocator::free_blocks_dis(int64_t num_blocks, int64_t *block_list)
{
  alloc_lock(false);

  for (int64_t i = 0; i < num_blocks; i++) {
    free_blocks_int(block_list[i], 1);
  }

  debug_assert(get_used_blocks() > 0);
  sub_used_blocks(num_blocks);
  alloc_unlock();
}
