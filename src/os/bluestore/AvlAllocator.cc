// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "AvlAllocator.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef  dout_prefix
#define dout_prefix *_dout << "AvlAllocator "

MEMPOOL_DEFINE_OBJECT_FACTORY(range_seg_t, range_seg_t, bluestore_alloc);

/*
 * NOTE: caller is responsible for all locking.
 */
static int
range_tree_compare(const void *x1, const void *x2)
{
  const range_seg_t *r1 = static_cast<const range_seg_t *>(x1);
  const range_seg_t *r2 = static_cast<const range_seg_t *>(x2);

  assert(r1->rs_start <= r1->rs_end);
  assert(r2->rs_start <= r2->rs_end);

  return ((r1->rs_start >= r2->rs_end) - (r1->rs_end <= r2->rs_start));
}

/*
 * Comparison function for the private size-ordered tree.
 * Tree is sorted by size, larger sizes at the end of the tree.
 */
static int
range_tree_size_compare(const void *x1, const void *x2)
{
  const range_seg_t *r1 = static_cast<const range_seg_t *>(x1);
  const range_seg_t *r2 = static_cast<const range_seg_t *>(x2);
  uint64_t rs_size1 = r1->rs_end - r1->rs_start;
  uint64_t rs_size2 = r2->rs_end - r2->rs_start;

  int cmp = AVL_CMP(rs_size1, rs_size2);
  if (likely(cmp))
    return (cmp);

  return (AVL_CMP(r1->rs_start, r2->rs_start));
}

range_seg_t *
AvlAllocator::_find_block(avl_tree_t *t, uint64_t start, uint64_t size)
{
  range_seg_t rsearch;
  avl_index_t where;

  rsearch.rs_start = start;
  rsearch.rs_end = start + size;

  void *p = avl_find(t, static_cast<void *>(&rsearch), &where);
  if (p == NULL) {
    p = avl_nearest(t, where, AVL_AFTER);
  }

  return static_cast<range_seg_t *>(p);
}

/*
 * This is a helper function that can be used by the allocator to find
 * a suitable block to allocate. This will search the specified AVL
 * tree looking for a block that matches the specified criteria.
 */
uint64_t AvlAllocator::_block_picker(avl_tree_t *t,
  uint64_t *cursor, uint64_t size, uint64_t align)
{
  range_seg_t *rs = _find_block(t, *cursor, size);
  while (rs != NULL) {
    uint64_t offset = P2ROUNDUP(rs->rs_start, align);

    if (offset + size <= rs->rs_end) {
      *cursor = offset + size;
      return offset;
    }
    void *p = AVL_NEXT(t, static_cast<void *>(rs));
    rs = static_cast<range_seg_t *>(p);
  }

  /*
   * If we know we've searched the whole tree (*cursor == 0), give up.
   * Otherwise, reset the cursor to the beginning and try again.
   */
   if (*cursor == 0) {
     return -1ULL;
   }

   *cursor = 0;
   return _block_picker(t, cursor, size, align);
}

void AvlAllocator::_add_to_tree(uint64_t start, uint64_t size)
{
  avl_index_t where;
  range_seg_t rsearch, *rs_before, *rs_after, *rs;
  uint64_t end = start + size;
  bool merge_before, merge_after;

  assert(size != 0);

  rsearch.rs_start = start;
  rsearch.rs_end = end;
  void *p = avl_find(&range_tree, static_cast<void *>(&rsearch), &where);
  rs = static_cast<range_seg_t *>(p);

  if (rs != NULL && rs->rs_start <= start && rs->rs_end >= end) {
    assert(0 == "freeing free segment");
  }

  /* Make sure we don't overlap with either of our neighbors */
  assert(rs == NULL);

  p = avl_nearest(&range_tree, where, AVL_BEFORE);
  rs_before = static_cast<range_seg_t *>(p);
  p = avl_nearest(&range_tree, where, AVL_AFTER);
  rs_after = static_cast<range_seg_t *>(p);

  merge_before = (rs_before != NULL && rs_before->rs_end == start);
  merge_after = (rs_after != NULL && rs_after->rs_start == end);

  if (merge_before && merge_after) {
    avl_remove(&range_tree, rs_before);
    avl_remove(&range_size_tree, rs_before);
    avl_remove(&range_size_tree, rs_after);

    rs_after->rs_start = rs_before->rs_start;
    delete rs_before;
    rs = rs_after;
  } else if (merge_before) {
    avl_remove(&range_size_tree, rs_before);

    rs_before->rs_end = end;
    rs = rs_before;
  } else if (merge_after) {
    avl_remove(&range_size_tree, rs_after);

    rs_after->rs_start = start;
    rs = rs_after;
  } else {
    rs = new range_seg_t;
    rs->rs_start = start;
    rs->rs_end = end;
    avl_insert(&range_tree, rs, where);
  }

  assert(rs != NULL);
  avl_add(&range_size_tree, rs);

  num_free += size;
}

void AvlAllocator::_remove_from_tree(uint64_t start, uint64_t size)
{
  avl_index_t where;
  range_seg_t rsearch, *rs, *newseg;
  uint64_t end = start + size;
  bool left_over, right_over;

  assert(size != 0);
  assert(size <= (uint64_t)num_free);

  rsearch.rs_start = start;
  rsearch.rs_end = end;
  void *p = avl_find(&range_tree, &rsearch, &where);
  rs = static_cast<range_seg_t *>(p);

  /* Make sure we completely overlap with someone */
  if (rs == NULL) {
    assert(0 == "allocating allocated segment");
  }

  assert(rs->rs_start <= start);
  assert(rs->rs_end >= end);

  left_over = (rs->rs_start != start);
  right_over = (rs->rs_end != end);

  avl_remove(&range_size_tree, rs);

  if (left_over && right_over) {
    newseg = new range_seg_t;
    newseg->rs_start = end;
    newseg->rs_end = rs->rs_end;

    rs->rs_end = start;

    avl_insert_here(&range_tree, newseg, rs, AVL_AFTER);
    avl_add(&range_size_tree, newseg);
  } else if (left_over) {
    rs->rs_end = start;
  } else if (right_over) {
    rs->rs_start = end;
  } else {
    avl_remove(&range_tree, rs);
    delete rs;
    rs = NULL;
  }

  if (rs != NULL) {
    avl_add(&range_size_tree, rs);
  }

  num_free -= size;
  assert(num_free >= 0);
}

int AvlAllocator::_allocate(
  uint64_t size,
  uint64_t unit,
  uint64_t *offset,
  uint64_t *length)
{
  std::lock_guard<std::mutex> l(lock);
  uint64_t max_size = 0;
  void *p = avl_last(&range_size_tree);
  range_seg_t *rs = static_cast<range_seg_t *>(p);
  if (rs != NULL) {
    max_size = rs->rs_end - rs->rs_start;
  }

  bool force_range_size_alloc = false;
  if (max_size < size) {
    if (max_size < unit) {
      return -ENOSPC;
    }
    size = P2ALIGN(max_size, unit);
    assert(size > 0);
    force_range_size_alloc = true;
  }

  /*
   * Find the largest power of 2 block size that evenly divides the
   * requested size. This is used to try to allocate blocks with similar
   * alignment from the same area (i.e. same cursor bucket) but it does
   * not guarantee that other allocations sizes may exist in the same
   * region.
   */
   uint64_t align = size & -size;
   assert(align != 0);
   uint64_t *cursor = &lbas[cbits(align) - 1];
   avl_tree_t *t = &range_tree;

   int free_pct = num_free * 100 / num_total;
   /*
    * If we're running low on space switch to using the size
    * sorted AVL tree (best-fit).
    */
   if (force_range_size_alloc ||
       max_size < range_size_alloc_threshold ||
       free_pct < range_size_alloc_free_pct) {
     t = &range_size_tree;
     *cursor = 0;
   }

   uint64_t start = _block_picker(t, cursor, size, unit);
   if (start == -1ULL) {
     return -ENOSPC;
   }

   _remove_from_tree(start, size);

   *offset = start;
   *length = size;
   num_reserved -= size;
   assert(num_reserved >= 0);
   return 0;
}

AvlAllocator::AvlAllocator(CephContext* cct, int64_t device_size) :
  cct(cct),
  num_total(device_size)
{
  avl_create(&range_tree, range_tree_compare,
    sizeof(range_seg_t), offsetof(range_seg_t, rs_node));
  avl_create(&range_size_tree, range_tree_size_compare,
    sizeof(range_seg_t), offsetof(range_seg_t, rs_pp_node));
}

int AvlAllocator::reserve(uint64_t need)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << std::hex
                 << " need 0x" << need
                 << " num_free 0x" << num_free
                 << " num_reserved 0x" << num_reserved
                 << std::dec << dendl;
  if ((int64_t)need > num_free - num_reserved)
    return -ENOSPC;
  num_reserved += need;
  return 0;
}

void AvlAllocator::unreserve(uint64_t unused)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << std::hex
                 << " unused 0x" << unused
                 << " num_free 0x" << num_free
                 << " num_reserved 0x" << num_reserved
                 << std::dec << dendl;
  assert(num_reserved >= (int64_t)unused);
  num_reserved -= unused;
}

int64_t AvlAllocator::allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint, // unused, for now!
  mempool::bluestore_alloc::vector<AllocExtent> *extents)
{
  ldout(cct, 10) << __func__ << std::hex
                 << " want 0x" << want
                 << " unit 0x" << unit
                 << " max_alloc_size 0x" << max_alloc_size
                 << " hint 0x" << hint
                 << std::dec << dendl;
  assert(ISP2(unit));
  assert(want % unit == 0);

  if (max_alloc_size == 0) {
    max_alloc_size = want;
  }

  ExtentList block_list = ExtentList(extents, 1, max_alloc_size);
  uint64_t allocated = 0;
  while (allocated < want) {
    uint64_t offset, length;
    int r = _allocate(std::min(max_alloc_size, want - allocated),
                      unit, &offset, &length);
    if (r < 0) {
      // Allocation failed.
      break;
    }
    block_list.add_extents(offset, length);
    allocated += length;
  }

  if (allocated) {
    return allocated;
  }

  return -ENOSPC;
}

void AvlAllocator::release(const interval_set<uint64_t>& release_set)
{
  std::lock_guard<std::mutex> l(lock);
  for (interval_set<uint64_t>::const_iterator p = release_set.begin();
       p != release_set.end();
       ++p) {
    const auto offset = p.get_start();
    const auto length = p.get_len();
    ldout(cct, 10) << __func__ << std::hex
                   << " offset 0x" << offset
                   << " length 0x" << length
                   << std::dec << dendl;
    _add_to_tree(offset, length);
  }
}

uint64_t AvlAllocator::get_free()
{
  std::lock_guard<std::mutex> l(lock);
  return num_free;
}

void AvlAllocator::dump()
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 0) << __func__ << " range_tree: " << dendl;
  for (void *p = avl_first(&range_tree);
       p;
       p = AVL_NEXT(&range_tree, p)) {
    range_seg_t *rs = static_cast<range_seg_t *>(p);
    ldout(cct, 0) << std::hex
                  << "0x" << rs->rs_start << "~" << rs->rs_end
                  << std::dec
                  << dendl;
  }

  ldout(cct, 0) << __func__ << " range_size_tree: " << dendl;
  for (void *p = avl_first(&range_size_tree);
       p;
       p = AVL_NEXT(&range_size_tree, p)) {
    range_seg_t *rs = static_cast<range_seg_t *>(p);
    ldout(cct, 0) << std::hex
                  << "0x" << rs->rs_start << "~" << rs->rs_end
                  << std::dec
                  << dendl;
  }
}

void AvlAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << std::hex
                 << " offset 0x" << offset
                 << " length 0x" << length
                 << std::dec << dendl;
  _add_to_tree(offset, length);
}

void AvlAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  std::lock_guard<std::mutex> l(lock);
  ldout(cct, 10) << __func__ << std::hex
                 << " offset 0x" << offset
                 << " length 0x" << length
                 << std::dec << dendl;
  _remove_from_tree(offset, length);
}

void AvlAllocator::shutdown()
{
  std::lock_guard<std::mutex> l(lock);
  void *cookie = NULL;
  void *p;

  while ((p = avl_destroy_nodes(&range_tree, &cookie)) != NULL) {
    delete (static_cast<range_seg_t *>(p));
  }
  avl_destroy(&range_tree);
}
