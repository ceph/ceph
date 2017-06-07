// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 *
 */

#include "BitAllocator.h"

#include "BitMapAllocator.h"
#include "bluestore_types.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "bitmapalloc:"


BitMapAllocator::BitMapAllocator(CephContext* cct, int64_t device_size,
				 int64_t block_size)
  : cct(cct)
{
  if (!ISP2(block_size)) {
    derr << __func__ << " block_size " << block_size
         << " not power of 2 aligned!"
         << dendl;
    assert(ISP2(block_size));
    return;
  }

  int64_t zone_size_blks = cct->_conf->bluestore_bitmapallocator_blocks_per_zone;
  if (!ISP2(zone_size_blks)) {
    derr << __func__ << " zone_size " << zone_size_blks
         << " not power of 2 aligned!"
         << dendl;
    assert(ISP2(zone_size_blks));
    return;
  }

  int64_t span_size = cct->_conf->bluestore_bitmapallocator_span_size;
  if (!ISP2(span_size)) {
    derr << __func__ << " span_size " << span_size
         << " not power of 2 aligned!"
         << dendl;
    assert(ISP2(span_size));
    return;
  }

  m_block_size = block_size;
  m_total_size = P2ALIGN(device_size, block_size);
  m_bit_alloc = new BitAllocator(cct, device_size / block_size,
				 zone_size_blks, CONCURRENT, true);
  if (!m_bit_alloc) {
    derr << __func__ << " Unable to intialize Bit Allocator" << dendl;
    assert(m_bit_alloc);
  }
  dout(10) << __func__ << " instance " << (uint64_t) this
           << " size 0x" << std::hex << device_size << std::dec
           << dendl;
}

BitMapAllocator::~BitMapAllocator()
{
  delete m_bit_alloc;
}

void BitMapAllocator::insert_free(uint64_t off, uint64_t len)
{
  dout(20) << __func__ << " instance " << (uint64_t) this
           << " off 0x" << std::hex << off
           << " len 0x" << len << std::dec
           << dendl;

  assert(!(off % m_block_size));
  assert(!(len % m_block_size));

  m_bit_alloc->free_blocks(off / m_block_size,
             len / m_block_size);
}

int BitMapAllocator::reserve(uint64_t need)
{
  int nblks = need / m_block_size; // apply floor
  assert(!(need % m_block_size));
  dout(10) << __func__ << " instance " << (uint64_t) this
           << " num_used " << m_bit_alloc->get_used_blocks()
           << " total " << m_bit_alloc->total_blocks()
           << dendl;

  if (!m_bit_alloc->reserve_blocks(nblks)) {
    return -ENOSPC;
  }
  return 0;
}

void BitMapAllocator::unreserve(uint64_t unused)
{
  int nblks = unused / m_block_size;
  assert(!(unused % m_block_size));

  dout(10) << __func__ << " instance " << (uint64_t) this
           << " unused " << nblks
           << " num used " << m_bit_alloc->get_used_blocks()
           << " total " << m_bit_alloc->total_blocks()
           << dendl;

  m_bit_alloc->unreserve_blocks(nblks);
}

int64_t BitMapAllocator::allocate(
  uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
  int64_t hint, mempool::bluestore_alloc::vector<AllocExtent> *extents)
{

  assert(!(alloc_unit % m_block_size));
  assert(alloc_unit);

  assert(!max_alloc_size || max_alloc_size >= alloc_unit);

  dout(10) << __func__ <<" instance "<< (uint64_t) this
     << " want_size " << want_size
     << " alloc_unit " << alloc_unit
     << " hint " << hint
     << dendl;
  hint = hint % m_total_size; // make hint error-tolerant
  return allocate_dis(want_size, alloc_unit / m_block_size,
                      max_alloc_size, hint / m_block_size, extents);
}

int64_t BitMapAllocator::allocate_dis(
  uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
  int64_t hint, mempool::bluestore_alloc::vector<AllocExtent> *extents)
{
  ExtentList block_list = ExtentList(extents, m_block_size, max_alloc_size);
  int64_t nblks = (want_size + m_block_size - 1) / m_block_size;
  int64_t num = 0;

  num = m_bit_alloc->alloc_blocks_dis_res(nblks, alloc_unit, hint, &block_list);
  if (num == 0) {
    return -ENOSPC;
  }

  return num * m_block_size;
}

void BitMapAllocator::release(
  uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " 0x"
           << std::hex << offset << "~" << length << std::dec
           << dendl;
  insert_free(offset, length);
}

uint64_t BitMapAllocator::get_free()
{
  assert(m_bit_alloc->total_blocks() >= m_bit_alloc->get_used_blocks());
  return ((
    m_bit_alloc->total_blocks() - m_bit_alloc->get_used_blocks()) *
    m_block_size);
}

void BitMapAllocator::dump()
{
  dout(0) << __func__ << " instance " << this << dendl;
  m_bit_alloc->dump();
}

void BitMapAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " instance " << (uint64_t) this
           << " offset 0x" << std::hex << offset
           << " length 0x" << length << std::dec
           << dendl;
  uint64_t size = m_bit_alloc->size() * m_block_size;

  uint64_t offset_adj = ROUND_UP_TO(offset, m_block_size);
  uint64_t length_adj = ((length - (offset_adj - offset)) /
                         m_block_size) * m_block_size;

  if ((offset_adj + length_adj) > size) {
    assert(((offset_adj + length_adj) - m_block_size) < size);
    length_adj = size - offset_adj;
  }

  insert_free(offset_adj, length_adj);
}

void BitMapAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  dout(10) << __func__ << " instance " << (uint64_t) this
           << " offset 0x" << std::hex << offset
           << " length 0x" << length << std::dec
           << dendl;

  // we use the same adjustment/alignment that init_add_free does
  // above so that we can yank back some of the space.
  uint64_t offset_adj = ROUND_UP_TO(offset, m_block_size);
  uint64_t length_adj = ((length - (offset_adj - offset)) /
                         m_block_size) * m_block_size;

  assert(!(offset_adj % m_block_size));
  assert(!(length_adj % m_block_size));

  int64_t first_blk = offset_adj / m_block_size;
  int64_t count = length_adj / m_block_size;

  if (count)
    m_bit_alloc->set_blocks_used(first_blk, count);
}


void BitMapAllocator::shutdown()
{
  dout(10) << __func__ << " instance " << (uint64_t) this << dendl;
  m_bit_alloc->shutdown();
}

