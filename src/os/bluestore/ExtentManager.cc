// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2016 Mirantis, Inc
*
* Author: Igor Fedotov <ifedotov@mirantis.com>
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation.  See file COPYING.
*
*/

#include "ExtentManager.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "ext_mgr:"


bluestore_blob_t* ExtentManager::get_blob(BlobRef blob)
{
  bluestore_blob_t* res = nullptr;
  bluestore_blob_map_t::iterator it = m_blobs.find(blob);
  if (it != m_blobs.end())
    res = &it->second;
  return res;
}

uint64_t ExtentManager::get_read_block_size(const bluestore_blob_t* blob) const
{
  uint64_t block_size = m_device.get_block_size();
  if(blob->csum_type != bluestore_blob_t::CSUM_NONE)
    block_size = MAX(blob->get_csum_block_size(), block_size);
  return block_size;
}

int ExtentManager::read(uint64_t offset, uint32_t length, void* opaque, bufferlist* result)
{
  result->clear();

  bluestore_lextent_map_t::iterator lext = m_lextents.upper_bound(offset);
  uint32_t l = length;
  uint64_t o = offset;
  if (lext == m_lextents.begin() && offset+length <= lext->first){
    result->append_zero(length);
    return 0;
  } else if(lext == m_lextents.begin()) {
    o = lext->first;
    l -= lext->first - offset;
  } else
    --lext;

  //build blob list to read
  blobs2read_t blobs2read;
  while (l > 0 && lext != m_lextents.end()) {
    bluestore_blob_t* bptr = get_blob(lext->second.blob);
    assert(bptr != nullptr);
    unsigned l2read;
    if(o >= lext->first && o < lext->first + lext->second.length) {
      unsigned r_off = o - lext->first;
      l2read = MIN(l, lext->second.length - r_off);
      regions2read_t& regions = blobs2read[bptr];
      regions.push_back(region_t(o, r_off + lext->second.x_offset, 0, l2read));
      ++lext;
    } else if(o >= lext->first + lext->second.length){
      //handling the case when the first lookup get into the previous block due to the hole
      l2read = 0;
      ++lext;
    } else {
      //hole found
      l2read = MIN(l, lext->first -o);
    }
    o += l2read;
    l -= l2read;
  }

  ready_regions_t ready_regions;

  //enumerate and read/decompress desired blobs
  blobs2read_t::iterator b2r_it = blobs2read.begin();
  while (b2r_it != blobs2read.end()) {
    const bluestore_blob_t* bptr = b2r_it->first;
    regions2read_t r2r = b2r_it->second;
    regions2read_t::const_iterator r2r_it = r2r.cbegin();
    if (bptr->has_flag(bluestore_blob_t::BLOB_COMPRESSED)) {
      bufferlist compressed_bl, raw_bl;

      int r = read_whole_blob(bptr, opaque, &compressed_bl);
      if (r < 0)
	return r;
      if(bptr->csum_type != bluestore_blob_t::CSUM_NONE){
        r = verify_csum(bptr, 0, compressed_bl, opaque);
        if (r < 0) {
          dout(20) << __func__ << "  blob reading " << r2r_it->logical_offset << "~" << bptr->length <<" csum verification failed."<< dendl;
          return r;
        }
      }

      r = m_compressor.decompress(compressed_bl, opaque, &raw_bl);
      if (r < 0)
	return r;

      while (r2r_it != r2r.end()) {
	ready_regions[r2r_it->logical_offset].substr_of(raw_bl, r2r_it->blob_xoffset, r2r_it->length);
	++r2r_it;
      }

    } else {
      extents2read_t e2r;
      int r = blob2read_to_extents2read(bptr, r2r_it, r2r.cend(), &e2r);
      if (r < 0)
	return r;

      extents2read_t::const_iterator it = e2r.cbegin();
      while (it != e2r.cend()) {
	int r = read_extent_sparse(bptr, it->first, it->second.cbegin(), it->second.cend(), opaque, &ready_regions);
	if (r < 0)
	  return r;
	++it;
      }
    }
    ++b2r_it;
  }

  //generate a resulting buffer
  ready_regions_t::iterator rr_it = ready_regions.begin();
  o = offset;

  while (rr_it != ready_regions.end()) {
    if (o < rr_it->first)
      result->append_zero(rr_it->first - o);
    o = rr_it->first + rr_it->second.length();
    assert(o <= offset + length);
    result->claim_append(rr_it->second);
    ++rr_it;
  }
  result->append_zero(offset + length - o);

  return 0;
}


int ExtentManager::read_whole_blob(const bluestore_blob_t* blob, void* opaque, bufferlist* result)
{
  result->clear();

  uint64_t block_size = m_device.get_block_size();

  uint32_t l = blob->length;
  uint64_t ext_pos = 0;
  auto it = blob->extents.cbegin();
  while (it != blob->extents.cend() && l > 0){
    uint32_t r_len = MIN(l, it->length);
    //uint32_t r_len = it->length;
    uint32_t x_len = ROUND_UP_TO(r_len, block_size);

    bufferlist bl;
    //  dout(30) << __func__ << "  reading " << it->offset << "~" << x_len << dendl;
    int r = m_device.read_block(it->offset, x_len, opaque, &bl);
    if (r < 0) {
      return r;
    }

    if (x_len == r_len){
      result->claim_append(bl);
    } else {
      bufferlist u;
      u.substr_of(bl, 0, r_len);
      result->claim_append(u);
    }
    l -= r_len;
    ext_pos += it->length;
    ++it;
  }

  return 0;
}

int ExtentManager::read_extent_sparse(const bluestore_blob_t* blob, const bluestore_extent_t* extent, ExtentManager::regions2read_t::const_iterator cur, ExtentManager::regions2read_t::const_iterator end, void* opaque, ExtentManager::ready_regions_t* result)
{
  //FIXME: this is a trivial implementation that reads each region independently - can be improved to read neighboring and/or close enough regions together.

  uint64_t block_size = get_read_block_size(blob);

  assert((extent->length % block_size) == 0);   // all physical extents has to be aligned with read block size

  while (cur != end) {

    assert(cur->ext_xoffset + cur->length <= extent->length);


    uint64_t r_off = cur->ext_xoffset;
    uint64_t front_extra = r_off % block_size;
    r_off -= front_extra;

    uint64_t x_len = cur->length;
    uint64_t r_len = ROUND_UP_TO(x_len + front_extra, block_size);

//    dout(30) << __func__ << "  reading " << r_off << "~" << r_len << dendl;
    bufferlist bl;
    int r = m_device.read_block(r_off + extent->offset, r_len, opaque, &bl);
    if (r < 0) {
      return r;
    }
    r = verify_csum(blob, cur->blob_xoffset, bl, opaque);
    if (r < 0) {
      return r;
    }

    bufferlist u;
    u.substr_of(bl, front_extra, x_len);
    (*result)[cur->logical_offset].claim_append(u);

    ++cur;
  }
  return 0;
}

int ExtentManager::blob2read_to_extents2read(const bluestore_blob_t* blob, ExtentManager::regions2read_t::const_iterator cur, ExtentManager::regions2read_t::const_iterator end, ExtentManager::extents2read_t* result)
{
  result->clear();

  vector<bluestore_extent_t>::const_iterator ext_it = blob->extents.cbegin();
  vector<bluestore_extent_t>::const_iterator ext_end = blob->extents.cend();

  uint64_t ext_pos = 0;
  uint64_t l = 0;
  while (cur != end && ext_it != ext_end) {

  assert(cur->ext_xoffset == 0);

    //bypass preceeding extents
    while (cur->blob_xoffset  >= ext_pos + ext_it->length && ext_it != ext_end) {
      ext_pos += ext_it->length;
      ++ext_it;
    }
    l = cur->length;
    uint64_t r_offs = cur->blob_xoffset - ext_pos;
    uint64_t l_offs = cur->logical_offset;
    while (l > 0 && ext_it != ext_end) {

      assert(blob->length >= ext_pos + r_offs);

      uint64_t r_len = MIN(blob->length - ext_pos - r_offs, ext_it->length - r_offs);
      if (r_len > 0) {
	r_len = MIN(r_len, l);
	const bluestore_extent_t* eptr = &(*ext_it);
	regions2read_t& regions = (*result)[eptr];
	regions.push_back(region_t(l_offs, ext_pos, r_offs, r_len));
	l -= r_len;
	l_offs += r_len;
      }

      //leave extent pointer as-is if current region's been fully processed - lookup will start from it for the next region
      if (l != 0) {
	ext_pos += ext_it->length;
	r_offs = 0;
	++ext_it;
      }
    }

    ++cur;
    assert(cur == end || l_offs <= cur->logical_offset); //region offsets to be ordered ascending and with no overlaps. Overwise ext_it(ext_pos) to be enumerated from the beginning on each region
  }

  if (cur != end || l > 0) {
    assert(l == 0);
    assert(cur == end);
    return -EFAULT;
  }

  return 0;
}

int ExtentManager::verify_csum(const bluestore_blob_t* blob, uint64_t blob_xoffset, const bufferlist& bl, void* opaque) const
{
  uint64_t block_size = blob->get_csum_block_size();
  size_t csum_len = blob->get_csum_value_size();

  assert((blob_xoffset % block_size) == 0);
  assert((bl.length() % block_size) == 0);

  uint64_t block0 = blob_xoffset / block_size;
  uint64_t blocks = bl.length() / block_size;

  assert(blob->csum_data.size() >= (block0 + blocks) * csum_len);

  vector<char> csum_data;
  csum_data.resize(blob->get_csum_value_size() * blocks);

  vector<char>::const_iterator start = blob->csum_data.cbegin();
  vector<char>::const_iterator end = blob->csum_data.cbegin();
  start += block0 * csum_len;
  end += (block0+blocks) * csum_len;

  std::copy(start, end, csum_data.begin());

  int r = m_csum_verifier.verify((bluestore_blob_t::CSumType)blob->csum_type, blob->get_csum_block_size(), csum_data, bl, opaque);
  return r;
}
