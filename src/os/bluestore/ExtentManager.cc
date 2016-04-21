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


bluestore_blob_map_t::iterator ExtentManager::get_blob_iterator(BlobRef blob_ref)
{
  bluestore_blob_map_t::iterator res = m_blobs.end();
  if (blob_ref == UNDEF_BLOB_REF)
    return res;
  res = m_blobs.find(blob_ref);
  return res;
}

bluestore_blob_t* ExtentManager::get_blob(BlobRef blob_ref)
{
  bluestore_blob_t* res = NULL;
  auto it = get_blob_iterator(blob_ref);
  if (it != m_blobs.end())
    res = &(it->second);
  return res;
}

void ExtentManager::ref_blob(BlobRef blob_ref)
{
  auto it = m_blobs.find(blob_ref);
  if (it != m_blobs.end())
    it->second.num_refs++;
}

void ExtentManager::ref_blob(bluestore_blob_map_t::iterator blob_it)
{
  if (blob_it != m_blobs.end()) {
    blob_it->second.num_refs++;
  }
}

void ExtentManager::deref_blob(bluestore_blob_map_t::iterator blob_it, bool zero, void* opaque)
{
  if (blob_it != m_blobs.end()) {
    blob_it->second.num_refs--;
    if (blob_it->second.num_refs == 0) {

      bluestore_blob_t* blob = &(blob_it->second);
      auto end_ext = blob->extents.end();
      auto ext = blob->extents.begin();
      auto l = blob->length;
      uint64_t x_offs = 0;
      while (ext != end_ext) {
        if (zero && x_offs < l) {
          uint32_t x_len = ROUND_UP_TO(MIN(ext->length, l - x_offs), m_blockop_inf.get_block_size());
          m_blockop_inf.zero_block(ext->offset, x_len, opaque);
        }
        m_blockop_inf.release_block(ext->offset, ext->length, opaque);
        x_offs += ext->length;
        ++ext;
      }
      m_blobs.erase(blob_it);
    }
  }
}

uint64_t ExtentManager::get_max_blob_size() const
{
  //FIXME: temporary implementation
  return 4 * get_min_alloc_size();
}
uint64_t ExtentManager::get_min_alloc_size() const
{
  //FIXME: temporary implementation
  return 0x10000;
}

uint64_t ExtentManager::get_read_block_size(const bluestore_blob_t* blob) const
{
  uint64_t block_size = m_blockop_inf.get_block_size();
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
  if (lext == m_lextents.begin() && offset + length <= lext->first){
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
    uint32_t l2read;
    if(o >= lext->first && o < lext->first + lext->second.length) {
      uint32_t r_off = o - lext->first;
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

  uint64_t block_size = m_blockop_inf.get_block_size();

  uint32_t l = blob->length;
  uint64_t ext_pos = 0;
  auto it = blob->extents.cbegin();
  while (it != blob->extents.cend() && l > 0){
    uint32_t r_len = MIN(l, it->length);
    //uint32_t r_len = it->length;
    uint32_t x_len = ROUND_UP_TO(r_len, block_size);

    bufferlist bl;
    //  dout(30) << __func__ << "  reading " << it->offset << "~" << x_len << dendl;
    int r = m_blockop_inf.read_block(it->offset, x_len, opaque, &bl);
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
    int r = m_blockop_inf.read_block(r_off + extent->offset, r_len, opaque, &bl);
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

  bluestore_extent_vector_t::const_iterator ext_it = blob->extents.cbegin();
  bluestore_extent_vector_t::const_iterator ext_end = blob->extents.cend();

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

  int r = m_csum_verifier.verify((bluestore_blob_t::CSumType)blob->csum_type, blob->get_csum_value_size(), blob->get_csum_block_size(), bl, opaque, csum_data);
  return r;
}

void ExtentManager::preprocess_changes(uint64_t offset, uint64_t length, bluestore_lextent_map_t* updated_lextents, ExtentManager::live_lextent_map_t* removed_lextents, list<BlobRef>* blobs2ref)
{
  auto lext_begin = m_lextents.begin();
  auto lext_end = m_lextents.end();

  //Check for append and position the iterator to the lextent prior to the specified offset
  bool append = false;
  auto lext_it = m_lextents.upper_bound(offset);
  if (lext_it == lext_end) {
    if (lext_it != lext_begin) {
      --lext_it;
      append = lext_it->first + lext_it->second.length <= offset;
    }
    else
      append = true;
  }
  else if (lext_it != lext_begin)
    --lext_it;

  if (!append) {
    uint64_t write_end_offset = offset + length;
    while (lext_it != lext_end && lext_it->first < write_end_offset) {

      uint64_t lext_end_offset = lext_it->first + lext_it->second.length;
      if (lext_it->first >= offset &&  lext_end_offset <= write_end_offset) { //totally overlapped lextent
	bluestore_lextent_t& lext = lext_it->second;
	auto blob_it = get_blob_iterator(lext.blob);
	removed_lextents->emplace(lext_it->first, live_lextent_t(blob_it, lext.blob, lext.x_offset, lext.length, lext.flags));
	ref_blob(blob_it);
      }
      else if (lext_it->first < offset
	&& lext_end_offset > offset
	&& lext_end_offset <= write_end_offset) { //partially overlapped at the end
	bluestore_lextent_t& upd_lext = (*updated_lextents)[lext_it->first];
	upd_lext = lext_it->second;
	upd_lext.length = offset - lext_it->first;
      }
      else if (lext_it->first >= offset && lext_end_offset > write_end_offset) { //partially overlapped at the start
	bluestore_lextent_t& upd_lext = (*updated_lextents)[write_end_offset];
	upd_lext = lext_it->second;
	upd_lext.x_offset += write_end_offset - lext_it->first;
	upd_lext.length = lext_end_offset - write_end_offset;
	removed_lextents->emplace(lext_it->first, live_lextent_t(m_blobs.end())); //the content of the live lextent has to be empty - we need to remove entry from the lextent map only, blob map to be unaffected
      }
      else if (lext_it->first < offset && lext_end_offset > write_end_offset) { //overlapped at the center
	bluestore_lextent_t& upd_lext1 = (*updated_lextents)[lext_it->first];
	upd_lext1 = lext_it->second;
	upd_lext1.length = offset - lext_it->first;
	bluestore_lextent_t& upd_lext2 = (*updated_lextents)[write_end_offset];
	upd_lext2 = lext_it->second;
	upd_lext2.x_offset += write_end_offset - lext_it->first;
	upd_lext2.length = lext_end_offset - write_end_offset;

	blobs2ref->push_back(lext_it->second.blob);
      }
      ++lext_it;
    }
  }
}

int ExtentManager::write(uint64_t offset, const bufferlist& bl, void* opaque, const ExtentManager::CheckSumInfo& check_info, const ExtentManager::CompressInfo* compress_info)
{
  int r;
  bluestore_lextent_map_t updated_lextents;
  live_lextent_map_t  new_lextents, removed_lextents;
  list<BlobRef> blobs2ref;

  preprocess_changes(offset, bl.length(), &updated_lextents, &removed_lextents, &blobs2ref);

  r = compress_info && bl.length() > get_min_alloc_size() ?
    write_compressed(offset, bl, opaque, check_info, *compress_info, &new_lextents) :
    write_uncompressed(offset, bl, opaque, check_info, &new_lextents);

  if (r > 0) {
    update_lextents(updated_lextents.begin(), updated_lextents.end());
    auto it = blobs2ref.begin();
    while (it != blobs2ref.end()) {
      ref_blob(*it);
      ++it;
    }
    release_lextents(removed_lextents.begin(), removed_lextents.end(), true, true, opaque);
    add_lextents(new_lextents.begin(), new_lextents.end());
  }
  return r >= 0 ? bl.length() : r;
}

int ExtentManager::zero(uint64_t offset, uint64_t len, void* opaque)
{
  bluestore_lextent_map_t updated_lextents;
  live_lextent_map_t  new_lextents, removed_lextents;
  list<BlobRef> blobs2ref;

  preprocess_changes(offset, len, &updated_lextents, &removed_lextents, &blobs2ref);

  update_lextents(updated_lextents.begin(), updated_lextents.end());
  auto it = blobs2ref.begin();
  while (it != blobs2ref.end()) {
    ref_blob(*it);
    ++it;
  }
  release_lextents(removed_lextents.begin(), removed_lextents.end(), true, true, opaque);
  return 0;
}

int ExtentManager::truncate(uint64_t offset, void* opaque)
{
  auto lext_begin = m_lextents.begin();
  auto lext_it = m_lextents.end();
  uint64_t len = 0;
  if (lext_it != lext_begin) {
    --lext_it;
    len = lext_it->first + lext_it->second.length;
    if (offset <= len)
      len -= offset;
  }
  int r = len ? zero(offset, len, opaque) : 0;
  return r;
}

int ExtentManager::write_uncompressed(uint64_t offset, const bufferlist& bl, void* opaque, const ExtentManager::CheckSumInfo& check_info, live_lextent_map_t* res_lextents)
{
  int r = 0;
  uint64_t o = offset;
  uint32_t l = bl.length();
  //create new lextents & blobs
  while (l > 0 && r >= 0) {
    BlobRef blob_ref = UNDEF_BLOB_REF;
    bluestore_blob_map_t::iterator blob_it;
    uint32_t to_allocate = MIN(l, get_max_blob_size());
    r = allocate_raw_blob(to_allocate, opaque, check_info, &blob_ref, &blob_it);
    if (r < 0) {
      release_lextents(res_lextents->begin(), res_lextents->end(), false, false, opaque);
      res_lextents->clear();
      //dout(0)<<" write failed, can't allocate"<< dendl;
    }
    else {
      res_lextents->emplace(o, live_lextent_t(blob_it, blob_ref, 0, to_allocate, 0));
      o += to_allocate;
      l -= to_allocate;
    }
  }

  if (r >= 0) {
    r = apply_lextents(*res_lextents, bl, NULL, opaque);
  }
  return r;
}

int ExtentManager::write_compressed(uint64_t offset, const bufferlist& bl, void* opaque, const ExtentManager::CheckSumInfo& check_info, const ExtentManager::CompressInfo& compress_info, live_lextent_map_t* res_lextents)
{
  int r = 0;
  std::vector<bufferlist> compressed_buffers;
  uint64_t o = offset;
  uint32_t l = bl.length();
  uint64_t input_offs = 0;
  //create new lextents & blobs
  while (l > 0 && r >= 0) {
    BlobRef blob_ref;
    bluestore_blob_map_t::iterator blob_it;
    size_t sz = compressed_buffers.size();
    compressed_buffers.resize(sz + 1);
    int processed = l > get_min_alloc_size() ?
      compress_and_allocate_blob(input_offs, bl, opaque, check_info, compress_info, &blob_ref, &blob_it, &compressed_buffers[sz]) :
      allocate_raw_blob(MIN(l, get_max_blob_size()), opaque, check_info, &blob_ref, &blob_it);
    if (processed < 0) {
      release_lextents(res_lextents->begin(), res_lextents->end(), false, false, opaque);
      res_lextents->clear();
      r = processed;
      //dout(0)<<" write failed, can't allocate"<< dendl;
    }
    else {
      res_lextents->emplace(o, live_lextent_t(blob_it, blob_ref, 0, processed, 0));
      o += processed;
      input_offs += processed;
      l -= processed;
    }
  }

  if (r >= 0) {
    r = apply_lextents(*res_lextents, bl, &compressed_buffers, opaque);
  }
  return r;
}

int ExtentManager::allocate_raw_blob(uint32_t length, void* opaque, const ExtentManager::CheckSumInfo& check_info, BlobRef* blob_ref, bluestore_blob_map_t::iterator* res_blob_it)
{
  assert(length <= get_max_blob_size());
  //allocate a new blob
  auto blob_it = m_blobs.end();
  if (blob_it != m_blobs.begin()) {
    --blob_it;
    *blob_ref = blob_it->first + 1;
  }
  else
    *blob_ref = FIRST_BLOB_REF;

  blob_it = m_blobs.emplace(*blob_ref, bluestore_blob_t(length, 0, check_info.csum_type, check_info.csum_block_order)).first;
  bluestore_blob_t& blob = blob_it->second;

  //allocate space for the new blob
  uint32_t to_allocate = ROUND_UP_TO(length, get_min_alloc_size());
  int r = m_blockop_inf.allocate_blocks(to_allocate, opaque, &blob.extents);
  if (r >= 0) {
    r = length;
    *res_blob_it = blob_it;
  } else {
    m_blobs.erase(blob_it);
    *blob_ref = UNDEF_BLOB_REF;
    *res_blob_it = m_blobs.end();
  }
  return r;
}

int ExtentManager::compress_and_allocate_blob(
    uint64_t input_offs,
    const bufferlist& bl,
    void* opaque,
    const ExtentManager::CheckSumInfo& check_info,
    const ExtentManager::CompressInfo& compress_info,
    BlobRef* blob_ref,
    bluestore_blob_map_t::iterator* res_blob_it,
    bufferlist* compressed_buffer)
{
  int r = 0;
  assert(input_offs <= bl.length());
  uint32_t len = bl.length() - input_offs;
  len = MIN(len, get_max_blob_size());

  compressed_buffer->clear();
  r = m_compressor.compress(compress_info, input_offs, len, bl, opaque, compressed_buffer);
  bool bypass = false;
  if(r >= 0) {
    uint32_t aligned_len1 = ROUND_UP_TO(len, get_min_alloc_size());
    uint32_t aligned_len2 = ROUND_UP_TO(compressed_buffer->length(), get_min_alloc_size());
    bypass = aligned_len2 > get_max_blob_size() || aligned_len2 >= aligned_len1; //no saving
  }

  if (r >= 0 && !bypass) {
    r = allocate_raw_blob(compressed_buffer->length(), opaque, check_info, blob_ref, res_blob_it);
    if (r >= 0) {
      (*res_blob_it)->second.set_flag(bluestore_blob_t::BLOB_COMPRESSED);
      r = len;
    }
  } else {
    dout(20) << __func__ << " compression bypassed, status:" << r << dendl;
    compressed_buffer->clear();
    r = allocate_raw_blob(len, opaque, check_info, blob_ref, res_blob_it);
    if (r >= 0) {
      r = len;
    }
  }
  return r;
}

int ExtentManager::write_blob(bluestore_blob_t& blob, uint64_t input_offs, const bufferlist& bl, void* opaque)
{
  int r = 0;
  uint64_t input_offs0 = input_offs;
  uint32_t ext_pos = 0;
  assert(input_offs <= bl.length());
  uint64_t len = MIN( blob.get_ondisk_length(), bl.length() - input_offs );
//  assert(blob.get_ondisk_length() >= len);

  if (blob.csum_type != bluestore_blob_t::CSUM_NONE) {
    blob.csum_data.clear();
    r = m_csum_verifier.calculate((bluestore_blob_t::CSumType)blob.csum_type, blob.get_csum_value_size(), blob.get_csum_block_size(), input_offs, len, bl, opaque, &(blob.csum_data));
    if (r < 0)
      derr << __func__ << " checksum("<< blob.csum_type<< ") calculation failure:" << r << dendl;
  }

  while (len > 0 && r >= 0) {
    assert(ext_pos < blob.extents.size());
    bluestore_extent_t& ext = blob.extents[ext_pos];
    uint64_t l = MIN(len, ext.length);

    uint64_t aligned_len = ROUND_UP_TO(l, m_blockop_inf.get_block_size());
    if (input_offs == 0 && aligned_len == bl.length()) //fast track, no need for input data slicing
      r = m_blockop_inf.write_block(ext.offset, bl, opaque);
    else {
      bufferlist tmp_bl;
      tmp_bl.substr_of(bl, input_offs, l);
      tmp_bl.append_zero(aligned_len - l);
      r = m_blockop_inf.write_block(ext.offset, tmp_bl, opaque);
    }
    ++ext_pos;
    input_offs += l;
    len -= l;
  }
  return r < 0 ? r : input_offs - input_offs0;
}

int ExtentManager::apply_lextents(
  live_lextent_map_t& new_lextents,
  const bufferlist& raw_buffer,
  std::vector<bufferlist>* compressed_buffers,
  void* opaque)
{
  int r = 0;
  uint64_t x_offs = 0;
  size_t lext_pos = 0;
  //write data to new lextents
  auto newext_it = new_lextents.begin();
  while (newext_it != new_lextents.end() && r >= 0) {
    bluestore_blob_t& blob = newext_it->second.blob_iterator->second;
    if (compressed_buffers && compressed_buffers->at(lext_pos).length() > 0 && blob.has_flag(bluestore_blob_t::BLOB_COMPRESSED) ) {
      r = write_blob(blob, 0, compressed_buffers->at(lext_pos), opaque);
      assert( r < 0 || r == (int)compressed_buffers->at(lext_pos).length());
    }
    else {
      r = write_blob(blob, x_offs, raw_buffer, opaque);
      assert( r < 0 || r == (int)newext_it->second.length);
    }
    x_offs += newext_it->second.length;
    ++lext_pos;
    ++newext_it;
  }
  assert(x_offs == raw_buffer.length());
  if (r < 0) {
    release_lextents(new_lextents.begin(), new_lextents.end(), false, false, opaque); //FIXME: we may need to zero some of already written blobs, mark lextent somehow?
    new_lextents.clear();
  }

  return r;
}

void ExtentManager::add_lextents(live_lextent_map_t::iterator cur, live_lextent_map_t::iterator end)
{
  //update specified lextents in the primary map
  while (cur != end) {
    m_lextents[cur->first] = cur->second;
    ++cur;
  }
}

void ExtentManager::update_lextents(bluestore_lextent_map_t::iterator cur, bluestore_lextent_map_t::iterator end)
{
  //update specified lextents in the primary map
  while (cur != end) {
    m_lextents[cur->first] = cur->second;
    ++cur;
  }
}

void ExtentManager::release_lextents(live_lextent_map_t::iterator cur, live_lextent_map_t::iterator end, bool zero, bool remove_from_primary_map, void* opaque)
{
  while (cur != end){

    auto blob_it = cur->second.blob_iterator;
    if (blob_it != m_blobs.end()) {
      deref_blob(blob_it, zero, opaque);
    }
    if (remove_from_primary_map) {
      auto ext_it = m_lextents.find(cur->first);
      if (ext_it != m_lextents.end()) {
	if (blob_it == m_blobs.end())
	  blob_it = get_blob_iterator(cur->second.blob); //please note - we use blob ref from the live lextent instead of the one from the lextent map to be able to bypass blob lookup if needed.
	deref_blob(blob_it, zero, opaque);
	m_lextents.erase(ext_it);
      }
    }
    ++cur;
  }
}
