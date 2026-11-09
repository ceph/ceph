// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 The Ceph Foundation
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <random>
#include <memory>
#include <unistd.h>
#include <stdlib.h>
#include <algorithm>

#include <boost/container/flat_set.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/endian/conversion.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_real.hpp>

#include "common/dout.h"
#include "include/ceph_assert.h"
#include "BlueStore.h"
#include "BlueStore_objects.h"
#include "BlueStore_inlines.h"
#include "bluestore_common.h"
#include "simple_bitmap.h"
#include "include/intarith.h"
#include "common/debug.h"
#include "Allocator.h"
#include "FreelistManager.h"
#include "BlueFS.h"
#include "BlueRocksEnv.h"

using std::byte;
using std::make_unique;
using std::map;
using std::ostream;
using std::ostringstream;
using std::string;
using std::stringstream;
using std::unique_ptr;

using namespace bluestore;

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore

//================================================================================================================
// BlueStore is committing all allocation information (alloc/release) into RocksDB before the client Write is performed.
// This cause a delay in write path and add significant load to the CPU/Memory/Disk.
// The reason for the RocksDB updates is that it allows Ceph to survive any failure without losing the allocation state.
//
// We changed the code skiping RocksDB updates on allocation time and instead performing a full desatge of the allocator object
// with all the OSD allocation state in a single step during umount().
// This change leads to a 25% increase in IOPS and reduced latency in small random-write workload, but exposes the system
// to losing allocation info in failure cases where we don't call umount.
// We add code to perform a full allocation-map rebuild from information stored inside the ONode which is used in failure cases.
// When we perform a graceful shutdown there is no need for recovery and we simply read the allocation-map from a flat file
// where we store the allocation-map during umount().
//================================================================================================================

#undef dout_prefix
#define dout_prefix *_dout << "bluestore::NCB::" << __func__ << "::"

static const std::string allocator_dir    = "ALLOCATOR_NCB_DIR";
static const std::string allocator_file   = "ALLOCATOR_NCB_FILE";
static uint32_t    s_format_version = 0x01; // support future changes to allocator-map file
static uint32_t    s_serial         = 0x01;

// 48 Bytes header for on-disk alloator image
const uint64_t ALLOCATOR_IMAGE_VALID_SIGNATURE = 0x1FACE0FF;
struct allocator_image_header {
  uint32_t format_version;	// 0x00
  uint32_t valid_signature;	// 0x04
  utime_t  timestamp;		// 0x08
  uint32_t serial;		// 0x10
  uint32_t pad[0x7];		// 0x14

  allocator_image_header() {
    memset((char*)this, 0, sizeof(allocator_image_header));
  }

  // create header in CEPH format
  allocator_image_header(utime_t timestamp, uint32_t format_version, uint32_t serial) {
    this->format_version  = format_version;
    this->timestamp       = timestamp;
    this->valid_signature = ALLOCATOR_IMAGE_VALID_SIGNATURE;
    this->serial          = serial;
    memset(this->pad, 0, sizeof(this->pad));
  }

  friend std::ostream& operator<<(std::ostream& out, const allocator_image_header& header) {
    out << "format_version  = " << header.format_version << std::endl;
    out << "valid_signature = " << header.valid_signature << "/" << ALLOCATOR_IMAGE_VALID_SIGNATURE << std::endl;
    out << "timestamp       = " << header.timestamp << std::endl;
    out << "serial          = " << header.serial << std::endl;
    for (unsigned i = 0; i < sizeof(header.pad)/sizeof(uint32_t); i++) {
      if (header.pad[i]) {
	out << "header.pad[" << i << "] = " << header.pad[i] << std::endl;
      }
    }
    return out;
  }

  DENC(allocator_image_header, v, p) {
    denc(v.format_version, p);
    denc(v.valid_signature, p);
    denc(v.timestamp.tv.tv_sec, p);
    denc(v.timestamp.tv.tv_nsec, p);
    denc(v.serial, p);
    for (auto& pad: v.pad) {
      denc(pad, p);
    }
  }


  int verify(CephContext* cct, const std::string &path) {
    if (valid_signature == ALLOCATOR_IMAGE_VALID_SIGNATURE) {
      for (unsigned i = 0; i < (sizeof(pad) / sizeof(uint32_t)); i++) {
	if (this->pad[i]) {
	  derr << "Illegal Header - pad[" << i << "]="<< pad[i] << dendl;
	  return -1;
	}
      }
      return 0;
    }
    else {
      derr << "Illegal Header - signature="<< valid_signature << "(" << ALLOCATOR_IMAGE_VALID_SIGNATURE << ")" << dendl;
      return -1;
    }
  }
};
WRITE_CLASS_DENC(allocator_image_header)

// 56 Bytes trailer for on-disk alloator image
struct allocator_image_trailer {
  extent_t null_extent;         // 0x00

  uint32_t format_version;	// 0x10
  uint32_t valid_signature;	// 0x14

  utime_t  timestamp;		// 0x18

  uint32_t serial;		// 0x20
  uint32_t pad;		// 0x24
  uint64_t entries_count;	// 0x28
  uint64_t allocation_size;	// 0x30

  // trailer is created in CEPH format
  allocator_image_trailer(utime_t timestamp, uint32_t format_version, uint32_t serial, uint64_t entries_count, uint64_t allocation_size) {
    memset((char*)&(this->null_extent), 0, sizeof(this->null_extent));
    this->format_version  = format_version;
    this->valid_signature = ALLOCATOR_IMAGE_VALID_SIGNATURE;
    this->timestamp       = timestamp;
    this->serial          = serial;
    this->pad             = 0;
    this->entries_count   = entries_count;
    this->allocation_size = allocation_size;
  }

  allocator_image_trailer() {
    memset((char*)this, 0, sizeof(allocator_image_trailer));
  }

  friend std::ostream& operator<<(std::ostream& out, const allocator_image_trailer& trailer) {
    if (trailer.null_extent.offset || trailer.null_extent.length) {
      out << "trailer.null_extent.offset = " << trailer.null_extent.offset << std::endl;
      out << "trailer.null_extent.length = " << trailer.null_extent.length << std::endl;
    }
    out << "format_version  = " << trailer.format_version << std::endl;
    out << "valid_signature = " << trailer.valid_signature << "/" << ALLOCATOR_IMAGE_VALID_SIGNATURE << std::endl;
    out << "timestamp       = " << trailer.timestamp << std::endl;
    out << "serial          = " << trailer.serial << std::endl;
    if (trailer.pad) {
      out << "trailer.pad= " << trailer.pad << std::endl;
    }
    out << "entries_count   = " << trailer.entries_count   << std::endl;
    out << "allocation_size = " << trailer.allocation_size << std::endl;
    return out;
  }

  int verify(CephContext* cct, const std::string &path, const allocator_image_header *p_header, uint64_t entries_count, uint64_t allocation_size) {
    if (valid_signature == ALLOCATOR_IMAGE_VALID_SIGNATURE) {

      // trailer must starts with null extents (both fields set to zero) [no need to convert formats for zero)
      if (null_extent.offset || null_extent.length) {
	derr << "illegal trailer - null_extent = [" << null_extent.offset << "," << null_extent.length << "]"<< dendl;
	return -1;
      }

      if (serial != p_header->serial) {
	derr << "Illegal trailer: header->serial(" << p_header->serial << ") != trailer->serial(" << serial << ")" << dendl;
	return -1;
      }

      if (format_version != p_header->format_version) {
	derr << "Illegal trailer: header->format_version(" << p_header->format_version
	     << ") != trailer->format_version(" << format_version << ")" << dendl;
	return -1;
      }

      if (timestamp != p_header->timestamp) {
	derr << "Illegal trailer: header->timestamp(" << p_header->timestamp
	     << ") != trailer->timestamp(" << timestamp << ")" << dendl;
	return -1;
      }

      if (this->entries_count != entries_count) {
	derr << "Illegal trailer: entries_count(" << entries_count << ") != trailer->entries_count("
	     << this->entries_count << ")" << dendl;
	return -1;
      }

      if (this->allocation_size != allocation_size) {
	derr << "Illegal trailer: allocation_size(" << allocation_size << ") != trailer->allocation_size("
	     << this->allocation_size << ")" << dendl;
	return -1;
      }

      if (pad) {
	derr << "Illegal Trailer - pad="<< pad << dendl;
	return -1;
      }

      // if arrived here -> trailer is valid !!
      return 0;
    } else {
      derr << "Illegal Trailer - signature="<< valid_signature << "(" << ALLOCATOR_IMAGE_VALID_SIGNATURE << ")" << dendl;
      return -1;
    }
  }

  DENC(allocator_image_trailer, v, p) {
    denc(v.null_extent.offset, p);
    denc(v.null_extent.length, p);
    denc(v.format_version, p);
    denc(v.valid_signature, p);
    denc(v.timestamp.tv.tv_sec, p);
    denc(v.timestamp.tv.tv_nsec, p);
    denc(v.serial, p);
    denc(v.pad, p);
    denc(v.entries_count, p);
    denc(v.allocation_size, p);
  }
};
WRITE_CLASS_DENC(allocator_image_trailer)


//-------------------------------------------------------------------------------------
// invalidate old allocation file if exists so will go directly to recovery after failure
// we can safely ignore non-existing file
int BlueStore::invalidate_allocation_file_on_bluefs()
{
  // mark that allocation-file was invalidated and we should destage a new copy whne closing db
  need_to_destage_allocation_file = true;
  dout(10) << __func__ << " need_to_destage_allocation_file was set" << dendl;

  BlueFS::FileWriter *p_handle = nullptr;
  if (!bluefs->dir_exists(allocator_dir)) {
    dout(5) << "allocator_dir(" << allocator_dir << ") doesn't exist" << dendl;
    // nothing to do -> return
    return 0;
  }

  int ret = bluefs->stat(allocator_dir, allocator_file, nullptr, nullptr);
  if (ret != 0) {
    dout(5) << __func__ << " allocator_file(" << allocator_file << ") doesn't exist" << dendl;
    // nothing to do -> return
    return 0;
  }


  ret = bluefs->open_for_write(allocator_dir, allocator_file, &p_handle, true);
  if (ret != 0) {
    derr << __func__ << "::NCB:: Failed open_for_write with error-code "
         << ret << dendl;
    return -1;
  }

  dout(5) << "invalidate using bluefs->truncate(p_handle, 0)" << dendl;
  ret = bluefs->truncate(p_handle, 0);
  if (ret != 0) {
    derr << __func__ << "::NCB:: Failed truncaste with error-code "
         << ret << dendl;
    bluefs->close_writer(p_handle);
    return -1;
  }

  bluefs->fsync(p_handle);
  bluefs->close_writer(p_handle);

  return 0;
}

//-----------------------------------------------------------------------------------
int BlueStore::copy_allocator(Allocator* src_alloc, Allocator* dest_alloc, uint64_t* p_num_entries)
{
  *p_num_entries = 0;
  auto count_entries = [&](uint64_t extent_offset, uint64_t extent_length) {
    (*p_num_entries)++;
  };
  src_alloc->foreach(count_entries);

  dout(5) << "count num_entries=" << *p_num_entries << dendl;

  // add 16K extra entries in case new allocation happened
  (*p_num_entries) += 16*1024;
  unique_ptr<extent_t[]> arr;
  try {
    arr = make_unique<extent_t[]>(*p_num_entries);
  } catch (std::bad_alloc&) {
    derr << "****Failed dynamic allocation, num_entries=" << *p_num_entries << dendl;
    return -1;
  }

  uint64_t idx         = 0;
  auto copy_entries = [&](uint64_t extent_offset, uint64_t extent_length) {
    if (extent_length > 0) {
      if (idx < *p_num_entries) {
	arr[idx] = {extent_offset, extent_length};
      }
      idx++;
    }
    else {
      derr << "zero length extent!!! offset=" << extent_offset << ", index=" << idx << dendl;
    }
  };
  src_alloc->foreach(copy_entries);

  dout(5) << "copy num_entries=" << idx << dendl;
  if (idx > *p_num_entries) {
    derr << "****spillover, num_entries=" << *p_num_entries << ", spillover=" << (idx - *p_num_entries) << dendl;
    ceph_assert(idx <= *p_num_entries);
  }

  *p_num_entries = idx;

  for (idx = 0; idx < *p_num_entries; idx++) {
    const extent_t *p_extent = &arr[idx];
    dest_alloc->init_add_free(p_extent->offset, p_extent->length);
  }

  return 0;
}

//-----------------------------------------------------------------------------------
static uint32_t flush_extent_buffer_with_crc(BlueFS::FileWriter *p_handle, const char* buffer, const char *p_curr, uint32_t crc)
{
  std::ptrdiff_t length = p_curr - buffer;
  p_handle->append(buffer, length);

  crc = ceph_crc32c(crc, (const uint8_t*)buffer, length);
  uint32_t encoded_crc = boost::endian::native_to_little(crc);
  p_handle->append((byte*)&encoded_crc, sizeof(encoded_crc));

  return crc;
}

const unsigned MAX_EXTENTS_IN_BUFFER = 4 * 1024; // 4K extents = 64KB of data
// write the allocator to a flat bluefs file - 4K extents at a time
//-----------------------------------------------------------------------------------
int BlueStore::store_allocator(Allocator* src_allocator)
{
  // when storing allocations to file we must be sure there is no background compactions
  // the easiest way to achieve it is to make sure db is closed
  ceph_assert(db == nullptr);
  utime_t  start_time = ceph_clock_now();
  int ret = 0;

  // create dir if doesn't exist already
  if (!bluefs->dir_exists(allocator_dir) ) {
    ret = bluefs->mkdir(allocator_dir);
    if (ret != 0) {
      derr << "Failed mkdir with error-code " << ret << dendl;
      return -1;
    }
  }
  bluefs->compact_log();
  // reuse previous file-allocation if exists
  ret = bluefs->stat(allocator_dir, allocator_file, nullptr, nullptr);
  bool overwrite_file = (ret == 0);
  BlueFS::FileWriter *p_handle = nullptr;
  ret = bluefs->open_for_write(allocator_dir, allocator_file, &p_handle, overwrite_file);
  if (ret != 0) {
    derr <<  __func__ << "Failed open_for_write with error-code " << ret << dendl;
    return -1;
  }

  uint64_t file_size = p_handle->file->fnode.size;
  uint64_t allocated = p_handle->file->fnode.get_allocated();
  dout(10) << "file_size=" << file_size << ", allocated=" << allocated << dendl;

  bluefs->sync_metadata(false);
  unique_ptr<Allocator> allocator(clone_allocator_without_bluefs(src_allocator));
  if (!allocator) {
    bluefs->close_writer(p_handle);
    return -1;
  }
  // remove allocations that are used by bdev label copies
  if (bdev_label_multi == true) {
    _main_bdev_label_remove(allocator.get());
  }

  // store all extents (except for the bluefs extents we removed) in a single flat file
  utime_t                 timestamp = ceph_clock_now();
  uint32_t                crc       = -1;
  {
    allocator_image_header  header(timestamp, s_format_version, s_serial);
    bufferlist              header_bl;
    encode(header, header_bl);
    crc = header_bl.crc32c(crc);
    encode(crc, header_bl);
    p_handle->append(header_bl);
  }

  crc = -1;					 // reset crc
  extent_t        buffer[MAX_EXTENTS_IN_BUFFER]; // 64KB
  extent_t       *p_curr          = buffer;
  const extent_t *p_end           = buffer + MAX_EXTENTS_IN_BUFFER;
  uint64_t        extent_count    = 0;
  uint64_t        allocation_size = 0;
  auto iterated_allocation = [&](uint64_t extent_offset, uint64_t extent_length) {
    if (extent_length == 0) {
      derr <<  __func__ << "" << extent_count << "::[" << extent_offset << "," << extent_length << "]" << dendl;
      ret = -1;
      return;
    }
    p_curr->offset = boost::endian::native_to_little(extent_offset);
    p_curr->length = boost::endian::native_to_little(extent_length);
    extent_count++;
    allocation_size += extent_length;
    p_curr++;

    if (p_curr == p_end) {
      crc = flush_extent_buffer_with_crc(p_handle, (const char*)buffer, (const char*)p_curr, crc);
      p_curr = buffer; // recycle the buffer
    }
  };
  allocator->foreach(iterated_allocation);
  // if got null extent -> fail the operation
  if (ret != 0) {
    derr << "Illegal extent, fail store operation" << dendl;
    derr << "invalidate using bluefs->truncate(p_handle, 0)" << dendl;
    bluefs->truncate(p_handle, 0);
    bluefs->close_writer(p_handle);
    return -1;
  }

  // if we got any leftovers -> add crc and append to file
  if (p_curr > buffer) {
    crc = flush_extent_buffer_with_crc(p_handle, (const char*)buffer, (const char*)p_curr, crc);
  }

  {
    allocator_image_trailer trailer(timestamp, s_format_version, s_serial, extent_count, allocation_size);
    bufferlist trailer_bl;
    encode(trailer, trailer_bl);
    uint32_t crc = -1;
    crc = trailer_bl.crc32c(crc);
    encode(crc, trailer_bl);
    p_handle->append(trailer_bl);
  }

  bluefs->fsync(p_handle);
  bluefs->truncate(p_handle, p_handle->get_pos());
  bluefs->fsync(p_handle);

  utime_t duration = ceph_clock_now() - start_time;
  dout(5) <<"WRITE-extent_count=" << extent_count << ", allocation_size=" << allocation_size << ", serial=" << s_serial << dendl;
  dout(5) <<"p_handle->pos=" << p_handle->get_pos() << " WRITE-duration=" << duration << " seconds" << dendl;

  bluefs->close_writer(p_handle);
  need_to_destage_allocation_file = false;
  return 0;
}

//-----------------------------------------------------------------------------------
Allocator* BlueStore::create_bitmap_allocator(uint64_t bdev_size) {
  // create allocator
  uint64_t alloc_size = min_alloc_size;
  Allocator* alloc = Allocator::create(cct, "bitmap", bdev_size, alloc_size,
				       "recovery");
  if (alloc) {
    return alloc;
  } else {
    derr << "Failed Allocator Creation" << dendl;
    return nullptr;
  }
}

//-----------------------------------------------------------------------------------
size_t calc_allocator_image_header_size()
{
  utime_t                 timestamp = ceph_clock_now();
  allocator_image_header  header(timestamp, s_format_version, s_serial);
  bufferlist              header_bl;
  encode(header, header_bl);
  uint32_t crc = -1;
  crc = header_bl.crc32c(crc);
  encode(crc, header_bl);

  return header_bl.length();
}

//-----------------------------------------------------------------------------------
int calc_allocator_image_trailer_size()
{
  utime_t                 timestamp       = ceph_clock_now();
  uint64_t                extent_count    = -1;
  uint64_t                allocation_size = -1;
  uint32_t                crc             = -1;
  bufferlist              trailer_bl;
  allocator_image_trailer trailer(timestamp, s_format_version, s_serial, extent_count, allocation_size);

  encode(trailer, trailer_bl);
  crc = trailer_bl.crc32c(crc);
  encode(crc, trailer_bl);
  return trailer_bl.length();
}

//-----------------------------------------------------------------------------------
int BlueStore::__restore_allocator(Allocator* allocator, uint64_t *num, uint64_t *bytes)
{
  if (cct->_conf->bluestore_debug_inject_allocation_from_file_failure > 0) {
     boost::mt11213b rng(time(NULL));
    boost::uniform_real<> ur(0, 1);
    if (ur(rng) < cct->_conf->bluestore_debug_inject_allocation_from_file_failure) {
      derr << __func__ << " failure injected." << dendl;
      return -1;
    }
  }
  utime_t start_time = ceph_clock_now();
  BlueFS::FileReader *p_temp_handle = nullptr;
  int ret = bluefs->open_for_read(allocator_dir, allocator_file, &p_temp_handle, false);
  if (ret != 0) {
    dout(1) << "Failed open_for_read with error-code " << ret << dendl;
    return -1;
  }
  unique_ptr<BlueFS::FileReader> p_handle(p_temp_handle);
  uint64_t read_alloc_size = 0;
  uint64_t file_size = p_handle->file->fnode.size;
  dout(5) << "file_size=" << file_size << ",sizeof(extent_t)=" << sizeof(extent_t) << dendl;

  // make sure we were able to store a valid copy
  if (file_size == 0) {
    dout(1) << "No Valid allocation info on disk (empty file)" << dendl;
    return -1;
  }

  // first read the header
  size_t                 offset = 0;
  allocator_image_header header;
  int                    header_size = calc_allocator_image_header_size();
  {
    bufferlist header_bl,temp_bl;
    int        read_bytes = bluefs->read(p_handle.get(), offset, header_size, &temp_bl, nullptr);
    if (read_bytes != header_size) {
      derr << "Failed bluefs->read() for header::read_bytes=" << read_bytes << ", req_bytes=" << header_size << dendl;
      return -1;
    }

    offset += read_bytes;

    header_bl.claim_append(temp_bl);
    auto p = header_bl.cbegin();
    decode(header, p);
    if (header.verify(cct, path) != 0 ) {
      derr << "header = \n" << header << dendl;
      return -1;
    }

    uint32_t crc_calc = -1, crc;
    crc_calc = header_bl.cbegin().crc32c(p.get_off(), crc_calc); //crc from begin to current pos
    decode(crc, p);
    if (crc != crc_calc) {
      derr << "crc mismatch!!! crc=" << crc << ", crc_calc=" << crc_calc << dendl;
      derr << "header = \n" << header << dendl;
      return -1;
    }

    // increment version for next store
    s_serial = header.serial + 1;
  }

  // then read the payload (extents list) using a recycled buffer
  extent_t        buffer[MAX_EXTENTS_IN_BUFFER]; // 64KB
  uint32_t        crc                = -1;
  int             trailer_size       = calc_allocator_image_trailer_size();
  uint64_t        extent_count       = 0;
  uint64_t        extents_bytes_left = file_size - (header_size + trailer_size + sizeof(crc));
  while (extents_bytes_left) {
    int req_bytes  = std::min(extents_bytes_left, static_cast<uint64_t>(sizeof(buffer)));
    int read_bytes = bluefs->read(p_handle.get(), offset, req_bytes, nullptr, (char*)buffer);
    if (read_bytes != req_bytes) {
      derr << "Failed bluefs->read()::read_bytes=" << read_bytes << ", req_bytes=" << req_bytes << dendl;
      return -1;
    }

    offset             += read_bytes;
    extents_bytes_left -= read_bytes;

    const unsigned  num_extent_in_buffer = read_bytes/sizeof(extent_t);
    const extent_t *p_end                = buffer + num_extent_in_buffer;
    for (const extent_t *p_ext = buffer; p_ext < p_end; p_ext++) {
      uint64_t offset = boost::endian::little_to_native(p_ext->offset);
      uint64_t length = boost::endian::little_to_native(p_ext->length);
      read_alloc_size += length;

      if (length > 0) {
	allocator->init_add_free(offset, length);
	extent_count ++;
      } else {
	derr << "extent with zero length at idx=" << extent_count << dendl;
	return -1;
      }
    }

    uint32_t calc_crc = ceph_crc32c(crc, (const uint8_t*)buffer, read_bytes);
    read_bytes        = bluefs->read(p_handle.get(), offset, sizeof(crc), nullptr, (char*)&crc);
    if (read_bytes == sizeof(crc) ) {
      crc     = boost::endian::little_to_native(crc);
      if (crc != calc_crc) {
	derr << "data crc mismatch!!! crc=" << crc << ", calc_crc=" << calc_crc << dendl;
	derr << "extents_bytes_left=" << extents_bytes_left << ", offset=" << offset << ", extent_count=" << extent_count << dendl;
	return -1;
      }

      offset += read_bytes;
      if (extents_bytes_left) {
	extents_bytes_left -= read_bytes;
      }
    } else {
      derr << "Failed bluefs->read() for crc::read_bytes=" << read_bytes << ", req_bytes=" << sizeof(crc) << dendl;
      return -1;
    }

  }

  // finally, read the trailer and verify it is in good shape and that we got all the extents
  {
    bufferlist trailer_bl,temp_bl;
    int        read_bytes = bluefs->read(p_handle.get(), offset, trailer_size, &temp_bl, nullptr);
    if (read_bytes != trailer_size) {
      derr << "Failed bluefs->read() for trailer::read_bytes=" << read_bytes << ", req_bytes=" << trailer_size << dendl;
      return -1;
    }
    offset += read_bytes;

    trailer_bl.claim_append(temp_bl);
    uint32_t crc_calc = -1;
    uint32_t crc;
    allocator_image_trailer trailer;
    auto p = trailer_bl.cbegin();
    decode(trailer, p);
    if (trailer.verify(cct, path, &header, extent_count, read_alloc_size) != 0 ) {
      derr << "trailer=\n" << trailer << dendl;
      return -1;
    }

    crc_calc = trailer_bl.cbegin().crc32c(p.get_off(), crc_calc); //crc from begin to current pos
    decode(crc, p);
    if (crc != crc_calc) {
      derr << "trailer crc mismatch!::crc=" << crc << ", crc_calc=" << crc_calc << dendl;
      derr << "trailer=\n" << trailer << dendl;
      return -1;
    }
  }

  utime_t duration = ceph_clock_now() - start_time;
  dout(5) << "READ--extent_count=" << extent_count << ", read_alloc_size=  "
	    << read_alloc_size << ", file_size=" << file_size << dendl;
  dout(5) << "READ duration=" << duration << " seconds, s_serial=" << header.serial << dendl;
  *num   = extent_count;
  *bytes = read_alloc_size;
  return 0;
}

//-----------------------------------------------------------------------------------
int BlueStore::restore_allocator(Allocator* dest_allocator, uint64_t *num, uint64_t *bytes)
{
  utime_t    start = ceph_clock_now();
  auto temp_allocator = unique_ptr<Allocator>(create_bitmap_allocator(bdev->get_size()));
  int ret = __restore_allocator(temp_allocator.get(), num, bytes);
  if (ret != 0) {
    return ret;
  }

  uint64_t num_entries = 0;
  dout(5) << " calling copy_allocator(bitmap_allocator -> shared_alloc.a)" << dendl;
  copy_allocator(temp_allocator.get(), dest_allocator, &num_entries);
  utime_t duration = ceph_clock_now() - start;
  dout(5) << "restored in " << duration << " seconds, num_entries=" << num_entries << dendl;
  return ret;
}

//-----------------------------------------------------------------------------------
void BlueStore::set_allocation_in_simple_bmap(SimpleBitmap* sbmap, uint64_t offset, uint64_t length)
{
  dout(30) << __func__ << " 0x" << std::hex
           << offset << "~" << length
           << " " << min_alloc_size_mask
           << dendl;
  ceph_assert((offset & min_alloc_size_mask) == 0);
  ceph_assert((length & min_alloc_size_mask) == 0);
  sbmap->set(offset >> min_alloc_size_order, length >> min_alloc_size_order);
}

class ExtentDecoderPartial : public bluestore::ExtentMap::ExtentDecoder {
  using Extent = bluestore::Extent;
  using Blob = bluestore::Blob;
  using BlobRef = bluestore::BlobRef;
  using Collection = bluestore::Collection;

  BlueStore& store;
  read_alloc_stats_t& stats;
  SimpleBitmap& sbmap;
  sb_info_space_efficient_map_t& sb_info;
  uint8_t min_alloc_size_order;
  Extent extent;
  ghobject_t oid;
  volatile_statfs* per_pool_statfs = nullptr;
  bluestore::blob_map_t blobs;
  bluestore::blob_map_t spanning_blobs;
  virtual BlobRef decode_create_blob(
    bptr_c_it_t& p,
    __u8 struct_v,
    uint64_t* sbid,
    bool include_ref_map,
    Collection* c) override;
  void _consume_new_blob(bool spanning,
                          uint64_t extent_no,
                          uint64_t sbid,
                          BlobRef b);
protected:
  void consume_blobid(Extent*, bool spanning, uint64_t blobid) override;
  void consume_blob(Extent* le,
                    uint64_t extent_no,
                    uint64_t sbid,
                    BlobRef b) override;
  void consume_spanning_blob(uint64_t sbid, BlobRef b) override;
  Extent* get_next_extent() override {
    ++stats.extent_count;
    extent = Extent();
    return &extent;
  }
  void add_extent(Extent*) override {
  }
public:
  ExtentDecoderPartial(BlueStore& _store,
                        read_alloc_stats_t& _stats,
                        SimpleBitmap& _sbmap,
                        sb_info_space_efficient_map_t& _sb_info,
                        uint8_t _min_alloc_size_order)
    : store(_store), stats(_stats), sbmap(_sbmap), sb_info(_sb_info),
      min_alloc_size_order(_min_alloc_size_order)
  {}
  const ghobject_t& get_oid() const {
    return oid;
  }
  void reset(const ghobject_t _oid,
    volatile_statfs* _per_pool_statfs);
};

BlueStore::BlobRef ExtentDecoderPartial::decode_create_blob(
  bptr_c_it_t& p,
  __u8 struct_v,
  uint64_t* sbid,
  bool include_ref_map,
  Collection* c) {
  BlobRef b = c ? c->new_blob() : new Blob(nullptr);
  b->decode<true>(p, struct_v, sbid, include_ref_map, c);
  return b;
}
void ExtentDecoderPartial::_consume_new_blob(bool spanning,
                                                        uint64_t extent_no,
                                                        uint64_t sbid,
                                                        BlobRef b)
{
  [[maybe_unused]] auto cct = store.cct;
  ceph_assert(per_pool_statfs);
  ceph_assert(oid != ghobject_t());

  auto &blob = b->get_blob();
  if(spanning) {
    dout(20) << __func__ << " " << spanning << " " << b->id << dendl;
    ceph_assert_decode(b->id >= 0);
    spanning_blobs[b->id] = b;
    ++stats.spanning_blob_count;
  } else {
    dout(20) << __func__ << " " << spanning << " " << extent_no << dendl;
    blobs[extent_no] = b;
  }
  bool compressed = blob.is_compressed();
  if (!blob.is_shared()) {
    for (auto& pe : blob.get_extents()) {
      if (pe.offset == bluestore_pextent_t::INVALID_OFFSET) {
        ++stats.skipped_illegal_extent;
        continue;
      }
      store.set_allocation_in_simple_bmap(&sbmap, pe.offset, pe.length);

      per_pool_statfs->allocated() += pe.length;
      if (compressed) {
        per_pool_statfs->compressed_allocated() += pe.length;
      }
    }
    if (compressed) {
      per_pool_statfs->compressed() +=
        blob.get_compressed_payload_length();
      ++stats.compressed_blob_count;
    }
  } else {
    auto it = sb_info.find(sbid);
    if (it != sb_info.end()) {
      auto &sbi = *it;
      auto pool_id = oid.hobj.get_logical_pool();
      if (sbi.pool_id == sb_info_t::INVALID_POOL_ID) {
        sbi.pool_id = pool_id;
        size_t alloc_delta = sbi.allocated_chunks << min_alloc_size_order;
        per_pool_statfs->allocated() += alloc_delta;
        if (compressed) {
          per_pool_statfs->compressed_allocated() += alloc_delta;
          ++stats.compressed_blob_count;
        }
      }
    } else {
      derr << __func__ << " shared blob not found:" << sbid << dendl;
      /*
       * When the shared_blob key is missing or fails to decode, it is necessary to
       * scan the blob's pextents directly as the sole authoritative source to
       * verify allocated blocks and prevent double-allocation.
       */
      for (auto& pe : blob.get_extents()) {
        if (pe.offset == bluestore_pextent_t::INVALID_OFFSET) {
          continue;
        }
        store.set_allocation_in_simple_bmap(&sbmap, pe.offset, pe.length);
      }
    }
    if (compressed) {
      per_pool_statfs->compressed() +=
        blob.get_compressed_payload_length();
    }
  }
}

void ExtentDecoderPartial::consume_blobid(Extent* le,
                                                     bool spanning,
                                                     uint64_t blobid)
{
  [[maybe_unused]] auto cct = store.cct;
  dout(20) << __func__ << " " << spanning << " " << blobid << dendl;
  auto &map = spanning ? spanning_blobs : blobs;
  auto it = map.find(blobid);
  ceph_assert_decode(it != map.end());
  per_pool_statfs->stored() += le->length;
  if (it->second->get_blob().is_compressed()) {
    per_pool_statfs->compressed_original() += le->length;
  }
}

void ExtentDecoderPartial::consume_blob(Extent* le,
                                                   uint64_t extent_no,
                                                   uint64_t sbid,
                                                   BlobRef b)
{
  _consume_new_blob(false, extent_no, sbid, b);
  per_pool_statfs->stored() += le->length;
  if (b->get_blob().is_compressed()) {
    per_pool_statfs->compressed_original() += le->length;
  }
}

void ExtentDecoderPartial::consume_spanning_blob(uint64_t sbid,
                                                            BlobRef b)
{
  _consume_new_blob(true, 0/*doesn't matter*/, sbid, b);
}

void ExtentDecoderPartial::reset(const ghobject_t _oid,
                                            volatile_statfs* _per_pool_statfs)
{
  oid = _oid;
  per_pool_statfs = _per_pool_statfs;
  bluestore::blob_map_t empty;
  bluestore::blob_map_t empty2;
  std::swap(blobs, empty);
  std::swap(spanning_blobs, empty2);
}

int BlueStore::read_allocation_from_onodes(SimpleBitmap *sbmap, read_alloc_stats_t& stats)
{
  const bool tolerate = _alloc_recovery_tolerates_corruption();

  sb_info_space_efficient_map_t sb_info;
  // iterate over all shared blobs
  auto it = db->get_iterator(PREFIX_SHARED_BLOB, KeyValueDB::ITERATOR_NOCACHE);
  if (!it) {
    derr << "failed getting shared blob's iterator" << dendl;
    return -ENOENT;
  }
  if (it) {
    for (it->lower_bound(string()); it->valid(); it->next()) {
      const auto& key = it->key();
      dout(20) << __func__ << " decode sb " << pretty_binary_string(key) << dendl;
      uint64_t sbid = 0;
      if (get_key_shared_blob(key, &sbid) != 0) {
	derr << __func__ << " bad shared blob key '" << pretty_binary_string(key)
	     << "'" << dendl;
      }
      bluestore_shared_blob_t shared_blob(sbid);
      bufferlist bl = it->value();
      auto blp = bl.cbegin();
      try {
        decode(shared_blob, blp);
      }
      catch (ceph::buffer::error& e) {
	derr << __func__ << " failed to decode Shared Blob"
	     << pretty_binary_string(key) << dendl;
	continue;
      }
      dout(20) << __func__ << "  " << shared_blob << dendl;
      uint64_t allocated = 0;
      for (auto& r : shared_blob.ref_map.ref_map) {
        ceph_assert(r.first != bluestore_pextent_t::INVALID_OFFSET);
        set_allocation_in_simple_bmap(sbmap, r.first, r.second.length);
        allocated += r.second.length;
      }
      auto &sbi = sb_info.add_or_adopt(sbid);
      ceph_assert(p2phase(allocated, min_alloc_size) == 0);
      sbi.allocated_chunks += (allocated >> min_alloc_size_order);
      ++stats.shared_blob_count;
    }
  }

  it = db->get_iterator(PREFIX_OBJ, KeyValueDB::ITERATOR_NOCACHE);
  if (!it) {
    derr << "failed getting onode's iterator" << dendl;
    return -ENOENT;
  }

  uint64_t            kv_count       = 0;
  uint64_t            count_interval = 1'000'000;
  ExtentDecoderPartial edecoder(*this,
                                stats,
                                *sbmap,
                                sb_info,
                                min_alloc_size_order);

  // Tolerate undecodable onodes only when the caller opted in; only read-only
  // fsck does, since its recovered allocation is never destaged. Every other
  // caller aborts as before.
  bool current_onode_valid = false;

  // iterate over all ONodes stored in RocksDB
  for (it->lower_bound(string()); it->valid(); it->next(), kv_count++) {
    // trace an even after every million processed objects (typically every 5-10 seconds)
    if (kv_count && (kv_count % count_interval == 0) ) {
      dout(5) << __func__ << " processed objects count = " << kv_count << dendl;
    }

    auto key = it->key();
    auto okey = key;
    dout(20) << __func__ << " decode onode " << pretty_binary_string(key) << dendl;
    ghobject_t oid;
    if (!is_extent_shard_key(it->key())) {
      int r = get_key_object(okey, &oid);
      if (r != 0) {
        derr << __func__ << " failed to decode onode key = "
             << pretty_binary_string(okey) << dendl;
        return -EIO;
      }
      edecoder.reset(oid,
        &stats.actual_pool_vstatfs[oid.hobj.get_logical_pool()]);
      current_onode_valid = false;
      try {
        bluestore_decode::throwing_guard g(tolerate);
        Onode dummy_on(cct);
        Onode::decode_raw(&dummy_on,
          it->value(),
          edecoder,
          segment_size != 0);
        current_onode_valid = true;
        ++stats.onode_count;
      } catch (const ceph::buffer::error& e) {
        if (!tolerate) { throw; }
        ++alloc_recovery_skipped_onodes;
        derr << __func__ << " skipping undecodable onode "
             << pretty_binary_string(key) << ": " << e.what() << dendl;
        continue;
      }
    } else {
      uint32_t offset;
      int r = get_key_extent_shard(key, &okey, &offset);
      if (r != 0) {
        derr << __func__ << " failed to decode onode extent key = "
             << pretty_binary_string(key) << dendl;
        return -EIO;
      }
      r = get_key_object(okey, &oid);
      if (r != 0) {
        derr << __func__
             << " failed to decode onode key= " << pretty_binary_string(okey)
             << " from extent key= " << pretty_binary_string(key)
             << dendl;
        return -EIO;
      }
      if (!current_onode_valid || oid != edecoder.get_oid()) {
        derr << __func__ << " shard " << pretty_binary_string(okey)
             << " oid: " << oid
             << " without a valid current onode" << dendl;
        continue;
      }
      try {
        bluestore_decode::throwing_guard g(tolerate);
        edecoder.decode_some(it->value(), nullptr);
        ++stats.shard_count;
      } catch (const ceph::buffer::error& e) {
        if (!tolerate) { throw; }
        derr << __func__ << " skipping undecodable extent shard "
             << pretty_binary_string(key) << ": " << e.what() << dendl;
        continue;
      }
    }
  }
  return 0;
}

//---------------------------------------------------------
int BlueStore::reconstruct_allocations(SimpleBitmap *sbmap, read_alloc_stats_t &stats)
{
  int ret;
  double chance = cct->_conf.get_val<double>("bluestore_debug_fast_recovery_compare_chance");
  if (cct->_conf.get_val<uint64_t>("bluestore_allocation_recovery_threads") == 0) {
    // do not compare if multithread is disabled
    chance = 0;
  }
  double roll = 0;
  if (chance > 0) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    roll = dis(gen);
  }

  // first set all space taken by Objects
  if (chance > 0 && chance > roll) {
    ret = allocation_recover_and_compare(sbmap, stats);
    ceph_assert((ret == 0) && "comparing allocator recovery failed");
  } else {
    if (cct->_conf.get_val<uint64_t>("bluestore_allocation_recovery_threads") == 0) {
      ret = read_allocation_from_onodes(sbmap, stats);
    } else {
      ret = read_allocation_from_onodes_mt(sbmap, stats);
    }
  }

  if (ret < 0) {
    derr << "failed read_allocation_from_onodes()" << dendl;
    return ret;
  }
  // then set space used by superblock
  auto super_length = std::max<uint64_t>(min_alloc_size, SUPER_RESERVED);
  set_allocation_in_simple_bmap(sbmap, 0, super_length);
  stats.extent_count++;

  std::lock_guard l(vstatfs_lock);
  store_statfs_t s;
  osd_pools.clear();
  for (auto& p : stats.actual_pool_vstatfs) {
    if (per_pool_stat_collection) {
      osd_pools[p.first] = p.second;
    }
    stats.actual_store_vstatfs += p.second;
    p.second.publish(&s);
    dout(5) << __func__ << " recovered pool "
            << std::hex
            << p.first << "->" << s
            << std::dec
            << " per-pool:" << per_pool_stat_collection
            << dendl;
  }
  vstatfs = stats.actual_store_vstatfs;
  vstatfs.publish(&s);
  dout(5) << __func__ << " recovered " << s
          << dendl;

  return 0;
}

//-----------------------------------------------------------------------------------
static void copy_simple_bitmap_to_allocator(SimpleBitmap* sbmap, Allocator* dest_alloc, uint64_t alloc_size)
{
  int alloc_size_shift = std::countr_zero(alloc_size);
  uint64_t offset = 0;
  extent_t ext    = sbmap->get_next_clr_extent(offset);
  while (ext.length != 0) {
    dest_alloc->init_add_free(ext.offset << alloc_size_shift, ext.length << alloc_size_shift);
    offset = ext.offset + ext.length;
    ext = sbmap->get_next_clr_extent(offset);
  }
}

//---------------------------------------------------------
int BlueStore::read_allocation_from_drive_on_startup()
{
  int ret = 0;

  ret = _open_collections();
  if (ret < 0) {
    return ret;
  }
  auto shutdown_cache = make_scope_guard([&] {
    _shutdown_cache();
  });

  utime_t            start = ceph_clock_now();
  read_alloc_stats_t stats = {};
  SimpleBitmap sbmap(cct, (bdev->get_size()/ min_alloc_size));
  ret = reconstruct_allocations(&sbmap, stats);
  if (ret != 0) {
    return ret;
  }

  copy_simple_bitmap_to_allocator(&sbmap, alloc, min_alloc_size);

  utime_t duration = ceph_clock_now() - start;
  dout(1) << "::Allocation Recovery was completed in " << duration << " seconds, extent_count=" << stats.extent_count << dendl;
  return ret;
}




// Only used for debugging purposes - we build a secondary allocator from the Onodes and compare it to the existing one
// Not meant to be run by customers
#ifdef CEPH_BLUESTORE_TOOL_RESTORE_ALLOCATION

//---------------------------------------------------------
int cmpfunc (const void * a, const void * b)
{
  if ( ((extent_t*)a)->offset > ((extent_t*)b)->offset ) {
    return 1;
  }
  else if( ((extent_t*)a)->offset < ((extent_t*)b)->offset ) {
    return -1;
  }
  else {
    return 0;
  }
}

// compare the allocator built from Onodes with the system allocator (CF-B)
//---------------------------------------------------------
int BlueStore::compare_allocators(Allocator* alloc1, Allocator* alloc2, uint64_t req_extent_count, uint64_t memory_target)
{
  uint64_t allocation_size = std::min((req_extent_count) * sizeof(extent_t), memory_target / 3);
  uint64_t extent_count    = allocation_size/sizeof(extent_t);
  dout(5) << "req_extent_count=" << req_extent_count << ", granted extent_count="<< extent_count << dendl;

  unique_ptr<extent_t[]> arr1;
  unique_ptr<extent_t[]> arr2;
  try {
    arr1 = make_unique<extent_t[]>(extent_count);
    arr2 = make_unique<extent_t[]>(extent_count);
  } catch (std::bad_alloc&) {
    derr << "****Failed dynamic allocation, extent_count=" << extent_count << dendl;
    return -1;
  }

  // copy the extents from the allocators into simple array and then compare them
  uint64_t size1 = 0, size2 = 0;
  uint64_t idx1  = 0, idx2  = 0;
  auto iterated_mapper1 = [&](uint64_t offset, uint64_t length) {
    size1 += length;
    if (idx1 < extent_count) {
      arr1[idx1++] = {offset, length};
    }
    else if (idx1 == extent_count) {
      derr << "(2)compare_allocators:: spillover"  << dendl;
      idx1 ++;
    }

  };

  auto iterated_mapper2 = [&](uint64_t offset, uint64_t length) {
    size2 += length;
    if (idx2 < extent_count) {
      arr2[idx2++] = {offset, length};
    }
    else if (idx2 == extent_count) {
      derr << "(2)compare_allocators:: spillover"  << dendl;
      idx2 ++;
    }
  };

  alloc1->foreach(iterated_mapper1);
  alloc2->foreach(iterated_mapper2);

  qsort(arr1.get(), std::min(idx1, extent_count), sizeof(extent_t), cmpfunc);
  qsort(arr2.get(), std::min(idx2, extent_count), sizeof(extent_t), cmpfunc);

  if (idx1 == idx2) {
    idx1 = idx2 = std::min(idx1, extent_count);
    if (memcmp(arr1.get(), arr2.get(), sizeof(extent_t) * idx2) == 0) {
      return 0;
    }
    derr << "Failed memcmp(arr1, arr2, sizeof(extent_t)*idx2)"  << dendl;
    for (uint64_t i = 0; i < idx1; i++) {
      if (memcmp(arr1.get()+i, arr2.get()+i, sizeof(extent_t)) != 0) {
	derr << "!!!![" << i << "] arr1::<" << arr1[i].offset << "," << arr1[i].length << ">" << dendl;
	derr << "!!!![" << i << "] arr2::<" << arr2[i].offset << "," << arr2[i].length << ">" << dendl;
	return -1;
      }
    }
    return 0;
  } else {
    derr << "mismatch:: idx1=" << idx1 << " idx2=" << idx2 << dendl;
    return -1;
  }
}

//---------------------------------------------------------
int BlueStore::add_existing_bluefs_allocation(Allocator* allocator, read_alloc_stats_t &stats)
{
  // then add space used by bluefs to store rocksdb
  unsigned extent_count = 0;
  if (bluefs) {
    bluefs->foreach_block_extents(
      bluefs_layout.shared_bdev,
      [&](uint64_t start, uint32_t len) {
        allocator->init_rm_free(start, len);
        stats.extent_count++;
      }
    );
  }

  dout(5) << "bluefs extent_count=" << extent_count << dendl;
  return 0;
}

//---------------------------------------------------------
int BlueStore::read_allocation_from_drive_for_bluestore_tool()
{
  dout(5) << __func__ << dendl;
  int ret = 0;
  uint64_t memory_target = cct->_conf.get_val<Option::size_t>("osd_memory_target");
  ret = _open_db_and_around(true, false);
  if (ret < 0) {
    return ret;
  }

  ret = _open_collections();
  if (ret < 0) {
    _close_db_and_around();
    return ret;
  }

  utime_t            duration;
  read_alloc_stats_t stats = {};
  utime_t            start = ceph_clock_now();

  auto shutdown_cache = make_scope_guard([&] {
    dout(1) << "Allocation Recovery was completed in " << duration
	    << " seconds; insert_count=" << stats.insert_count
	    << "; extent_count=" << stats.extent_count << dendl;
    _shutdown_cache();
    _close_db_and_around();
  });

  {
    auto allocator = unique_ptr<Allocator>(create_bitmap_allocator(bdev->get_size()));
    //reconstruct allocations into a temp simple-bitmap and copy into allocator
    {
      SimpleBitmap sbmap(cct, (bdev->get_size()/ min_alloc_size));
      ret = reconstruct_allocations(&sbmap, stats);
      if (ret != 0) {
	return ret;
      }
      copy_simple_bitmap_to_allocator(&sbmap, allocator.get(), min_alloc_size);
    }

    // add allocation space used by the bluefs itself
    ret = add_existing_bluefs_allocation(allocator.get(), stats);
    if (ret < 0) {
      return ret;
    }
    if (bdev_label_multi) {
      uint64_t lsize = std::max(BDEV_LABEL_BLOCK_SIZE, min_alloc_size);
      for (uint64_t p : bdev_label_valid_locations) {
	if (p != BDEV_FIRST_LABEL_POSITION) {
	  allocator->init_rm_free(p, lsize);
	}
      }
    }

    duration = ceph_clock_now() - start;
    stats.insert_count = 0;
    auto count_entries = [&](uint64_t extent_offset, uint64_t extent_length) {
      stats.insert_count++;
    };
    allocator->foreach(count_entries);
    ret = compare_allocators(allocator.get(), alloc, stats.insert_count, memory_target);
    if (ret == 0) {
      dout(5) << "Allocator drive - file integrity check OK" << dendl;
    } else {
      derr << "FAILURE. Allocator from file and allocator from metadata differ::ret=" << ret << dendl;
    }
  }

  dout(1) << stats << dendl;
  return ret;
}

int BlueStore::compare_allocation_recovery_for_bluestore_tool(ostream& out)
{
  dout(5) << __func__ << dendl;
  int ret = 0;
  ret = _open_db_and_around(true, false);
  if (ret < 0) {
    return ret;
  }

  ret = _open_collections();
  if (ret < 0) {
    _close_db_and_around();
    return ret;
  }
  auto shutdown_cache = make_scope_guard([&] {
    _shutdown_cache();
    _close_db_and_around();
  });

  SimpleBitmap old_bitmap(cct, (bdev->get_size()/ min_alloc_size));
  read_alloc_stats_t old_stats = {};
  ret = allocation_recover_and_compare(&old_bitmap, old_stats, &out);
  return ret;
}

int BlueStore::allocation_recover_and_compare(
  SimpleBitmap *sbmap,
  read_alloc_stats_t &stats,
  ostream* extra_out)
{
  int ret = 0;
  SimpleBitmap& old_bitmap = *sbmap;
  read_alloc_stats_t& old_stats = stats;
  SimpleBitmap mt_bitmap(cct, (bdev->get_size()/ min_alloc_size));
  read_alloc_stats_t mt_stats = {};
  utime_t start;
  utime_t duration;
  ostringstream out;
  auto flushl = [&]() {
    dout(0) << out.str() << dendl;
    if (extra_out) {
      (*extra_out) << out.str() << std::endl;
      std::flush(*extra_out);
    }
    out.str("");
  };

  if (cct->_conf.get_val<uint64_t>("bluestore_allocation_recovery_threads") != 0) {
    out << "New recovery start"; flushl();
    start = ceph_clock_now();
    ret = read_allocation_from_onodes_mt(&mt_bitmap, mt_stats);
    duration = ceph_clock_now() - start;
    out << "New recovery result=" << ret << " took " << duration << " seconds"; flushl();
    out << "New recovery stats=" << std::endl << mt_stats; flushl();
  }

  out << "Legacy recovery start"; flushl();
  start = ceph_clock_now();
  ret = read_allocation_from_onodes(&old_bitmap, old_stats);
  duration = ceph_clock_now() - start;
  out << "Legacy recovery result=" << ret << " took " << duration << " seconds"; flushl();
  out << "Legacy recovery stats=" << std::endl << old_stats; flushl();

  if (cct->_conf.get_val<uint64_t>("bluestore_allocation_recovery_threads") == 0) {
    out << "bluestore_allocation_recovery_threads = 0"; flushl();
    out << "No multithread recovery to compare."; flushl();
    return 0;
  }
  if (old_stats.actual_pool_vstatfs == mt_stats.actual_pool_vstatfs) {
    out << "FSstats the same."; flushl();
  } else {
    out << "FSTATS DIFFERENT !"; flushl();
    ret = -1;
  }

  extent_t ext_a;
  extent_t ext_b;
  uint64_t offset = 0;
  do {
    ext_a = mt_bitmap.get_next_set_extent(offset);
    ext_b = old_bitmap.get_next_set_extent(offset);
    if (ext_a != ext_b) {
      out << "ALLOCATOR DIFFERENT !, first:"; flushl();
      out << ext_a.offset << "~" << ext_a.length; flushl();
      out << ext_b.offset << "~" << ext_b.length; flushl();
      offset = 1;
      ret = -1;
      break;
    }
    offset = ext_a.offset + ext_a.length;
  } while (offset != 0);
  if (offset == 0) {
    out << "Allocators the same."; flushl();
  }
  return ret;
}

//---------------------------------------------------------
Allocator* BlueStore::clone_allocator_without_bluefs(Allocator *src_allocator)
{
  uint64_t   bdev_size = bdev->get_size();
  Allocator* allocator = create_bitmap_allocator(bdev_size);
  if (allocator) {
    dout(5) << "bitmap-allocator=" << allocator << dendl;
  } else {
    derr << "****failed create_bitmap_allocator()" << dendl;
    return nullptr;
  }

  uint64_t num_entries = 0;
  copy_allocator(src_allocator, allocator, &num_entries);

  // BlueFS stores its internal allocation outside RocksDB (FM) so we should not destage them to the allcoator-file
  // we are going to hide bluefs allocation during allocator-destage as they are stored elsewhere
  {
    bluefs->foreach_block_extents(
      bluefs_layout.shared_bdev,
      [&] (uint64_t start, uint32_t len) {
        allocator->init_add_free(start, len);
      }
    );
  }

  return allocator;
}

//---------------------------------------------------------
static void clear_allocation_objects_from_rocksdb(KeyValueDB *db, CephContext *cct, const std::string &path)
{
  dout(5) << "t->rmkeys_by_prefix(PREFIX_ALLOC_BITMAP)" << dendl;
  KeyValueDB::Transaction t = db->get_transaction();
  t->rmkeys_by_prefix(PREFIX_ALLOC_BITMAP);
  db->submit_transaction_sync(t);
}

//---------------------------------------------------------
void BlueStore::copy_allocator_content_to_fm(Allocator *allocator, FreelistManager *real_fm)
{
  unsigned max_txn = 1024;
  dout(5) << "max_transaction_submit=" << max_txn << dendl;
  uint64_t size = 0, idx = 0;
  KeyValueDB::Transaction txn = db->get_transaction();
  auto iterated_insert = [&](uint64_t offset, uint64_t length) {
    size += length;
    real_fm->release(offset, length, txn);
    if ((++idx % max_txn) == 0) {
      db->submit_transaction_sync(txn);
      txn = db->get_transaction();
    }
  };
  allocator->foreach(iterated_insert);
  if (idx % max_txn != 0) {
    db->submit_transaction_sync(txn);
  }
  dout(5) << "size=" << size << ", num extents=" << idx  << dendl;
}

//---------------------------------------------------------
Allocator* BlueStore::initialize_allocator_from_freelist(FreelistManager *real_fm)
{
  dout(5) << "real_fm->enumerate_next" << dendl;
  Allocator* allocator2 = create_bitmap_allocator(bdev->get_size());
  if (allocator2) {
    dout(5) << "bitmap-allocator=" << allocator2 << dendl;
  } else {
    return nullptr;
  }

  uint64_t size2 = 0, idx2 = 0;
  real_fm->enumerate_reset();
  uint64_t offset, length;
  while (real_fm->enumerate_next(db, &offset, &length)) {
    allocator2->init_add_free(offset, length);
    ++idx2;
    size2 += length;
  }
  real_fm->enumerate_reset();

  dout(5) << "size2=" << size2 << ", num2=" << idx2 << dendl;
  return allocator2;
}

//---------------------------------------------------------
// close the active fm and open it in a new mode like makefs()
// but make sure to mark the full device space as allocated
// later we will mark all exetents from the allocator as free
int BlueStore::reset_fm_for_restore()
{
  dout(5) << "<<==>> fm->clear_null_manager()" << dendl;
  fm->shutdown();
  delete fm;
  fm = nullptr;
  freelist_type = "bitmap";
  KeyValueDB::Transaction t = db->get_transaction();
  // call _open_fm() with fm_restore set to TRUE
  // this will mark the full device space as allocated (and not just the reserved space)
  _open_fm(t, true, true, true);
  if (fm == nullptr) {
    derr << "Failed _open_fm()" << dendl;
    return -1;
  }
  db->submit_transaction_sync(t);
  ceph_assert(!fm->is_null_manager());
  dout(5) << "fm was reactivated in full mode" << dendl;
  return 0;
}


//---------------------------------------------------------
// create a temp allocator filled with allocation state from the fm
// and compare it to the base allocator passed in
int BlueStore::verify_rocksdb_allocations(Allocator *allocator)
{
  dout(5) << "verify that alloc content is identical to FM" << dendl;
  // initialize from freelist
  Allocator* temp_allocator = initialize_allocator_from_freelist(fm);
  if (temp_allocator == nullptr) {
    return -1;
  }

  uint64_t insert_count = 0;
  auto count_entries = [&](uint64_t extent_offset, uint64_t extent_length) {
    insert_count++;
  };
  temp_allocator->foreach(count_entries);
  uint64_t memory_target = cct->_conf.get_val<Option::size_t>("osd_memory_target");
  int ret = compare_allocators(allocator, temp_allocator, insert_count, memory_target);

  delete temp_allocator;

  if (ret == 0) {
    dout(5) << "SUCCESS!!! compare(allocator, temp_allocator)" << dendl;
    return 0;
  } else {
    derr << "**** FAILURE compare(allocator, temp_allocator)::ret=" << ret << dendl;
    return -1;
  }
}

//---------------------------------------------------------
int BlueStore::db_cleanup(int ret)
{
  _shutdown_cache();
  _close_db_and_around();
  return ret;
}

//---------------------------------------------------------
// convert back the system from null-allocator to using rocksdb to store allocation
int BlueStore::push_allocation_to_rocksdb()
{
  if (cct->_conf->bluestore_allocation_from_file) {
    derr << "cct->_conf->bluestore_allocation_from_file must be cleared first" << dendl;
    derr << "please change default to false in ceph.conf file>" << dendl;
    return -1;
  }

  dout(5) << "calling open_db_and_around() in read/write mode" << dendl;
  int ret = _open_db_and_around(false);
  if (ret < 0) {
    return ret;
  }

  if (!fm->is_null_manager()) {
    derr << "This is not a NULL-MANAGER -> nothing to do..." << dendl;
    return db_cleanup(0);
  }

  // start by creating a clone copy of the shared-allocator
  unique_ptr<Allocator> allocator(clone_allocator_without_bluefs(alloc));
  if (!allocator) {
    return db_cleanup(-1);
  }

  // remove all objects of PREFIX_ALLOC_BITMAP from RocksDB to guarantee a clean start
  clear_allocation_objects_from_rocksdb(db, cct, path);

  // then open fm in new mode with the full devie marked as alloctaed
  if (reset_fm_for_restore() != 0) {
    return db_cleanup(-1);
  }

  // push the free-space from the allocator (shared-alloc without bfs) to rocksdb
  copy_allocator_content_to_fm(allocator.get(), fm);

  // compare the allocator info with the info stored in the fm/rocksdb
  if (verify_rocksdb_allocations(allocator.get()) == 0) {
    // all is good -> we can commit to rocksdb allocator
    commit_to_real_manager();
  } else {
    return db_cleanup(-1);
  }

  // can't be too paranoid :-)
  dout(5) << "Running full scale verification..." << dendl;
  // close db/fm/allocator and start fresh
  db_cleanup(0);
  dout(5) << "calling open_db_and_around() in read-only mode" << dendl;
  ret = _open_db_and_around(true);
  if (ret < 0) {
    return db_cleanup(ret);
  }
  ceph_assert(!fm->is_null_manager());
  ceph_assert(verify_rocksdb_allocations(allocator.get()) == 0);

  return db_cleanup(ret);
}

#endif // CEPH_BLUESTORE_TOOL_RESTORE_ALLOCATION

//-------------------------------------------------------------------------------------
int BlueStore::commit_freelist_type()
{
  // When freelist_type to "bitmap" we will store allocation in RocksDB
  // When allocation-info is stored in a single file we set freelist_type to "null"
  // This will direct the startup code to read allocation from file and not RocksDB
  KeyValueDB::Transaction t = db->get_transaction();
  if (t == nullptr) {
    derr << "db->get_transaction() failed!!!" << dendl;
    return -1;
  }

  bufferlist bl;
  bl.append(freelist_type);
  t->set(PREFIX_SUPER, "freelist_type", bl);

  int ret = db->submit_transaction_sync(t);
  if (ret != 0) {
    derr << "Failed db->submit_transaction_sync(t)" << dendl;
  }
  return ret;
}

//-------------------------------------------------------------------------------------
int BlueStore::commit_to_null_manager()
{
  dout(5) << __func__ << " Set FreelistManager to NULL FM..." << dendl;
  fm->set_null_manager();
  freelist_type = "null";
#if 1
  return commit_freelist_type();
#else
  // should check how long this step take on a big configuration as deletes are expensive
  if (commit_freelist_type() == 0) {
    // remove all objects of PREFIX_ALLOC_BITMAP from RocksDB to guarantee a clean start
    clear_allocation_objects_from_rocksdb(db, cct, path);
  }
#endif
}


//-------------------------------------------------------------------------------------
int BlueStore::commit_to_real_manager()
{
  dout(5) << "Set FreelistManager to Real FM..." << dendl;
  ceph_assert(!fm->is_null_manager());
  freelist_type = "bitmap";
  int ret = commit_freelist_type();
  if (ret == 0) {
    //remove the allocation_file
    invalidate_allocation_file_on_bluefs();
    ret = bluefs->unlink(allocator_dir, allocator_file);
    bluefs->sync_metadata(false);
    if (ret == 0) {
      dout(5) << "Remove Allocation File successfully" << dendl;
    }
    else {
      derr << "Remove Allocation File ret_code=" << ret << dendl;
    }
  }

  return ret;
}

//================================================================================================================
//================================================================================================================
