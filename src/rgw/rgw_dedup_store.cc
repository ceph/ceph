// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/rados/rados_types.hpp"
#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "svc_zone.h"
#include "common/config.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "rgw_common.h"
#include "include/denc.h"
#include "rgw_sal.h"
#include "driver/rados/rgw_sal_rados.h"
#include "rgw_dedup_utils.h"
#include "rgw_dedup.h"
#include "rgw_dedup_store.h"
#include "fmt/ranges.h"
#include <span>

namespace rgw::dedup {

  //---------------------------------------------------------------------------
  disk_record_t::disk_record_t(const rgw::sal::Bucket *p_bucket,
                               const std::string      &obj_name,
                               const parsed_etag_t    *p_parsed_etag,
                               uint64_t                obj_size,
                               const std::string      &storage_class)
  {
    this->s.rec_version     = 0;
    this->s.flags           = 0;
    this->s.num_parts       = p_parsed_etag->num_parts;
    this->obj_name          = obj_name;
    this->s.obj_name_len    = this->obj_name.length();
    this->bucket_name       = p_bucket->get_name();
    this->s.bucket_name_len = this->bucket_name.length();

    this->s.md5_high        = p_parsed_etag->md5_high;
    this->s.md5_low         = p_parsed_etag->md5_low;
    this->s.obj_bytes_size  = obj_size;
    this->s.object_version  = 0;

    this->bucket_id         = p_bucket->get_bucket_id();
    this->s.bucket_id_len   = this->bucket_id.length();
    this->tenant_name       = p_bucket->get_tenant();
    this->s.tenant_name_len = this->tenant_name.length();
    this->stor_class        = storage_class;
    this->s.stor_class_len  = storage_class.length();

    this->s.ref_tag_len     = 0;
    this->s.manifest_len    = 0;

    this->s.shared_manifest = 0;
    memset(this->s.sha256, 0, sizeof(this->s.sha256));
    this->ref_tag           = "";
    this->manifest_bl.clear();
  }

  //---------------------------------------------------------------------------
  disk_record_t::disk_record_t(const char *buff)
  {
    disk_record_t *p_rec = (disk_record_t*)buff;
    this->s.rec_version     = p_rec->s.rec_version;
    // wrong version, bail out
    if (unlikely(p_rec->s.rec_version != 0)) {
      return;
    }

    this->s.flags           = p_rec->s.flags;
    this->s.num_parts       = CEPHTOH_16(p_rec->s.num_parts);
    this->s.obj_name_len    = CEPHTOH_16(p_rec->s.obj_name_len);
    this->s.bucket_name_len = CEPHTOH_16(p_rec->s.bucket_name_len);

    this->s.md5_high        = CEPHTOH_64(p_rec->s.md5_high);
    this->s.md5_low         = CEPHTOH_64(p_rec->s.md5_low);
    this->s.obj_bytes_size  = CEPHTOH_64(p_rec->s.obj_bytes_size);
    this->s.object_version  = CEPHTOH_64(p_rec->s.object_version);

    this->s.bucket_id_len   = CEPHTOH_16(p_rec->s.bucket_id_len);
    this->s.tenant_name_len = CEPHTOH_16(p_rec->s.tenant_name_len);
    this->s.stor_class_len  = CEPHTOH_16(p_rec->s.stor_class_len);
    this->s.ref_tag_len     = CEPHTOH_16(p_rec->s.ref_tag_len);
    this->s.manifest_len    = CEPHTOH_16(p_rec->s.manifest_len);

    const char *p = buff + sizeof(this->s);
    this->obj_name = std::string(p, this->s.obj_name_len);
    p += p_rec->s.obj_name_len;

    this->bucket_name = std::string(p, this->s.bucket_name_len);
    p += p_rec->s.bucket_name_len;

    this->bucket_id = std::string(p, this->s.bucket_id_len);
    p += p_rec->s.bucket_id_len;

    this->tenant_name = std::string(p, this->s.tenant_name_len);
    p += p_rec->s.tenant_name_len;

    this->stor_class = std::string(p, this->s.stor_class_len);
    p += p_rec->s.stor_class_len;

    if (p_rec->s.flags.is_fastlane()) {
      // TBD:: remove asserts
      ceph_assert(this->s.ref_tag_len == 0);
      ceph_assert(this->s.manifest_len == 0);
    }
    else {
      this->s.shared_manifest = CEPHTOH_64(p_rec->s.shared_manifest);
      for (int i = 0; i < 4; i++) {
        this->s.sha256[i] = CEPHTOH_64(p_rec->s.sha256[i]);
      }
      this->ref_tag = std::string(p, this->s.ref_tag_len);
      p += p_rec->s.ref_tag_len;

      this->manifest_bl.append(p, this->s.manifest_len);
    }
  }

  //---------------------------------------------------------------------------
  size_t disk_record_t::serialize(char *buff) const
  {
    ceph_assert(this->s.rec_version  == 0);
    disk_record_t *p_rec = (disk_record_t*)buff;
    p_rec->s.rec_version     = 0;
    p_rec->s.flags           = this->s.flags;
    p_rec->s.num_parts       = HTOCEPH_16(this->s.num_parts);
    p_rec->s.obj_name_len    = HTOCEPH_16(this->obj_name.length());
    p_rec->s.bucket_name_len = HTOCEPH_16(this->bucket_name.length());

    p_rec->s.md5_high        = HTOCEPH_64(this->s.md5_high);
    p_rec->s.md5_low         = HTOCEPH_64(this->s.md5_low);
    p_rec->s.obj_bytes_size  = HTOCEPH_64(this->s.obj_bytes_size);
    p_rec->s.object_version  = HTOCEPH_64(this->s.object_version);

    p_rec->s.bucket_id_len   = HTOCEPH_16(this->bucket_id.length());
    p_rec->s.tenant_name_len = HTOCEPH_16(this->tenant_name.length());
    p_rec->s.stor_class_len  = HTOCEPH_16(this->stor_class.length());
    p_rec->s.ref_tag_len     = HTOCEPH_16(this->ref_tag.length());
    p_rec->s.manifest_len    = HTOCEPH_16(this->manifest_bl.length());
    char *p = buff + sizeof(this->s);
    unsigned len = this->obj_name.length();
    std::memcpy(p, this->obj_name.data(), len);
    p += len;

    len = this->bucket_name.length();
    std::memcpy(p, this->bucket_name.data(), len);
    p += len;

    len = this->bucket_id.length();
    std::memcpy(p, this->bucket_id.data(), len);
    p += len;

    len = this->tenant_name.length();
    std::memcpy(p, this->tenant_name.data(), len);
    p += len;

    len = this->stor_class.length();
    std::memcpy(p, this->stor_class.data(), len);
    p += len;

    if (this->s.flags.is_fastlane()) {
      // TBD:: remove asserts
      ceph_assert(this->s.ref_tag_len == 0);
      ceph_assert(this->s.manifest_len == 0);
    }
    else {
      p_rec->s.shared_manifest = HTOCEPH_64(this->s.shared_manifest);
      for (int i = 0; i < 4; i++) {
        p_rec->s.sha256[i] = HTOCEPH_64(this->s.sha256[i]);
      }
      len = this->ref_tag.length();
      std::memcpy(p, this->ref_tag.data(), len);
      p += len;

      len = this->manifest_bl.length();
      const char *p_manifest = const_cast<disk_record_t*>(this)->manifest_bl.c_str();
      std::memcpy(p, p_manifest, len);
      p += len;
    }
    return (p - buff);
  }

  //---------------------------------------------------------------------------
  size_t disk_record_t::length() const
  {
    return (sizeof(this->s) +
            this->obj_name.length() +
            this->bucket_name.length() +
            this->bucket_id.length() +
            this->tenant_name.length() +
            this->stor_class.length() +
            this->ref_tag.length() +
            this->manifest_bl.length());
  }

  //---------------------------------------------------------------------------
  int disk_record_t::validate(const char *caller,
                              const DoutPrefixProvider* dpp,
                              disk_block_id_t block_id,
                              record_id_t rec_id) const
  {
    // optimistic approach
    if (likely((this->s.rec_version == 0) && (this->length() <= MAX_REC_SIZE))) {
      ldpp_dout(dpp, 20) << __func__ << "::success" << dendl;
      return 0;
    }

    // wrong version
    if (this->s.rec_version != 0) {
      // TBD
      //p_stats->failed_wrong_ver++;
      ldpp_dout(dpp, 5) << __func__ << "::" << caller << "::ERR: Bad record version: "
                        << this->s.rec_version
                        << "::block_id=" << block_id
                        << "::rec_id=" << rec_id
                        << dendl;
      return -EPROTO;           // Protocol error
    }

    // if arrived here record size is too large
    // TBD
    //p_stats->failed_rec_overflow++;
    ldpp_dout(dpp, 5) << __func__ << "::" << caller << "::ERR: record size too big: "
                      << this->length()
                      << "::block_id=" << block_id
                      << "::rec_id=" << rec_id
                      << dendl;
    return -EOVERFLOW; // maybe should use -E2BIG ??
  }

  //---------------------------------------------------------------------------
  std::ostream &operator<<(std::ostream &stream, const disk_record_t & rec)
  {
    stream << rec.obj_name << "::" << rec.s.obj_name_len << "\n";
    stream << rec.bucket_name << "::" << rec.s.bucket_name_len << "\n";
    stream << rec.bucket_id << "::" << rec.s.bucket_id_len << "\n";
    stream << rec.tenant_name << "::" << rec.s.tenant_name_len << "\n";
    stream << rec.stor_class << "::" << rec.s.stor_class_len  << "\n";
    stream << rec.ref_tag << "::" << rec.s.ref_tag_len << "\n";
    stream << "num_parts = " << rec.s.num_parts << "\n";
    stream << "obj_size  = " << rec.s.obj_bytes_size/1024 <<" KiB"  << "\n";
    stream << "MD5       = " << std::hex << rec.s.md5_high << rec.s.md5_low << "\n";
    stream << "SHA256    = ";
    for (int i =0; i < 4; i++) {
      stream << rec.s.sha256[i];
    }
    stream << "\n";

    if (rec.has_shared_manifest()) {
      stream << "Shared Manifest Object\n";
    }
    else {
      stream << "Dedicated Manifest Object\n";
    }
    stream << "Manifest len=" << rec.s.manifest_len << "\n";
    return stream;
  }

  //---------------------------------------------------------------------------
  void disk_block_t::init(work_shard_t worker_id, uint32_t seq_number)
  {
    disk_block_header_t *p_header = get_header();
    p_header->offset = sizeof(disk_block_header_t);
    p_header->rec_count = 0;
    p_header->block_id  = disk_block_id_t(worker_id, seq_number);
  }

  //---------------------------------------------------------------------------
  int disk_block_header_t::verify(disk_block_id_t expected_block_id, const DoutPrefixProvider* dpp)
  {
    if (unlikely(offset != BLOCK_MAGIC && offset != LAST_BLOCK_MAGIC)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR::bad magic number (0x" << std::hex << offset << std::dec << ")" << dendl;
      return -EINVAL;
    }

    if (unlikely(rec_count > MAX_REC_IN_BLOCK) ) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR::rec_count=" << rec_count << " > MAX_REC_IN_BLOCK" << dendl;
      return -EINVAL;
    }

    if (unlikely(this->block_id != expected_block_id)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR::block_id=" << block_id
                        << "!= expected_block_id=" << expected_block_id << dendl;
      return -EINVAL;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  record_id_t disk_block_t::add_record(const disk_record_t *p_rec,
                                       const DoutPrefixProvider *dpp)
  {
    disk_block_header_t *p_header = get_header();
    if (unlikely(p_header->rec_count >= MAX_REC_IN_BLOCK)) {
      ldpp_dout(dpp, 20)  << __func__ << "::rec_count=" << p_header->rec_count
                          << ", MAX_REC_IN_BLOCK=" << MAX_REC_IN_BLOCK << dendl;
      return MAX_REC_IN_BLOCK;
    }

    if ((DISK_BLOCK_SIZE - p_header->offset) >= p_rec->length()) {
      p_header->rec_offsets[p_header->rec_count] = p_header->offset;
      unsigned rec_id = p_header->rec_count;
      p_header->rec_count ++;
      p_rec->serialize(data+p_header->offset);
      p_header->offset += p_rec->length();
      return rec_id;
    }
    else {
      return MAX_REC_IN_BLOCK;
    }
  }

  //---------------------------------------------------------------------------
  void disk_block_t::close_block(const DoutPrefixProvider* dpp, bool has_more)
  {
    disk_block_header_t *p_header = get_header();
    ldpp_dout(dpp, 20) << __func__ << "::rec_count=" << p_header->rec_count
                       << ", has_more=" << (has_more? "TRUE" : "FALSE") << dendl;

    memset(data + p_header->offset, 0, (DISK_BLOCK_SIZE - p_header->offset));
    if (has_more) {
      p_header->offset = HTOCEPH_16(BLOCK_MAGIC);
    }
    else {
      p_header->offset = HTOCEPH_16(LAST_BLOCK_MAGIC);
    }
    for (unsigned i = 0; i < p_header->rec_count; i++) {
      p_header->rec_offsets[i] = HTOCEPH_16(p_header->rec_offsets[i]);
    }
    p_header->rec_count = HTOCEPH_16(p_header->rec_count);
    p_header->block_id  = HTOCEPH_32((uint32_t)p_header->block_id);
    // TBD: CRC
  }

  //---------------------------------------------------------------------------
  void disk_block_header_t::deserialize()
  {
    this->offset    = CEPHTOH_16(this->offset);
    this->rec_count = CEPHTOH_16(this->rec_count);
    this->block_id  = CEPHTOH_32((uint32_t)this->block_id);
    for (unsigned i = 0; i < this->rec_count; i++) {
      this->rec_offsets[i] = CEPHTOH_16(this->rec_offsets[i]);
    }
  }

  //---------------------------------------------------------------------------
  disk_block_seq_t::disk_block_seq_t(const DoutPrefixProvider* dpp_in,
                                     disk_block_t *p_arr_in,
                                     work_shard_t worker_id,
                                     md5_shard_t md5_shard,
                                     worker_stats_t *p_stats_in)
  {
    activate(dpp_in, p_arr_in, worker_id, md5_shard, p_stats_in);
  }

  //---------------------------------------------------------------------------
  void disk_block_seq_t::activate(const DoutPrefixProvider* dpp_in,
                                  disk_block_t *p_arr_in,
                                  work_shard_t worker_id,
                                  md5_shard_t md5_shard,
                                  worker_stats_t *p_stats_in)
  {
    dpp          = dpp_in;
    p_arr        = p_arr_in;
    d_worker_id  = worker_id;
    d_md5_shard  = md5_shard;
    p_stats      = p_stats_in;
    p_curr_block = nullptr;
    d_seq_number = 0;

    memset(p_arr, 0, sizeof(disk_block_t));
    slab_reset();
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static int print_manifest(const DoutPrefixProvider *dpp,
                                            RGWRados                 *rados,
                                            const bufferlist         &manifest_bl)
  {
    RGWObjManifest manifest;
    try {
      auto bl_iter = manifest_bl.cbegin();
      decode(manifest, bl_iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1)  << __func__ << "::ERROR: unable to decode manifest" << dendl;
      return -EINVAL;
    }

    unsigned idx = 0;
    for (auto p = manifest.obj_begin(dpp); p != manifest.obj_end(dpp); ++p, ++idx) {
      rgw_raw_obj raw_obj = p.get_location().get_raw_obj(rados);
      ldpp_dout(dpp, 20) << idx << "] " << raw_obj.oid << dendl;
    }
    ldpp_dout(dpp, 20) << "==============================================" << dendl;
    return 0;
  }

  //---------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream& out, const disk_block_id_t& block_id)
  {
    std::ios_base::fmtflags flags = out.flags();
    out << std::hex << "0x"
        << (uint32_t)block_id.get_work_shard_id() << "::"
        << (uint32_t)block_id.get_slab_id() << "::"
        << (uint32_t)block_id.get_block_offset();

    if (flags & std::ios::dec) {
      out << std::dec;
    }
    return out;
  }

  //---------------------------------------------------------------------------
  std::string disk_block_id_t::get_slab_name(md5_shard_t md5_shard) const
  {
    // SLAB.MD5_ID.WORKER_ID.SLAB_SEQ_ID
    const char *SLAB_NAME_FORMAT = "SLB.%03X.%02X.%04X";
    static constexpr uint32_t SLAB_NAME_SIZE = 16;
    char name_buf[SLAB_NAME_SIZE];
    slab_id_t slab_id = get_slab_id();
    work_shard_t work_id = get_work_shard_id();
    unsigned n = snprintf(name_buf, sizeof(name_buf), SLAB_NAME_FORMAT,
                          md5_shard, work_id, slab_id);
    std::string oid(name_buf, n);
    return oid;
  }

  //---------------------------------------------------------------------------
  int load_record(librados::IoCtx          &ioctx,
                  const disk_record_t      *p_tgt_rec,
                  disk_record_t            *p_src_rec, /* OUT */
                  disk_block_id_t           block_id,
                  record_id_t               rec_id,
                  md5_shard_t               md5_shard,
                  const DoutPrefixProvider *dpp)
  {
    std::string oid(block_id.get_slab_name(md5_shard));
    int read_len = DISK_BLOCK_SIZE;
    static_assert(sizeof(disk_block_t) == DISK_BLOCK_SIZE);
    int byte_offset = block_id.get_block_offset() * DISK_BLOCK_SIZE;
    bufferlist bl;
    int ret = ioctx.read(oid, bl, read_len, byte_offset);
    if (unlikely(ret != read_len)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR: failed to read block from " << oid
                        << "::ret=" << ret << "::err=" << cpp_strerror(-ret)<<dendl;
      return ret;
    }
    else {
      ldpp_dout(dpp, 20) << __func__ << "::oid=" << oid << "::ret=" << ret
                         << "::len=" << bl.length() << dendl;
    }

    const char *p = bl.c_str();
    disk_block_t *p_disk_block = (disk_block_t*)p;
    disk_block_header_t *p_header = p_disk_block->get_header();
    p_header->deserialize();
    ret = p_header->verify(block_id, dpp);
    if (ret != 0) {
      return ret;
    }

    unsigned offset = p_header->rec_offsets[rec_id];
    // We deserialize the record inside the CTOR
    disk_record_t rec(p + offset);
    ret = rec.validate(__func__, dpp, block_id, rec_id);
    if (unlikely(ret != 0)) {
      //p_stats->failed_rec_load++;
      return ret;
    }

    if (rec.s.md5_high       == p_tgt_rec->s.md5_high       &&
        rec.s.md5_low        == p_tgt_rec->s.md5_low        &&
        rec.s.num_parts      == p_tgt_rec->s.num_parts      &&
        rec.s.obj_bytes_size == p_tgt_rec->s.obj_bytes_size &&
        rec.stor_class       == p_tgt_rec->stor_class) {

      *p_src_rec = rec;
      return 0;
    }
    else {
      ldpp_dout(dpp, 5) << __func__ << "::ERR: Bad record in block=" << block_id
                        << ", rec_id=" << rec_id << dendl;
      return -EIO;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  [[maybe_unused]]static void
  copy_bl_multi_parts(const bufferlist &bl_in, bufferlist &bl_out,
                      const DoutPrefixProvider* dpp)
  {
    const size_t MAX = 260*1024;
    char buff[MAX];
    std::srand(std::time({}));

    std::vector<int> vec;
    auto bl_itr = bl_in.cbegin();
    size_t len = bl_in.length();
    while (len) {
      const int random_value = std::rand();
      size_t req_len = std::min((random_value % MAX), len);
      if (len < MAX) {
        req_len = len;
      }
      vec.push_back(req_len);
      const char *p = get_next_data_ptr(bl_itr, buff, req_len, dpp);
      bufferptr ptr(p, req_len);
      bl_out.append(ptr);
      len -= req_len;
    }
    ldpp_dout(dpp, 20) << __func__ << "::req_len=" << vec << dendl;
  }

  //---------------------------------------------------------------------------
  int load_slab(librados::IoCtx &ioctx,
                bufferlist &bl_out,
                md5_shard_t md5_shard,
                work_shard_t worker_id,
                uint32_t seq_number,
                const DoutPrefixProvider* dpp)
  {
    disk_block_id_t block_id(worker_id, seq_number);
    std::string oid(block_id.get_slab_name(md5_shard));
    ldpp_dout(dpp, 20) << __func__ << "::worker_id=" << (uint32_t)worker_id
                       << ", md5_shard=" << (uint32_t)md5_shard
                       << ", seq_number=" << seq_number
                       << ":: oid=" << oid << dendl;
#ifndef DEBUG_FRAGMENTED_BUFFERLIST
    int ret = ioctx.read(oid, bl_out, 0, 0);
    if (ret > 0) {
      ldpp_dout(dpp, 20) << __func__ << "::oid=" << oid << ", len="
                         << bl_out.length() << dendl;
    }
#else
    // DEBUG MODE to test with fragmented bufferlist
    bufferlist bl_in;
    // read full object
    int ret = ioctx.read(oid, bl_in, 0, 0);
    if (ret > 0) {
      ldpp_dout(dpp, 20) << __func__ << "::oid=" << oid << ", len="
                         << bl_in.length() << dendl;
      copy_bl_multi_parts(bl_in, bl_out, dpp);
    }
#endif
    else {
      if (ret == 0) {
        // no error reported, but we read nothing which should never happen
        ldpp_dout(dpp, 1) << __func__ << "::ERR: Empty SLAB " << oid << dendl;
        ret = -ENODATA;
      }
      ldpp_dout(dpp, 5) << __func__ << "::ERR: failed to read " << oid
                        << ", error is " << cpp_strerror(-ret) << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  int store_slab(librados::IoCtx &ioctx,
                 bufferlist &bl,
                 md5_shard_t md5_shard,
                 work_shard_t worker_id,
                 uint32_t seq_number,
                 const DoutPrefixProvider* dpp)
  {
    disk_block_id_t block_id(worker_id, seq_number);
    std::string oid(block_id.get_slab_name(md5_shard));
    ldpp_dout(dpp, 20) << __func__ << "::oid=" << oid << ", len="
                       << bl.length() << dendl;
    ceph_assert(bl.length());

    int ret = ioctx.write_full(oid, bl);
    if (ret == (int)bl.length()) {
      ldpp_dout(dpp, 20) << __func__ << "::wrote " << bl.length() << " bytes to "
                         << oid << dendl;
    }
    else {
      if (ret == 0) {
        // no error reported, but we wrote nothing which should never happen
        ldpp_dout(dpp, 5) << __func__ << "::ERR: No Data was written to " << oid
                          << ", bl.length()=" << bl.length() << dendl;
        ret = -ENODATA;
      }
      ldpp_dout(dpp, 1) << "ERROR: failed to write " << oid
                        << " with: " << cpp_strerror(-ret) << dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  int disk_block_seq_t::flush(librados::IoCtx &ioctx)
  {
    unsigned len = (p_curr_block + 1 - p_arr) * sizeof(disk_block_t);
    bufferlist bl = bufferlist::static_from_mem((char*)p_arr, len);
    int ret = store_slab(ioctx, bl, d_md5_shard, d_worker_id, d_seq_number, dpp);
    // Need to make sure the call to rgw_put_system_obj was fully synchronous

    // d_seq_number++ must be called **after** flush!!
    d_seq_number++;
    p_stats->egress_slabs++;
    slab_reset();
    return ret;
  }

  //---------------------------------------------------------------------------
  int disk_block_seq_t::flush_disk_records(librados::IoCtx &ioctx)
  {
    ceph_assert(p_arr);
    ldpp_dout(dpp, 20) << __func__ << "::worker_id=" << (uint32_t)d_worker_id
                       << ", md5_shard=" << (uint32_t)d_md5_shard << dendl;

    // we need to force flush at the end of a cycle even if there was no work done
    // it is used as a signal to worker in the next step
    if (p_curr_block == &p_arr[0] && p_curr_block->is_empty()) {
      ldpp_dout(dpp, 20) << __func__ << "::Empty buffers, generate terminating block" << dendl;
    }
    p_stats->egress_blocks++;
    p_curr_block->close_block(dpp, false);

    int ret = flush(ioctx);
    return ret;
  }

  //---------------------------------------------------------------------------
  int disk_block_seq_t::add_record(librados::IoCtx     &ioctx,
                                   const disk_record_t *p_rec, // IN-OUT
                                   record_info_t       *p_rec_info) // OUT-PARAM
  {
    disk_block_id_t null_block_id;
    int ret = p_rec->validate(__func__, dpp, null_block_id, MAX_REC_IN_BLOCK);
    if (unlikely(ret != 0)) {
      // TBD
      //p_stats->failed_rec_store++;
      return ret;
    }

    p_stats->egress_records ++;
    // first, try and add the record to the current open block
    p_rec_info->rec_id = p_curr_block->add_record(p_rec, dpp);
    if (p_rec_info->rec_id < MAX_REC_IN_BLOCK) {
      p_rec_info->block_id = p_curr_block->get_block_id();
      return 0;
    }
    else {
      // Not enough space left in current block, close it and open the next block
      ldpp_dout(dpp, 20) << __func__ << "::Block is full-> close and move to next" << dendl;
      p_stats->egress_blocks++;
      p_curr_block->close_block(dpp, true);
    }

    // Do we have more Blocks in the block-array ?
    if (p_curr_block < last_block()) {
      p_curr_block ++;
      d_seq_number ++;
      p_curr_block->init(d_worker_id, d_seq_number);
      p_rec_info->rec_id = p_curr_block->add_record(p_rec, dpp);
    }
    else {
      ldpp_dout(dpp, 20)  << __func__ << "::calling flush()" << dendl;
      ret = flush(ioctx);
      p_rec_info->rec_id = p_curr_block->add_record(p_rec, dpp);
    }

    p_rec_info->block_id = p_curr_block->get_block_id();
    return ret;
  }

  //---------------------------------------------------------------------------
  disk_block_array_t::disk_block_array_t(const DoutPrefixProvider* dpp,
                                         uint8_t *raw_mem,
                                         uint64_t raw_mem_size,
                                         work_shard_t worker_id,
                                         worker_stats_t *p_stats,
                                         md5_shard_t num_md5_shards)
  {
    d_num_md5_shards = num_md5_shards;
    d_worker_id = worker_id;
    disk_block_t *p     = (disk_block_t *)raw_mem;
    disk_block_t *p_end = (disk_block_t *)(raw_mem + raw_mem_size);

    for (unsigned md5_shard = 0; md5_shard < d_num_md5_shards; md5_shard++) {
      ldpp_dout(dpp, 20) << __func__ << "::p=" << p << "::p_end=" << p_end << dendl;
      if (p + DISK_BLOCK_COUNT <= p_end) {
        d_disk_arr[md5_shard].activate(dpp, p, d_worker_id, md5_shard, p_stats);
        p += DISK_BLOCK_COUNT;
      }
      else {
        ldpp_dout(dpp, 1) << __func__ << "::ERR: buffer overflow! "
                          << "::md5_shard=" << md5_shard << "/" << d_num_md5_shards
                          << "::raw_mem_size=" << raw_mem_size << dendl;
        ldpp_dout(dpp, 1) << __func__
                          << "::sizeof(disk_block_t)=" << sizeof(disk_block_t)
                          << "::DISK_BLOCK_COUNT=" << DISK_BLOCK_COUNT << dendl;
        ceph_abort();
      }
    }
  }

  //---------------------------------------------------------------------------
  void disk_block_array_t::flush_output_buffers(const DoutPrefixProvider* dpp,
                                                librados::IoCtx &ioctx)
  {
    for (md5_shard_t md5_shard = 0; md5_shard < d_num_md5_shards; md5_shard++) {
      ldpp_dout(dpp, 20) <<__func__ << "::flush buffers:: worker_id="
                         << d_worker_id<< ", md5_shard=" << md5_shard << dendl;
      d_disk_arr[md5_shard].flush_disk_records(ioctx);
    }
  }
} // namespace rgw::dedup
