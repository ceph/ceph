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

#include "rgw_dedup_utils.h"
#include "common/ceph_crypto.h"

namespace rgw::dedup {
  //---------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream &out, const dedup_req_type_t& dedup_type)
  {
    if (dedup_type == dedup_req_type_t::DEDUP_TYPE_NONE) {
      out << "DEDUP_TYPE_NONE";
    }
    else if (dedup_type == dedup_req_type_t::DEDUP_TYPE_ESTIMATE) {
      out << "DEDUP_TYPE_ESTIMATE";
    }
    else if (dedup_type == dedup_req_type_t::DEDUP_TYPE_FULL) {
      out << "DEDUP_TYPE_FULL";
    }
    else {
      out << "\n*** unexpected dedup_type ***\n";
    }

    return out;
  }

  //---------------------------------------------------------------------------
  dedup_stats_t& dedup_stats_t::operator+=(const dedup_stats_t& other)
  {
    this->singleton_count += other.singleton_count;
    this->unique_count += other.unique_count;
    this->duplicate_count += other.duplicate_count;
    this->dedup_bytes_estimate += other.dedup_bytes_estimate;
    return *this;
  }

  //---------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream &out, const dedup_stats_t& stats)
  {
    out << "::singleton_count="  << stats.singleton_count
        << "::unique_count="     << stats.unique_count
        << "::duplicate_count="  << stats.duplicate_count
        << "::duplicated_bytes=" << stats.dedup_bytes_estimate;
    return out;
  }

  //---------------------------------------------------------------------------
  void encode(const dedup_stats_t& ds, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(ds.singleton_count, bl);
    encode(ds.unique_count, bl);
    encode(ds.duplicate_count, bl);
    encode(ds.dedup_bytes_estimate, bl);
    ENCODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  void decode(dedup_stats_t& ds, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(ds.singleton_count, bl);
    decode(ds.unique_count, bl);
    decode(ds.duplicate_count, bl);
    decode(ds.dedup_bytes_estimate, bl);
    DECODE_FINISH(bl);
  }

  // convert a hex-string to a 64bit integer (max 16 hex digits)
  //---------------------------------------------------------------------------
  bool hex2int(const char *p, const char *p_end, uint64_t *p_val)
  {
    if (p_end - p <= (int)(sizeof(uint64_t) * 2)) {
      uint64_t val = 0;
      while (p < p_end) {
        // get current character then increment
        uint8_t byte = *p++;
        // transform hex character to the 4bit equivalent number, using the ASCII table indexes
        if (byte >= '0' && byte <= '9') {
          byte = byte - '0';
        }
        else if (byte >= 'a' && byte <='f') {
          byte = byte - 'a' + 10;
        }
        else if (byte >= 'A' && byte <='F') {
          byte = byte - 'A' + 10;
        }
        else {
          // terminate on the first non hex char
          return false;
        }
        // shift 4 to make space for new digit, and add the 4 bits of the new digit
        val = (val << 4) | (byte & 0xF);
      }
      *p_val = val;
      return true;
    }
    else {
      return false;
    }
  }

  //---------------------------------------------------------------------------
  bool dec2int(const char *p, const char* p_end, uint16_t *p_val)
  {
    uint16_t val = 0;
    while (p < p_end) {
      uint8_t byte = *p++;
      if (byte >= '0' && byte <= '9') {
        val = val * 10 + (byte - '0');
      }
      else {
        // terminate on the first non hex char
        return false;
      }
    }
    *p_val = val;
    return true;
  }

  // 16Bytes MD5 takes 32 chars
  const unsigned MD5_LENGTH = 32;

  //---------------------------------------------------------------------------
  static bool get_num_parts(const std::string & etag, uint16_t *p_num_parts)
  {
    // Amazon S3 multipart upload Maximum number = 10,000
    const unsigned MAX_PARTS = 10000;
    if (etag.length() <= MD5_LENGTH) {
      // i.e. no multipart
      *p_num_parts = 0;
      return true;
    }

    // Amazon S3 multipart upload Maximum number = 10,000 (5 decimal digits)
    // We need 1 extra byte for the '-' delimiter and 1 extra byte for '"' at the end
    // 7 Bytes should suffice, but we roundup to 8 Bytes
    const unsigned MAX_PART_LEN = 8;
    if (unlikely(etag.length() > MD5_LENGTH + MAX_PART_LEN)) {
      // illegal ETAG
      return false;
    }

    std::string::size_type n = etag.find('-', etag.length() - MAX_PART_LEN);
    if (n != std::string::npos) {
      char buff[MAX_PART_LEN];
      // again, 1 extra byte for the '-' delimiter
      unsigned copy_size = etag.length() - (n + 1);
      if (copy_size <= MAX_PART_LEN) {
        unsigned nbytes = etag.copy(buff, copy_size, n+1);
        uint16_t num_parts;
        const unsigned MAX_UINT16_DIGITS = 5; // 65536
        if (nbytes <= MAX_UINT16_DIGITS) {
          if (dec2int(buff, buff+nbytes, &num_parts) && num_parts <= MAX_PARTS) {
            *p_num_parts = num_parts;
            return true;
          } // else, not all digits are legal
        }   // else, more than 5 digits
      }     // else, copy len too large
    }       // else, '-' delimiter was not found

    // illegal number of parts
    return false;
  }

  //---------------------------------------------------------------------------
  bool parse_etag_string(const std::string& etag, parsed_etag_t *parsed_etag)
  {
    char buff[MD5_LENGTH*2];
    uint16_t num_parts = 0;
    if (get_num_parts(etag, &num_parts)) {
      etag.copy(buff, MD5_LENGTH, 0);
      uint64_t high, low;
      if (hex2int(buff, buff+16, &high)) {
        if (hex2int(buff+16, buff+32, &low)) {
          parsed_etag->md5_high  = high;      // High Bytes of the Object Data MD5
          parsed_etag->md5_low   = low;       // Low  Bytes of the Object Data MD5
          parsed_etag->num_parts = num_parts; // How many parts were used in multipart upload
          return true;
        }
      }
    }

    // an illegal etag string
    return false;
  }

  //---------------------------------------------------------------------------
  void etag_to_bufferlist(uint64_t md5_high, uint64_t md5_low, uint16_t num_parts,
                          ceph::bufferlist *bl)
  {
    char buff[64];
    int n = snprintf(buff, sizeof(buff), "%016lx%016lx", md5_high, md5_low);
    if (num_parts >= 1) {
      n += snprintf(buff + n, sizeof(buff) - n, "-%u", num_parts);
    }
    bl->append(buff, n);
  }

  //---------------------------------------------------------------------------
  const char* get_next_data_ptr(bufferlist::const_iterator &bl_itr,
                                char data_buff[],
                                size_t len,
                                const DoutPrefixProvider* dpp)
  {
    const char *p = nullptr;
    size_t n = bl_itr.get_ptr_and_advance(len, &p);
    if (n == len) {
      // we got a zero-copy raw pointer to contiguous data on the buffer-list
      return p;
    }

    std::vector<int> vec;
    // otherwise - copy the data to the @data_buff
    char *p_buff = data_buff;
    do {
      vec.push_back(n);
      std::memcpy(p_buff, p, n);
      p_buff += n;
      len -= n;
      if (len > 0) {
        n = bl_itr.get_ptr_and_advance(len, &p);
      }
    } while (len > 0);

    ldpp_dout(dpp, 20) << __func__ << "::vec=" << vec << dendl;
    return data_buff;
  }

  static const char* s_urgent_msg_names[] = {
    "URGENT_MSG_NONE",
    "URGENT_MSG_ABORT",
    "URGENT_MSG_PASUE",
    "URGENT_MSG_RESUME",
    "URGENT_MSG_RESTART",
    "URGENT_MSG_INVALID"
  };

  //---------------------------------------------------------------------------
  const char* get_urgent_msg_names(int msg)
  {
    if (msg <= URGENT_MSG_INVALID && msg >= URGENT_MSG_NONE) {
      return s_urgent_msg_names[msg];
    }
    else {
      return s_urgent_msg_names[URGENT_MSG_INVALID];
    }
  }

  //---------------------------------------------------------------------------
  worker_stats_t& worker_stats_t::operator+=(const worker_stats_t& other)
  {
    this->ingress_obj += other.ingress_obj;
    this->ingress_obj_bytes += other.ingress_obj_bytes;
    this->egress_records += other.egress_records;
    this->egress_blocks += other.egress_blocks;
    this->egress_slabs += other.egress_slabs;
    this->single_part_objs += other.single_part_objs;
    this->multipart_objs += other.multipart_objs;
    this->small_multipart_obj += other.small_multipart_obj;
    this->default_storage_class_objs += other.default_storage_class_objs;
    this->default_storage_class_objs_bytes += other.default_storage_class_objs_bytes;
    this->non_default_storage_class_objs += other.non_default_storage_class_objs;
    this->non_default_storage_class_objs_bytes += other.non_default_storage_class_objs_bytes;
    this->ingress_corrupted_etag += other.ingress_corrupted_etag;
    this->ingress_skip_too_small_bytes += other.ingress_skip_too_small_bytes;
    this->ingress_skip_too_small += other.ingress_skip_too_small;
    this->ingress_skip_too_small_64KB_bytes += other.ingress_skip_too_small_64KB_bytes;
    this->ingress_skip_too_small_64KB += other.ingress_skip_too_small_64KB;

    return *this;
  }
  //---------------------------------------------------------------------------
  void worker_stats_t::dump(Formatter *f) const
  {
    // main section
    {
      Formatter::ObjectSection main(*f, "main");

      f->dump_unsigned("Ingress Objs count", this->ingress_obj);
      f->dump_unsigned("Accum byte size Ingress Objs", this->ingress_obj_bytes);
      f->dump_unsigned("Egress Records count", this->egress_records);
      f->dump_unsigned("Egress Blocks count", this->egress_blocks);
      f->dump_unsigned("Egress Slabs count", this->egress_slabs);
      f->dump_unsigned("Single part obj count", this->single_part_objs);
      f->dump_unsigned("Multipart obj count", this->multipart_objs);
      if (this->small_multipart_obj) {
        f->dump_unsigned("Small Multipart obj count", this->small_multipart_obj);
      }
    }

    {
      Formatter::ObjectSection notify(*f, "notify");

      if(this->non_default_storage_class_objs) {
        f->dump_unsigned("non default storage class objs",
                         this->non_default_storage_class_objs);
        f->dump_unsigned("non default storage class objs bytes",
                         this->non_default_storage_class_objs_bytes);
      }
      else {
        ceph_assert(this->default_storage_class_objs == this->ingress_obj);
        ceph_assert(this->default_storage_class_objs_bytes == this->ingress_obj_bytes);
      }
    }

    {
      Formatter::ObjectSection skipped(*f, "skipped");
      if(this->ingress_skip_too_small) {
        f->dump_unsigned("Ingress skip: too small objs",
                         this->ingress_skip_too_small);
        f->dump_unsigned("Ingress skip: too small bytes",
                         this->ingress_skip_too_small_bytes);

        if(this->ingress_skip_too_small_64KB) {
          f->dump_unsigned("Ingress skip: 64KB<=size<=4MB Obj",
                           this->ingress_skip_too_small_64KB);
          f->dump_unsigned("Ingress skip: 64KB<=size<=4MB Bytes",
                           this->ingress_skip_too_small_64KB_bytes);
        }
      }
    }

    {
      Formatter::ObjectSection failed(*f, "failed");
      if(this->ingress_corrupted_etag) {
        f->dump_unsigned("Corrupted ETAG", this->ingress_corrupted_etag);
      }
    }
  }

  //---------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream &out, const worker_stats_t &s)
  {
    JSONFormatter formatter(false);
    s.dump(&formatter);
    std::stringstream sstream;
    formatter.flush(sstream);
    out << sstream.str();
    return out;
  }

  //---------------------------------------------------------------------------
  void encode(const worker_stats_t& w, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(w.ingress_obj, bl);
    encode(w.ingress_obj_bytes, bl);
    encode(w.egress_records, bl);
    encode(w.egress_blocks, bl);
    encode(w.egress_slabs, bl);

    encode(w.single_part_objs, bl);
    encode(w.multipart_objs, bl);
    encode(w.small_multipart_obj, bl);

    encode(w.default_storage_class_objs, bl);
    encode(w.default_storage_class_objs_bytes, bl);
    encode(w.non_default_storage_class_objs, bl);
    encode(w.non_default_storage_class_objs_bytes, bl);

    encode(w.ingress_corrupted_etag, bl);

    encode(w.ingress_skip_too_small_bytes, bl);
    encode(w.ingress_skip_too_small, bl);

    encode(w.ingress_skip_too_small_64KB_bytes, bl);
    encode(w.ingress_skip_too_small_64KB, bl);

    encode(w.duration, bl);
    ENCODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  void decode(worker_stats_t& w, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(w.ingress_obj, bl);
    decode(w.ingress_obj_bytes, bl);
    decode(w.egress_records, bl);
    decode(w.egress_blocks, bl);
    decode(w.egress_slabs, bl);
    decode(w.single_part_objs, bl);
    decode(w.multipart_objs, bl);
    decode(w.small_multipart_obj, bl);
    decode(w.default_storage_class_objs, bl);
    decode(w.default_storage_class_objs_bytes, bl);
    decode(w.non_default_storage_class_objs, bl);
    decode(w.non_default_storage_class_objs_bytes, bl);
    decode(w.ingress_corrupted_etag, bl);
    decode(w.ingress_skip_too_small_bytes, bl);
    decode(w.ingress_skip_too_small, bl);
    decode(w.ingress_skip_too_small_64KB_bytes, bl);
    decode(w.ingress_skip_too_small_64KB, bl);

    decode(w.duration, bl);
    DECODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  md5_stats_t& md5_stats_t::operator+=(const md5_stats_t& other)
  {
    this->small_objs_stat               += other.small_objs_stat;
    this->big_objs_stat                 += other.big_objs_stat;
    this->ingress_failed_load_bucket    += other.ingress_failed_load_bucket;
    this->ingress_failed_get_object     += other.ingress_failed_get_object;
    this->ingress_failed_get_obj_attrs  += other.ingress_failed_get_obj_attrs;
    this->ingress_corrupted_etag        += other.ingress_corrupted_etag;
    this->ingress_corrupted_obj_attrs   += other.ingress_corrupted_obj_attrs;
    this->ingress_skip_encrypted        += other.ingress_skip_encrypted;
    this->ingress_skip_encrypted_bytes  += other.ingress_skip_encrypted_bytes;
    this->ingress_skip_compressed       += other.ingress_skip_compressed;
    this->ingress_skip_compressed_bytes += other.ingress_skip_compressed_bytes;
    this->ingress_skip_changed_objs     += other.ingress_skip_changed_objs;
    this->shared_manifest_dedup_bytes   += other.shared_manifest_dedup_bytes;

    this->skipped_shared_manifest += other.skipped_shared_manifest;
    this->skipped_purged_small    += other.skipped_purged_small;
    this->skipped_singleton       += other.skipped_singleton;
    this->skipped_singleton_bytes += other.skipped_singleton_bytes;
    this->skipped_source_record   += other.skipped_source_record;
    this->duplicate_records       += other.duplicate_records;
    this->size_mismatch           += other.size_mismatch;
    this->sha256_mismatch         += other.sha256_mismatch;
    this->failed_src_load         += other.failed_src_load;
    this->failed_rec_load         += other.failed_rec_load;
    this->failed_block_load       += other.failed_block_load;

    this->valid_sha256_attrs      += other.valid_sha256_attrs;
    this->invalid_sha256_attrs    += other.invalid_sha256_attrs;
    this->set_sha256_attrs        += other.set_sha256_attrs;
    this->skip_sha256_cmp         += other.skip_sha256_cmp;

    this->set_shared_manifest_src += other.set_shared_manifest_src;
    this->loaded_objects          += other.loaded_objects;
    this->processed_objects       += other.processed_objects;
    this->dup_head_bytes_estimate += other.dup_head_bytes_estimate;
    this->deduped_objects         += other.deduped_objects;
    this->deduped_objects_bytes   += other.deduped_objects_bytes;
    this->dup_head_bytes          += other.dup_head_bytes;

    this->failed_dedup            += other.failed_dedup;
    this->failed_table_load       += other.failed_table_load;
    this->failed_map_overflow     += other.failed_map_overflow;
    return *this;
  }

  //---------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream &out, const md5_stats_t &s)
  {
    JSONFormatter formatter(false);
    s.dump(&formatter);
    std::stringstream sstream;
    formatter.flush(sstream);
    out << sstream.str();
    return out;
  }

  //---------------------------------------------------------------------------
  void md5_stats_t::dump(Formatter *f) const
  {
    // main section
    {
      Formatter::ObjectSection main(*f, "main");

      f->dump_unsigned("Total processed objects", this->processed_objects);
      f->dump_unsigned("Loaded objects", this->loaded_objects);
      f->dump_unsigned("Set Shared-Manifest SRC", this->set_shared_manifest_src);
      f->dump_unsigned("Deduped Obj (this cycle)", this->deduped_objects);
      f->dump_unsigned("Deduped Bytes(this cycle)", this->deduped_objects_bytes);
      f->dump_unsigned("Dup head bytes (not dedup)", this->dup_head_bytes);
      f->dump_unsigned("Already Deduped bytes (prev cycles)",
                       this->shared_manifest_dedup_bytes);

      const dedup_stats_t &ds = this->big_objs_stat;
      f->dump_unsigned("Singleton Obj", ds.singleton_count);
      f->dump_unsigned("Unique Obj", ds.unique_count);
      f->dump_unsigned("Duplicate Obj", ds.duplicate_count);
      f->dump_unsigned("Dedup Bytes Estimate", ds.dedup_bytes_estimate);
    }

    // Potential Dedup Section:
    // What could be gained by allowing dedup for smaller objects (64KB-4MB)
    // Space wasted because of duplicated head-object (4MB)
    {
      Formatter::ObjectSection potential(*f, "Potential Dedup");
      const dedup_stats_t &ds = this->small_objs_stat;
      f->dump_unsigned("Singleton Obj (64KB-4MB)", ds.singleton_count);
      f->dump_unsigned("Unique Obj (64KB-4MB)", ds.unique_count);
      f->dump_unsigned("Duplicate Obj (64KB-4MB)", ds.duplicate_count);
      f->dump_unsigned("Dedup Bytes Estimate (64KB-4MB)", ds.dedup_bytes_estimate);
      f->dump_unsigned("Duplicated Head Bytes Estimate",
                       this->dup_head_bytes_estimate);
      f->dump_unsigned("Duplicated Head Bytes", this->dup_head_bytes);
    }

    {
      Formatter::ObjectSection notify(*f, "notify");
      if (this->failed_table_load) {
        f->dump_unsigned("Failed Table Load", this->failed_table_load);
      }
      if (this->failed_map_overflow) {
        f->dump_unsigned("Failed Remap Overflow", this->failed_map_overflow);
      }

      f->dump_unsigned("Valid SHA256 attrs", this->valid_sha256_attrs);
      f->dump_unsigned("Invalid SHA256 attrs", this->invalid_sha256_attrs);

      if (this->set_sha256_attrs) {
        f->dump_unsigned("Set SHA256", this->set_sha256_attrs);
      }

      if (this->skip_sha256_cmp) {
        f->dump_unsigned("Can't run SHA256 compare", this->skip_sha256_cmp);
      }
    }

    {
      Formatter::ObjectSection skipped(*f, "skipped");
      f->dump_unsigned("Skipped shared_manifest", this->skipped_shared_manifest);
      f->dump_unsigned("Skipped purged small objs", this->skipped_purged_small);
      f->dump_unsigned("Skipped singleton objs", this->skipped_singleton);
      if (this->skipped_singleton) {
        f->dump_unsigned("Skipped singleton Bytes", this->skipped_singleton_bytes);
      }
      f->dump_unsigned("Skipped source record", this->skipped_source_record);

      if (this->ingress_skip_encrypted) {
        f->dump_unsigned("Skipped Encrypted objs", this->ingress_skip_encrypted);
        f->dump_unsigned("Skipped Encrypted Bytes",this->ingress_skip_encrypted_bytes);
      }
      if (this->ingress_skip_compressed) {
        f->dump_unsigned("Skipped Compressed objs", this->ingress_skip_compressed);
        f->dump_unsigned("Skipped Compressed Bytes", this->ingress_skip_compressed_bytes);
      }
      if (this->ingress_skip_changed_objs) {
        f->dump_unsigned("Skipped Changed Object", this->ingress_skip_changed_objs);
      }
    }

    {
      Formatter::ObjectSection sys_failures(*f, "system failures");
      if (this->ingress_failed_load_bucket) {
        f->dump_unsigned("Failed load_bucket()", this->ingress_failed_load_bucket);
      }
      if (this->ingress_failed_get_object) {
        f->dump_unsigned("Failed get_object()", this->ingress_failed_get_object);
      }
      if (this->ingress_failed_get_obj_attrs) {
        f->dump_unsigned("Failed get_obj_attrs", this->ingress_failed_get_obj_attrs);
      }
      if (this->ingress_corrupted_etag) {
        f->dump_unsigned("Corrupted ETAG", this->ingress_corrupted_etag);
      }
      if (this->ingress_corrupted_obj_attrs) {
        f->dump_unsigned("Corrupted obj attributes", this->ingress_corrupted_obj_attrs);
      }
      if (this->failed_src_load) {
        f->dump_unsigned("Failed SRC-Load ", this->failed_src_load);
      }
      if (this->failed_rec_load) {
        f->dump_unsigned("Failed Record-Load ", this->failed_rec_load);
      }
      if (this->failed_block_load) {
        f->dump_unsigned("Failed Block-Load ", this->failed_block_load);
      }
      if (this->failed_dedup) {
        f->dump_unsigned("Failed Dedup", this->failed_dedup);
      }
    }

    {
      Formatter::ObjectSection logical_failures(*f, "logical failures");
      if (this->sha256_mismatch) {
        f->dump_unsigned("SHA256 mismatch", this->sha256_mismatch);
      }
      if (this->duplicate_records) {
        f->dump_unsigned("Duplicate SRC/TGT", this->duplicate_records);
      }
      if (this->size_mismatch) {
        f->dump_unsigned("Size mismatch SRC/TGT", this->size_mismatch);
      }
    }
  }

  //---------------------------------------------------------------------------
  void encode(const md5_stats_t& m, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);

    encode(m.small_objs_stat, bl);
    encode(m.big_objs_stat, bl);
    encode(m.ingress_failed_load_bucket, bl);
    encode(m.ingress_failed_get_object, bl);
    encode(m.ingress_failed_get_obj_attrs, bl);
    encode(m.ingress_corrupted_etag, bl);
    encode(m.ingress_corrupted_obj_attrs, bl);
    encode(m.ingress_skip_encrypted, bl);
    encode(m.ingress_skip_encrypted_bytes, bl);
    encode(m.ingress_skip_compressed, bl);
    encode(m.ingress_skip_compressed_bytes, bl);
    encode(m.ingress_skip_changed_objs, bl);
    encode(m.shared_manifest_dedup_bytes, bl);

    encode(m.skipped_shared_manifest, bl);
    encode(m.skipped_purged_small, bl);
    encode(m.skipped_singleton, bl);
    encode(m.skipped_singleton_bytes, bl);
    encode(m.skipped_source_record, bl);
    encode(m.duplicate_records, bl);
    encode(m.size_mismatch, bl);
    encode(m.sha256_mismatch, bl);
    encode(m.failed_src_load, bl);
    encode(m.failed_rec_load, bl);
    encode(m.failed_block_load, bl);

    encode(m.valid_sha256_attrs, bl);
    encode(m.invalid_sha256_attrs, bl);
    encode(m.set_sha256_attrs, bl);
    encode(m.skip_sha256_cmp, bl);
    encode(m.set_shared_manifest_src, bl);

    encode(m.loaded_objects, bl);
    encode(m.processed_objects, bl);
    encode(m.dup_head_bytes_estimate, bl);
    encode(m.deduped_objects, bl);
    encode(m.deduped_objects_bytes, bl);
    encode(m.dup_head_bytes, bl);
    encode(m.failed_dedup, bl);
    encode(m.failed_table_load, bl);
    encode(m.failed_map_overflow, bl);

    encode(m.duration, bl);
    ENCODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  void decode(md5_stats_t& m, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(m.small_objs_stat, bl);
    decode(m.big_objs_stat, bl);
    decode(m.ingress_failed_load_bucket, bl);
    decode(m.ingress_failed_get_object, bl);
    decode(m.ingress_failed_get_obj_attrs, bl);
    decode(m.ingress_corrupted_etag, bl);
    decode(m.ingress_corrupted_obj_attrs, bl);
    decode(m.ingress_skip_encrypted, bl);
    decode(m.ingress_skip_encrypted_bytes, bl);
    decode(m.ingress_skip_compressed, bl);
    decode(m.ingress_skip_compressed_bytes, bl);
    decode(m.ingress_skip_changed_objs, bl);
    decode(m.shared_manifest_dedup_bytes, bl);

    decode(m.skipped_shared_manifest, bl);
    decode(m.skipped_purged_small, bl);
    decode(m.skipped_singleton, bl);
    decode(m.skipped_singleton_bytes, bl);
    decode(m.skipped_source_record, bl);
    decode(m.duplicate_records, bl);
    decode(m.size_mismatch, bl);
    decode(m.sha256_mismatch, bl);
    decode(m.failed_src_load, bl);
    decode(m.failed_rec_load, bl);
    decode(m.failed_block_load, bl);

    decode(m.valid_sha256_attrs, bl);
    decode(m.invalid_sha256_attrs, bl);
    decode(m.set_sha256_attrs, bl);
    decode(m.skip_sha256_cmp, bl);
    decode(m.set_shared_manifest_src, bl);

    decode(m.loaded_objects, bl);
    decode(m.processed_objects, bl);
    decode(m.dup_head_bytes_estimate, bl);
    decode(m.deduped_objects, bl);
    decode(m.deduped_objects_bytes, bl);
    decode(m.dup_head_bytes, bl);
    decode(m.failed_dedup, bl);
    decode(m.failed_table_load, bl);
    decode(m.failed_map_overflow, bl);

    decode(m.duration, bl);
    DECODE_FINISH(bl);
  }
} //namespace rgw::dedup
