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
  const char* get_urgent_msg_names(int msg) {
    if (msg <= URGENT_MSG_INVALID && msg >= URGENT_MSG_NONE) {
      return s_urgent_msg_names[msg];
    }
    else {
      return s_urgent_msg_names[URGENT_MSG_INVALID];
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
  void worker_stats_t::dump(Formatter *f) const
  {
    // main section
    {
      Formatter::ObjectSection notify(*f, "main");

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
      Formatter::ObjectSection notify(*f, "main");

      f->dump_unsigned("Total processed objects", this->processed_objects);
      f->dump_unsigned("Loaded objects", this->loaded_objects);
      f->dump_unsigned("Set Shared-Manifest SRC", this->set_shared_manifest_src);
      f->dump_unsigned("Deduped Obj (this cycle)", this->deduped_objects);
      f->dump_unsigned("Deduped Bytes(this cycle)", this->deduped_objects_bytes);
      f->dump_unsigned("Already Deduped bytes (prev cycles)",
                       this->shared_manifest_dedup_bytes);
      f->dump_unsigned("Singleton Obj", this->singleton_count);
      f->dump_unsigned("Unique Obj", this->unique_count);
      f->dump_unsigned("Duplicate Obj", this->duplicate_count);
      f->dump_unsigned("Dedup Bytes Estimate", this->dedup_bytes_estimate);
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
} //namespace rgw::dedup
