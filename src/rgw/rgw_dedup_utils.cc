#include "rgw_dedup_utils.h"
#include "common/ceph_crypto.h"

namespace rgw::dedup {
  // convert a hex-string to a 64bit integer (max 16 hex digits)
  //---------------------------------------------------------------------------
  uint64_t hex2int(const char *p, const char* p_end)
  {
    if (p_end - p <= (int)(sizeof(uint64_t) * 2)) {
      uint64_t val = 0;
      while (p < p_end) {
	// get current character then increment
	uint8_t byte = *p++;
	// transform hex character to the 4bit equivalent number, using the ascii table indexes
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
	  return val;
	}
	// shift 4 to make space for new digit, and add the 4 bits of the new digit
	val = (val << 4) | (byte & 0xF);
      }
      return val;
    }
    else {
      //derr << __func__ << "Value size too big: " << (p_end - p) << dendl;
      return 0;
    }
  }

  //---------------------------------------------------------------------------
  uint16_t dec2int(const char *p, const char* p_end)
  {
    constexpr unsigned max_uint16_digits = 5; // 65536
    if (p_end - p <= max_uint16_digits) {
      uint16_t val = 0;
      while (p < p_end) {
	uint8_t byte = *p++;
	if (byte >= '0' && byte <= '9') {
	  val = val * 10 + (byte - '0');
	}
	else {
	  // terminate on the first non hex char
	  return val;
	}
      }
      return val;
    }
    else {
      //derr << __func__ << "Value size too big: " << (p_end - p) << dendl;
      return 0;
    }
  }

  //---------------------------------------------------------------------------
  uint16_t get_num_parts(const std::string & etag)
  {
    // 16Bytes MD5 takes 32 chars
    if (etag.length() <= 32) {
      // i.e. no multipart
      return 0;
    }
    // Amazon S3 multipart upload Maximum number = 10,000 (5 decimal digits)
    // We need 1 extra byte for the '-' delimiter and 1 extra byte for '"' at the end
    // 7 Bytes should suffice, but we roundup to 8 Bytes
    constexpr unsigned max_part_len = 8;
    std::string::size_type n = etag.find('-', etag.length() - max_part_len);
    if (n != std::string::npos) {
      // again, 1 extra byte for the '-' delimiter
      unsigned copy_size = etag.length() - (n + 1);
      char buff[copy_size+1];
      unsigned nbytes = etag.copy(buff, copy_size, n+1);
      uint64_t num_parts = dec2int(buff, buff+nbytes);
      return num_parts;
    }
    else {
      //derr << "Bad MD5=" << etag << dendl;
      return -1;
    }
  }

  //---------------------------------------------------------------------------
  void parse_etag_string(const std::string& etag, parsed_etag_t *parsed_etag)
  {
    char buff[64];
    const uint16_t num_parts = get_num_parts(etag);
    etag.copy(buff, 32, 0);
    const uint64_t high      = hex2int(buff, buff+16);
    const uint64_t low       = hex2int(buff+16, buff+32);

    parsed_etag->md5_high  = high;      // High Bytes of the Object Data MD5
    parsed_etag->md5_low   = low;       // Low  Bytes of the Object Data MD5
    parsed_etag->num_parts = num_parts; // How many parts were used in multipart upload
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
  void sha256_to_bufferlist(uint64_t sha256a,
			    uint64_t sha256b,
			    uint64_t sha256c,
			    uint64_t sha256d,
			    ceph::bufferlist *bl)
  {
    // add one extra byte for the null termination
    char buff[64+1];
    snprintf(buff, sizeof(buff), "%016lx%016lx%016lx%016lx", sha256a, sha256b, sha256c, sha256d);
    // append the hex string including the null termination
    bl->append(buff, sizeof(buff));
  }

  //---------------------------------------------------------------------------
  std::string calc_refcount_tag_hash(const std::string &bucket_name, const std::string &obj_name)
  {
    bufferlist bl;
    bl.append(bucket_name);
    bl.append(obj_name);
    return crypto::digest<crypto::SHA1>(bl).to_str();
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
    out << "Ingress Objs count             = " << s.ingress_obj << "\n";
    out << "Accum byte size Ingress Objs   = " << s.ingress_obj_bytes << "\n";
    out << "Egress  Records count          = " << s.egress_records << "\n";
    out << "Egress  Blocks count           = " << s.egress_blocks << "\n";
    out << "Egress  Slabs count            = " << s.egress_slabs << "\n";

    out << "Single part obj count          = " << s.single_part_objs << "\n";
    out << "Multipart obj count            = " << s.multipart_objs << "\n";
    if (s.small_multipart_obj) {
      out << "Small Multipart obj count      = " << s.small_multipart_obj << "\n";
    }
#if 1
    if(s.non_default_storage_class_objs) {
      out << "non_default_storage_class_objs = " << s.non_default_storage_class_objs << "\n";
      out << "non_default_storage_class_objs_bytes = " << s.non_default_storage_class_objs_bytes << "\n";
    }
    else {
      ceph_assert(s.default_storage_class_objs == s.ingress_obj);
      ceph_assert(s.default_storage_class_objs_bytes == s.ingress_obj_bytes);
    }
#endif
    if(s.ingress_failed_get_object) {
      out << "Ingress failed get_object()    = "
	  << s.ingress_failed_get_object << "\n";
    }
    if(s.ingress_failed_get_obj_attrs) {
      out << "Ingress failed get_obj_attrs() = "
	  << s.ingress_failed_get_obj_attrs << "\n";
    }
    if(s.ingress_skip_too_small) {
      out << "Ingress skip: too small objs   = "
	  << s.ingress_skip_too_small << "\n";
      out << "Ingress skip: too small bytes  = "
	  << s.ingress_skip_too_small_bytes << "\n";

      if(s.ingress_skip_too_small_64KB) {
	out << "Ingress skip: 64KB<=size<=4MB Obj   = "
	    << s.ingress_skip_too_small_64KB << "\n";
	out << "Ingress skip: 64KB<=size<=4MB Bytes =  "
	    << s.ingress_skip_too_small_64KB_bytes << "\n";
      }
    }
    return out;
  }

  //---------------------------------------------------------------------------
  std::ostream& operator<<(std::ostream &out, const md5_stats_t &s)
  {
    out << "Total processed objects  = " << s.processed_objects << "\n";
    out << "Loaded objects           = " << s.loaded_objects << "\n";
    out << "Valid SHA256 attrs       = " << s.valid_sha256_attrs << "\n";
    out << "inValid SHA256 attrs     = " << s.invalid_sha256_attrs << "\n";

    out << "Skipped shared_manifest  = " << s.skipped_shared_manifest << "\n";
    out << "Skipped singleton objs   = " << s.skipped_singleton << "\n";
    if (s.skipped_singleton) {
      out << "Skipped singleton Bytes  = " << s.skipped_singleton_bytes << "\n";
    }
    out << "Skipped source record    = " << s.skipped_source_record << "\n";

    if(s.ingress_skip_encrypted) {
      out << "Skipped Encrypted objs = " << s.ingress_skip_encrypted << "\n";
      out << "Skipped Encrypted Bytes= " << s.ingress_skip_encrypted_bytes << "\n";
    }
    if(s.ingress_skip_compressed) {
      out << "Skipped Compressed objs = " << s.ingress_skip_compressed << "\n";
      out << "Skipped Compressed Bytes= " << s.ingress_skip_compressed_bytes << "\n";
    }
    if(s.ingress_skip_changed_objs) {
      out << "Skipped Changed Object = " << s.ingress_skip_changed_objs << "\n";
    }

    if(s.ingress_failed_get_object) {
      out << "Failed Get Object      = " << s.ingress_failed_get_object << "\n";
    }

    if(s.ingress_failed_get_obj_attrs) {
      out << "Failed Get Object ATTR = " << s.ingress_failed_get_obj_attrs << "\n";
    }

    if (s.skipped_duplicate) {
      out << "\n***ERR:Skipped duplicate = " << s.skipped_duplicate << "***\n";
    }

    if (s.skipped_bad_sha256) {
      out << "\n***ERR:Skipped SHA256 = " << s.skipped_bad_sha256 << "***\n";
    }
    if (s.skipped_failed_src_load) {
      out << "\n***ERR:Skipped SRC-Load = " << s.skipped_failed_src_load << "***\n";
    }
    out << "================================\n";
    out << "Skipped total            = " << s.get_skipped_total() << "\n\n";

    if (s.skip_sha256_cmp) {
      out << "Can't run SHA256 compare = " << s.skip_sha256_cmp << "\n";
    }
    if (s.failed_dedup) {
      out << "\nFailed Dedup count     = " << s.failed_dedup << "\n";
    }

    out << "Set Shared-Manifest      = " << s.set_shared_manifest << "\n";
    out << "Deduped Obj (this cycle) = " << s.deduped_objects << "\n";
    out << "Deduped Bytes(this cycle)= " << s.deduped_objects_bytes << "\n";
    out << "Singleton Obj            = " << s.singleton_count << "\n";
    out << "Unique Obj               = " << s.unique_count << "\n";
    out << "Duplicate Obj            = " << s.duplicate_count << "\n";
    out << "Duplicate Blocks Bytes   = " << s.duplicated_blocks_bytes << "\n";

    return out;
  }
} //namespace rgw::dedup
