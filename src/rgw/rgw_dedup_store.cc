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
static constexpr auto dout_subsys = ceph_subsys_rgw;

namespace rgw::dedup {

  rgw_pool pool(DEDUP_POOL_NAME);
  //---------------------------------------------------------------------------
  disk_record_t::disk_record_t(const char *buff)
  {
    disk_record_t *p_rec = (disk_record_t*)buff;
    ceph_assert(p_rec->s.pad8  == 0);
    this->s.md5_high        = CEPHTOH_64(p_rec->s.md5_high);
    this->s.md5_low         = CEPHTOH_64(p_rec->s.md5_low);
    this->s.obj_bytes_size  = CEPHTOH_64(p_rec->s.obj_bytes_size);
    this->s.version         = CEPHTOH_64(p_rec->s.version);

    this->s.flags           = p_rec->s.flags;
    this->s.pad8            = 0;
    this->s.num_parts       = CEPHTOH_16(p_rec->s.num_parts);

    this->s.obj_name_len    = CEPHTOH_16(p_rec->s.obj_name_len);
    this->s.bucket_name_len = CEPHTOH_16(p_rec->s.bucket_name_len);
    this->s.bucket_id_len   = CEPHTOH_16(p_rec->s.bucket_id_len);
    this->s.tenant_name_len = CEPHTOH_16(p_rec->s.tenant_name_len);
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

      manifest_bl.append(p, this->s.manifest_len);
    }
  }

  //---------------------------------------------------------------------------
  size_t disk_record_t::serialize(char *buff) const
  {
    ceph_assert(this->s.pad8  == 0);
    disk_record_t *p_rec = (disk_record_t*)buff;
    p_rec->s.md5_high        = HTOCEPH_64(this->s.md5_high);
    p_rec->s.md5_low         = HTOCEPH_64(this->s.md5_low);
    p_rec->s.obj_bytes_size  = HTOCEPH_64(this->s.obj_bytes_size);
    p_rec->s.version         = HTOCEPH_64(this->s.version);

    p_rec->s.flags           = this->s.flags;
    p_rec->s.pad8            = 0;
    p_rec->s.num_parts       = HTOCEPH_16(this->s.num_parts);

    p_rec->s.obj_name_len    = HTOCEPH_16(this->obj_name.length());
    p_rec->s.bucket_name_len = HTOCEPH_16(this->bucket_name.length());
    p_rec->s.bucket_id_len   = HTOCEPH_16(this->bucket_id.length());
    p_rec->s.tenant_name_len = HTOCEPH_16(this->tenant_name.length());
    p_rec->s.ref_tag_len     = HTOCEPH_16(this->ref_tag.length());
    p_rec->s.manifest_len    = HTOCEPH_16(this->manifest_bl.length());

    p_rec->s.shared_manifest = HTOCEPH_64(this->s.shared_manifest);
    for (int i = 0; i < 4; i++) {
      p_rec->s.sha256[i] = HTOCEPH_64(this->s.sha256[i]);
    }

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

    if (this->s.flags.is_fastlane()) {
      // TBD:: remove asserts
      ceph_assert(this->s.ref_tag_len == 0);
      ceph_assert(this->s.manifest_len == 0);
    }
    else {
      len = this->ref_tag.length();
      std::memcpy(p, this->ref_tag.data(), len);
      p += len;

      const std::string & manifest = this->manifest_bl.to_str();
      len = manifest.length();
      std::memcpy(p, manifest.data(), len);
      p += len;
    }
    return (p - buff);
  }

  //---------------------------------------------------------------------------
  bool disk_record_t::operator==(const disk_record_t &other) const
  {
    unsigned len = offsetof(packed_rec_t, obj_name_len);
    return (this->obj_name    == other.obj_name    &&
	    this->bucket_name == other.bucket_name &&
	    this->bucket_id   == other.bucket_id &&
	    this->tenant_name == other.tenant_name &&
	    this->ref_tag     == other.ref_tag &&
	    this->manifest_bl == other.manifest_bl &&
	    memcmp((char*)&this->s.flags, (char*)&other.s.flags, len) == 0);
  }

  //---------------------------------------------------------------------------
  size_t disk_record_t::length() const
  {
    return (sizeof(this->s) +
	    this->obj_name.length() +
	    this->bucket_name.length() +
	    this->bucket_id.length() +
	    this->tenant_name.length() +
	    this->ref_tag.length() +
	    this->manifest_bl.length());
  }

  //---------------------------------------------------------------------------
  std::ostream &operator<<(std::ostream &stream, const disk_record_t & rec)
  {
    stream << rec.obj_name << "::" << rec.s.obj_name_len << "\n";
    stream << rec.bucket_name << "::" << rec.s.bucket_name_len << "\n";
    stream << rec.bucket_id << "::" << rec.s.bucket_id_len << "\n";
    stream << rec.tenant_name << "::" << rec.s.tenant_name_len << "\n";
    stream << rec.ref_tag << "::" << rec.s.ref_tag_len << "\n";
    stream << "num_parts = " << rec.s.num_parts << "\n";
    stream << "obj_size  = " << rec.s.obj_bytes_size/1024 <<" KiB"  << "\n";
    stream << "MD5       = " << std::hex << rec.s.md5_high << rec.s.md5_low << "\n";
    if (rec.has_valid_sha256()) {
      stream << "SHA256    = ";
      for (int i =0; i < 4; i++) {
	stream << rec.s.sha256[i];
      }
      stream << "\n";
    }
    else {
      stream << "SHA256 is undefined\n";
    }

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
      return -2;
    }

    if (unlikely(rec_count > MAX_REC_IN_BLOCK) ) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR::rec_count=" << rec_count << " > MAX_REC_IN_BLOCK" << dendl;
      return -2;
    }

    if (unlikely(this->block_id != expected_block_id)) {
      ldpp_dout(dpp, 1) << __func__ << "::ERR::block_id=" << block_id
			<< "!= expected_block_id=" << expected_block_id << dendl;
      return -3;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  record_id_t disk_block_t::add_record(const disk_record_t *p_rec, const DoutPrefixProvider *dpp)
  {
    disk_block_header_t *p_header = get_header();
    if (unlikely(p_header->rec_count >= MAX_REC_IN_BLOCK)) {
      ldpp_dout(dpp, 1)  << __func__ << "::rec_count=" << p_header->rec_count
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
      //ldpp_dout(dpp, 1) << i << "] offset = " << p_header->rec_offsets[i] << dendl;
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
  int disk_block_array_t::fill_disk_record(disk_record_t          *p_rec,
					   const rgw::sal::Bucket *p_bucket,
					   const rgw::sal::Object *p_obj,
					   const parsed_etag_t    *p_parsed_etag,
					   const std::string      &obj_name,
					   uint64_t                obj_size)
  {
    p_rec->s.md5_high        = p_parsed_etag->md5_high;
    p_rec->s.md5_low         = p_parsed_etag->md5_low;
    p_rec->s.obj_bytes_size  = obj_size;
    p_rec->s.version         = 0;

    p_rec->s.flags           = 0;
    p_rec->s.pad8            = 0;
    p_rec->s.num_parts       = p_parsed_etag->num_parts;

    p_rec->obj_name          = obj_name;
    p_rec->s.obj_name_len    = obj_name.length();
    p_rec->bucket_name       = p_bucket->get_name();
    p_rec->s.bucket_name_len = p_rec->bucket_name.length();
    p_rec->bucket_id         = p_bucket->get_bucket_id();
    p_rec->s.bucket_id_len   = p_rec->bucket_id.length();
    p_rec->tenant_name       = p_bucket->get_tenant();
    p_rec->s.tenant_name_len = p_rec->tenant_name.length();

    if (p_obj == nullptr) {
      // First pass using only ETAG and size taken from bucket-index
      p_rec->s.flags.set_fastlane();
      p_rec->ref_tag = "";
      p_rec->s.ref_tag_len  = 0;
      p_rec->manifest_bl.clear();
      p_rec->s.manifest_len = 0;
      memset(p_rec->s.sha256, 0, sizeof(p_rec->s.sha256));
      memset(&p_rec->s.shared_manifest, 0, sizeof(p_rec->s.shared_manifest));
      return 0;
    }

    const rgw::sal::Attrs& attrs = p_obj->get_attrs();

    // if TAIL_TAG exists -> use it as ref-tag, eitherwise take ID_TAG
    auto itr = attrs.find(RGW_ATTR_TAIL_TAG);
    if (itr != attrs.end()) {
      p_rec->ref_tag = itr->second.to_str();
    }
    else {
      itr = attrs.find(RGW_ATTR_ID_TAG);
      if (itr != attrs.end()) {
	p_rec->ref_tag = itr->second.to_str();
      }
      else {
	ldpp_dout(dpp, 1) << __func__ << "::No TAIL_TAG and no ID_TAG" << dendl;
	return -1;
      }
    }
    p_rec->s.ref_tag_len = p_rec->ref_tag.length();

    // clear bufferlist first
    p_rec->manifest_bl.clear();
    ceph_assert(p_rec->manifest_bl.length() == 0);

    itr = attrs.find(RGW_ATTR_MANIFEST);
    if (itr != attrs.end()) {
      const bufferlist &bl = itr->second;
      RGWObjManifest manifest;
      try {
	auto bl_iter = bl.cbegin();
	decode(manifest, bl_iter);
      } catch (buffer::error& err) {
	ldpp_dout(dpp, 1)  << __func__
			   << "::ERROR: unable to decode manifest" << dendl;
	return -1;
      }

      // force explicit tail_placement as the dedup could be on another bucket
      const rgw_bucket_placement& tail_placement = manifest.get_tail_placement();
      if (tail_placement.bucket.name.empty()) {
	ldpp_dout(dpp, 10) << "dedup::updating tail placement" << dendl;
	rgw_bucket b{p_rec->tenant_name, p_rec->bucket_name, p_rec->bucket_id};
	manifest.set_tail_placement(tail_placement.placement_rule, b);
	encode(manifest, p_rec->manifest_bl);
      }
      else {
	p_rec->manifest_bl = bl;
      }
      p_rec->s.manifest_len = p_rec->manifest_bl.length();
    }
    else {
      ldpp_dout(dpp, 1)  << __func__ << "::ERROR: no manifest" << dendl;
      return -1;
    }

#if 0
    // optional attributes:
    itr = attrs.find(RGW_ATTR_PG_VER);
    if (itr != attrs.end() && itr->second.length() > 0) {
      uint64_t pg_ver = 0;
      try {
	bufferlist bl = itr->second;
	auto bl_iter = bl.cbegin();
	decode(pg_ver, bl_iter);
	ldpp_dout(dpp, 1)  << __func__ << "::pg_ver=" << bl.to_str() << dendl;;
	p_rec->s.version = pg_ver;
	p_rec->s.flags |= RGW_DEDUP_FLAG_PG_VER;
      } catch (buffer::error& err) {
	ldpp_dout(dpp, 1)  << __func__
			   << "::ERROR: no failed to decode pg ver" << dendl;
      }
    }
#endif

    itr = attrs.find(RGW_ATTR_SHA256);
    if (itr != attrs.end()) {
      char buff[4*HEX_UNIT_SIZE];
      const std::string sha256 = itr->second.to_str();
      sha256.copy(buff, sizeof(buff), 0);
      unsigned idx = 0;
      for (const char *p = buff; p < buff+sizeof(buff); p += HEX_UNIT_SIZE) {
	p_rec->s.sha256[idx++] = hex2int(p, p+HEX_UNIT_SIZE);
      }
      p_rec->s.flags.set_valid_sha256();
      p_stats->valid_sha256++;
#if 0
      bufferlist sha_bl;
      sha256_to_bufferlist(p_rec->s.sha256[0], p_rec->s.sha256[1],
			   p_rec->s.sha256[2], p_rec->s.sha256[3], &sha_bl);
      if (sha256 == sha_bl.to_str()) {
	ldpp_dout(dpp, 1)  << __func__ << "::Valid SHA256!!" << dendl;
      }
      else {
	ldpp_dout(dpp, 1)  << __func__ << "::>>Invalid SHA256<<" << dendl;
	ldpp_dout(dpp, 1)  << "SHA256(A):|:" << sha256 << ":|:" << dendl;
	ldpp_dout(dpp, 1)  << "SHA256(B):|:" << sha_bl.to_str() << ":|:" << dendl;
      }
#endif
    }
    else {
      p_stats->invalid_sha256++;
      memset(p_rec->s.sha256, 0, sizeof(p_rec->s.sha256));
    }

    itr = attrs.find(RGW_ATTR_SHARE_MANIFEST);
    if (itr != attrs.end()) {
      char buff[HEX_UNIT_SIZE];
      const std::string shared_manifest = itr->second.to_str();
      shared_manifest.copy(buff, sizeof(buff), 0);
      p_rec->s.shared_manifest = hex2int(buff, buff+HEX_UNIT_SIZE);
      p_rec->s.flags.set_shared_manifest();
    }
    else {
      memset(&p_rec->s.shared_manifest, 0, sizeof(p_rec->s.shared_manifest));
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  static int print_manifest(const DoutPrefixProvider *dpp,
			    RGWRados                 *rados,
			    const bufferlist         &manifest_bl)
  {
    RGWObjManifest manifest;
    try {
      auto bl_iter = manifest_bl.cbegin();
      decode(manifest, bl_iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1)  << __func__ << "::ERROR: unable to decode manifest" << dendl;
      return -1;
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
  [[maybe_unused]] static void display_record(const DoutPrefixProvider *dpp,
					      RGWRados                 *rados,
					      const disk_record_t      &rec)
  {
    char buff[DISK_BLOCK_SIZE];
    ldpp_dout(dpp, 1)  << __func__ << "::Disk Record:\n" << rec << dendl;
    print_manifest(dpp, rados, rec.manifest_bl);

    ldpp_dout(dpp, 1)  << __func__ << "::Disk Record length = " << rec.length() << dendl;
    int n = rec.serialize(buff);
    ldpp_dout(dpp, 1)  << __func__ << "::Disk Record serialized length = " << n << dendl;
    disk_record_t rec2(buff);
    ldpp_dout(dpp, 1)  << __func__ << "::Disk Record2:\n" << rec2 << dendl;
    print_manifest(dpp, rados, rec2.manifest_bl);
    ldpp_dout(dpp, 1)  << __func__ << "::Disk Record length = " << rec2.length() << dendl;
    n = rec2.serialize(buff);
    ldpp_dout(dpp, 1)  << __func__ << "::Disk Record serialized length = " << n << dendl;

    //if (std::memcmp(&rec, &rec2, rec2.length()) == 0) {}
    if (rec == rec2) {
      ldpp_dout(dpp, 1)  << __func__ << "::Disk Records match!" << dendl;
    }
    else {
      ldpp_dout(dpp, 1)  << __func__ << "::Disk Records mismatch!" << dendl;
    }
    ldpp_dout(dpp, 1)  << __func__ << "::manifest.length()=" << rec.s.manifest_len << dendl;
    ldpp_dout(dpp, 1)  << __func__ << "::sizeof(packed_rec_t)=" << sizeof(rec.s) << dendl;
  }

  //---------------------------------------------------------------------------
  ostream& operator<<(std::ostream& out, const disk_block_id_t& block_id)
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
    const char *SLAB_NAME_FORMAT = "SLAB.%02X.%02X.%04X";
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
  int load_record(librados::IoCtx          *p_ioctx,
		  disk_record_t            *p_rec, /* OUT */
		  disk_block_id_t           block_id,
		  record_id_t               rec_id,
		  md5_shard_t               md5_shard,
		  const struct key_t       *p_key,
		  const DoutPrefixProvider *dpp)
  {
    std::string oid(block_id.get_slab_name(md5_shard));
    //p_obj_ioctx->set_namespace(itr->get_nspace());
    int read_len = DISK_BLOCK_SIZE;
    int byte_offset = block_id.get_block_offset() * DISK_BLOCK_SIZE;
    bufferlist bl;
    int ret = p_ioctx->read(oid, bl, read_len, byte_offset);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "ERR: failed to read block from "
			<< oid << ", error is " << cpp_strerror(ret) << dendl;
      return ret;
    }

    const char *p = nullptr;
    auto bl_itr = bl.cbegin();
    size_t n = bl_itr.get_ptr_and_advance(sizeof(disk_block_t), &p);
    if (n == sizeof(disk_block_t)) {
      disk_block_t *p_disk_block = (disk_block_t*)p;
      disk_block_header_t *p_header = p_disk_block->get_header();
      p_header->deserialize();
      if (p_header->verify(block_id, dpp) != 0) {
	return -1;
      }

      unsigned offset = p_header->rec_offsets[rec_id];
      // We deserialize the record inside the CTOR
      disk_record_t rec(p + offset);
      uint32_t size_4k_units = byte_size_to_disk_blocks(rec.s.obj_bytes_size);
      struct key_t key(rec.s.md5_high, rec.s.md5_low, size_4k_units, rec.s.num_parts);
      if (key == *p_key) {
	*p_rec = rec;
	return 0;
      }
      else {
	ldpp_dout(dpp, 1) << __func__ << "::Bad record in block=" << block_id
			  << ", rec_id=" << rec_id << dendl;
	return -1;
      }
    }
    else {
      ldpp_dout(dpp, 1) << __func__ << "::unexpected short read n=" << n << dendl;
      return -1;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  int load_slab(librados::IoCtx *p_ioctx,
		bufferlist &bl,
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

    int ret = p_ioctx->read_full(oid, bl);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "ERR: failed to read " << oid
			<< ", error is " << cpp_strerror(ret) << dendl;
    }
    return ret;
  }

  //---------------------------------------------------------------------------
  int store_slab(librados::IoCtx *p_ioctx,
		 bufferlist &bl,
		 md5_shard_t md5_shard,
		 work_shard_t worker_id,
		 uint32_t seq_number,
		 const DoutPrefixProvider* dpp)
  {
    disk_block_id_t block_id(worker_id, seq_number);
    std::string oid(block_id.get_slab_name(md5_shard));
    ldpp_dout(dpp, 20) << __func__ << "::oid=" << oid << ", len=" << bl.length() << dendl;
    int ret = p_ioctx->write_full(oid, bl);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to write " << oid
			<< " with: " << cpp_strerror(ret) << dendl;
    }

    return ret;
  }

  //---------------------------------------------------------------------------
  int disk_block_array_t::flush(librados::IoCtx *p_ioctx)
  {
    unsigned len = (p_curr_block + 1 - d_arr) * sizeof(disk_block_t);
    bufferlist bl = bufferlist::static_from_mem((char*)d_arr, len);
    int ret = store_slab(p_ioctx, bl, d_md5_shard, d_worker_id, d_seq_number, dpp);
    // TBD: Can we recycle the buffers?
    // Need to make sure the call to rgw_put_system_obj was fully synchronous

    // d_seq_number++ must be called **after** flush!!
    d_seq_number++;
    p_stats->egress_slabs++;
    slab_reset();
    return ret;
  }

  //---------------------------------------------------------------------------
  int disk_block_array_t::flush_disk_records(librados::IoCtx *p_ioctx)
  {
    ldpp_dout(dpp, 20) << __func__ << "::worker_id=" << (uint32_t)d_worker_id
		       << ", md5_shard=" << (uint32_t)d_md5_shard << dendl;

    // we need to force flush at the end of a cycle even if there was no work done
    // it is used as a signal to worker in the next step
    if (p_curr_block == &d_arr[0] && p_curr_block->is_empty()) {
      ldpp_dout(dpp, 20) << __func__ << "::Empty buffers, generate terminating block" << dendl;
    }
    p_stats->egress_blocks++;
    p_curr_block->close_block(dpp, false);

    int ret = flush(p_ioctx);
    return ret;
  }

  //---------------------------------------------------------------------------
  int disk_block_array_t::add_record(librados::IoCtx        *p_ioctx,
				     const rgw::sal::Bucket *p_bucket,
				     const rgw::sal::Object *p_obj,
				     const parsed_etag_t    *p_parsed_etag,
				     const std::string      &obj_name,
				     uint64_t                obj_size,
				     record_info_t          *p_rec_info) // OUT-PARAM
  {
    ldpp_dout(dpp, 20) << __func__  << "::worker_id=" << (uint32_t)d_worker_id
		       << ", md5_shard=" << (uint32_t)d_md5_shard
		       << "::" << p_bucket->get_name() << "/" << obj_name << dendl;
    disk_record_t rec;
    int ret = fill_disk_record(&rec, p_bucket, p_obj, p_parsed_etag, obj_name, obj_size);
    if (unlikely(ret != 0)) {
      return ret;
    }
    p_rec_info->has_shared_manifest = rec.has_shared_manifest();
    p_rec_info->has_valid_sha256    = rec.has_valid_sha256();
    p_stats->egress_records ++;
    // first, try and add the record to the current open block
    p_rec_info->rec_id = p_curr_block->add_record(&rec, dpp);
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
      p_rec_info->rec_id = p_curr_block->add_record(&rec, dpp);
    }
    else {
      ldpp_dout(dpp, 20)  << __func__ << "::calling flush()" << dendl;
      ret = flush(p_ioctx);
      p_rec_info->rec_id = p_curr_block->add_record(&rec, dpp);
    }

    p_rec_info->block_id = p_curr_block->get_block_id();
    return ret;
  }

} // namespace rgw::dedup
