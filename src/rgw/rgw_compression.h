// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "compressor/Compressor.h"
#include "rgw_putobj.h"
#include "rgw_op.h"
#include "rgw_compression_types.h"
#include "rgw_crypt.h"

int rgw_compression_info_from_attr(const bufferlist& attr,
                                   bool& need_decompress,
                                   RGWCompressionInfo& cs_info);
int rgw_compression_info_from_attrset(const std::map<std::string, bufferlist>& attrs,
                                      bool& need_decompress,
                                      RGWCompressionInfo& cs_info);

class RGWGetObj_Decompress : public RGWGetObj_Filter
{
  CephContext* cct;
  CompressorRef compressor;
  RGWCompressionInfo* cs_info;
  bool partial_content;
  std::vector<compression_block>::iterator first_block, last_block;
  off_t q_ofs, q_len;
  uint64_t cur_ofs;
  bufferlist waiting;
public:
  RGWGetObj_Decompress(CephContext* cct_, 
                       RGWCompressionInfo* cs_info_, 
                       bool partial_content_,
                       RGWGetObj_Filter* next);
  virtual ~RGWGetObj_Decompress() override {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override;
  int fixup_range(off_t& ofs, off_t& end) override;

};

class RGWPutObj_Compress : public rgw::putobj::Pipe
{
  CephContext* cct;
  bool compressed{false};
  CompressorRef compressor;
  std::optional<int32_t> compressor_message;
  std::vector<compression_block> blocks;
  uint64_t compressed_ofs{0};
public:
  RGWPutObj_Compress(CephContext* cct_, CompressorRef compressor,
                     rgw::sal::DataProcessor *next)
    : Pipe(next), cct(cct_), compressor(compressor) {}
  virtual ~RGWPutObj_Compress() override {};

  int process(bufferlist&& data, uint64_t logical_offset) override;

  bool is_compressed() { return compressed; }
  std::vector<compression_block>& get_compression_blocks() { return blocks; }
  std::optional<int32_t> get_compressor_message() { return compressor_message; }

}; /* RGWPutObj_Compress */

/*
 * Base class for data-processor factories that implement the
 * decrypt -> decompress -> recompress -> re-encrypt pipeline.
 */
class RGWRecompressDPF : public rgw::sal::DataProcessorFactory {
protected:
  CephContext* cct;
  uint64_t& orig_size;

  /*
   * Source attrs are COPIED into the DPF. In the transition path the
   * same map is used for both source metadata and destination attrs
   * passed to copy_obj_data(). Storing by reference would destroy
   * source metadata when set_writer() strips crypt attrs.
   */
  rgw::sal::Attrs src_attrs;

  DataProcessorFilter cb;
  RGWGetObj_Filter* filter{&cb};

  RGWCompressionInfo decompress_info;
  std::optional<RGWGetObj_Decompress> decomp;
  std::unique_ptr<RGWGetObj_BlockDecrypt> decrypt_filter;

  std::unique_ptr<RGWPutObj_BlockEncrypt> encrypt_filter;
  CompressorRef compressor_plugin;
  std::optional<RGWPutObj_Compress> compressor;

  virtual int get_decrypt_crypt(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                const rgw::sal::Attrs& src_attrs,
                                std::unique_ptr<BlockCrypt>* crypt) = 0;

  virtual int get_encrypt_crypt(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                rgw::sal::Attrs& dest_attrs,
                                std::unique_ptr<BlockCrypt>* crypt) = 0;

  virtual const std::string& get_dest_compression() = 0;
  virtual bool supports_compress_encrypted() = 0;

  virtual CompressorRef create_compressor(const std::string& type) {
    return Compressor::create(cct, type);
  }

  virtual void mark_compressed() = 0;
  virtual void on_obj_size_changed(uint64_t new_size) {}

public:
  RGWRecompressDPF(CephContext* cct_,
                   uint64_t& obj_size,
                   const rgw::sal::Attrs& src_attrs_)
    : cct(cct_), orig_size(obj_size),
      src_attrs(src_attrs_) {}

  ~RGWRecompressDPF() override = default;

  int set_writer(rgw::sal::DataProcessor* writer,
                 rgw::sal::Attrs& attrs,
                 const DoutPrefixProvider* dpp,
                 optional_yield y) override
  {
    bool src_compressed = false;
    RGWCompressionInfo cs_info;
    int ret = rgw_compression_info_from_attrset(src_attrs, src_compressed,
                                                cs_info);
    if (ret < 0) return ret;

    uint64_t on_disk_size = orig_size;
    bool src_encrypted = src_attrs.count(RGW_ATTR_CRYPT_MODE);
    const std::string src_mode = get_str_attribute(src_attrs, RGW_ATTR_CRYPT_MODE);
    const bool aead_src = src_encrypted && is_aead_mode(src_mode);

    // capture codec before potential cs_info move
    std::string src_comp_type;
    if (src_compressed) {
      src_comp_type = cs_info.compression_type;
    }

    // determine decrypt availability first — needed for decompress decision
    std::unique_ptr<BlockCrypt> decrypt_crypt;
    ret = get_decrypt_crypt(dpp, y, src_attrs, &decrypt_crypt);
    if (ret < 0) return ret;

    /*
     * Read multipart part lengths (shared logic — both DPFs need this).
     * Hard-fail on decode errors to avoid misdecrypting multipart objects.
     */
    std::vector<size_t> decrypt_parts;
    std::vector<std::pair<uint32_t, std::string>> decrypt_part_keys;
    if (decrypt_crypt) {
      auto parts_iter = src_attrs.find(RGW_ATTR_CRYPT_PARTS);
      if (parts_iter != src_attrs.end()) {
        /*
         * CRYPT_PARTS present — decode it. An empty vector is the
         * sentinel for single-stream re-encrypted objects; do NOT
         * fall back to the manifest in that case.
         */
        auto bl = parts_iter->second;
        auto iter = bl.cbegin();
        try {
          decode(decrypt_parts, iter);
        } catch (buffer::error&) {
          ldpp_dout(dpp, 0) << "ERROR: malformed "
              RGW_ATTR_CRYPT_PARTS << dendl;
          return -EIO;
        }
      } else {
        auto manifest_iter = src_attrs.find(RGW_ATTR_MANIFEST);
        if (manifest_iter != src_attrs.end()) {
          int r = RGWGetObj_BlockDecrypt::read_manifest_parts(
              dpp, manifest_iter->second, decrypt_parts);
          if (r < 0) {
            ldpp_dout(dpp, 0) << "ERROR: failed to read "
                "manifest parts" << dendl;
            return r;
          }
        }
      }

      /*
       * AEAD multipart objects also store the (part_num, salt) pairs
       * for per-part IV/key derivation. Empty → single-part.
       */
      if (aead_src) {
        if (auto it = src_attrs.find(RGW_ATTR_CRYPT_PART_NUMS);
            it != src_attrs.end()) {
          auto bl = it->second;
          auto iter = bl.cbegin();
          try {
            decode(decrypt_part_keys, iter);
          } catch (buffer::error&) {
            ldpp_dout(dpp, 0) << "ERROR: malformed "
                RGW_ATTR_CRYPT_PART_NUMS << dendl;
            return -EIO;
          }
        }
      }
    }

    // determine compression match — needed for decompress decision
    std::string dest_comp = get_dest_compression();
    bool compression_matches =
        dest_comp != "random" &&
        ((!src_compressed && dest_comp == "none") ||
         (src_compressed && src_comp_type == dest_comp));

    /*
     * Decompress decision — must happen after decrypt and
     * compression_matches are known. Decompress when source is
     * compressed AND:
     *   - source is not encrypted (always safe to decompress), OR
     *   - source is encrypted AND decrypt is available AND
     *     compression needs to change (different codec or "none")
     *
     * When encrypted + same codec, skip decompress — compressed
     * data passes through after decrypt/re-encrypt.
     */
    bool need_decompress = src_compressed &&
        !compression_matches &&
        (!src_encrypted || decrypt_crypt);

    if (need_decompress) {
      decompress_info = std::move(cs_info);
      decomp.emplace(cct, &decompress_info, false, filter);
      filter = &*decomp;
      orig_size = decompress_info.orig_size;
      on_obj_size_changed(orig_size);
    } else if (src_compressed) {
      /*
       * Compressed-passthrough (same codec): data stays compressed on
       * the wire, but bucket logging and event notifications expect
       * orig_size to reflect the S3-logical (plaintext) size recorded
       * in the source's compression metadata.
       */
      orig_size = cs_info.orig_size;
      on_obj_size_changed(orig_size);
    } else if (aead_src && decrypt_crypt) {
      /*
       * AEAD without compression: prefer the stored ORIGINAL_SIZE attr
       * (authoritative plaintext) and fall back to the chunk/tag
       * derivation formula. Both equal plaintext for non-compressed
       * sources; the attr matches even if a chunk size ever changes.
       */
      uint64_t pt_size = 0;
      if (rgw_get_aead_original_size(dpp, src_attrs, &pt_size) ||
          rgw_get_aead_decrypted_size(dpp, src_attrs, on_disk_size, &pt_size)) {
        orig_size = pt_size;
        on_obj_size_changed(orig_size);
      }
    }

    // read: decrypt wraps decompress (outer)
    if (decrypt_crypt) {
      if (aead_src) {
        decrypt_filter = std::make_unique<RGWGetObj_BlockDecrypt>(
            dpp, cct, filter, std::move(decrypt_crypt),
            std::move(decrypt_parts), std::move(decrypt_part_keys),
            static_cast<off_t>(on_disk_size),
            static_cast<bool>(decomp), y);
      } else {
        decrypt_filter = std::make_unique<RGWGetObj_BlockDecrypt>(
            dpp, cct, filter, std::move(decrypt_crypt),
            std::move(decrypt_parts), y);
      }
      filter = decrypt_filter.get();
    }

    /*
     * Crypt attr handling for re-encryption. Two modes:
     *
     * CopyObject: get_encrypt_crypt() strips all crypt attrs itself
     * then writes fresh ones (may change key, mode, etc.)
     *
     * LC transition: same mode and same upstream key are preserved.
     * CRYPT_PARTS / CRYPT_PART_NUMS are always cleared (re-encryption
     * produces a single stream). For AEAD modes, get_encrypt_crypt()
     * also regenerates CRYPT_SALT so the re-derived per-object key
     * differs from the source — required to avoid GCM nonce reuse
     * when the plaintext changes (e.g., after recompression).
     */
    if (decrypt_filter) {
      attrs.erase(RGW_ATTR_CRYPT_PARTS);
      attrs.erase(RGW_ATTR_CRYPT_PART_NUMS);
    }

    // write: encrypt (inner) then compress (outer)
    rgw::sal::DataProcessor* processor = writer;

    std::unique_ptr<BlockCrypt> encrypt_crypt;
    ret = get_encrypt_crypt(dpp, y, attrs, &encrypt_crypt);
    if (ret < 0) return ret;

    if (encrypt_crypt) {
      encrypt_filter = std::make_unique<RGWPutObj_BlockEncrypt>(
          dpp, cct, processor, std::move(encrypt_crypt), y);
      processor = encrypt_filter.get();

      // write empty CRYPT_PARTS sentinel for re-encrypted objects
      if (decrypt_filter) {
        std::vector<size_t> empty_parts;
        bufferlist parts_bl;
        encode(empty_parts, parts_bl);
        attrs[RGW_ATTR_CRYPT_PARTS] = std::move(parts_bl);
      }
    } else if (decrypt_filter) {
      // decrypt without re-encrypt (SSE-C -> plaintext) — strip all crypt attrs
      auto it = attrs.lower_bound(RGW_ATTR_CRYPT_PREFIX);
      while (it != attrs.end() &&
             it->first.compare(0, sizeof(RGW_ATTR_CRYPT_PREFIX) - 1,
                               RGW_ATTR_CRYPT_PREFIX) == 0) {
        it = attrs.erase(it);
      }
    }

    if (!compression_matches && dest_comp != "none" &&
        (encrypt_filter == nullptr || supports_compress_encrypted())) {
      compressor_plugin = create_compressor(dest_comp);
      if (compressor_plugin) {
        compressor.emplace(cct, compressor_plugin, processor);
        processor = &*compressor;
        mark_compressed();
      }
    } else if (src_compressed && compression_matches) {
      /*
       * Source stays compressed through the pipeline (same codec) —
       * hint the backend that this data is already incompressible.
       */
      mark_compressed();
    }
    if (decomp || compressor) {
      attrs.erase(RGW_ATTR_COMPRESSION);
    }

    cb.set_processor(processor);

    // skip fixup for empty objects — size-1 underflows to max uint
    if (on_disk_size == 0) {
      return 0;
    }

    /*
     * fixup_range is called with the encrypted on-disk size as the
     * "plaintext range" argument. RGWGetObj_BlockDecrypt::fixup_range
     * expects plaintext-domain input, but project_encrypt_range()
     * clamps the projected encrypted range to encrypted_total_size,
     * so the final read range is correct regardless. When a decomp
     * filter is downstream, the decrypt call cascades into decomp
     * with that intermediate (encrypted-as-plaintext) range, which
     * is wrong for the decomp side — the second decomp->fixup_range()
     * below re-initializes it with the proper plaintext range.
     */
    if (decrypt_filter) {
      off_t ofs = 0;
      off_t end = static_cast<off_t>(on_disk_size) - 1;
      filter->fixup_range(ofs, end);
    }
    if (decomp) {
      off_t ofs = 0;
      off_t end = orig_size - 1;
      decomp->fixup_range(ofs, end);
    }

    return 0;
  }

  void finalize_attrs(rgw::sal::Attrs& attrs) override {
    if (compressor && compressor->is_compressed()) {
      bufferlist tmp;
      RGWCompressionInfo cs_info;
      cs_info.compression_type = compressor_plugin->get_type_name();
      cs_info.orig_size = orig_size;
      cs_info.compressor_message = compressor->get_compressor_message();
      cs_info.blocks = std::move(compressor->get_compression_blocks());
      encode(cs_info, tmp);
      attrs[RGW_ATTR_COMPRESSION] = tmp;
    }
    /*
     * AEAD bookkeeping for the re-encrypted dest: ORIGINAL_SIZE must
     * match the plaintext payload size. set_writer() guarantees
     * orig_size is plaintext on every path (decomp output, compressed-
     * passthrough cs_info.orig_size, AEAD ORIGINAL_SIZE/formula, or
     * the source's logical size). PART_NUMS is cleared because re-
     * encryption always produces a single stream. SALT and
     * PREFETCH_ALIGN are written by get_encrypt_crypt().
     */
    if (encrypt_filter &&
        is_aead_mode(get_str_attribute(attrs, RGW_ATTR_CRYPT_MODE))) {
      bufferlist sz_bl;
      sz_bl.append(std::to_string(orig_size));
      attrs[RGW_ATTR_CRYPT_ORIGINAL_SIZE] = std::move(sz_bl);
      attrs.erase(RGW_ATTR_CRYPT_PART_NUMS);
    }
  }

  uint64_t get_accounted_size(uint64_t default_size) override {
    if (decomp)
      return orig_size;
    return default_size;
  }

  // LC only constructs DPF when transformation is decided
  bool need_copy_data() override {
    return true;
  }

  RGWGetObj_Filter* get_filter() override { return filter; }
};
