// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/**
 * Crypto filters for Put/Post/Get operations.
 */

#pragma once

#include <string_view>

#include <rgw/rgw_op.h>
#include <rgw/rgw_rest.h>
#include <rgw/rgw_rest_s3.h>
#include "rgw_putobj.h"
#include "common/async/yield_context.h"

struct rgw_crypt_src_identity {
  std::string bucket_id;
  std::string bucket;
  std::string object;

  bool valid() const {
    return !bucket_id.empty() && !bucket.empty() && !object.empty();
  }
};

/**
 * \brief Interface for block encryption methods
 *
 * Encrypts and decrypts data.
 * Operations are performed in context of larger stream being divided into blocks.
 * Each block can be processed independently, but only as a whole.
 * Part block cannot be properly processed.
 * Each request must start on block-aligned offset.
 * Each request should have length that is multiply of block size.
 * Request with unaligned length is only acceptable for last part of stream.
 */
class BlockCrypt {
public:
  BlockCrypt(){};
  virtual ~BlockCrypt(){};

  /**
    * Determines size of encryption block.
    * This is usually multiply of key size.
    * It determines size of chunks that should be passed to \ref encrypt and \ref decrypt.
    */
  virtual size_t get_block_size() = 0;

  /**
   * Returns size of encrypted block (ciphertext + metadata like auth tags).
   * For most ciphers this equals get_block_size(), but for AEAD modes like GCM
   * it includes the authentication tag.
   */
  virtual size_t get_encrypted_block_size() {
    return get_block_size();
  }

  /**
   * Encrypts data.
   * Argument \ref stream_offset shows where in generalized stream chunk is located.
   * Input for encryption is \ref input buffer, with relevant data in range <in_ofs, in_ofs+size).
   * \ref input and \output may not be the same buffer.
   *
   * \params
   * input - source buffer of data
   * in_ofs - offset of chunk inside input
   * size - size of chunk, must be chunk-aligned unless last part is processed
   * output - destination buffer to encrypt to
   * stream_offset - location of <in_ofs,in_ofs+size) chunk in data stream, must be chunk-aligned
   * \return true iff successfully encrypted
   */
  virtual bool encrypt(bufferlist& input,
                       off_t in_ofs,
                       size_t size,
                       bufferlist& output,
                       off_t stream_offset,
                       optional_yield y) = 0;

  /**
   * Decrypts data.
   * Argument \ref stream_offset shows where in generalized stream chunk is located.
   * Input for decryption is \ref input buffer, with relevant data in range <in_ofs, in_ofs+size).
   * \ref input and \output may not be the same buffer.
   *
   * \params
   * input - source buffer of data
   * in_ofs - offset of chunk inside input
   * size - size of chunk, must be chunk-aligned unless last part is processed
   * output - destination buffer to encrypt to
   * stream_offset - location of <in_ofs,in_ofs+size) chunk in data stream, must be chunk-aligned
   * \return true iff successfully encrypted
   */
  virtual bool decrypt(bufferlist& input,
                       off_t in_ofs,
                       size_t size,
                       bufferlist& output,
                       off_t stream_offset,
                       optional_yield y) = 0;

  /**
   * Set the part number for multipart object decryption.
   * AEAD modes use this for per-part IV derivation.
   * Default is no-op; CBC derives IVs from block offsets instead.
   */
  virtual void set_part_number(uint32_t part_number) {}
};

static const size_t AES_256_KEYSIZE = 256 / 8;
static const size_t AES_256_GCM_IV_SIZE = 96 / 8;  // 12 bytes, GCM standard
static const size_t AES_256_GCM_SALT_SIZE = 32;  // 256-bit random salt for HMAC-based key derivation

/**
 * AEAD chunk size constants used for size calculations across RGW.
 * All supported AEAD ciphers use 128-bit (16-byte) authentication tags.
 */
static constexpr size_t AEAD_CHUNK_SIZE = 4096;
static constexpr size_t AEAD_TAG_SIZE = 16;
static constexpr size_t AEAD_ENCRYPTED_CHUNK_SIZE = AEAD_CHUNK_SIZE + AEAD_TAG_SIZE;  // 4112

/**
 * Check if encryption mode is AEAD.
 * AEAD modes have ciphertext expansion from auth tags and need special
 * handling for size calculations and multipart part numbers.
 *
 * All AEAD modes currently end in "-GCM". When adding non-GCM AEAD modes
 * (e.g., ChaCha20-Poly1305), update this function to match them.
 */
inline bool is_aead_mode(const std::string& mode) {
  return mode.size() >= 4 && mode.compare(mode.size() - 4, 4, "-GCM") == 0;
}

/**
 * Check if encryption mode string indicates CBC mode.
 * CBC modes: SSE-C-AES256, SSE-KMS, RGW-AUTO, AES256
 */
inline bool is_cbc_mode(const std::string& mode) {
  return mode == "SSE-C-AES256" || mode == "SSE-KMS" ||
         mode == "RGW-AUTO" || mode == "AES256";
}

/**
 * Convert encrypted size to plaintext size for AEAD modes.
 * Each AEAD_CHUNK_SIZE-byte plaintext chunk becomes AEAD_ENCRYPTED_CHUNK_SIZE bytes
 * (plaintext + AEAD_TAG_SIZE-byte auth tag).
 * Pass dpp to enable warning logging for malformed data.
 */
inline uint64_t aead_encrypted_to_plaintext_size(uint64_t encrypted_size,
                                                  const DoutPrefixProvider* dpp = nullptr) {
  uint64_t full_chunks = encrypted_size / AEAD_ENCRYPTED_CHUNK_SIZE;
  uint64_t remainder = encrypted_size % AEAD_ENCRYPTED_CHUNK_SIZE;

  if (remainder > 0 && remainder <= AEAD_TAG_SIZE) {
    // Malformed: partial chunk has no ciphertext, only tag bytes
    if (dpp) {
      ldpp_dout(dpp, 1) << "WARNING: aead_encrypted_to_plaintext_size: "
          << "partial chunk size " << remainder
          << " is <= tag size " << AEAD_TAG_SIZE
          << " - data may be corrupted" << dendl;
    }
    return full_chunks * AEAD_CHUNK_SIZE;
  }

  uint64_t partial = (remainder > AEAD_TAG_SIZE) ? (remainder - AEAD_TAG_SIZE) : 0;
  return full_chunks * AEAD_CHUNK_SIZE + partial;
}

/**
 * Convert plaintext size to encrypted size for AEAD modes.
 * Each AEAD_CHUNK_SIZE-byte plaintext chunk becomes AEAD_ENCRYPTED_CHUNK_SIZE bytes.
 */
inline uint64_t aead_plaintext_to_encrypted_size(uint64_t plaintext_size) {
  if (plaintext_size == 0) return 0;
  uint64_t num_chunks = (plaintext_size + AEAD_CHUNK_SIZE - 1) / AEAD_CHUNK_SIZE;
  return plaintext_size + (num_chunks * AEAD_TAG_SIZE);
}

/**
 * Convert plaintext offset to encrypted offset for AEAD modes.
 * Accounts for AEAD_TAG_SIZE-byte auth tag per chunk.
 */
inline uint64_t aead_plaintext_to_encrypted_offset(uint64_t plaintext_ofs) {
  uint64_t chunk_idx = plaintext_ofs / AEAD_CHUNK_SIZE;
  uint64_t offset_in_chunk = plaintext_ofs % AEAD_CHUNK_SIZE;
  return chunk_idx * AEAD_ENCRYPTED_CHUNK_SIZE + offset_in_chunk;
}

bool AES_256_ECB_encrypt(const DoutPrefixProvider* dpp,
                         CephContext* cct,
                         const uint8_t* key,
                         size_t key_size,
                         const uint8_t* data_in,
                         uint8_t* data_out,
                         size_t data_size);

/**
 * Create an AES-256-GCM BlockCrypt instance.
 *
 * For encryption: Pass salt=nullptr to generate a random 32-byte salt.
 *                 After creation, call AES_256_GCM_get_salt() to retrieve it for storage.
 *
 * For decryption: Pass the stored salt from RGW_ATTR_CRYPT_SALT.
 */
std::unique_ptr<BlockCrypt> AES_256_GCM_create(const DoutPrefixProvider* dpp,
                                                CephContext* cct,
                                                const uint8_t* key,
                                                size_t key_len,
                                                const uint8_t* salt = nullptr,
                                                size_t salt_len = 0,
                                                uint32_t part_number = 0);

/**
 * Retrieve the salt from a BlockCrypt instance for storage in RGW_ATTR_CRYPT_SALT.
 * Returns empty string if the BlockCrypt is not an AES_256_GCM instance.
 */
std::string AES_256_GCM_get_salt(BlockCrypt* block_crypt);

class RGWGetObj_BlockDecrypt : public RGWGetObj_Filter {
  friend class TestableBlockDecrypt;  // For unit testing private members
  const DoutPrefixProvider *dpp;
  CephContext* cct;
  std::unique_ptr<BlockCrypt> crypt; /**< already configured stateless BlockCrypt
                                          for operations when enough data is accumulated */
  off_t enc_begin_skip; /**< amount of data to skip from beginning of received data */
  off_t ofs; /**< plaintext stream offset of data we expect to show up next through \ref handle_data */
  off_t enc_ofs; /**< encrypted stream offset, for comparing against parts_len which contains encrypted sizes */
  off_t end; /**< stream offset of last byte that is requested */
  off_t encrypted_total_size; /**< total encrypted object size (for clamping ranges in fixup_range) */
  bool has_compression{false}; /**< true if a decompression filter is present downstream */
  bufferlist cache; /**< stores extra data that could not (yet) be processed by BlockCrypt */
  size_t block_size; /**< snapshot of \ref BlockCrypt.get_block_size() (plaintext block size) */
  size_t encrypted_block_size; /**< snapshot of \ref BlockCrypt.get_encrypted_block_size() (includes auth tag for GCM) */
  optional_yield y;
  std::vector<size_t> parts_len; /**< size of parts of multipart object, parsed from manifest */
  std::vector<uint32_t> part_nums; /**< actual S3 part numbers for multipart (e.g., [1,3,5]) */
  uint32_t current_part_num = 0; /**< current part number (1-based, 0 means single-part object) */

  int process(bufferlist& cipher, size_t part_ofs, size_t size);

  /**
   * Process cached data across part boundaries.
   * Updates current_part_num and calls crypt->set_part_number() as needed.
   * Returns 0 on success, negative error code on failure.
   * On success, plain_part_ofs_out contains the plaintext offset for remaining data.
   */
  int process_part_boundaries(size_t& plain_part_ofs_out);

  /**
   * Result of finding which part contains a plaintext offset.
   */
  struct PartLocation {
    size_t part_idx;            /**< Index into parts_len */
    off_t offset_in_part;       /**< Plaintext offset within the part */
    off_t cumulative_encrypted; /**< Total encrypted bytes before this part */
  };

  /**
   * Find which part contains a given plaintext offset.
   * @param plaintext_ofs  The plaintext offset to locate
   * @param clamp_to_last  If true, stop at second-to-last part (for end offsets)
   */
  PartLocation find_part_for_plaintext_offset(off_t plaintext_ofs, bool clamp_to_last) const;

  /**
   * Align an encrypted offset up to the end of an encrypted block.
   * For GCM: ensures we read complete blocks including auth tags.
   */
  off_t align_to_encrypted_block_end(off_t enc_ofs) const {
    if (block_size == encrypted_block_size) {
      return enc_ofs;  // CBC - no alignment needed
    }
    return (enc_ofs / encrypted_block_size) * encrypted_block_size + (encrypted_block_size - 1);
  }

  /**
   * Convert a logical (plaintext) offset to encrypted (storage) offset.
   * For AEAD modes: accounts for auth tag overhead per chunk.
   */
  off_t logical_to_encrypted_offset(off_t logical_ofs) const {
    if (block_size == encrypted_block_size) {
      return logical_ofs; // Non-AEAD (CBC) - no conversion needed
    }
    return aead_plaintext_to_encrypted_offset(logical_ofs);
  }

  /**
   * Convert an encrypted size to plaintext size.
   * For AEAD modes: removes the auth tag overhead per chunk.
   */
  size_t encrypted_to_plaintext_size(size_t encrypted_size) const {
    if (block_size == encrypted_block_size) {
      return encrypted_size;  // Non-AEAD (CBC) - no conversion needed
    }
    return aead_encrypted_to_plaintext_size(encrypted_size, dpp);
  }

public:
  RGWGetObj_BlockDecrypt(const DoutPrefixProvider *dpp,
                         CephContext* cct,
                         RGWGetObj_Filter* next,
                         std::unique_ptr<BlockCrypt> crypt,
                         std::vector<size_t> parts_len,
                         std::vector<uint32_t> part_nums,
                         off_t encrypted_total_size,
                         bool has_compression,
                         optional_yield y);
  // Backward-compatible constructor for CBC mode (no size expansion)
  RGWGetObj_BlockDecrypt(const DoutPrefixProvider *dpp,
                         CephContext* cct,
                         RGWGetObj_Filter* next,
                         std::unique_ptr<BlockCrypt> crypt,
                         std::vector<size_t> parts_len,
                         optional_yield y)
    : RGWGetObj_BlockDecrypt(dpp, cct, next, std::move(crypt),
                             std::move(parts_len), {}, 0, false, y) {}
  virtual ~RGWGetObj_BlockDecrypt();

  virtual int fixup_range(off_t& bl_ofs,
                          off_t& bl_end) override;
  virtual int handle_data(bufferlist& bl,
                          off_t bl_ofs,
                          off_t bl_len) override;
  virtual int flush() override;

  static int read_manifest_parts(const DoutPrefixProvider *dpp,
                                 const bufferlist& manifest_bl,
                                 std::vector<size_t>& parts_len);

  /**
   * Returns true if this cipher expands the data size (e.g., AEAD adds auth tags).
   * Used to determine if obj_size needs adjustment for Content-Length.
   */
  bool has_size_expansion() const {
    return block_size != encrypted_block_size;
  }

  /**
   * Calculate the plaintext size from encrypted size.
   * For AEAD: removes the 16-byte auth tag overhead per chunk.
   * Public wrapper for use by callers that need to adjust Content-Length.
   */
  uint64_t get_plaintext_size(uint64_t encrypted_size) const {
    return encrypted_to_plaintext_size(encrypted_size);
  }
}; /* RGWGetObj_BlockDecrypt */


class RGWPutObj_BlockEncrypt : public rgw::putobj::Pipe
{
  const DoutPrefixProvider *dpp;
  CephContext* cct;
  std::unique_ptr<BlockCrypt> crypt; /**< already configured stateless BlockCrypt
                                          for operations when enough data is accumulated */
  bufferlist cache; /**< stores extra data that could not (yet) be processed by BlockCrypt */
  const size_t block_size; /**< snapshot of \ref BlockCrypt.get_block_size() (plaintext block size) */
  uint64_t encrypted_offset = 0; /**< tracks write position in encrypted stream (differs from plaintext for GCM) */
  optional_yield y;
public:
  RGWPutObj_BlockEncrypt(const DoutPrefixProvider *dpp,
                         CephContext* cct,
                         rgw::sal::DataProcessor *next,
                         std::unique_ptr<BlockCrypt> crypt,
                         optional_yield y);

  int process(bufferlist&& data, uint64_t logical_offset) override;
}; /* RGWPutObj_BlockEncrypt */


int rgw_s3_prepare_encrypt(req_state* s, optional_yield y,
                           std::map<std::string, ceph::bufferlist>& attrs,
                           std::unique_ptr<BlockCrypt>* block_crypt,
                           std::map<std::string,
                                    std::string>& crypt_http_responses,
                           uint32_t part_number = 0);

int rgw_s3_prepare_decrypt(req_state* s, optional_yield y,
                           std::map<std::string, ceph::bufferlist>& attrs,
                           std::unique_ptr<BlockCrypt>* block_crypt,
                           std::map<std::string, std::string>* crypt_http_responses,
                           bool copy_source,
                           uint32_t part_number = 0,
                           const rgw_crypt_src_identity* src_identity = nullptr);

/**
 * Get the original (uncompressed, unencrypted) size from ORIGINAL_SIZE attr.
 * Use for: Content-Length, bucket index, quota.
 * Returns false if attr missing, not AEAD mode, or parse failure.
 */
bool rgw_get_aead_original_size(const DoutPrefixProvider* dpp,
                                const std::map<std::string, ceph::bufferlist>& attrs,
                                uint64_t* original_size);

/**
 * Get the decrypted (but possibly still compressed) size.
 * Uses CRYPT_PARTS if available, otherwise calculates from encrypted size.
 * Use for: Copy path where data stays in compressed domain.
 * WARNING: Never use when caller expects original (uncompressed) size AND
 *          compression may be present. This yields compressed-domain sizes.
 */
bool rgw_get_aead_decrypted_size(const DoutPrefixProvider* dpp,
                                 const std::map<std::string, ceph::bufferlist>& attrs,
                                 uint64_t encrypted_size,
                                 uint64_t* decrypted_size);

static inline void set_attr(std::map<std::string, bufferlist>& attrs,
                            const char* key,
                            std::string_view value)
{
  bufferlist bl;
  bl.append(value.data(), value.size());
  attrs[key] = std::move(bl);
}

static inline std::string get_str_attribute(const std::map<std::string, bufferlist>& attrs,
                                            const char *name)
{
  auto iter = attrs.find(name);
  if (iter == attrs.end()) {
    return {};
  }
  return iter->second.to_str();
}

int rgw_remove_sse_s3_bucket_key(req_state *s, optional_yield y);
