// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
};

static const size_t AES_256_KEYSIZE = 256 / 8;
bool AES_256_ECB_encrypt(const DoutPrefixProvider* dpp,
                         CephContext* cct,
                         const uint8_t* key,
                         size_t key_size,
                         const uint8_t* data_in,
                         uint8_t* data_out,
                         size_t data_size);

class RGWGetObj_BlockDecrypt : public RGWGetObj_Filter {
  const DoutPrefixProvider *dpp;
  CephContext* cct;
  std::unique_ptr<BlockCrypt> crypt; /**< already configured stateless BlockCrypt
                                          for operations when enough data is accumulated */
  off_t enc_begin_skip; /**< amount of data to skip from beginning of received data */
  off_t ofs; /**< stream offset of data we expect to show up next through \ref handle_data */
  off_t end; /**< stream offset of last byte that is requested */
  bufferlist cache; /**< stores extra data that could not (yet) be processed by BlockCrypt */
  size_t block_size; /**< snapshot of \ref BlockCrypt.get_block_size() */
  optional_yield y;
  std::vector<size_t> parts_len; /**< size of parts of multipart object, parsed from manifest */

  int process(bufferlist& cipher, size_t part_ofs, size_t size);

public:
  RGWGetObj_BlockDecrypt(const DoutPrefixProvider *dpp,
                         CephContext* cct,
                         RGWGetObj_Filter* next,
                         std::unique_ptr<BlockCrypt> crypt,
                         std::vector<size_t> parts_len,
                         optional_yield y);
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
}; /* RGWGetObj_BlockDecrypt */


class RGWPutObj_BlockEncrypt : public rgw::putobj::Pipe
{
  const DoutPrefixProvider *dpp;
  CephContext* cct;
  std::unique_ptr<BlockCrypt> crypt; /**< already configured stateless BlockCrypt
                                          for operations when enough data is accumulated */
  bufferlist cache; /**< stores extra data that could not (yet) be processed by BlockCrypt */
  const size_t block_size; /**< snapshot of \ref BlockCrypt.get_block_size() */
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
                                    std::string>& crypt_http_responses);

int rgw_s3_prepare_decrypt(req_state* s, optional_yield y,
                           std::map<std::string, ceph::bufferlist>& attrs,
                           std::unique_ptr<BlockCrypt>* block_crypt,
                           std::map<std::string,
                                    std::string>& crypt_http_responses);

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
