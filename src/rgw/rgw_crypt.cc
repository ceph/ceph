// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/**
 * Crypto filters for Put/Post/Get operations.
 */

#include <string_view>

#include <rgw/rgw_op.h>
#include <rgw/rgw_crypt.h>
#include <auth/Crypto.h>
#include <rgw/rgw_b64.h>
#include <rgw/rgw_rest_s3.h>
#include "include/ceph_assert.h"
#include "crypto/crypto_accel.h"
#include "crypto/crypto_plugin.h"
#include "rgw/rgw_kms.h"

#include <openssl/evp.h>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace rgw;


CryptoAccelRef get_crypto_accel(CephContext *cct)
{
  CryptoAccelRef ca_impl = nullptr;
  stringstream ss;
  PluginRegistry *reg = cct->get_plugin_registry();
  string crypto_accel_type = cct->_conf->plugin_crypto_accelerator;

  CryptoPlugin *factory = dynamic_cast<CryptoPlugin*>(reg->get_with_load("crypto", crypto_accel_type));
  if (factory == nullptr) {
    lderr(cct) << __func__ << " cannot load crypto accelerator of type " << crypto_accel_type << dendl;
    return nullptr;
  }
  int err = factory->factory(&ca_impl, &ss);
  if (err) {
    lderr(cct) << __func__ << " factory return error " << err <<
        " with description: " << ss.str() << dendl;
  }
  return ca_impl;
}


template <std::size_t KeySizeV, std::size_t IvSizeV>
static inline
bool evp_sym_transform(CephContext* const cct,
                       const EVP_CIPHER* const type,
                       unsigned char* const out,
                       const unsigned char* const in,
                       const size_t size,
                       const unsigned char* const iv,
                       const unsigned char* const key,
                       const bool encrypt)
{
  using pctx_t = \
    std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>;
  pctx_t pctx{ EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free };

  if (!pctx) {
    return false;
  }

  if (1 != EVP_CipherInit_ex(pctx.get(), type, nullptr,
                             nullptr, nullptr, encrypt)) {
    ldout(cct, 5) << "EVP: failed to 1st initialization stage" << dendl;
    return false;
  }

  // we want to support ciphers that don't use IV at all like AES-256-ECB
  if constexpr (static_cast<bool>(IvSizeV)) {
    ceph_assert(EVP_CIPHER_CTX_iv_length(pctx.get()) == IvSizeV);
    ceph_assert(EVP_CIPHER_CTX_block_size(pctx.get()) == IvSizeV);
  }
  ceph_assert(EVP_CIPHER_CTX_key_length(pctx.get()) == KeySizeV);

  if (1 != EVP_CipherInit_ex(pctx.get(), nullptr, nullptr, key, iv, encrypt)) {
    ldout(cct, 5) << "EVP: failed to 2nd initialization stage" << dendl;
    return false;
  }

  // disable padding
  if (1 != EVP_CIPHER_CTX_set_padding(pctx.get(), 0)) {
    ldout(cct, 5) << "EVP: cannot disable PKCS padding" << dendl;
    return false;
  }

  // operate!
  int written = 0;
  ceph_assert(size <= static_cast<size_t>(std::numeric_limits<int>::max()));
  if (1 != EVP_CipherUpdate(pctx.get(), out, &written, in, size)) {
    ldout(cct, 5) << "EVP: EVP_CipherUpdate failed" << dendl;
    return false;
  }

  int finally_written = 0;
  static_assert(sizeof(*out) == 1);
  if (1 != EVP_CipherFinal_ex(pctx.get(), out + written, &finally_written)) {
    ldout(cct, 5) << "EVP: EVP_CipherFinal_ex failed" << dendl;
    return false;
  }

  // padding is disabled so EVP_CipherFinal_ex should not append anything
  ceph_assert(finally_written == 0);
  return (written + finally_written) == static_cast<int>(size);
}


/**
 * Encryption in CBC mode. Chunked to 4K blocks. Offset is used as IV for each 4K block.
 *
 *
 *
 * A. Encryption
 * 1. Input is split to 4K chunks + remainder in one, smaller chunk
 * 2. Each full chunk is encrypted separately with CBC chained mode, with initial IV derived from offset
 * 3. Last chunk is 16*m + n.
 * 4. 16*m bytes are encrypted with CBC chained mode, with initial IV derived from offset
 * 5. Last n bytes are xor-ed with pattern obtained by CBC encryption of
 *    last encrypted 16 byte block <16m-16, 16m-15) with IV = {0}.
 * 6. (Special case) If m == 0 then last n bytes are xor-ed with pattern
 *    obtained by CBC encryption of {0} with IV derived from offset
 *
 * B. Decryption
 * 1. Input is split to 4K chunks + remainder in one, smaller chunk
 * 2. Each full chunk is decrypted separately with CBC chained mode, with initial IV derived from offset
 * 3. Last chunk is 16*m + n.
 * 4. 16*m bytes are decrypted with CBC chained mode, with initial IV derived from offset
 * 5. Last n bytes are xor-ed with pattern obtained by CBC ENCRYPTION of
 *    last (still encrypted) 16 byte block <16m-16,16m-15) with IV = {0}
 * 6. (Special case) If m == 0 then last n bytes are xor-ed with pattern
 *    obtained by CBC ENCRYPTION of {0} with IV derived from offset
 */
class AES_256_CBC : public BlockCrypt {
public:
  static const size_t AES_256_KEYSIZE = 256 / 8;
  static const size_t AES_256_IVSIZE = 128 / 8;
  static const size_t CHUNK_SIZE = 4096;
private:
  static const uint8_t IV[AES_256_IVSIZE];
  CephContext* cct;
  uint8_t key[AES_256_KEYSIZE];
public:
  explicit AES_256_CBC(CephContext* cct): cct(cct) {
  }
  ~AES_256_CBC() {
    ::ceph::crypto::zeroize_for_security(key, AES_256_KEYSIZE);
  }
  bool set_key(const uint8_t* _key, size_t key_size) {
    if (key_size != AES_256_KEYSIZE) {
      return false;
    }
    memcpy(key, _key, AES_256_KEYSIZE);
    return true;
  }
  size_t get_block_size() {
    return CHUNK_SIZE;
  }

  bool cbc_transform(unsigned char* out,
                     const unsigned char* in,
                     const size_t size,
                     const unsigned char (&iv)[AES_256_IVSIZE],
                     const unsigned char (&key)[AES_256_KEYSIZE],
                     bool encrypt)
  {
    return evp_sym_transform<AES_256_KEYSIZE, AES_256_IVSIZE>(
      cct, EVP_aes_256_cbc(), out, in, size, iv, key, encrypt);
  }

  bool cbc_transform(unsigned char* out,
                     const unsigned char* in,
                     size_t size,
                     off_t stream_offset,
                     const unsigned char (&key)[AES_256_KEYSIZE],
                     bool encrypt)
  {
    static std::atomic<bool> failed_to_get_crypto(false);
    CryptoAccelRef crypto_accel;
    if (! failed_to_get_crypto.load())
    {
      crypto_accel = get_crypto_accel(cct);
      if (!crypto_accel)
        failed_to_get_crypto = true;
    }
    bool result = true;
    unsigned char iv[AES_256_IVSIZE];
    for (size_t offset = 0; result && (offset < size); offset += CHUNK_SIZE) {
      size_t process_size = offset + CHUNK_SIZE <= size ? CHUNK_SIZE : size - offset;
      prepare_iv(iv, stream_offset + offset);
      if (crypto_accel != nullptr) {
        if (encrypt) {
          result = crypto_accel->cbc_encrypt(out + offset, in + offset,
                                             process_size, iv, key);
        } else {
          result = crypto_accel->cbc_decrypt(out + offset, in + offset,
                                             process_size, iv, key);
        }
      } else {
        result = cbc_transform(
            out + offset, in + offset, process_size,
            iv, key, encrypt);
      }
    }
    return result;
  }


  bool encrypt(bufferlist& input,
               off_t in_ofs,
               size_t size,
               bufferlist& output,
               off_t stream_offset)
  {
    bool result = false;
    size_t aligned_size = size / AES_256_IVSIZE * AES_256_IVSIZE;
    size_t unaligned_rest_size = size - aligned_size;
    output.clear();
    buffer::ptr buf(aligned_size + AES_256_IVSIZE);
    unsigned char* buf_raw = reinterpret_cast<unsigned char*>(buf.c_str());
    const unsigned char* input_raw = reinterpret_cast<const unsigned char*>(input.c_str());

    /* encrypt main bulk of data */
    result = cbc_transform(buf_raw,
                           input_raw + in_ofs,
                           aligned_size,
                           stream_offset, key, true);
    if (result && (unaligned_rest_size > 0)) {
      /* remainder to encrypt */
      if (aligned_size % CHUNK_SIZE > 0) {
        /* use last chunk for unaligned part */
        unsigned char iv[AES_256_IVSIZE] = {0};
        result = cbc_transform(buf_raw + aligned_size,
                               buf_raw + aligned_size - AES_256_IVSIZE,
                               AES_256_IVSIZE,
                               iv, key, true);
      } else {
        /* 0 full blocks in current chunk, use IV as base for unaligned part */
        unsigned char iv[AES_256_IVSIZE] = {0};
        unsigned char data[AES_256_IVSIZE];
        prepare_iv(data, stream_offset + aligned_size);
        result = cbc_transform(buf_raw + aligned_size,
                               data,
                               AES_256_IVSIZE,
                               iv, key, true);
      }
      if (result) {
        for(size_t i = aligned_size; i < size; i++) {
          *(buf_raw + i) ^= *(input_raw + in_ofs + i);
        }
      }
    }
    if (result) {
      ldout(cct, 25) << "Encrypted " << size << " bytes"<< dendl;
      buf.set_length(size);
      output.append(buf);
    } else {
      ldout(cct, 5) << "Failed to encrypt" << dendl;
    }
    return result;
  }


  bool decrypt(bufferlist& input,
               off_t in_ofs,
               size_t size,
               bufferlist& output,
               off_t stream_offset)
  {
    bool result = false;
    size_t aligned_size = size / AES_256_IVSIZE * AES_256_IVSIZE;
    size_t unaligned_rest_size = size - aligned_size;
    output.clear();
    buffer::ptr buf(aligned_size + AES_256_IVSIZE);
    unsigned char* buf_raw = reinterpret_cast<unsigned char*>(buf.c_str());
    unsigned char* input_raw = reinterpret_cast<unsigned char*>(input.c_str());

    /* decrypt main bulk of data */
    result = cbc_transform(buf_raw,
                           input_raw + in_ofs,
                           aligned_size,
                           stream_offset, key, false);
    if (result && unaligned_rest_size > 0) {
      /* remainder to decrypt */
      if (aligned_size % CHUNK_SIZE > 0) {
        /*use last chunk for unaligned part*/
        unsigned char iv[AES_256_IVSIZE] = {0};
        result = cbc_transform(buf_raw + aligned_size,
                               input_raw + in_ofs + aligned_size - AES_256_IVSIZE,
                               AES_256_IVSIZE,
                               iv, key, true);
      } else {
        /* 0 full blocks in current chunk, use IV as base for unaligned part */
        unsigned char iv[AES_256_IVSIZE] = {0};
        unsigned char data[AES_256_IVSIZE];
        prepare_iv(data, stream_offset + aligned_size);
        result = cbc_transform(buf_raw + aligned_size,
                               data,
                               AES_256_IVSIZE,
                               iv, key, true);
      }
      if (result) {
        for(size_t i = aligned_size; i < size; i++) {
          *(buf_raw + i) ^= *(input_raw + in_ofs + i);
        }
      }
    }
    if (result) {
      ldout(cct, 25) << "Decrypted " << size << " bytes"<< dendl;
      buf.set_length(size);
      output.append(buf);
    } else {
      ldout(cct, 5) << "Failed to decrypt" << dendl;
    }
    return result;
  }


  void prepare_iv(unsigned char (&iv)[AES_256_IVSIZE], off_t offset) {
    off_t index = offset / AES_256_IVSIZE;
    off_t i = AES_256_IVSIZE - 1;
    unsigned int val;
    unsigned int carry = 0;
    while (i>=0) {
      val = (index & 0xff) + IV[i] + carry;
      iv[i] = val;
      carry = val >> 8;
      index = index >> 8;
      i--;
    }
  }
};


std::unique_ptr<BlockCrypt> AES_256_CBC_create(CephContext* cct, const uint8_t* key, size_t len)
{
  auto cbc = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(cct));
  cbc->set_key(key, AES_256_KEYSIZE);
  return cbc;
}


const uint8_t AES_256_CBC::IV[AES_256_CBC::AES_256_IVSIZE] =
    { 'a', 'e', 's', '2', '5', '6', 'i', 'v', '_', 'c', 't', 'r', '1', '3', '3', '7' };


bool AES_256_ECB_encrypt(CephContext* cct,
                         const uint8_t* key,
                         size_t key_size,
                         const uint8_t* data_in,
                         uint8_t* data_out,
                         size_t data_size)
{
  if (key_size == AES_256_KEYSIZE) {
    return evp_sym_transform<AES_256_KEYSIZE, 0 /* no IV in ECB */>(
      cct, EVP_aes_256_ecb(),  data_out, data_in, data_size,
      nullptr /* no IV in ECB */, key, true /* encrypt */);
  } else {
    ldout(cct, 5) << "Key size must be 256 bits long" << dendl;
    return false;
  }
}


RGWGetObj_BlockDecrypt::RGWGetObj_BlockDecrypt(CephContext* cct,
                                               RGWGetObj_Filter* next,
                                               std::unique_ptr<BlockCrypt> crypt):
    RGWGetObj_Filter(next),
    cct(cct),
    crypt(std::move(crypt)),
    enc_begin_skip(0),
    ofs(0),
    end(0),
    cache()
{
  block_size = this->crypt->get_block_size();
}

RGWGetObj_BlockDecrypt::~RGWGetObj_BlockDecrypt() {
}

int RGWGetObj_BlockDecrypt::read_manifest(bufferlist& manifest_bl) {
  parts_len.clear();
  RGWObjManifest manifest;
  if (manifest_bl.length()) {
    auto miter = manifest_bl.cbegin();
    try {
      decode(manifest, miter);
    } catch (buffer::error& err) {
      ldout(cct, 0) << "ERROR: couldn't decode manifest" << dendl;
      return -EIO;
    }
    RGWObjManifest::obj_iterator mi;
    for (mi = manifest.obj_begin(); mi != manifest.obj_end(); ++mi) {
      if (mi.get_cur_stripe() == 0) {
        parts_len.push_back(0);
      }
      parts_len.back() += mi.get_stripe_size();
    }
    if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      for (size_t i = 0; i<parts_len.size(); i++) {
        ldout(cct, 20) << "Manifest part " << i << ", size=" << parts_len[i] << dendl;
      }
    }
  }
  return 0;
}

int RGWGetObj_BlockDecrypt::fixup_range(off_t& bl_ofs, off_t& bl_end) {
  off_t inp_ofs = bl_ofs;
  off_t inp_end = bl_end;
  if (parts_len.size() > 0) {
    off_t in_ofs = bl_ofs;
    off_t in_end = bl_end;

    size_t i = 0;
    while (i<parts_len.size() && (in_ofs >= (off_t)parts_len[i])) {
      in_ofs -= parts_len[i];
      i++;
    }
    //in_ofs is inside block i
    size_t j = 0;
    while (j<(parts_len.size() - 1) && (in_end >= (off_t)parts_len[j])) {
      in_end -= parts_len[j];
      j++;
    }
    //in_end is inside part j, OR j is the last part

    size_t rounded_end = ( in_end & ~(block_size - 1) ) + (block_size - 1);
    if (rounded_end > parts_len[j]) {
      rounded_end = parts_len[j] - 1;
    }

    enc_begin_skip = in_ofs & (block_size - 1);
    ofs = bl_ofs - enc_begin_skip;
    end = bl_end;
    bl_end += rounded_end - in_end;
    bl_ofs = std::min(bl_ofs - enc_begin_skip, bl_end);
  }
  else
  {
    enc_begin_skip = bl_ofs & (block_size - 1);
    ofs = bl_ofs & ~(block_size - 1);
    end = bl_end;
    bl_ofs = bl_ofs & ~(block_size - 1);
    bl_end = ( bl_end & ~(block_size - 1) ) + (block_size - 1);
  }
  ldout(cct, 20) << "fixup_range [" << inp_ofs << "," << inp_end
      << "] => [" << bl_ofs << "," << bl_end << "]" << dendl;
  return 0;
}

int RGWGetObj_BlockDecrypt::process(bufferlist& in, size_t part_ofs, size_t size)
{
  bufferlist data;
  if (!crypt->decrypt(in, 0, size, data, part_ofs)) {
    return -ERR_INTERNAL_ERROR;
  }
  off_t send_size = size - enc_begin_skip;
  if (ofs + enc_begin_skip + send_size > end + 1) {
    send_size = end + 1 - ofs - enc_begin_skip;
  }
  int res = next->handle_data(data, enc_begin_skip, send_size);
  enc_begin_skip = 0;
  ofs += size;
  in.splice(0, size);
  return res;
}

int RGWGetObj_BlockDecrypt::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len, const Span& parent_span) {
  ldout(cct, 25) << "Decrypt " << bl_len << " bytes" << dendl;
  bl.begin(bl_ofs).copy(bl_len, cache);

  int res = 0;
  size_t part_ofs = ofs;
  for (size_t part : parts_len) {
    if (part_ofs >= part) {
      part_ofs -= part;
    } else if (part_ofs + cache.length() >= part) {
      // flush data up to part boundaries, aligned or not
      res = process(cache, part_ofs, part - part_ofs);
      if (res < 0) {
        return res;
      }
      part_ofs = 0;
    } else {
      break;
    }
  }
  // write up to block boundaries, aligned only
  off_t aligned_size = cache.length() & ~(block_size - 1);
  if (aligned_size > 0) {
    res = process(cache, part_ofs, aligned_size);
  }
  return res;
}

/**
 * flush remainder of data to output
 */
int RGWGetObj_BlockDecrypt::flush() {
  ldout(cct, 25) << "Decrypt flushing " << cache.length() << " bytes" << dendl;
  int res = 0;
  size_t part_ofs = ofs;
  for (size_t part : parts_len) {
    if (part_ofs >= part) {
      part_ofs -= part;
    } else if (part_ofs + cache.length() >= part) {
      // flush data up to part boundaries, aligned or not
      res = process(cache, part_ofs, part - part_ofs);
      if (res < 0) {
        return res;
      }
      part_ofs = 0;
    } else {
      break;
    }
  }
  // flush up to block boundaries, aligned or not
  if (cache.length() > 0) {
    res = process(cache, part_ofs, cache.length());
  }
  return res;
}

RGWPutObj_BlockEncrypt::RGWPutObj_BlockEncrypt(CephContext* cct,
                                               rgw::putobj::DataProcessor *next,
                                               std::unique_ptr<BlockCrypt> crypt)
  : Pipe(next),
    cct(cct),
    crypt(std::move(crypt)),
    block_size(this->crypt->get_block_size())
{
}

int RGWPutObj_BlockEncrypt::process(bufferlist&& data, uint64_t logical_offset)
{
  ldout(cct, 25) << "Encrypt " << data.length() << " bytes" << dendl;

  // adjust logical offset to beginning of cached data
  ceph_assert(logical_offset >= cache.length());
  logical_offset -= cache.length();

  const bool flush = (data.length() == 0);
  cache.claim_append(data);

  uint64_t proc_size = cache.length() & ~(block_size - 1);
  if (flush) {
    proc_size = cache.length();
  }
  if (proc_size > 0) {
    bufferlist in, out;
    cache.splice(0, proc_size, &in);
    if (!crypt->encrypt(in, 0, proc_size, out, logical_offset)) {
      return -ERR_INTERNAL_ERROR;
    }
    int r = Pipe::process(std::move(out), logical_offset);
    logical_offset += proc_size;
    if (r < 0)
      return r;
  }

  if (flush) {
    /*replicate 0-sized handle_data*/
    return Pipe::process({}, logical_offset);
  }
  return 0;
}


std::string create_random_key_selector(CephContext * const cct) {
  char random[AES_256_KEYSIZE];
  cct->random()->get_bytes(&random[0], sizeof(random));
  return std::string(random, sizeof(random));
}

static inline void set_attr(map<string, bufferlist>& attrs,
                            const char* key,
                            std::string_view value)
{
  bufferlist bl;
  bl.append(value.data(), value.size());
  attrs[key] = std::move(bl);
}

static inline std::string get_str_attribute(map<string, bufferlist>& attrs,
                                            const char *name)
{
  auto iter = attrs.find(name);
  if (iter == attrs.end()) {
    return {};
  }
  return iter->second.to_str();
}

typedef enum {
  X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM=0,
  X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
  X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
  X_AMZ_SERVER_SIDE_ENCRYPTION,
  X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
  X_AMZ_SERVER_SIDE_ENCRYPTION_LAST
} crypt_option_e;

typedef struct {
  const char* http_header_name;
  const std::string post_part_name;
} crypt_option_names;

static const crypt_option_names crypt_options[] = {
    {"HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM",  "x-amz-server-side-encryption-customer-algorithm"},
    {"HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY",        "x-amz-server-side-encryption-customer-key"},
    {"HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5",    "x-amz-server-side-encryption-customer-key-md5"},
    {"HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION",                     "x-amz-server-side-encryption"},
    {"HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID",      "x-amz-server-side-encryption-aws-kms-key-id"},
};

static std::string_view get_crypt_attribute(
    const RGWEnv* env,
    std::map<std::string,
             RGWPostObj_ObjStore::post_form_part,
             const ltstr_nocase>* parts,
    crypt_option_e option)
{
  static_assert(
      X_AMZ_SERVER_SIDE_ENCRYPTION_LAST == sizeof(crypt_options)/sizeof(*crypt_options),
      "Missing items in crypt_options");
  if (parts != nullptr) {
    auto iter
      = parts->find(crypt_options[option].post_part_name);
    if (iter == parts->end())
      return std::string_view();
    bufferlist& data = iter->second.data;
    std::string_view str = std::string_view(data.c_str(), data.length());
    return rgw_trim_whitespace(str);
  } else {
    const char* hdr = env->get(crypt_options[option].http_header_name, nullptr);
    if (hdr != nullptr) {
      return std::string_view(hdr);
    } else {
      return std::string_view();
    }
  }
}


int rgw_s3_prepare_encrypt(struct req_state* s,
                           std::map<std::string, ceph::bufferlist>& attrs,
                           std::map<std::string,
                                    RGWPostObj_ObjStore::post_form_part,
                                    const ltstr_nocase>* parts,
                           std::unique_ptr<BlockCrypt>* block_crypt,
                           std::map<std::string, std::string>& crypt_http_responses)
{
  int res = 0;
  crypt_http_responses.clear();
  {
    std::string_view req_sse_ca =
        get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM);
    if (! req_sse_ca.empty()) {
      if (req_sse_ca != "AES256") {
        ldout(s->cct, 5) << "ERROR: Invalid value for header "
                         << "x-amz-server-side-encryption-customer-algorithm"
                         << dendl;
        s->err.message = "The requested encryption algorithm is not valid, must be AES256.";
        return -ERR_INVALID_ENCRYPTION_ALGORITHM;
      }
      if (s->cct->_conf->rgw_crypt_require_ssl &&
          !rgw_transport_is_secure(s->cct, *s->info.env)) {
        ldout(s->cct, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
        return -ERR_INVALID_REQUEST;
      }

      std::string key_bin;
      try {
        key_bin = from_base64(
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY) );
      } catch (...) {
        ldout(s->cct, 5) << "ERROR: rgw_s3_prepare_encrypt invalid encryption "
                         << "key which contains character that is not base64 encoded."
                         << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide an appropriate secret key.";
        return -EINVAL;
      }

      if (key_bin.size() != AES_256_CBC::AES_256_KEYSIZE) {
        ldout(s->cct, 5) << "ERROR: invalid encryption key size" << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide an appropriate secret key.";
        return -EINVAL;
      }

      std::string_view keymd5 =
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5);

      std::string keymd5_bin;
      try {
        keymd5_bin = from_base64(keymd5);
      } catch (...) {
        ldout(s->cct, 5) << "ERROR: rgw_s3_prepare_encrypt invalid encryption key "
                         << "md5 which contains character that is not base64 encoded."
                         << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide an appropriate secret key md5.";
        return -EINVAL;
      }

      if (keymd5_bin.size() != CEPH_CRYPTO_MD5_DIGESTSIZE) {
        ldout(s->cct, 5) << "ERROR: Invalid key md5 size" << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide an appropriate secret key md5.";
        return -EINVAL;
      }

      MD5 key_hash;
      unsigned char key_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
      key_hash.Update(reinterpret_cast<const unsigned char*>(key_bin.c_str()), key_bin.size());
      key_hash.Final(key_hash_res);

      if (memcmp(key_hash_res, keymd5_bin.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) {
        ldout(s->cct, 5) << "ERROR: Invalid key md5 hash" << dendl;
        s->err.message = "The calculated MD5 hash of the key did not match the hash that was provided.";
        return -EINVAL;
      }

      set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-C-AES256");
      set_attr(attrs, RGW_ATTR_CRYPT_KEYMD5, keymd5_bin);

      if (block_crypt) {
        auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s->cct));
        aes->set_key(reinterpret_cast<const uint8_t*>(key_bin.c_str()), AES_256_KEYSIZE);
        *block_crypt = std::move(aes);
      }

      crypt_http_responses["x-amz-server-side-encryption-customer-algorithm"] = "AES256";
      crypt_http_responses["x-amz-server-side-encryption-customer-key-MD5"] = std::string(keymd5);
      return 0;
    } else {
      std::string_view customer_key =
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY);
      if (!customer_key.empty()) {
        ldout(s->cct, 5) << "ERROR: SSE-C encryption request is missing the header "
                         << "x-amz-server-side-encryption-customer-algorithm"
                         << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide a valid encryption algorithm.";
        return -EINVAL;
      }

      std::string_view customer_key_md5 =
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5);
      if (!customer_key_md5.empty()) {
        ldout(s->cct, 5) << "ERROR: SSE-C encryption request is missing the header "
                         << "x-amz-server-side-encryption-customer-algorithm"
                         << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide a valid encryption algorithm.";
        return -EINVAL;
      }
    }

    /* AMAZON server side encryption with KMS (key management service) */
    std::string_view req_sse =
        get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION);
    if (! req_sse.empty()) {

      if (s->cct->_conf->rgw_crypt_require_ssl &&
          !rgw_transport_is_secure(s->cct, *s->info.env)) {
        ldout(s->cct, 5) << "ERROR: insecure request, rgw_crypt_require_ssl is set" << dendl;
        return -ERR_INVALID_REQUEST;
      }

      if (req_sse == "aws:kms") {
	std::string_view key_id =
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID);
	if (key_id.empty()) {
	  ldout(s->cct, 5) << "ERROR: not provide a valid key id" << dendl;
	  s->err.message = "Server Side Encryption with KMS managed key requires "
	    "HTTP header x-amz-server-side-encryption-aws-kms-key-id";
	  return -ERR_INVALID_ACCESS_KEY;
	}
	/* try to retrieve actual key */
	std::string key_selector = create_random_key_selector(s->cct);
	std::string actual_key;
	res = get_actual_key_from_kms(s->cct, key_id, key_selector, actual_key);
	if (res != 0) {
	  ldout(s->cct, 5) << "ERROR: failed to retrieve actual key from key_id: " << key_id << dendl;
	  s->err.message = "Failed to retrieve the actual key, kms-keyid: " + std::string(key_id);
	  return res;
	}
	if (actual_key.size() != AES_256_KEYSIZE) {
	  ldout(s->cct, 5) << "ERROR: key obtained from key_id:" <<
            key_id << " is not 256 bit size" << dendl;
	  s->err.message = "KMS provided an invalid key for the given kms-keyid.";
	  return -ERR_INVALID_ACCESS_KEY;
	}
	set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-KMS");
	set_attr(attrs, RGW_ATTR_CRYPT_KEYID, key_id);
	set_attr(attrs, RGW_ATTR_CRYPT_KEYSEL, key_selector);

	if (block_crypt) {
	  auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s->cct));
	  aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);
	  *block_crypt = std::move(aes);
	}
	actual_key.replace(0, actual_key.length(), actual_key.length(), '\000');

	crypt_http_responses["x-amz-server-side-encryption"] = "aws:kms";
	crypt_http_responses["x-amz-server-side-encryption-aws-kms-key-id"] = std::string(key_id);
	return 0;
      } else if (req_sse == "AES256") {
	/* if a default encryption key was provided, we will use it for SSE-S3 */
      } else {
        ldout(s->cct, 5) << "ERROR: Invalid value for header x-amz-server-side-encryption"
                         << dendl;
        s->err.message = "Server Side Encryption with KMS managed key requires "
	  "HTTP header x-amz-server-side-encryption : aws:kms or AES256";
        return -EINVAL;
      }
    } else {
      /* x-amz-server-side-encryption not present or empty */
      std::string_view key_id =
	get_crypt_attribute(s->info.env, parts,
			    X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID);
      if (!key_id.empty()) {
        ldout(s->cct, 5) << "ERROR: SSE-KMS encryption request is missing the header "
                         << "x-amz-server-side-encryption"
                         << dendl;
        s->err.message = "Server Side Encryption with KMS managed key requires "
                         "HTTP header x-amz-server-side-encryption : aws:kms";
        return -EINVAL;
      }
    }

    /* no other encryption mode, check if default encryption is selected */
    if (s->cct->_conf->rgw_crypt_default_encryption_key != "") {
      std::string master_encryption_key;
      try {
        master_encryption_key = from_base64(s->cct->_conf->rgw_crypt_default_encryption_key);
      } catch (...) {
        ldout(s->cct, 5) << "ERROR: rgw_s3_prepare_encrypt invalid default encryption key "
                         << "which contains character that is not base64 encoded."
                         << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide an appropriate secret key.";
        return -EINVAL;
      }

      if (master_encryption_key.size() != 256 / 8) {
        ldout(s->cct, 0) << "ERROR: failed to decode 'rgw crypt default encryption key' to 256 bit string" << dendl;
        /* not an error to return; missing encryption does not inhibit processing */
        return 0;
      }

      set_attr(attrs, RGW_ATTR_CRYPT_MODE, "RGW-AUTO");
      std::string key_selector = create_random_key_selector(s->cct);
      set_attr(attrs, RGW_ATTR_CRYPT_KEYSEL, key_selector);

      uint8_t actual_key[AES_256_KEYSIZE];
      if (AES_256_ECB_encrypt(s->cct,
                              reinterpret_cast<const uint8_t*>(master_encryption_key.c_str()), AES_256_KEYSIZE,
                              reinterpret_cast<const uint8_t*>(key_selector.c_str()),
                              actual_key, AES_256_KEYSIZE) != true) {
        ::ceph::crypto::zeroize_for_security(actual_key, sizeof(actual_key));
        return -EIO;
      }
      if (block_crypt) {
        auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s->cct));
        aes->set_key(reinterpret_cast<const uint8_t*>(actual_key), AES_256_KEYSIZE);
        *block_crypt = std::move(aes);
      }
      ::ceph::crypto::zeroize_for_security(actual_key, sizeof(actual_key));
      return 0;
    }
  }
  /*no encryption*/
  return 0;
}


int rgw_s3_prepare_decrypt(struct req_state* s,
                       map<string, bufferlist>& attrs,
                       std::unique_ptr<BlockCrypt>* block_crypt,
                       std::map<std::string, std::string>& crypt_http_responses)
{
  int res = 0;
  std::string stored_mode = get_str_attribute(attrs, RGW_ATTR_CRYPT_MODE);
  ldout(s->cct, 15) << "Encryption mode: " << stored_mode << dendl;

  const char *req_sse = s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION", NULL);
  if (nullptr != req_sse && (s->op == OP_GET || s->op == OP_HEAD)) {
    return -ERR_INVALID_REQUEST;
  }

  if (stored_mode == "SSE-C-AES256") {
    if (s->cct->_conf->rgw_crypt_require_ssl &&
        !rgw_transport_is_secure(s->cct, *s->info.env)) {
      ldout(s->cct, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
      return -ERR_INVALID_REQUEST;
    }
    const char *req_cust_alg =
        s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM", NULL);

    if (nullptr == req_cust_alg)  {
      ldout(s->cct, 5) << "ERROR: Request for SSE-C encrypted object missing "
                       << "x-amz-server-side-encryption-customer-algorithm"
                       << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide a valid encryption algorithm.";
      return -EINVAL;
    } else if (strcmp(req_cust_alg, "AES256") != 0) {
      ldout(s->cct, 5) << "ERROR: The requested encryption algorithm is not valid, must be AES256." << dendl;
      s->err.message = "The requested encryption algorithm is not valid, must be AES256.";
      return -ERR_INVALID_ENCRYPTION_ALGORITHM;
    }

    std::string key_bin;
    try {
      key_bin = from_base64(s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY", ""));
    } catch (...) {
      ldout(s->cct, 5) << "ERROR: rgw_s3_prepare_decrypt invalid encryption key "
                       << "which contains character that is not base64 encoded."
                       << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key.";
      return -EINVAL;
    }

    if (key_bin.size() != AES_256_CBC::AES_256_KEYSIZE) {
      ldout(s->cct, 5) << "ERROR: Invalid encryption key size" << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key.";
      return -EINVAL;
    }

    std::string keymd5 =
        s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5", "");
    std::string keymd5_bin;
    try {
      keymd5_bin = from_base64(keymd5);
    } catch (...) {
      ldout(s->cct, 5) << "ERROR: rgw_s3_prepare_decrypt invalid encryption key md5 "
                       << "which contains character that is not base64 encoded."
                       << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key md5.";
      return -EINVAL;
    }


    if (keymd5_bin.size() != CEPH_CRYPTO_MD5_DIGESTSIZE) {
      ldout(s->cct, 5) << "ERROR: Invalid key md5 size " << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key md5.";
      return -EINVAL;
    }

    MD5 key_hash;
    uint8_t key_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
    key_hash.Update(reinterpret_cast<const unsigned char*>(key_bin.c_str()), key_bin.size());
    key_hash.Final(key_hash_res);

    if ((memcmp(key_hash_res, keymd5_bin.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) ||
        (get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYMD5) != keymd5_bin)) {
      s->err.message = "The calculated MD5 hash of the key did not match the hash that was provided.";
      return -EINVAL;
    }
    auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s->cct));
    aes->set_key(reinterpret_cast<const uint8_t*>(key_bin.c_str()), AES_256_CBC::AES_256_KEYSIZE);
    if (block_crypt) *block_crypt = std::move(aes);

    crypt_http_responses["x-amz-server-side-encryption-customer-algorithm"] = "AES256";
    crypt_http_responses["x-amz-server-side-encryption-customer-key-MD5"] = keymd5;
    return 0;
  }

  if (stored_mode == "SSE-KMS") {
    if (s->cct->_conf->rgw_crypt_require_ssl &&
        !rgw_transport_is_secure(s->cct, *s->info.env)) {
      ldout(s->cct, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
      return -ERR_INVALID_REQUEST;
    }
    /* try to retrieve actual key */
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    std::string key_selector = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYSEL);
    std::string actual_key;
    res = get_actual_key_from_kms(s->cct, key_id, key_selector, actual_key);
    if (res != 0) {
      ldout(s->cct, 10) << "ERROR: failed to retrieve actual key from key_id: " << key_id << dendl;
      s->err.message = "Failed to retrieve the actual key, kms-keyid: " + key_id;
      return res;
    }
    if (actual_key.size() != AES_256_KEYSIZE) {
      ldout(s->cct, 0) << "ERROR: key obtained from key_id:" <<
          key_id << " is not 256 bit size" << dendl;
      s->err.message = "KMS provided an invalid key for the given kms-keyid.";
      return -ERR_INVALID_ACCESS_KEY;
    }

    auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s->cct));
    aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);
    actual_key.replace(0, actual_key.length(), actual_key.length(), '\000');
    if (block_crypt) *block_crypt = std::move(aes);

    crypt_http_responses["x-amz-server-side-encryption"] = "aws:kms";
    crypt_http_responses["x-amz-server-side-encryption-aws-kms-key-id"] = key_id;
    return 0;
  }

  if (stored_mode == "RGW-AUTO") {
    std::string master_encryption_key;
    try {
      master_encryption_key = from_base64(std::string(s->cct->_conf->rgw_crypt_default_encryption_key));
    } catch (...) {
      ldout(s->cct, 5) << "ERROR: rgw_s3_prepare_decrypt invalid default encryption key "
                       << "which contains character that is not base64 encoded."
                       << dendl;
      s->err.message = "The default encryption key is not valid base64.";
      return -EINVAL;
    }

    if (master_encryption_key.size() != 256 / 8) {
      ldout(s->cct, 0) << "ERROR: failed to decode 'rgw crypt default encryption key' to 256 bit string" << dendl;
      return -EIO;
    }
    std::string attr_key_selector = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYSEL);
    if (attr_key_selector.size() != AES_256_CBC::AES_256_KEYSIZE) {
      ldout(s->cct, 0) << "ERROR: missing or invalid " RGW_ATTR_CRYPT_KEYSEL << dendl;
      return -EIO;
    }
    uint8_t actual_key[AES_256_KEYSIZE];
    if (AES_256_ECB_encrypt(s->cct,
                            reinterpret_cast<const uint8_t*>(master_encryption_key.c_str()),
                            AES_256_KEYSIZE,
                            reinterpret_cast<const uint8_t*>(attr_key_selector.c_str()),
                            actual_key, AES_256_KEYSIZE) != true) {
      ::ceph::crypto::zeroize_for_security(actual_key, sizeof(actual_key));
      return -EIO;
    }
    auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s->cct));
    aes->set_key(actual_key, AES_256_KEYSIZE);
    ::ceph::crypto::zeroize_for_security(actual_key, sizeof(actual_key));
    if (block_crypt) *block_crypt = std::move(aes);
    return 0;
  }
  /*no decryption*/
  return 0;
}
