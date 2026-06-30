// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/**
 * Crypto filters for Put/Post/Get operations.
 */

#include <string_view>
#include <atomic>
#include <mutex>

#include <rgw/rgw_op.h>
#include <rgw/rgw_crypt.h>
#include <auth/Crypto.h>
#include <rgw/rgw_b64.h>
#include <rgw/rgw_rest_s3.h>
#include "include/ceph_assert.h"
#include "include/function2.hpp"
#include "crypto/crypto_accel.h"
#include "crypto/crypto_plugin.h"
#include "rgw/rgw_kms.h"
#include "rgw_range_projection.h"
#include "rgw/rgw_process_env.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/error/error.h"
#include "rapidjson/error/en.h"
#include <unicode/normalizer2.h>	// libicu

#include <openssl/evp.h>
#include "common/ceph_crypto.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

/**
 * Ensure CryptoAccel GCM constants match RGW GCM constants.
 * Prevents silent IV/tag size mismatches between acceleration layer and RGW.
 */
static_assert(CryptoAccel::AES_GCM_IV_SIZE == AES_256_GCM_IV_SIZE,
              "CryptoAccel and RGW GCM IV sizes must match");
static_assert(CryptoAccel::AES_GCM_TAGSIZE == AEAD_TAG_SIZE,
              "CryptoAccel and RGW GCM tag sizes must match");

using namespace std;
using namespace rgw;

template<typename M>
class canonical_char_sorter {
private:
    const DoutPrefixProvider *dpp;
    const icu::Normalizer2* normalizer;
    CephContext *cct;
public:
    canonical_char_sorter(const DoutPrefixProvider *dpp, CephContext *cct) : dpp(dpp), cct(cct) {
        UErrorCode status = U_ZERO_ERROR;
        normalizer = icu::Normalizer2::getNFCInstance(status);
        if (U_FAILURE(status)) {
            ldpp_dout(this->dpp, -1) << "ERROR: can't get nfc instance, error = " << status << dendl;
            normalizer = 0;
        }
    }
    bool compare_helper (const M *, const M *);
    bool make_string_canonical(rapidjson::Value &,
        rapidjson::Document::AllocatorType&);
};

template<typename M>
bool
canonical_char_sorter<M>::compare_helper (const M*a, const M*b)
{
    UErrorCode status = U_ZERO_ERROR;
    const std::string as{a->name.GetString(), a->name.GetStringLength()},
        bs{b->name.GetString(), b->name.GetStringLength()};
    icu::UnicodeString aw{icu::UnicodeString::fromUTF8(as)}, bw{icu::UnicodeString::fromUTF8(bs)};
    int32_t afl{ aw.countChar32()}, bfl{bw.countChar32()};
    std::u32string af, bf;
    af.resize(afl); bf.resize(bfl);
    auto *astr{af.c_str()}, *bstr{bf.c_str()};
    aw.toUTF32((int32_t*)astr, afl, status);
    bw.toUTF32((int32_t*)bstr, bfl, status);
    bool r{af < bf};
    return r;
}

template<typename M>
bool
canonical_char_sorter<M>::make_string_canonical (rapidjson::Value &v, rapidjson::Document::AllocatorType&a)
{
    UErrorCode status = U_ZERO_ERROR;
    const std::string as{v.GetString(), v.GetStringLength()};

    if (!normalizer)
        return false;
    const icu::UnicodeString aw{icu::UnicodeString::fromUTF8(as)};
    icu::UnicodeString an{normalizer->normalize(aw, status)};
    if (U_FAILURE(status)) {
        ldpp_dout(this->dpp, 5) << "conversion error; code=" << status <<
            " on string " << as << dendl;
        return false;
    }
    std::string ans;
    an.toUTF8String(ans);
    v.SetString(ans.c_str(), ans.length(), a);
    return true;
}

typedef
rapidjson::GenericMember<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<> >
MyMember;

template<typename H>
bool
sort_and_write(rapidjson::Value &d, H &writer, canonical_char_sorter<MyMember>& ccs)
{
    bool r;
    switch(d.GetType()) {
    case rapidjson::kObjectType: {
    struct comparer {
        canonical_char_sorter<MyMember> &r;
        comparer(canonical_char_sorter<MyMember> &r) : r(r) {};
        bool operator()(const MyMember*a, const MyMember*b) {
            return r.compare_helper(a,b);
        }
    } cmp_functor{ccs};
        if (!(r = writer.StartObject()))
            break;
        std::vector<MyMember*> q;
        for (auto &m: d.GetObject())
            q.push_back(&m);
        std::sort(q.begin(), q.end(), cmp_functor);
        for (auto m: q) {
            assert(m->name.IsString());
            if (!(r = writer.Key(m->name.GetString(), m->name.GetStringLength())))
                goto Done;
            if (!(r = sort_and_write(m->value, writer, ccs)))
                goto Done;
        }
        r = writer.EndObject();
        break; }
    case rapidjson::kArrayType:
        if (!(r = writer.StartArray()))
            break;
        for (auto &v: d.GetArray()) {
            if (!(r = sort_and_write(v, writer, ccs)))
                goto Done;
        }
        r = writer.EndArray();
        break;
    default:
        r = d.Accept(writer);
        break;
    }
Done:
    return r;
}

enum struct mec_option {
empty = 0, number_ok = 1
};

enum struct mec_error {
success = 0, conversion, number
};

mec_error
make_everything_canonical(rapidjson::Value &d, rapidjson::Document::AllocatorType&a, canonical_char_sorter<MyMember>& ccs, mec_option f = mec_option::empty )
{
    mec_error r;
    switch(d.GetType()) {
    case rapidjson::kObjectType:
        for (auto &m: d.GetObject()) {
            assert(m.name.IsString());
            if (!ccs.make_string_canonical(m.name, a)) {
                r = mec_error::conversion;
                goto Error;
            }
            if ((r = make_everything_canonical(m.value, a, ccs, f)) != mec_error::success)
                goto Error;
        }
        break;
    case rapidjson::kArrayType:
        for (auto &v: d.GetArray()) {
            if ((r = make_everything_canonical(v, a, ccs, f)) != mec_error::success)
                goto Error;
        }
        break;
    case rapidjson::kStringType:
        if (!ccs.make_string_canonical(d, a)) {
            r = mec_error::conversion;
            goto Error;
        }
        break;
    case rapidjson::kNumberType:
        if (static_cast<int>(f) & static_cast<int>(mec_option::number_ok))
            break;
        r = mec_error::number;
        goto Error;
    default:
        break;
    }
    r = mec_error::success;
Error:
    return r;
}

bool
add_object_to_context(rgw_obj &obj, rapidjson::Document &d)
{
    ARN a{obj};
    const char aws_s3_arn[] { "aws:s3:arn" };
    std::string as{a.to_string()};
    rapidjson::Document::AllocatorType &allocator { d.GetAllocator() };
    rapidjson::Value name, val;

    if (!d.IsObject())
        return false;
    if (d.HasMember(aws_s3_arn))
        return true;
    val.SetString(as.c_str(), as.length(), allocator);
    name.SetString(aws_s3_arn, sizeof aws_s3_arn - 1, allocator);
    d.AddMember(name, val, allocator);
    return true;
}

static inline const std::string &
get_tenant_or_id(req_state *s)
{
    const std::string &tenant{ s->user->get_tenant() };
    if (!tenant.empty()) return tenant;
    return s->user->get_id().id;
}

int
make_canonical_context(req_state *s,
    std::string_view &context,
    std::string &cooked_context)
{
    rapidjson::Document d;
    bool b = false;
mec_option options {
//mec_option::number_ok :	SEE BOTTOM OF FILE
mec_option::empty };
    rgw_obj obj;
    std::ostringstream oss;
    canonical_char_sorter<MyMember> ccs{s, s->cct};

    obj.bucket.tenant = get_tenant_or_id(s);
    obj.bucket.name = s->bucket->get_name();
    obj.key.name = s->object->get_name();
    std::string iline;
    rapidjson::Document::AllocatorType &allocator { d.GetAllocator() };

    try {
	iline = rgw::from_base64(context);
    } catch (const std::exception& e) {
	oss << "bad context: " << e.what();
	s->err.message = oss.str();
        return -ERR_INVALID_REQUEST;
    }
    rapidjson::StringStream isw(iline.c_str());
    if (!iline.length())
        d.SetObject();
//    else if (qflag)		SEE BOTTOM OF FILE
//       d.ParseStream<rapidjson::kParseNumbersAsStringsFlag>(isw);
    else
        d.ParseStream<rapidjson::kParseFullPrecisionFlag>(isw);
    if (isw.Tell() != iline.length()) {
        oss << "bad context: did not consume all of input: @ "
	    << isw.Tell();
	s->err.message = oss.str();
        return -ERR_INVALID_REQUEST;
    }
    if (d.HasParseError()) {
        oss << "bad context: parse error: @ " << d.GetErrorOffset()
	    << " " << rapidjson::GetParseError_En(d.GetParseError());
	s->err.message = oss.str();
        return -ERR_INVALID_REQUEST;
    }
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    if (!add_object_to_context(obj, d)) {
	ldpp_dout(s, -1) << "ERROR: can't add default value to context" << dendl;
	s->err.message = "context: internal error adding defaults";
        return -ERR_INVALID_REQUEST;
    }
    b = make_everything_canonical(d, allocator, ccs, options) == mec_error::success;
    if (!b) {
	ldpp_dout(s, -1) << "ERROR: can't make canonical json <"
	    << context << ">" << dendl;
	s->err.message = "context: can't make canonical";
        return -ERR_INVALID_REQUEST;
    }
    b = sort_and_write(d, writer, ccs);
    if (!b) {
	    ldpp_dout(s, 5) << "format error <" << context
	    << ">: partial.results=" << buf.GetString() << dendl;
	s->err.message = "unable to reformat json";
        return -ERR_INVALID_REQUEST;
    }
    cooked_context = rgw::to_base64(buf.GetString());
    return 0;
}


CryptoAccelRef get_crypto_accel(const DoutPrefixProvider* dpp, CephContext *cct, const size_t chunk_size, const size_t max_requests)
{
  CryptoAccelRef ca_impl = nullptr;
  stringstream ss;
  PluginRegistry *reg = cct->get_plugin_registry();
  string crypto_accel_type = cct->_conf->plugin_crypto_accelerator;

  CryptoPlugin *factory = dynamic_cast<CryptoPlugin*>(reg->get_with_load("crypto", crypto_accel_type));
  if (factory == nullptr) {
    ldpp_dout(dpp, -1) << __func__ << " cannot load crypto accelerator of type " << crypto_accel_type << dendl;
    return nullptr;
  }
  int err = factory->factory(&ca_impl, &ss, chunk_size, max_requests);
  if (err) {
    ldpp_dout(dpp, -1) << __func__ << " factory return error " << err <<
        " with description: " << ss.str() << dendl;
  }
  return ca_impl;
}


template <std::size_t KeySizeV, std::size_t IvSizeV>
static inline
bool evp_sym_transform(const DoutPrefixProvider* dpp,
                       CephContext* const cct,
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
    ldpp_dout(dpp, 5) << "EVP: failed to 1st initialization stage" << dendl;
    return false;
  }

  // we want to support ciphers that don't use IV at all like AES-256-ECB
  if constexpr (static_cast<bool>(IvSizeV)) {
    ceph_assert(EVP_CIPHER_CTX_iv_length(pctx.get()) == IvSizeV);
    ceph_assert(EVP_CIPHER_CTX_block_size(pctx.get()) == IvSizeV);
  }
  ceph_assert(EVP_CIPHER_CTX_key_length(pctx.get()) == KeySizeV);

  if (1 != EVP_CipherInit_ex(pctx.get(), nullptr, nullptr, key, iv, encrypt)) {
    ldpp_dout(dpp, 5) << "EVP: failed to 2nd initialization stage" << dendl;
    return false;
  }

  // disable padding
  if (1 != EVP_CIPHER_CTX_set_padding(pctx.get(), 0)) {
    ldpp_dout(dpp, 5) << "EVP: cannot disable PKCS padding" << dendl;
    return false;
  }

  // operate!
  int written = 0;
  ceph_assert(size <= static_cast<size_t>(std::numeric_limits<int>::max()));
  if (1 != EVP_CipherUpdate(pctx.get(), out, &written, in, size)) {
    ldpp_dout(dpp, 5) << "EVP: EVP_CipherUpdate failed" << dendl;
    return false;
  }

  int finally_written = 0;
  static_assert(sizeof(*out) == 1);
  if (1 != EVP_CipherFinal_ex(pctx.get(), out + written, &finally_written)) {
    ldpp_dout(dpp, 5) << "EVP: EVP_CipherFinal_ex failed" << dendl;
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
  static const size_t QAT_MIN_SIZE = 65536;
  const DoutPrefixProvider* dpp;
private:
  static const uint8_t IV[AES_256_IVSIZE];
  CephContext* cct;
  uint8_t key[AES_256_KEYSIZE];
public:
  explicit AES_256_CBC(const DoutPrefixProvider* dpp, CephContext* cct): dpp(dpp), cct(cct) {
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
      dpp, cct, EVP_aes_256_cbc(), out, in, size, iv, key, encrypt);
  }

  bool cbc_transform(unsigned char* out,
                     const unsigned char* in,
                     size_t size,
                     off_t stream_offset,
                     const unsigned char (&key)[AES_256_KEYSIZE],
                     bool encrypt,
                     optional_yield y)
  {
    static std::atomic<bool> failed_to_get_crypto(false);
    CryptoAccelRef crypto_accel;
    if (! failed_to_get_crypto.load())
    {
      static size_t max_requests = g_ceph_context->_conf->rgw_thread_pool_size;
      crypto_accel = get_crypto_accel(this->dpp, cct, CHUNK_SIZE, max_requests);
      if (!crypto_accel)
        failed_to_get_crypto = true;
    }
    bool result = false;
    static std::string accelerator = cct->_conf->plugin_crypto_accelerator;
    if (accelerator == "crypto_qat" && crypto_accel != nullptr && size >= QAT_MIN_SIZE) {
      // now, batch mode is only for QAT plugin
      size_t iv_num = size / CHUNK_SIZE;
      if (size % CHUNK_SIZE) ++iv_num;
      auto iv = new unsigned char[iv_num][AES_256_IVSIZE];
      for (size_t offset = 0, i = 0; offset < size; offset += CHUNK_SIZE, i++) {
        prepare_iv(iv[i], stream_offset + offset);
      }
      if (encrypt) {
        result = crypto_accel->cbc_encrypt_batch(out, in, size, iv, key, y);
      } else {
        result = crypto_accel->cbc_decrypt_batch(out, in, size, iv, key, y);
      }
      delete[] iv;
    }
    if (result == false) {
      // If QAT don't have free instance, we can fall back to this
      result = true;
      unsigned char iv[AES_256_IVSIZE];
      for (size_t offset = 0; result && (offset < size); offset += CHUNK_SIZE) {
        size_t process_size = offset + CHUNK_SIZE <= size ? CHUNK_SIZE : size - offset;
        prepare_iv(iv, stream_offset + offset);
        if (crypto_accel != nullptr && accelerator != "crypto_qat") {
          if (encrypt) {
            result = crypto_accel->cbc_encrypt(out + offset, in + offset,
                                              process_size, iv, key, y);
          } else {
            result = crypto_accel->cbc_decrypt(out + offset, in + offset,
                                              process_size, iv, key, y);
          }
        } else {
          result = cbc_transform(
              out + offset, in + offset, process_size,
              iv, key, encrypt);
        }
      }
    }
    return result;
  }


  bool encrypt(bufferlist& input,
               off_t in_ofs,
               size_t size,
               bufferlist& output,
               off_t stream_offset,
               optional_yield y)
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
                           stream_offset, key, true, y);
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
      ldpp_dout(this->dpp, 25) << "Encrypted " << size << " bytes"<< dendl;
      buf.set_length(size);
      output.append(buf);
    } else {
      ldpp_dout(this->dpp, 5) << "Failed to encrypt" << dendl;
    }
    return result;
  }


  bool decrypt(bufferlist& input,
               off_t in_ofs,
               size_t size,
               bufferlist& output,
               off_t stream_offset,
               optional_yield y)
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
                           stream_offset, key, false, y);
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
      ldpp_dout(this->dpp, 25) << "Decrypted " << size << " bytes"<< dendl;
      buf.set_length(size);
      output.append(buf);
    } else {
      ldpp_dout(this->dpp, 5) << "Failed to decrypt" << dendl;
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


std::unique_ptr<BlockCrypt> AES_256_CBC_create(const DoutPrefixProvider* dpp, CephContext* cct, const uint8_t* key, size_t len)
{
  auto cbc = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(dpp, cct));
  cbc->set_key(key, AES_256_KEYSIZE);
  return cbc;
}


const uint8_t AES_256_CBC::IV[AES_256_CBC::AES_256_IVSIZE] =
    { 'a', 'e', 's', '2', '5', '6', 'i', 'v', '_', 'c', 't', 'r', '1', '3', '3', '7' };


/**
 * AES-256-GCM encryption implementation
 * Provides authenticated encryption with 96-bit IVs and 128-bit authentication tags
 */
class AES_256_GCM : public BlockCrypt {
public:
  static const size_t AES_256_KEYSIZE = 256 / 8;        // 32 bytes
  static const size_t AES_256_IVSIZE = 96 / 8;          // 12 bytes (GCM standard)
  static const size_t GCM_TAG_SIZE = 128 / 8;           // 16 bytes
  static const size_t CHUNK_SIZE = 4096;
  static const size_t ENCRYPTED_CHUNK_SIZE = CHUNK_SIZE + GCM_TAG_SIZE; // 4112

  const DoutPrefixProvider* dpp;
private:
  CephContext* cct;
  uint8_t key[AES_256_KEYSIZE];
  uint8_t base_key[AES_256_KEYSIZE];  // For SSE-C: stores object key before part derivation
  bool has_base_key = false;          // True if base_key is valid (SSE-C with key derivation)
  uint8_t salt[AES_256_GCM_SALT_SIZE];
  bool salt_initialized = false;
  uint32_t part_number_ = 0;  // For multipart: ensures unique IVs across parts
  bool part_salt_applied_ = false;
  std::once_flag gcm_accel_init_once;
  CryptoAccelRef gcm_accel;

public:
  explicit AES_256_GCM(const DoutPrefixProvider* dpp, CephContext* cct)
    : dpp(dpp), cct(cct) {
    memset(salt, 0, AES_256_GCM_SALT_SIZE);
  }

  ~AES_256_GCM() {
    ::ceph::crypto::zeroize_for_security(key, AES_256_KEYSIZE);
    ::ceph::crypto::zeroize_for_security(base_key, AES_256_KEYSIZE);
    ::ceph::crypto::zeroize_for_security(salt, AES_256_GCM_SALT_SIZE);
  }

  bool set_key(const uint8_t* _key, size_t key_size) {
    if (key_size != AES_256_KEYSIZE) {
      return false;
    }
    memcpy(key, _key, AES_256_KEYSIZE);
    return true;
  }

  /**
   * Generate a random salt for HMAC-based key derivation.
   * Called during encryption to create a unique salt per object.
   */
  bool generate_salt() {
    cct->random()->get_bytes(reinterpret_cast<char*>(salt), AES_256_GCM_SALT_SIZE);
    salt_initialized = true;
    return true;
  }

  /**
   * Set the salt from a stored value.
   * Called during decryption to restore the object's salt.
   */
  bool set_salt(const uint8_t* _salt, size_t len) {
    if (len != AES_256_GCM_SALT_SIZE) {
      return false;
    }
    memcpy(salt, _salt, AES_256_GCM_SALT_SIZE);
    salt_initialized = true;
    return true;
  }

  /**
   * Get the salt for storage in object attributes.
   */
  std::string get_salt() const {
    return std::string(reinterpret_cast<const char*>(salt), AES_256_GCM_SALT_SIZE);
  }

  bool is_salt_initialized() const {
    return salt_initialized;
  }

  /*
   * For a multipart part, re-derive the part key from base_key with the salt.
   * has_base_key holds for all GCM modes.
   */
  void set_part_number(uint32_t part_number,
                       std::string_view part_salt = {}) override {
    this->part_number_ = part_number;
    this->part_salt_applied_ = !part_salt.empty();

    if (has_base_key && part_number > 0) {
      memcpy(this->key, this->base_key, AES_256_KEYSIZE);
      derive_part_key(part_number, part_salt);
    } else if (has_base_key && part_number == 0) {
      // Part 0 means single-part or init - use base key directly
      memcpy(this->key, this->base_key, AES_256_KEYSIZE);
    }
  }

  /**
   * Derive object-specific encryption key from user key + object identity.
   * This provides cryptographic binding between ciphertext and object identity:
   * - If object is moved/renamed at RADOS level → wrong key → decrypt fails
   *
   * Key derivation formula:
   *   ObjectKey = HMAC-SHA256(key, salt || domain || bucket_id || object)
   *
   * @param user_key The user-provided encryption key (32 bytes)
   * @param key_len Length of user_key (must be 32)
   * @param bucket Bucket name
   * @param object Object name
   * @param part_number Part number for multipart uploads (0 for non-multipart)
   * @param domain Domain separator string for algorithm binding (e.g., "SSE-C-GCM", "RGW-AUTO-GCM")
   * @return true on success
   */
  bool derive_object_key(
      const uint8_t* user_key,
      size_t key_len,
      const std::string& bucket_id,
      const std::string& object,
      uint32_t part_number,
      const std::string& domain = "SSE-C-GCM")
  {
    if (key_len != AES_256_KEYSIZE) {
      ldpp_dout(dpp, 0) << "ERROR: derive_object_key: invalid key length "
                        << key_len << ", expected " << AES_256_KEYSIZE << dendl;
      return false;
    }
    if (!salt_initialized) {
      ldpp_dout(dpp, 0) << "ERROR: derive_object_key: salt not initialized" << dendl;
      return false;
    }

    // HMAC-SHA256(user_key, salt || domain || bucket_id || object)
    try {
      ceph::crypto::HMACSHA256 hmac(user_key, key_len);

      // Helper to encode length as 4-byte big-endian and update HMAC
      // Length-prefixing prevents ambiguous concatenation attacks
      auto hmac_update_with_length = [&hmac](const std::string_view& data) {
        uint32_t len = static_cast<uint32_t>(data.size());
        uint8_t len_buf[4] = {
          static_cast<uint8_t>((len >> 24) & 0xFF),
          static_cast<uint8_t>((len >> 16) & 0xFF),
          static_cast<uint8_t>((len >> 8) & 0xFF),
          static_cast<uint8_t>(len & 0xFF)
        };
        hmac.Update(len_buf, 4);
        if (!data.empty()) {
          hmac.Update(reinterpret_cast<const uint8_t*>(data.data()), data.size());
        }
      };

      // Include salt in key derivation
      hmac.Update(salt, AES_256_GCM_SALT_SIZE);

      // Domain separator (length-prefixed for consistency)
      hmac_update_with_length(domain);

      // Include bucket_id/object with length prefixes
      // Format: salt || len(domain) || domain || len(bucket_id) || bucket_id || len(object) || object
      hmac_update_with_length(bucket_id);
      hmac_update_with_length(object);

      hmac.Final(this->key);
    } catch (const ceph::crypto::DigestException& e) {
      ldpp_dout(dpp, 0) << "ERROR: derive_object_key: HMAC failed: " << e.what() << dendl;
      ::ceph::crypto::zeroize_for_security(this->key, AES_256_KEYSIZE);
      return false;
    }

    // Store part_number for IV derivation (ensures unique IVs across parts)
    this->part_number_ = part_number;

    // Save base key for later part key derivation (needed for multipart GET)
    // This allows set_part_number() to re-derive the correct part key
    memcpy(this->base_key, this->key, AES_256_KEYSIZE);
    this->has_base_key = true;

    ldpp_dout(dpp, 20) << "derive_object_key: derived key for bucket_id=" << bucket_id
                       << " object=" << object
                       << " part_number=" << part_number << dendl;

    // For multipart, derive part-specific key
    if (part_number > 0) {
      return derive_part_key(part_number);
    }
    return true;
  }

  /*
   * PartKey = HMAC-SHA256(ObjectKey, BE32(part_number) || part_salt).
   * Binds the key to the part number, and with a non-empty salt to the upload
   * so re-uploading a part can't reuse (key, IV).
   */
  bool derive_part_key(uint32_t part_number, std::string_view part_salt = {}) {
    // Encode part number as big-endian 4 bytes
    uint8_t part_bytes[4];
    part_bytes[0] = (part_number >> 24) & 0xFF;
    part_bytes[1] = (part_number >> 16) & 0xFF;
    part_bytes[2] = (part_number >> 8) & 0xFF;
    part_bytes[3] = part_number & 0xFF;

    uint8_t derived[AES_256_KEYSIZE];

    try {
      ceph::crypto::HMACSHA256 hmac(this->key, AES_256_KEYSIZE);
      hmac.Update(part_bytes, 4);
      if (!part_salt.empty()) {
        hmac.Update(reinterpret_cast<const uint8_t*>(part_salt.data()), part_salt.size());
      }
      hmac.Final(derived);
    } catch (const ceph::crypto::DigestException& e) {
      ldpp_dout(dpp, 0) << "ERROR: derive_part_key: HMAC failed: " << e.what() << dendl;
      ::ceph::crypto::zeroize_for_security(derived, AES_256_KEYSIZE);
      ::ceph::crypto::zeroize_for_security(this->key, AES_256_KEYSIZE);
      return false;
    }

    memcpy(this->key, derived, AES_256_KEYSIZE);
    ::ceph::crypto::zeroize_for_security(derived, AES_256_KEYSIZE);

    ldpp_dout(dpp, 20) << "derive_part_key: derived key for part " << part_number << dendl;
    return true;
  }

  size_t get_block_size() override {
    return CHUNK_SIZE;
  }

  size_t get_encrypted_block_size() override {
    return ENCRYPTED_CHUNK_SIZE;
  }

  // Encode chunk index as 8-byte big-endian AAD for chunk reordering protection.
  static void encode_chunk_aad(uint8_t (&aad)[8], uint64_t chunk_index) {
    uint64_t be = boost::endian::native_to_big(chunk_index);
    memcpy(aad, &be, sizeof(be));
  }

  CryptoAccelRef get_gcm_accel()
  {
    static std::atomic<bool> failed_to_get_crypto_gcm(false);
    if (failed_to_get_crypto_gcm.load(std::memory_order_acquire)) {
      return nullptr;
    }

    std::call_once(gcm_accel_init_once, [this]() {
      static const size_t max_requests = g_ceph_context->_conf->rgw_thread_pool_size;
      gcm_accel = get_crypto_accel(dpp, cct, CHUNK_SIZE, max_requests);
      if (!gcm_accel) {
        failed_to_get_crypto_gcm.store(true, std::memory_order_release);
      }
    });

    return gcm_accel;
  }

  // GCM transform using OpenSSL EVP
  bool gcm_transform(unsigned char* out,
                     const unsigned char* in,
                     size_t size,
                     const unsigned char (&iv)[AES_256_IVSIZE],
                     const unsigned char (&key)[AES_256_KEYSIZE],
                     unsigned char* tag,
                     uint64_t chunk_index,
                     bool encrypt,
                     EVP_CIPHER_CTX* evp_ctx = nullptr)
  {
    using pctx_t = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>;
    pctx_t owned_ctx{nullptr, EVP_CIPHER_CTX_free};
    EVP_CIPHER_CTX* ctx = evp_ctx;

    if (!ctx) {
      owned_ctx.reset(EVP_CIPHER_CTX_new());
      ctx = owned_ctx.get();
      if (!ctx) {
        ldpp_dout(dpp, 5) << "EVP: failed to create cipher context" << dendl;
        return false;
      }
      if (encrypt) {
        if (1 != EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(),
                                     nullptr, nullptr, nullptr)) {
          ldpp_dout(dpp, 5) << "EVP: failed to initialize GCM" << dendl;
          return false;
        }
      } else {
        if (1 != EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(),
                                     nullptr, nullptr, nullptr)) {
          ldpp_dout(dpp, 5) << "EVP: failed to initialize GCM" << dendl;
          return false;
        }
      }
    }

    uint8_t aad[8];
    encode_chunk_aad(aad, chunk_index);

    if (encrypt) {
      if (1 != EVP_EncryptInit_ex(ctx, nullptr, nullptr, key, iv)) {
        ldpp_dout(dpp, 5) << "EVP: failed to set key/IV" << dendl;
        return false;
      }

      int aad_len = 0;
      if (1 != EVP_EncryptUpdate(ctx, nullptr, &aad_len, aad, sizeof(aad))) {
        ldpp_dout(dpp, 5) << "EVP: failed to set AAD" << dendl;
        return false;
      }

      int written = 0;
      ceph_assert(size <= CHUNK_SIZE);
      if (1 != EVP_EncryptUpdate(ctx, out, &written, in, size)) {
        ldpp_dout(dpp, 5) << "EVP: EncryptUpdate failed" << dendl;
        return false;
      }

      int finally_written = 0;
      if (1 != EVP_EncryptFinal_ex(ctx, out + written, &finally_written)) {
        ldpp_dout(dpp, 5) << "EVP: EncryptFinal_ex failed" << dendl;
        return false;
      }

      if (1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG,
                                    GCM_TAG_SIZE, tag)) {
        ldpp_dout(dpp, 5) << "EVP: failed to get GCM tag" << dendl;
        return false;
      }

      return (written + finally_written) == static_cast<int>(size);
    } else {
      if (1 != EVP_DecryptInit_ex(ctx, nullptr, nullptr, key, iv)) {
        ldpp_dout(dpp, 5) << "EVP: failed to set key/IV" << dendl;
        return false;
      }

      int aad_len = 0;
      if (1 != EVP_DecryptUpdate(ctx, nullptr, &aad_len, aad, sizeof(aad))) {
        ldpp_dout(dpp, 5) << "EVP: failed to set AAD" << dendl;
        return false;
      }

      int written = 0;
      ceph_assert(size <= CHUNK_SIZE);
      if (1 != EVP_DecryptUpdate(ctx, out, &written, in, size)) {
        ldpp_dout(dpp, 5) << "EVP: DecryptUpdate failed" << dendl;
        return false;
      }

      if (1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG,
                                    GCM_TAG_SIZE, const_cast<unsigned char*>(tag))) {
        ldpp_dout(dpp, 5) << "EVP: failed to set GCM tag" << dendl;
        return false;
      }

      int finally_written = 0;
      if (1 != EVP_DecryptFinal_ex(ctx, out + written, &finally_written)) {
        ldpp_dout(dpp, 5) << "EVP: DecryptFinal_ex failed - authentication failure" << dendl;
        memset(out, 0, size);
        return false;
      }

      return (written + finally_written) == static_cast<int>(size);
    }
  }

  /**
   * GCM transform with hardware acceleration support.
   *
   * When a hardware accelerator is available, use it exclusively. If the
   * accelerated operation fails, that indicates a real crypto error (e.g.,
   * authentication tag mismatch on decrypt). Only fall back to the OpenSSL
   * EVP path when no accelerator is available.
   */
  bool gcm_transform(unsigned char* out,
                     const unsigned char* in,
                     size_t size,
                     const unsigned char (&iv)[AES_256_IVSIZE],
                     const unsigned char (&key)[AES_256_KEYSIZE],
                     unsigned char* tag,
                     uint64_t chunk_index,
                     bool encrypt,
                     optional_yield y,
                     const CryptoAccelRef& accel,
                     EVP_CIPHER_CTX* evp_ctx = nullptr)
  {
    if (accel) {
      uint8_t aad[8];
      encode_chunk_aad(aad, chunk_index);

      if (encrypt) {
        return accel->gcm_encrypt(out, in, size, iv, key,
                                  aad, sizeof(aad), tag, y);
      } else {
        return accel->gcm_decrypt(out, in, size, iv, key,
                                  aad, sizeof(aad), tag, y);
      }
    }

    return gcm_transform(out, in, size, iv, key, tag, chunk_index, encrypt, evp_ctx);
  }

  bool encrypt(bufferlist& input,
               off_t in_ofs,
               size_t size,
               bufferlist& output,
               off_t stream_offset,
               optional_yield y) override
  {
    output.clear();

    // write-path nonce-uniqueness guards: fresh per-part salt + chunk-aligned offset
    if (part_number_ > 0 && !part_salt_applied_) {
      ldpp_dout(dpp, 0) << "GCM: multipart part " << part_number_
                        << " missing per-part salt; refusing to encrypt" << dendl;
      return false;
    }
    if (stream_offset % static_cast<off_t>(CHUNK_SIZE) != 0) {
      ldpp_dout(dpp, 0) << "GCM: stream_offset " << stream_offset
                        << " not chunk-aligned (" << CHUNK_SIZE << ")" << dendl;
      return false;
    }

    // Calculate output size: each CHUNK_SIZE plaintext becomes CHUNK_SIZE + GCM_TAG_SIZE
    size_t num_full_chunks = size / CHUNK_SIZE;
    size_t remainder = size % CHUNK_SIZE;
    size_t output_size = num_full_chunks * ENCRYPTED_CHUNK_SIZE;
    if (remainder > 0) {
      output_size += remainder + GCM_TAG_SIZE;
    }

    buffer::ptr buf(buffer::create_aligned(output_size, 64));
    unsigned char* buf_raw = reinterpret_cast<unsigned char*>(buf.c_str());

    if (in_ofs < 0 || static_cast<uint64_t>(in_ofs) > input.length()) {
      ldpp_dout(dpp, 5) << "GCM: invalid input offset " << in_ofs << dendl;
      return false;
    }

    // Resolve accelerator once for all chunks
    CryptoAccelRef accel = get_gcm_accel();

    // Pre-create EVP context for the fallback path
    using pctx_t = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>;
    pctx_t evp_ctx{nullptr, EVP_CIPHER_CTX_free};
    if (!accel) {
      evp_ctx.reset(EVP_CIPHER_CTX_new());
      if (!evp_ctx) {
        ldpp_dout(dpp, 5) << "EVP: failed to create cipher context" << dendl;
        return false;
      }
      if (1 != EVP_EncryptInit_ex(evp_ctx.get(), EVP_aes_256_gcm(),
                                   nullptr, nullptr, nullptr)) {
        ldpp_dout(dpp, 5) << "EVP: failed to initialize GCM" << dendl;
        return false;
      }
    }

    // Linearize input if fragmented (bounded by rgw_max_chunk_size, typically 4MB)
    const unsigned char* input_raw =
        reinterpret_cast<const unsigned char*>(input.c_str() + in_ofs);

    size_t out_pos = 0;

    // Initialize IV cursor: zero IV base once, then emit + increment per chunk
    iv_cursor cursor;
    if (!init_iv_cursor(cursor, stream_offset)) {
      return false;
    }

    // Process full chunks
    for (size_t offset = 0; offset < num_full_chunks * CHUNK_SIZE; offset += CHUNK_SIZE) {
      unsigned char iv[AES_256_IVSIZE];
      cursor.emit(iv);
      const unsigned char* input_ptr = input_raw + offset;

      unsigned char* ciphertext = buf_raw + out_pos;
      unsigned char* tag = buf_raw + out_pos + CHUNK_SIZE;

      if (!gcm_transform(ciphertext, input_ptr, CHUNK_SIZE,
                         iv, key, tag, cursor.chunk_index, true, y, accel, evp_ctx.get())) {
        ldpp_dout(dpp, 5) << "Failed to encrypt chunk at offset " << offset << dendl;
        return false;
      }

      cursor.advance();
      out_pos += ENCRYPTED_CHUNK_SIZE;
    }

    // Process remainder (if any)
    if (remainder > 0) {
      unsigned char iv[AES_256_IVSIZE];
      cursor.emit(iv);
      const unsigned char* input_ptr = input_raw + num_full_chunks * CHUNK_SIZE;

      unsigned char* ciphertext = buf_raw + out_pos;
      unsigned char* tag = buf_raw + out_pos + remainder;

      if (!gcm_transform(ciphertext, input_ptr,
                         remainder, iv, key, tag, cursor.chunk_index, true, y, accel, evp_ctx.get())) {
        ldpp_dout(dpp, 5) << "Failed to encrypt final chunk" << dendl;
        return false;
      }
    }

    ldpp_dout(dpp, 25) << "GCM: Encrypted " << size << " bytes to "
                       << output_size << " bytes" << dendl;
    buf.set_length(output_size);
    output.append(buf);
    return true;
  }

  bool decrypt(bufferlist& input,
               off_t in_ofs,
               size_t size,
               bufferlist& output,
               off_t stream_offset,
               optional_yield y) override
  {
    output.clear();

    // Input is organized as encrypted chunks (ciphertext + tag)
    size_t num_full_chunks = size / ENCRYPTED_CHUNK_SIZE;
    size_t remainder = size % ENCRYPTED_CHUNK_SIZE;
    size_t output_size = num_full_chunks * CHUNK_SIZE;

    if (remainder > 0) {
      if (remainder <= GCM_TAG_SIZE) {
        ldpp_dout(dpp, 5) << "GCM: Invalid encrypted data size: " << size << dendl;
        return false;
      }
      output_size += remainder - GCM_TAG_SIZE;
    }

    buffer::ptr buf(buffer::create_aligned(output_size, 64));
    unsigned char* buf_raw = reinterpret_cast<unsigned char*>(buf.c_str());

    if (in_ofs < 0 || static_cast<uint64_t>(in_ofs) > input.length()) {
      ldpp_dout(dpp, 5) << "GCM: invalid input offset " << in_ofs << dendl;
      return false;
    }

    // Resolve accelerator once for all chunks
    CryptoAccelRef accel = get_gcm_accel();

    // Pre-create EVP context for the fallback path
    using pctx_t = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>;
    pctx_t evp_ctx{nullptr, EVP_CIPHER_CTX_free};
    if (!accel) {
      evp_ctx.reset(EVP_CIPHER_CTX_new());
      if (!evp_ctx) {
        ldpp_dout(dpp, 5) << "EVP: failed to create cipher context" << dendl;
        return false;
      }
      if (1 != EVP_DecryptInit_ex(evp_ctx.get(), EVP_aes_256_gcm(),
                                   nullptr, nullptr, nullptr)) {
        ldpp_dout(dpp, 5) << "EVP: failed to initialize GCM" << dendl;
        return false;
      }
    }

    // Linearize input if fragmented (bounded by rgw_max_chunk_size, typically 4MB)
    const unsigned char* input_raw =
        reinterpret_cast<const unsigned char*>(input.c_str() + in_ofs);

    size_t out_pos = 0;

    // Initialize IV cursor: zero IV base once, then emit + increment per chunk
    iv_cursor cursor;
    if (!init_iv_cursor(cursor, stream_offset)) {
      return false;
    }

    // Process full chunks
    for (size_t i = 0; i < num_full_chunks; i++) {
      unsigned char iv[AES_256_IVSIZE];
      cursor.emit(iv);
      const unsigned char* chunk_ptr = input_raw + i * ENCRYPTED_CHUNK_SIZE;

      if (!gcm_transform(buf_raw + out_pos, chunk_ptr, CHUNK_SIZE,
                         iv, key, const_cast<unsigned char*>(chunk_ptr + CHUNK_SIZE),
                         cursor.chunk_index, false, y, accel, evp_ctx.get())) {
        ldpp_dout(dpp, 5) << "GCM: Failed to decrypt chunk " << i
                          << " - authentication failed" << dendl;
        return false;
      }

      cursor.advance();
      out_pos += CHUNK_SIZE;
    }

    // Process remainder (if any)
    if (remainder > 0) {
      size_t plaintext_size = remainder - GCM_TAG_SIZE;
      unsigned char iv[AES_256_IVSIZE];
      cursor.emit(iv);
      const unsigned char* chunk_ptr = input_raw + num_full_chunks * ENCRYPTED_CHUNK_SIZE;

      if (!gcm_transform(buf_raw + out_pos, chunk_ptr, plaintext_size,
                         iv, key, const_cast<unsigned char*>(chunk_ptr + plaintext_size),
                         cursor.chunk_index, false, y, accel, evp_ctx.get())) {
        ldpp_dout(dpp, 5) << "GCM: Failed to decrypt final chunk - authentication failed" << dendl;
        return false;
      }
    }

    ldpp_dout(dpp, 25) << "GCM: Decrypted " << size << " bytes to "
                       << output_size << " bytes" << dendl;
    buf.set_length(output_size);
    output.append(buf);
    return true;
  }

  /*
   * IV cursor for efficient sequential IV generation.
   * Emits a 96-bit IV and increments per chunk.
   *
   * IV layout (96 bits):
   *   - Upper 32 bits: part_number (supports up to 4B parts; S3 limit is 10K)
   *   - Lower 64 bits: chunk_index (supports up to 64 EB per part at 4KB chunks)
   */
  struct iv_cursor {
    uint32_t hi;           // part_number
    uint64_t lo;           // chunk_index
    uint64_t chunk_index;  // for AAD

    void emit(unsigned char (&iv)[AES_256_IVSIZE]) const {
      uint32_t be_hi = boost::endian::native_to_big(hi);
      uint64_t be_lo = boost::endian::native_to_big(lo);
      memcpy(iv, &be_hi, sizeof(be_hi));
      memcpy(iv + 4, &be_lo, sizeof(be_lo));
    }

    void advance() {
      lo++;
      chunk_index++;
    }
  };

  bool init_iv_cursor(iv_cursor& cursor, off_t stream_offset) {
    ceph_assert(salt_initialized);
    cursor.hi = part_number_;
    cursor.lo = stream_offset / CHUNK_SIZE;
    cursor.chunk_index = cursor.lo;
    return true;
  }
};


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
                                                const uint8_t* salt,
                                                size_t salt_len,
                                                uint32_t part_number)
{
  // Validate key_len to prevent OOB read if caller passes smaller buffer
  if (key_len != AES_256_GCM::AES_256_KEYSIZE) {
    ldpp_dout(dpp, 5) << "AES_256_GCM_create: invalid key size " << key_len
                      << ", expected " << AES_256_GCM::AES_256_KEYSIZE << dendl;
    return nullptr;
  }

  auto gcm = std::unique_ptr<AES_256_GCM>(new AES_256_GCM(dpp, cct));
  if (!gcm->set_key(key, key_len)) {
    return nullptr;
  }

  // Set part_number for multipart IV derivation (ensures unique IVs across parts)
  gcm->set_part_number(part_number);

  if (salt != nullptr) {
    // Decryption path: use the provided stored salt
    if (!gcm->set_salt(salt, salt_len)) {
      ldpp_dout(dpp, 5) << "AES_256_GCM_create: invalid salt size " << salt_len << dendl;
      return nullptr;
    }
  } else {
    // Encryption path: generate a random salt
    gcm->generate_salt();
  }

  return gcm;
}


/**
 * Retrieve the salt from a BlockCrypt instance for storage.
 * Returns empty string if the BlockCrypt is not an AES_256_GCM instance.
 */
std::string AES_256_GCM_get_salt(BlockCrypt* block_crypt)
{
  auto* gcm = dynamic_cast<AES_256_GCM*>(block_crypt);
  if (gcm && gcm->is_salt_initialized()) {
    return gcm->get_salt();
  }
  return {};
}

/**
 * Test helper: derive object key for a BlockCrypt instance.
 * Returns true on success.
 */
bool AES_256_GCM_derive_object_key(BlockCrypt* block_crypt,
                                    const uint8_t* user_key,
                                    size_t key_len,
                                    const std::string& bucket_id,
                                    const std::string& object,
                                    uint32_t part_number,
                                    const std::string& domain)
{
  auto* gcm = dynamic_cast<AES_256_GCM*>(block_crypt);
  if (!gcm) {
    return false;
  }
  return gcm->derive_object_key(user_key, key_len, bucket_id, object,
                                 part_number, domain);
}


bool AES_256_ECB_encrypt(const DoutPrefixProvider* dpp,
                         CephContext* cct,
                         const uint8_t* key,
                         size_t key_size,
                         const uint8_t* data_in,
                         uint8_t* data_out,
                         size_t data_size)
{
  if (key_size == AES_256_KEYSIZE) {
    return evp_sym_transform<AES_256_KEYSIZE, 0 /* no IV in ECB */>(
      dpp, cct, EVP_aes_256_ecb(),  data_out, data_in, data_size,
      nullptr /* no IV in ECB */, key, true /* encrypt */);
  } else {
    ldpp_dout(dpp, 5) << "Key size must be 256 bits long" << dendl;
    return false;
  }
}


RGWGetObj_BlockDecrypt::RGWGetObj_BlockDecrypt(const DoutPrefixProvider *dpp,
                                               CephContext* cct,
                                               RGWGetObj_Filter* next,
                                               std::unique_ptr<BlockCrypt> crypt,
                                               std::vector<size_t> parts_len,
                                               std::vector<std::pair<uint32_t, std::string>> part_keys,
                                               off_t encrypted_total_size,
                                               bool has_compression,
                                               optional_yield y)
    :
    RGWGetObj_Filter(next),
    dpp(dpp),
    cct(cct),
    crypt(std::move(crypt)),
    enc_begin_skip(0),
    ofs(0),
    enc_ofs(0),
    end(0),
    encrypted_total_size(encrypted_total_size),
    has_compression(has_compression),
    cache(),
    y(y),
    parts_len(std::move(parts_len)),
    part_keys(std::move(part_keys)),
    current_part_num(0)
{
  block_size = this->crypt->get_block_size();
  encrypted_block_size = this->crypt->get_encrypted_block_size();

  /**
   * Sanity check: when BOTH part_keys and parts_len are populated, they must
   * match in size. A mismatch indicates data corruption or a bug.
   *
   * When parts_len is empty (e.g., GET ?partNumber=N where CRYPT_PARTS is
   * intentionally skipped and the part object has no manifest), we can only
   * trust a single fallback part.
   */
  if (!this->part_keys.empty() && !this->parts_len.empty() &&
      this->part_keys.size() != this->parts_len.size()) {
    ldpp_dout(dpp, 0) << "ERROR: part_keys.size()=" << this->part_keys.size()
                      << " != parts_len.size()=" << this->parts_len.size()
                      << " - possible data corruption" << dendl;
    this->part_keys.clear();
  }
  if (this->parts_len.empty() && this->part_keys.size() > 1) {
    ldpp_dout(dpp, 0) << "ERROR: part_keys.size()=" << this->part_keys.size()
                      << " but parts_len is empty - cannot map part boundaries"
                      << dendl;
    this->part_keys.clear();
  }

  // Initialize with first part's key if multipart
  if (!this->part_keys.empty()) {
    current_part_num = this->part_keys[0].first;
    this->crypt->set_part_number(current_part_num, this->part_keys[0].second);
  }
}

RGWGetObj_BlockDecrypt::~RGWGetObj_BlockDecrypt() {
}

int RGWGetObj_BlockDecrypt::read_manifest_parts(const DoutPrefixProvider *dpp,
                                                const bufferlist& manifest_bl,
                                                std::vector<size_t>& parts_len)
{
  RGWObjManifest manifest;
  if (manifest_bl.length()) {
    auto miter = manifest_bl.cbegin();
    try {
      decode(manifest, miter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: couldn't decode manifest" << dendl;
      return -EIO;
    }
    RGWObjManifest::obj_iterator mi;
    for (mi = manifest.obj_begin(dpp); mi != manifest.obj_end(dpp); ++mi) {
      if (mi.get_cur_stripe() == 0) {
        parts_len.push_back(0);
      }
      parts_len.back() += mi.get_stripe_size();
    }
    for (size_t i = 0; i<parts_len.size(); i++) {
      ldpp_dout(dpp, 20) << "Manifest part " << i << ", size=" << parts_len[i] << dendl;
    }
  }
  return 0;
}

int RGWGetObj_BlockDecrypt::fixup_range(off_t& bl_ofs, off_t& bl_end) {
  off_t inp_ofs = bl_ofs;
  off_t inp_end = bl_end;

  // If compression is present, let the decompression filter remap the range
  // from plaintext to compressed domain first. We'll then map compressed -> encrypted.
  if (has_compression && next) {
    int r = next->fixup_range(bl_ofs, bl_end);
    if (r < 0) {
      return r;
    }
  }

  // Save the pre-encryption input offsets for filter state
  off_t pre_enc_ofs = bl_ofs;
  off_t pre_enc_end = bl_end;

  auto range = project_encrypt_range(bl_ofs, bl_end, block_size,
                               encrypted_block_size, encrypted_total_size,
                               parts_len);

  // Initialize filter state from projection result.
  // ofs/end are plaintext-space offsets used by process().
  enc_begin_skip = range.enc_begin_skip;
  ofs = pre_enc_ofs - enc_begin_skip;
  end = pre_enc_end;
  enc_ofs = range.ofs;

  bl_ofs = range.ofs;
  bl_end = range.end;

  ldpp_dout(this->dpp, 20) << "fixup_range [" << inp_ofs << "," << inp_end
      << "] => [" << bl_ofs << "," << bl_end << "]"
      << " (block_size=" << block_size
      << ", encrypted_block_size=" << encrypted_block_size << ")" << dendl;

  if (next && !has_compression)
    return next->fixup_range(bl_ofs, bl_end);

  return 0;
}

int RGWGetObj_BlockDecrypt::process(bufferlist& in, size_t part_ofs, size_t size)
{
  bufferlist data;
  if (!crypt->decrypt(in, 0, size, data, part_ofs, y)) {
    return -ERR_INTERNAL_ERROR;
  }

  /**
   * data.length() is the decrypted (plaintext) size.
   * For GCM: data.length() < size (auth tags removed during decryption)
   * For CBC: data.length() == size (no overhead)
   */
  off_t decrypted_size = data.length();

  off_t send_size = decrypted_size - enc_begin_skip;
  if (ofs + enc_begin_skip + send_size > end + 1) {
    send_size = end + 1 - ofs - enc_begin_skip;
  }
  int res = next->handle_data(data, enc_begin_skip, send_size);
  enc_begin_skip = 0;
  ofs += decrypted_size;  // Advance plaintext position
  enc_ofs += size;        // Advance encrypted position (for part boundary tracking)
  in.splice(0, size);     // Remove encrypted data from input buffer
  return res;
}

int RGWGetObj_BlockDecrypt::process_part_boundaries(size_t& plain_part_ofs_out) {
  size_t enc_part_ofs = enc_ofs;
  size_t plain_part_ofs = ofs;
  const bool is_multipart = !part_keys.empty();
  uint32_t part_idx = 0;
  int res = 0;

  for (size_t part : parts_len) {
    // Get actual S3 part number + salt from the attribute (not calculated!)
    uint32_t this_part_num = 0;
    std::string_view this_salt;
    if (is_multipart && part_idx < part_keys.size()) {
      this_part_num = part_keys[part_idx].first;
      this_salt = part_keys[part_idx].second;
    }

    if (enc_part_ofs >= part) {
      // Past this part entirely, skip to next
      enc_part_ofs -= part;
      plain_part_ofs -= encrypted_to_plaintext_size(part);
      part_idx++;
    } else if (enc_part_ofs + cache.length() >= part) {
      // Ensure cipher has correct part number
      if (is_multipart && current_part_num != this_part_num) {
        current_part_num = this_part_num;
        crypt->set_part_number(current_part_num, this_salt);
      }

      // Data crosses part boundary - process up to boundary
      size_t enc_bytes_this_part = part - enc_part_ofs;
      res = process(cache, plain_part_ofs, enc_bytes_this_part);
      if (res < 0) {
        return res;
      }

      // Move to next part
      part_idx++;
      uint32_t next_part_num = 0;
      std::string_view next_salt;
      if (is_multipart && part_idx < part_keys.size()) {
        next_part_num = part_keys[part_idx].first;
        next_salt = part_keys[part_idx].second;
      }
      if (is_multipart && part_idx < parts_len.size() && current_part_num != next_part_num) {
        current_part_num = next_part_num;
        crypt->set_part_number(current_part_num, next_salt);
      }

      enc_part_ofs = 0;
      plain_part_ofs = 0;
    } else {
      // Ensure cipher has correct part number
      if (is_multipart && current_part_num != this_part_num) {
        current_part_num = this_part_num;
        crypt->set_part_number(current_part_num, this_salt);
      }
      break;
    }
  }

  plain_part_ofs_out = plain_part_ofs;
  return 0;
}

int RGWGetObj_BlockDecrypt::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
  ldpp_dout(this->dpp, 25) << "Decrypt " << bl_len << " bytes"
      << " (encrypted_block_size=" << encrypted_block_size << ")" << dendl;
  bl.begin(bl_ofs).copy(bl_len, cache);

  size_t plain_part_ofs;
  int res = process_part_boundaries(plain_part_ofs);
  if (res < 0) return res;

  // Process up to encrypted block boundaries
  off_t aligned_size = (cache.length() / encrypted_block_size) * encrypted_block_size;
  if (aligned_size > 0) {
    res = process(cache, plain_part_ofs, aligned_size);
  }
  return res;
}

/**
 * flush remainder of data to output
 */
int RGWGetObj_BlockDecrypt::flush() {
  ldpp_dout(this->dpp, 25) << "Decrypt flushing " << cache.length() << " bytes" << dendl;

  size_t plain_part_ofs;
  int res = process_part_boundaries(plain_part_ofs);
  if (res < 0) return res;

  // Flush remaining data (possibly unaligned final block)
  if (cache.length() > 0) {
    res = process(cache, plain_part_ofs, cache.length());
    if (res < 0) return res;
  }

  return next ? next->flush() : 0;
}

RGWPutObj_BlockEncrypt::RGWPutObj_BlockEncrypt(const DoutPrefixProvider *dpp,
                                               CephContext* cct,
                                               rgw::sal::DataProcessor *next,
                                               std::unique_ptr<BlockCrypt> crypt,
                                               optional_yield y)
  : Pipe(next),
    dpp(dpp),
    cct(cct),
    crypt(std::move(crypt)),
    block_size(this->crypt->get_block_size()),
    y(y)
{
}

int RGWPutObj_BlockEncrypt::process(bufferlist&& data, uint64_t logical_offset)
{
  ldpp_dout(this->dpp, 25) << "Encrypt " << data.length() << " bytes" << dendl;

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
    if (!crypt->encrypt(in, 0, proc_size, out, logical_offset, y)) {
      return -ERR_INTERNAL_ERROR;
    }
    // For size-expanding ciphers (GCM), out.length() > proc_size
    // Use encrypted_offset for downstream writes, not plaintext logical_offset
    int r = Pipe::process(std::move(out), encrypted_offset);
    encrypted_offset += out.length();
    if (r < 0)
      return r;
  }

  if (flush) {
    /*replicate 0-sized handle_data*/
    return Pipe::process({}, encrypted_offset);
  }
  return 0;
}


std::string create_random_key_selector(CephContext * const cct) {
  char random[AES_256_KEYSIZE];
  cct->random()->get_bytes(&random[0], sizeof(random));
  return std::string(random, sizeof(random));
}

typedef enum {
  X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM=0,
  X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
  X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
  X_AMZ_SERVER_SIDE_ENCRYPTION,
  X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID,
  X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT,
  X_AMZ_SERVER_SIDE_ENCRYPTION_LAST
} crypt_option_e;
struct crypt_option_names {
  const std::string post_part_name;
};

static const crypt_option_names crypt_options[] = {
    {  "x-amz-server-side-encryption-customer-algorithm"},
    {        "x-amz-server-side-encryption-customer-key"},
    {    "x-amz-server-side-encryption-customer-key-md5"},
    {                     "x-amz-server-side-encryption"},
    {      "x-amz-server-side-encryption-aws-kms-key-id"},
    {             "x-amz-server-side-encryption-context"},
};

struct CryptAttributes {
  meta_map_t &x_meta_map;

  CryptAttributes(req_state *s)
    : x_meta_map(s->info.crypt_attribute_map) {
  }

  std::string_view get(crypt_option_e option)
  {
    static_assert(
	X_AMZ_SERVER_SIDE_ENCRYPTION_LAST == sizeof(crypt_options)/sizeof(*crypt_options),
	"Missing items in crypt_options");
    auto hdr { x_meta_map.find(crypt_options[option].post_part_name) };
    if (hdr != x_meta_map.end()) {
      return std::string_view(hdr->second);
    } else {
      return std::string_view();
    }
  }
};

std::string fetch_bucket_key_id(req_state *s)
{
  auto kek_iter = s->bucket_attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_KEY_ID);
  if (kek_iter == s->bucket_attrs.end())
    return std::string();
  std::string a_key { kek_iter->second.to_str() };
  // early code appends a nul; pretend that didn't happen
  auto l { a_key.length() };
  if (l > 0 && a_key[l-1] == '\0') {
    a_key.resize(--l);
  }
  return a_key;
}

const std::string cant_expand_key{ "\uFFFD" };
std::string expand_key_name(req_state *s, const std::string_view&t)
{
  std::string r;
  size_t i, j;
  for (i = 0;;) {
    i = t.find('%', (j = i));
    if (i != j) {
      if (i == std::string_view::npos)
        r.append( t.substr(j) );
      else
        r.append( t.substr(j, i-j) );
    }
    if (i == std::string_view::npos) {
      break;
    }
    if (t[i+1] == '%') {
      r.append("%");
      i += 2;
      continue;
    }
    if (t.compare(i+1, 9, "bucket_id") == 0) {
      r.append(s->bucket->get_marker());
      i += 10;
      continue;
    }
    if (t.compare(i+1, 8, "owner_id") == 0) {
      r.append(std::visit(fu2::overload(
          [] (const rgw_user& user_id) -> const std::string& {
            return user_id.id;
          },
          [] (const rgw_account_id& account_id) -> const std::string& {
            return account_id;
          }), s->bucket->get_info().owner));
      i += 9;
      continue;
    }
    return cant_expand_key;
  }
  return r;
}

static int get_sse_s3_bucket_key(req_state *s, optional_yield y,
                                 std::string &key_id)
{
  int res;
  std::string saved_key;

  key_id = expand_key_name(s, s->cct->_conf->rgw_crypt_sse_s3_key_template);

  if (key_id == cant_expand_key) {
    ldpp_dout(s, 5) << "ERROR: unable to expand key_id " <<
      s->cct->_conf->rgw_crypt_sse_s3_key_template << " on bucket" << dendl;
    s->err.message = "Server side error - unable to expand key_id";
    return -EINVAL;
  }

  saved_key = fetch_bucket_key_id(s);
  if (saved_key != "") {
    ldpp_dout(s, 5) << "Found KEK ID: " << key_id << dendl;
  }
  if (saved_key != key_id) {
    res = create_sse_s3_bucket_key(s, key_id, y);
    if (res != 0) {
      return res;
    }
    bufferlist key_id_bl;
    key_id_bl.append(key_id.c_str(), key_id.length());
    for (int count = 0; count < 15; ++count) {
      rgw::sal::Attrs attrs = s->bucket->get_attrs();
      attrs[RGW_ATTR_BUCKET_ENCRYPTION_KEY_ID] = key_id_bl;
      res = s->bucket->merge_and_store_attrs(s, attrs, s->yield);
      if (res != -ECANCELED) {
        break;
      }
      res = s->bucket->try_refresh_info(s, nullptr, s->yield);
      if (res != 0) {
        break;
      }
    }
    if (res != 0) {
      ldpp_dout(s, 5) << "ERROR: unable to save new key_id on bucket" << dendl;
      s->err.message = "Server side error - unable to save key_id";
      return res;
    }
  }
  return 0;
}


/**
 * Generate random salt for HMAC-based key derivation and store in attributes.
 */
static std::string generate_gcm_salt(
    req_state* s,
    std::map<std::string, ceph::bufferlist>& attrs)
{
  std::string salt(AES_256_GCM_SALT_SIZE, '\0');
  s->cct->random()->get_bytes(salt.data(), AES_256_GCM_SALT_SIZE);
  set_attr(attrs, RGW_ATTR_CRYPT_SALT, salt);
  return salt;
}

/**
 * Store plaintext size for GCM objects (needed for size accounting).
 * Note: Only set when content_length is actually known (> 0).
 * For chunked uploads without x-amz-decoded-content-length, content_length
 * is 0 and we must NOT set ORIGINAL_SIZE here - it will be set later in
 * RGWPutObj::execute() after all data is processed.
 */
static void set_gcm_plaintext_size(
    req_state* s,
    std::map<std::string, ceph::bufferlist>& attrs,
    bool is_copy)
{
  if (s->content_length > 0 && !is_copy) {
    set_attr(attrs, RGW_ATTR_CRYPT_ORIGINAL_SIZE,
             std::to_string(s->content_length));
  }
}

/**
 * Retrieve and validate stored salt for HMAC-based key derivation.
 * Returns empty string on error (caller should return -EIO).
 */
static std::string get_gcm_salt(
    const DoutPrefixProvider* dpp,
    req_state* s,
    const std::map<std::string, ceph::bufferlist>& attrs,
    std::string_view mode_name)
{
  std::string stored_salt = get_str_attribute(attrs, RGW_ATTR_CRYPT_SALT);
  if (stored_salt.empty()) {
    ldpp_dout(dpp, 5) << "ERROR: " << mode_name << " decryption failed: "
                      << "salt attribute is missing" << dendl;
    s->err.message = "Object encryption metadata is corrupted.";
    return {};
  }
  if (stored_salt.size() != AES_256_GCM_SALT_SIZE) {
    ldpp_dout(dpp, 5) << "ERROR: " << mode_name << " decryption failed: "
                      << "stored salt has invalid size " << stored_salt.size()
                      << " (expected " << AES_256_GCM_SALT_SIZE << ")" << dendl;
    s->err.message = "Object encryption metadata is corrupted.";
    return {};
  }
  return stored_salt;
}

bool rgw_get_aead_original_size(const DoutPrefixProvider* dpp,
                                const std::map<std::string, bufferlist>& attrs,
                                uint64_t* original_size)
{
  if (!original_size) {
    return false;
  }

  const auto mode = get_str_attribute(attrs, RGW_ATTR_CRYPT_MODE);
  if (!is_aead_mode(mode)) {
    return false;
  }

  auto p = attrs.find(RGW_ATTR_CRYPT_ORIGINAL_SIZE);
  if (p == attrs.end()) {
    return false;
  }

  try {
    *original_size = std::stoull(p->second.to_str());
    return true;
  } catch (const std::exception& e) {
    if (dpp) {
      ldpp_dout(dpp, 0) << "ERROR: invalid RGW_ATTR_CRYPT_ORIGINAL_SIZE: "
                        << e.what() << dendl;
    }
    return false;
  }
}

bool rgw_get_aead_decrypted_size(const DoutPrefixProvider* dpp,
                                 const std::map<std::string, bufferlist>& attrs,
                                 uint64_t encrypted_size,
                                 uint64_t* decrypted_size)
{
  if (!decrypted_size) {
    return false;
  }

  const auto mode = get_str_attribute(attrs, RGW_ATTR_CRYPT_MODE);
  if (!is_aead_mode(mode)) {
    return false;
  }

  /* Try CRYPT_PARTS first (more accurate for multipart) */
  if (auto i = attrs.find(RGW_ATTR_CRYPT_PARTS); i != attrs.end()) {
    std::vector<size_t> parts_len;
    try {
      auto iter = i->second.cbegin();
      using ceph::decode;
      decode(parts_len, iter);
    } catch (const buffer::error&) {
      if (dpp) {
        ldpp_dout(dpp, 1) << "failed to decode RGW_ATTR_CRYPT_PARTS" << dendl;
      }
      parts_len.clear();
    }
    if (!parts_len.empty()) {
      uint64_t total = 0;
      for (size_t enc_part : parts_len) {
        total += aead_encrypted_to_plaintext_size(enc_part, dpp);
      }
      *decrypted_size = total;
      return true;
    }
  }

  /* Fallback: calculate from total encrypted size */
  *decrypted_size = aead_encrypted_to_plaintext_size(encrypted_size, dpp);
  if (dpp) {
    ldpp_dout(dpp, 20) << "AEAD: calculated decrypted size " << *decrypted_size
                       << " from encrypted " << encrypted_size << dendl;
  }
  return true;
}


/*
 * Write RGW_ATTR_CRYPT_PREFETCH_ALIGN for size-expanding ciphers
 * so cls/prefetch can compute on-disk read ranges from xattrs.
 */
static void maybe_write_prefetch_align(
    BlockCrypt* block_crypt,
    std::map<std::string, ceph::bufferlist>& attrs)
{
  if (!block_crypt) return;
  size_t plain_bs = block_crypt->get_block_size();
  size_t enc_bs = block_crypt->get_encrypted_block_size();
  if (enc_bs > plain_bs) {
    bufferlist bl;
    encode(static_cast<uint32_t>(plain_bs), bl);
    encode(static_cast<uint32_t>(enc_bs), bl);
    attrs[RGW_ATTR_CRYPT_PREFETCH_ALIGN] = std::move(bl);
  }
}

int rgw_s3_prepare_encrypt(req_state* s, optional_yield y,
                           std::map<std::string, ceph::bufferlist>& attrs,
                           std::unique_ptr<BlockCrypt>* block_crypt,
                           std::map<std::string, std::string>& crypt_http_responses,
                           uint32_t part_number)
{
  int res = 0;
  CryptAttributes crypt_attributes { s };
  const bool is_copy = (s->op_type == RGW_OP_COPY_OBJ);
  crypt_http_responses.clear();

  {
    std::string_view req_sse_ca =
        crypt_attributes.get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM);
    if (! req_sse_ca.empty()) {
      if (req_sse_ca != "AES256") {
        ldpp_dout(s, 5) << "ERROR: Invalid value for header "
                         << "x-amz-server-side-encryption-customer-algorithm"
                         << dendl;
        s->err.message = "The requested encryption algorithm is not valid, must be AES256.";
        return -ERR_INVALID_ENCRYPTION_ALGORITHM;
      }
      if (s->cct->_conf->rgw_crypt_require_ssl &&
          !rgw_transport_is_secure(s->cct, *s->info.env)) {
        ldpp_dout(s, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
        return -ERR_INVALID_REQUEST;
      }

      std::string key_bin;
      try {
        key_bin = from_base64(
          crypt_attributes.get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY) );
      } catch (...) {
        ldpp_dout(s, 5) << "ERROR: rgw_s3_prepare_encrypt invalid encryption "
                         << "key which contains character that is not base64 encoded."
                         << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide an appropriate secret key.";
        return -EINVAL;
      }

      if (key_bin.size() != AES_256_CBC::AES_256_KEYSIZE) {
        ldpp_dout(s, 5) << "ERROR: invalid encryption key size" << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide an appropriate secret key.";
        return -EINVAL;
      }

      std::string_view keymd5 =
          crypt_attributes.get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5);

      std::string keymd5_bin;
      try {
        keymd5_bin = from_base64(keymd5);
      } catch (...) {
        ldpp_dout(s, 5) << "ERROR: rgw_s3_prepare_encrypt invalid encryption key "
                         << "md5 which contains character that is not base64 encoded."
                         << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide an appropriate secret key md5.";
        return -EINVAL;
      }

      if (keymd5_bin.size() != CEPH_CRYPTO_MD5_DIGESTSIZE) {
        ldpp_dout(s, 5) << "ERROR: Invalid key md5 size" << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide an appropriate secret key md5.";
        return -EINVAL;
      }

      MD5 key_hash;
      // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
      key_hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
      unsigned char key_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
      key_hash.Update(reinterpret_cast<const unsigned char*>(key_bin.c_str()), key_bin.size());
      key_hash.Final(key_hash_res);

      if (memcmp(key_hash_res, keymd5_bin.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) {
        ldpp_dout(s, 5) << "ERROR: Invalid key md5 hash" << dendl;
        s->err.message = "The calculated MD5 hash of the key did not match the hash that was provided.";
        return -EINVAL;
      }

      set_attr(attrs, RGW_ATTR_CRYPT_KEYMD5, keymd5_bin);

      // Check config for encryption algorithm preference
      const bool use_gcm = (s->cct->_conf->rgw_crypt_sse_algorithm == "aes-256-gcm");

      if (use_gcm) {
        set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-C-AES256-GCM");
        if (!block_crypt) {
          // multipart-init: write prefetch align from AEAD constants
          bufferlist align_bl;
          encode(static_cast<uint32_t>(AEAD_CHUNK_SIZE), align_bl);
          encode(static_cast<uint32_t>(AEAD_ENCRYPTED_CHUNK_SIZE), align_bl);
          attrs[RGW_ATTR_CRYPT_PREFETCH_ALIGN] = std::move(align_bl);
        }
        std::string salt = generate_gcm_salt(s, attrs);
        set_gcm_plaintext_size(s, attrs, is_copy);

        if (block_crypt) {
          auto gcm = std::unique_ptr<AES_256_GCM>(new AES_256_GCM(s, s->cct));
          if (!gcm->set_salt(reinterpret_cast<const uint8_t*>(salt.c_str()), salt.size())) {
            ldpp_dout(s, 5) << "ERROR: SSE-C-AES256-GCM encryption failed: "
                             << "could not initialize salt" << dendl;
            ::ceph::crypto::zeroize_for_security(key_bin.data(), key_bin.length());
            return -EIO;
          }
          // Derive encryption key from user key + object identity
          if (!gcm->derive_object_key(
                  reinterpret_cast<const uint8_t*>(key_bin.c_str()),
                  AES_256_KEYSIZE,
                  s->bucket->get_info().bucket.bucket_id,
                  s->object->get_name(),
                  part_number)) {
            ldpp_dout(s, 5) << "ERROR: SSE-C-AES256-GCM key derivation failed for "
                             << s->bucket->get_name() << "/" << s->object->get_name() << dendl;
            s->err.message = "Failed to derive encryption key.";
            ::ceph::crypto::zeroize_for_security(key_bin.data(), key_bin.length());
            return -EIO;
          }
          *block_crypt = std::move(gcm);
        }
      } else {
        set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-C-AES256");
        if (block_crypt) {
          auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
          aes->set_key(reinterpret_cast<const uint8_t*>(key_bin.c_str()), AES_256_KEYSIZE);
          *block_crypt = std::move(aes);
        }
      }

      if (block_crypt && *block_crypt)
        maybe_write_prefetch_align(block_crypt->get(), attrs);
      crypt_http_responses["x-amz-server-side-encryption-customer-algorithm"] = "AES256";
      crypt_http_responses["x-amz-server-side-encryption-customer-key-MD5"] = std::string(keymd5);
      return 0;
    } else {
      std::string_view customer_key =
          crypt_attributes.get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY);
      if (!customer_key.empty()) {
        ldpp_dout(s, 5) << "ERROR: SSE-C encryption request is missing the header "
                         << "x-amz-server-side-encryption-customer-algorithm"
                         << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide a valid encryption algorithm.";
        return -EINVAL;
      }

      std::string_view customer_key_md5 =
          crypt_attributes.get(X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5);
      if (!customer_key_md5.empty()) {
        ldpp_dout(s, 5) << "ERROR: SSE-C encryption request is missing the header "
                         << "x-amz-server-side-encryption-customer-algorithm"
                         << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide a valid encryption algorithm.";
        return -EINVAL;
      }
    }

    /* AMAZON server side encryption with KMS (key management service) */
    std::string_view req_sse =
        crypt_attributes.get(X_AMZ_SERVER_SIDE_ENCRYPTION);
    if (! req_sse.empty()) {

      if (req_sse == "aws:kms") {
        if (s->cct->_conf->rgw_crypt_require_ssl &&
            !rgw_transport_is_secure(s->cct, *s->info.env)) {
          ldpp_dout(s, 5) << "ERROR: insecure request, rgw_crypt_require_ssl is set" << dendl;
          return -ERR_INVALID_REQUEST;
        }

        std::string_view context =
          crypt_attributes.get(X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT);
        std::string cooked_context;
        if ((res = make_canonical_context(s, context, cooked_context)))
          return res;
        std::string_view key_id =
          crypt_attributes.get(X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID);
        if (key_id.empty()) {
          ldpp_dout(s, 5) << "ERROR: not provide a valid key id" << dendl;
          s->err.message = "Server Side Encryption with KMS managed key requires "
            "HTTP header x-amz-server-side-encryption-aws-kms-key-id";
          return -EINVAL;
        }
        /* try to retrieve actual key */
        if (s->cct->_conf->rgw_crypt_s3_kms_backend == RGW_SSE_KMS_BACKEND_TESTING) {
          std::string key_selector = create_random_key_selector(s->cct);
          set_attr(attrs, RGW_ATTR_CRYPT_KEYSEL, key_selector);
        }
        set_attr(attrs, RGW_ATTR_CRYPT_KEYID, key_id);
        set_attr(attrs, RGW_ATTR_CRYPT_CONTEXT, cooked_context);
        std::string actual_key;
        res = make_actual_key_from_kms(s, attrs, s->penv.kms_cache.get(), y, actual_key);
        if (res != 0) {
          ldpp_dout(s, 5) << "ERROR: failed to retrieve actual key from key_id: " << key_id << dendl;
          s->err.message = "Failed to retrieve the actual key, kms-keyid: " + std::string(key_id);
          return res;
        }
        if (actual_key.size() != AES_256_KEYSIZE) {
          ldpp_dout(s, 5) << "ERROR: key obtained from key_id:" <<
            key_id << " is not 256 bit size" << dendl;
          s->err.message = "KMS provided an invalid key for the given kms-keyid.";
          return -EINVAL;
        }

        const bool use_gcm = (s->cct->_conf->rgw_crypt_sse_algorithm == "aes-256-gcm");

        if (use_gcm) {
          set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-KMS-GCM");
          if (!block_crypt) {
            bufferlist align_bl;
            encode(static_cast<uint32_t>(AEAD_CHUNK_SIZE), align_bl);
            encode(static_cast<uint32_t>(AEAD_ENCRYPTED_CHUNK_SIZE), align_bl);
            attrs[RGW_ATTR_CRYPT_PREFETCH_ALIGN] = std::move(align_bl);
          }
          std::string salt = generate_gcm_salt(s, attrs);
          set_gcm_plaintext_size(s, attrs, is_copy);

          if (block_crypt) {
            auto aes = AES_256_GCM_create(s, s->cct,
                                           reinterpret_cast<const uint8_t*>(actual_key.c_str()),
                                           AES_256_KEYSIZE,
                                           reinterpret_cast<const uint8_t*>(salt.c_str()),
                                           salt.size(),
                                           part_number);
            if (!aes) {
              ldpp_dout(s, 5) << "ERROR: Failed to create AES-256-GCM instance" << dendl;
              ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
              return -EIO;
            }
            auto* gcm = dynamic_cast<AES_256_GCM*>(aes.get());
            if (!gcm || !gcm->derive_object_key(
                    reinterpret_cast<const uint8_t*>(actual_key.c_str()),
                    AES_256_KEYSIZE,
                    s->bucket->get_info().bucket.bucket_id,
                    s->object->get_name(),
                    part_number,
                    "SSE-KMS-GCM")) {
              ldpp_dout(s, 5) << "ERROR: SSE-KMS-GCM key derivation failed" << dendl;
              ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
              return -EIO;
            }
            *block_crypt = std::move(aes);
          }
        } else {
          set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-KMS");
          if (block_crypt) {
            auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
            aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);
            *block_crypt = std::move(aes);
          }
        }
        ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());

        crypt_http_responses["x-amz-server-side-encryption"] = "aws:kms";
        if (block_crypt && *block_crypt)
          maybe_write_prefetch_align(block_crypt->get(), attrs);
        crypt_http_responses["x-amz-server-side-encryption-aws-kms-key-id"] = std::string(key_id);
        crypt_http_responses["x-amz-server-side-encryption-context"] = std::move(cooked_context);
        return 0;
      } else if (req_sse != "AES256") {
        ldpp_dout(s, 5) << "ERROR: Invalid value for header x-amz-server-side-encryption"
                         << dendl;
        s->err.message = "Server Side Encryption with KMS managed key requires "
          "HTTP header x-amz-server-side-encryption : aws:kms or AES256";
        return -EINVAL;
      }

      if (s->cct->_conf->rgw_crypt_sse_s3_backend != "vault") {
        s->err.message = "Request specifies Server Side Encryption "
            "but server configuration does not support this.";
        return -EINVAL;
      }

      ldpp_dout(s, 5) << "RGW_ATTR_BUCKET_ENCRYPTION ALGO: "
              <<  req_sse << dendl;
      std::string_view context = "";
      std::string cooked_context;
      if ((res = make_canonical_context(s, context, cooked_context)))
        return res;

      std::string key_id;
      res = get_sse_s3_bucket_key(s, y, key_id);
      if (res != 0) {
        return res;
      }

      set_attr(attrs, RGW_ATTR_CRYPT_CONTEXT, cooked_context);
      set_attr(attrs, RGW_ATTR_CRYPT_KEYID, key_id);
      std::string actual_key;
      res = make_actual_key_from_sse_s3(s, attrs, y, actual_key);
      if (res != 0) {
        ldpp_dout(s, 5) << "ERROR: failed to retrieve actual key from key_id: " << key_id << dendl;
        s->err.message = "Failed to retrieve the actual key";
        return res;
      }
      if (actual_key.size() != AES_256_KEYSIZE) {
        ldpp_dout(s, 5) << "ERROR: key obtained from key_id:" <<
                       key_id << " is not 256 bit size" << dendl;
        s->err.message = "SSE-S3 provided an invalid key for the given keyid.";
        return -EINVAL;
      }

      const bool use_gcm = (s->cct->_conf->rgw_crypt_sse_algorithm == "aes-256-gcm");

      if (use_gcm) {
        set_attr(attrs, RGW_ATTR_CRYPT_MODE, "AES256-GCM");
        if (!block_crypt) {
          bufferlist align_bl;
          encode(static_cast<uint32_t>(AEAD_CHUNK_SIZE), align_bl);
          encode(static_cast<uint32_t>(AEAD_ENCRYPTED_CHUNK_SIZE), align_bl);
          attrs[RGW_ATTR_CRYPT_PREFETCH_ALIGN] = std::move(align_bl);
        }
        std::string salt = generate_gcm_salt(s, attrs);
        set_gcm_plaintext_size(s, attrs, is_copy);

        if (block_crypt) {
          auto aes = AES_256_GCM_create(s, s->cct,
                                         reinterpret_cast<const uint8_t*>(actual_key.c_str()),
                                         AES_256_KEYSIZE,
                                         reinterpret_cast<const uint8_t*>(salt.c_str()),
                                         salt.size(),
                                         part_number);
          if (!aes) {
            ldpp_dout(s, 5) << "ERROR: Failed to create AES-256-GCM instance" << dendl;
            ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
            return -EIO;
          }
          auto* gcm = dynamic_cast<AES_256_GCM*>(aes.get());
          if (!gcm || !gcm->derive_object_key(
                  reinterpret_cast<const uint8_t*>(actual_key.c_str()),
                  AES_256_KEYSIZE,
                  s->bucket->get_info().bucket.bucket_id,
                  s->object->get_name(),
                  part_number,
                  "AES256-GCM")) {
            ldpp_dout(s, 5) << "ERROR: AES256-GCM key derivation failed" << dendl;
            ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
            return -EIO;
          }
          *block_crypt = std::move(aes);
        }
      } else {
        set_attr(attrs, RGW_ATTR_CRYPT_MODE, "AES256");
        if (block_crypt) {
          auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
          aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);
          *block_crypt = std::move(aes);
        }
      }
      ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());

      if (block_crypt && *block_crypt)
        maybe_write_prefetch_align(block_crypt->get(), attrs);
      crypt_http_responses["x-amz-server-side-encryption"] = "AES256";

      return 0;
    } else if (s->cct->_conf->rgw_crypt_default_encryption_key != "") {
      std::string master_encryption_key;
      try {
        master_encryption_key = from_base64(s->cct->_conf->rgw_crypt_default_encryption_key);
      } catch (...) {
        ldpp_dout(s, 5) << "ERROR: rgw_s3_prepare_encrypt invalid default encryption key "
                         << "which contains character that is not base64 encoded."
                         << dendl;
        s->err.message = "Requests specifying Server Side Encryption with Customer "
                         "provided keys must provide an appropriate secret key.";
        return -EINVAL;
      }

      if (master_encryption_key.size() != 256 / 8) {
        ldpp_dout(s, 0) << "ERROR: failed to decode 'rgw crypt default encryption key' to 256 bit string" << dendl;
        /* not an error to return; missing encryption does not inhibit processing */
        return 0;
      }

      const bool use_gcm = (s->cct->_conf->rgw_crypt_sse_algorithm == "aes-256-gcm");

      if (use_gcm) {
        set_attr(attrs, RGW_ATTR_CRYPT_MODE, "RGW-AUTO-GCM");
        if (!block_crypt) {
          bufferlist align_bl;
          encode(static_cast<uint32_t>(AEAD_CHUNK_SIZE), align_bl);
          encode(static_cast<uint32_t>(AEAD_ENCRYPTED_CHUNK_SIZE), align_bl);
          attrs[RGW_ATTR_CRYPT_PREFETCH_ALIGN] = std::move(align_bl);
        }
        std::string salt = generate_gcm_salt(s, attrs);
        set_gcm_plaintext_size(s, attrs, is_copy);

        if (block_crypt) {
          auto gcm = std::unique_ptr<AES_256_GCM>(new AES_256_GCM(s, s->cct));
          if (!gcm->set_salt(reinterpret_cast<const uint8_t*>(salt.c_str()), salt.size())) {
            ldpp_dout(s, 5) << "ERROR: RGW-AUTO-GCM: could not initialize salt" << dendl;
            return -EIO;
          }

          // Derive encryption key using HMAC-SHA256 with context binding
          // Key = HMAC-SHA256(master_key, salt || "RGW-AUTO-GCM" || bucket_id || object)
          if (!gcm->derive_object_key(
                  reinterpret_cast<const uint8_t*>(master_encryption_key.c_str()),
                  AES_256_KEYSIZE,
                  s->bucket->get_info().bucket.bucket_id,
                  s->object->get_name(),
                  part_number,
                  "RGW-AUTO-GCM")) {
            ldpp_dout(s, 5) << "ERROR: RGW-AUTO-GCM key derivation failed for "
                             << s->bucket->get_name() << "/" << s->object->get_name() << dendl;
            return -EIO;
          }
          *block_crypt = std::move(gcm);
        }
      } else {
        // CBC mode: use AES-ECB key derivation (legacy approach)
        std::string key_selector = create_random_key_selector(s->cct);
        set_attr(attrs, RGW_ATTR_CRYPT_KEYSEL, key_selector);

        uint8_t actual_key[AES_256_KEYSIZE];
        if (AES_256_ECB_encrypt(s, s->cct,
                                reinterpret_cast<const uint8_t*>(master_encryption_key.c_str()), AES_256_KEYSIZE,
                                reinterpret_cast<const uint8_t*>(key_selector.c_str()),
                                actual_key, AES_256_KEYSIZE) != true) {
          ::ceph::crypto::zeroize_for_security(actual_key, sizeof(actual_key));
          return -EIO;
        }

        set_attr(attrs, RGW_ATTR_CRYPT_MODE, "RGW-AUTO");
        if (block_crypt) {
          auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
          aes->set_key(actual_key, AES_256_KEYSIZE);
          *block_crypt = std::move(aes);
        }
        ::ceph::crypto::zeroize_for_security(actual_key, sizeof(actual_key));
      }
      if (block_crypt && *block_crypt)
        maybe_write_prefetch_align(block_crypt->get(), attrs);
      return 0;
    }
  }
  return 0;
}


static void pick_gcm_identity(req_state* s,
                              bool copy_source,
                              const rgw_crypt_src_identity* src_identity,
                              std::string& bucket_id,
                              std::string& object_name)
{
  if (copy_source) {
    if (src_identity && src_identity->valid()) {
      bucket_id = std::string(src_identity->bucket_id);
      object_name = std::string(src_identity->object);
      return;
    }
    if (s->src_object && s->src_object->get_bucket()) {
      bucket_id = s->src_object->get_bucket()->get_info().bucket.bucket_id;
      object_name = s->src_object->get_name();
      return;
    }
  }
  bucket_id = s->bucket->get_info().bucket.bucket_id;
  object_name = s->object->get_name();
}

int rgw_s3_prepare_decrypt(req_state* s, optional_yield y,
                           map<string, bufferlist>& attrs,
                           std::unique_ptr<BlockCrypt>* block_crypt,
                           std::map<std::string, std::string>* crypt_http_responses,
                           bool copy_source,
                           uint32_t part_number,
                           const rgw_crypt_src_identity* src_identity)
{
  int res = 0;
  std::string stored_mode = get_str_attribute(attrs, RGW_ATTR_CRYPT_MODE);
  ldpp_dout(s, 15) << "Encryption mode: " << stored_mode << dendl;

  const char *req_sse = s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION", NULL);
  if (nullptr != req_sse && (s->op == OP_GET || s->op == OP_HEAD)) {
    return -ERR_INVALID_REQUEST;
  }

  if (stored_mode == "SSE-C-AES256") {
    if (s->cct->_conf->rgw_crypt_require_ssl &&
        !rgw_transport_is_secure(s->cct, *s->info.env)) {
      ldpp_dout(s, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
      return -ERR_INVALID_REQUEST;
    }

    const char *sse_c_algo_hdr = copy_source ? "HTTP_X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM" :
                                               "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM";
    const char *req_cust_alg = s->info.env->get(sse_c_algo_hdr, NULL);
    if (nullptr == req_cust_alg)  {
      ldpp_dout(s, 5) << "ERROR: Request for SSE-C encrypted object missing "
                       << "x-amz-server-side-encryption-customer-algorithm"
                       << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide a valid encryption algorithm.";
      return -EINVAL;
    } else if (strcmp(req_cust_alg, "AES256") != 0) {
      ldpp_dout(s, 5) << "ERROR: The requested encryption algorithm is not valid, must be AES256." << dendl;
      s->err.message = "The requested encryption algorithm is not valid, must be AES256.";
      return -ERR_INVALID_ENCRYPTION_ALGORITHM;
    }

    const char *sse_c_key_hdr = copy_source ? "HTTP_X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY" :
                                              "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY";
    std::string key_bin;
    try {
      key_bin = from_base64(s->info.env->get(sse_c_key_hdr, ""));
    } catch (...) {
      ldpp_dout(s, 5) << "ERROR: rgw_s3_prepare_decrypt invalid encryption key "
                       << "which contains character that is not base64 encoded."
                       << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key.";
      return -EINVAL;
    }

    if (key_bin.size() != AES_256_CBC::AES_256_KEYSIZE) {
      ldpp_dout(s, 5) << "ERROR: Invalid encryption key size" << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key.";
      return -EINVAL;
    }

    const char *sse_c_key_md5_hdr = copy_source ? "HTTP_X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5" :
                                                  "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5";
    std::string keymd5 = s->info.env->get(sse_c_key_md5_hdr, "");
    std::string keymd5_bin;
    try {
      keymd5_bin = from_base64(keymd5);
    } catch (...) {
      ldpp_dout(s, 5) << "ERROR: rgw_s3_prepare_decrypt invalid encryption key md5 "
                       << "which contains character that is not base64 encoded."
                       << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key md5.";
      return -EINVAL;
    }

    if (keymd5_bin.size() != CEPH_CRYPTO_MD5_DIGESTSIZE) {
      ldpp_dout(s, 5) << "ERROR: Invalid key md5 size " << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key md5.";
      return -EINVAL;
    }

    MD5 key_hash;
    // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
    key_hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
    uint8_t key_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
    key_hash.Update(reinterpret_cast<const unsigned char*>(key_bin.c_str()), key_bin.size());
    key_hash.Final(key_hash_res);

    if ((memcmp(key_hash_res, keymd5_bin.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) ||
        (get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYMD5) != keymd5_bin)) {
      s->err.message = "The calculated MD5 hash of the key did not match the hash that was provided.";
      return -EINVAL;
    }
    auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
    aes->set_key(reinterpret_cast<const uint8_t*>(key_bin.c_str()), AES_256_CBC::AES_256_KEYSIZE);
    if (block_crypt) *block_crypt = std::move(aes);

    if (crypt_http_responses) {
      crypt_http_responses->emplace("x-amz-server-side-encryption-customer-algorithm", "AES256");
      crypt_http_responses->emplace("x-amz-server-side-encryption-customer-key-MD5", keymd5);
    }

    return 0;
  }

  if (stored_mode == "SSE-C-AES256-GCM") {
    if (s->cct->_conf->rgw_crypt_require_ssl &&
        !rgw_transport_is_secure(s->cct, *s->info.env)) {
      ldpp_dout(s, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
      return -ERR_INVALID_REQUEST;
    }

    const char *sse_c_algo_hdr = copy_source ? "HTTP_X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM" :
                                               "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM";
    const char *req_cust_alg = s->info.env->get(sse_c_algo_hdr, NULL);
    if (nullptr == req_cust_alg)  {
      ldpp_dout(s, 5) << "ERROR: Request for SSE-C encrypted object missing "
                       << "x-amz-server-side-encryption-customer-algorithm"
                       << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide a valid encryption algorithm.";
      return -EINVAL;
    } else if (strcmp(req_cust_alg, "AES256") != 0) {
      ldpp_dout(s, 5) << "ERROR: The requested encryption algorithm is not valid, must be AES256." << dendl;
      s->err.message = "The requested encryption algorithm is not valid, must be AES256.";
      return -ERR_INVALID_ENCRYPTION_ALGORITHM;
    }

    const char *sse_c_key_hdr = copy_source ? "HTTP_X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY" :
                                              "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY";
    std::string key_bin;
    try {
      key_bin = from_base64(s->info.env->get(sse_c_key_hdr, ""));
    } catch (...) {
      ldpp_dout(s, 5) << "ERROR: rgw_s3_prepare_decrypt invalid encryption key "
                       << "which contains character that is not base64 encoded."
                       << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key.";
      return -EINVAL;
    }

    if (key_bin.size() != AES_256_KEYSIZE) {
      ldpp_dout(s, 5) << "ERROR: Invalid encryption key size" << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key.";
      return -EINVAL;
    }

    const char *sse_c_key_md5_hdr = copy_source ? "HTTP_X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5" :
                                                  "HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5";
    std::string keymd5 = s->info.env->get(sse_c_key_md5_hdr, "");
    std::string keymd5_bin;
    try {
      keymd5_bin = from_base64(keymd5);
    } catch (...) {
      ldpp_dout(s, 5) << "ERROR: rgw_s3_prepare_decrypt invalid encryption key md5 "
                       << "which contains character that is not base64 encoded."
                       << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key md5.";
      return -EINVAL;
    }

    if (keymd5_bin.size() != CEPH_CRYPTO_MD5_DIGESTSIZE) {
      ldpp_dout(s, 5) << "ERROR: Invalid key md5 size " << dendl;
      s->err.message = "Requests specifying Server Side Encryption with Customer "
                       "provided keys must provide an appropriate secret key md5.";
      return -EINVAL;
    }

    MD5 key_hash;
    // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
    key_hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
    uint8_t key_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
    key_hash.Update(reinterpret_cast<const unsigned char*>(key_bin.c_str()), key_bin.size());
    key_hash.Final(key_hash_res);

    if ((memcmp(key_hash_res, keymd5_bin.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) ||
        (get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYMD5) != keymd5_bin)) {
      s->err.message = "The calculated MD5 hash of the key did not match the hash that was provided.";
      return -EINVAL;
    }

    std::string stored_salt = get_gcm_salt(s, s, attrs, "SSE-C-AES256-GCM");
    if (stored_salt.empty()) return -EIO;

    auto gcm = std::make_unique<AES_256_GCM>(s, s->cct);
    gcm->set_salt(reinterpret_cast<const uint8_t*>(stored_salt.c_str()),
                   stored_salt.size());
    // Re-derive encryption key from user key + object identity
    // For CopyObject, use the SOURCE object's identity (not destination)
    std::string bucket_id;
    std::string object_name;
    pick_gcm_identity(s, copy_source, src_identity, bucket_id, object_name);
    if (!gcm->derive_object_key(
            reinterpret_cast<const uint8_t*>(key_bin.c_str()),
            AES_256_KEYSIZE,
            bucket_id,
            object_name,
            part_number)) {
      ldpp_dout(s, 5) << "ERROR: SSE-C-AES256-GCM key derivation failed for "
                       << bucket_id << "/" << object_name << dendl;
      s->err.message = "Failed to derive decryption key.";
      return -EIO;
    }
    if (block_crypt) *block_crypt = std::move(gcm);

    if (crypt_http_responses) {
      crypt_http_responses->emplace("x-amz-server-side-encryption-customer-algorithm", "AES256");
      crypt_http_responses->emplace("x-amz-server-side-encryption-customer-key-MD5", keymd5);
    }

    return 0;
  }

  if (stored_mode == "SSE-KMS") {
    if (s->cct->_conf->rgw_crypt_require_ssl &&
        !rgw_transport_is_secure(s->cct, *s->info.env)) {
      ldpp_dout(s, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
      return -ERR_INVALID_REQUEST;
    }
    /* try to retrieve actual key */
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    std::string actual_key;

    res = reconstitute_actual_key_from_kms(s, attrs, s->penv.kms_cache.get(), y, actual_key);
    if (res != 0) {
      ldpp_dout(s, 10) << "ERROR: failed to retrieve actual key from key_id: " << key_id << dendl;
      s->err.message = "Failed to retrieve the actual key, kms-keyid: " + key_id;
      return res;
    }
    if (actual_key.size() != AES_256_KEYSIZE) {
      ldpp_dout(s, 0) << "ERROR: key obtained from key_id:" <<
          key_id << " is not 256 bit size" << dendl;
      s->err.message = "KMS provided an invalid key for the given kms-keyid.";
      return -EINVAL;
    }

    auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
    aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);
    ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
    if (block_crypt) *block_crypt = std::move(aes);

    if (crypt_http_responses) {
      crypt_http_responses->emplace("x-amz-server-side-encryption", "aws:kms");
      crypt_http_responses->emplace("x-amz-server-side-encryption-aws-kms-key-id", key_id);
    }

    return 0;
  }

  if (stored_mode == "SSE-KMS-GCM") {
    if (s->cct->_conf->rgw_crypt_require_ssl &&
        !rgw_transport_is_secure(s->cct, *s->info.env)) {
      ldpp_dout(s, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
      return -ERR_INVALID_REQUEST;
    }
    /* try to retrieve actual key */
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    std::string actual_key;
    res = reconstitute_actual_key_from_kms(s, attrs, s->penv.kms_cache.get(), y, actual_key);
    if (res != 0) {
      ldpp_dout(s, 10) << "ERROR: failed to retrieve actual key from key_id: " << key_id << dendl;
      s->err.message = "Failed to retrieve the actual key, kms-keyid: " + key_id;
      return res;
    }
    if (actual_key.size() != AES_256_KEYSIZE) {
      ldpp_dout(s, 0) << "ERROR: key obtained from key_id:" <<
          key_id << " is not 256 bit size" << dendl;
      s->err.message = "KMS provided an invalid key for the given kms-keyid.";
      return -EINVAL;
    }

    std::string stored_salt = get_gcm_salt(s, s, attrs, "SSE-KMS-GCM");
    if (stored_salt.empty()) {
      ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
      return -EIO;
    }

    auto aes = AES_256_GCM_create(s, s->cct,
                                   reinterpret_cast<const uint8_t*>(actual_key.c_str()),
                                   AES_256_KEYSIZE,
                                   reinterpret_cast<const uint8_t*>(stored_salt.c_str()),
                                   stored_salt.size(),
                                   part_number);
    if (!aes) {
      ldpp_dout(s, 5) << "ERROR: Failed to create AES-256-GCM instance for decryption" << dendl;
      ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
      return -EIO;
    }
    std::string bucket_id, object_name;
    pick_gcm_identity(s, copy_source, src_identity, bucket_id, object_name);
    auto* gcm = dynamic_cast<AES_256_GCM*>(aes.get());
    if (!gcm || !gcm->derive_object_key(
            reinterpret_cast<const uint8_t*>(actual_key.c_str()),
            AES_256_KEYSIZE,
            bucket_id,
            object_name,
            part_number,
            "SSE-KMS-GCM")) {
      ldpp_dout(s, 5) << "ERROR: SSE-KMS-GCM key derivation failed" << dendl;
      ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
      return -EIO;
    }
    ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
    if (block_crypt) *block_crypt = std::move(aes);

    if (crypt_http_responses) {
      crypt_http_responses->emplace("x-amz-server-side-encryption", "aws:kms");
      crypt_http_responses->emplace("x-amz-server-side-encryption-aws-kms-key-id", key_id);
    }

    return 0;
  }

  if (stored_mode == "RGW-AUTO") {
    std::string master_encryption_key;
    try {
      master_encryption_key = from_base64(std::string(s->cct->_conf->rgw_crypt_default_encryption_key));
    } catch (...) {
      ldpp_dout(s, 5) << "ERROR: rgw_s3_prepare_decrypt invalid default encryption key "
                       << "which contains character that is not base64 encoded."
                       << dendl;
      s->err.message = "The default encryption key is not valid base64.";
      return -EINVAL;
    }

    if (master_encryption_key.size() != 256 / 8) {
      ldpp_dout(s, 0) << "ERROR: failed to decode 'rgw crypt default encryption key' to 256 bit string" << dendl;
      return -EIO;
    }
    std::string attr_key_selector = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYSEL);
    if (attr_key_selector.size() != AES_256_CBC::AES_256_KEYSIZE) {
      ldpp_dout(s, 0) << "ERROR: missing or invalid " RGW_ATTR_CRYPT_KEYSEL << dendl;
      return -EIO;
    }
    uint8_t actual_key[AES_256_KEYSIZE];
    if (AES_256_ECB_encrypt(s, s->cct,
                            reinterpret_cast<const uint8_t*>(master_encryption_key.c_str()),
                            AES_256_KEYSIZE,
                            reinterpret_cast<const uint8_t*>(attr_key_selector.c_str()),
                            actual_key, AES_256_KEYSIZE) != true) {
      ::ceph::crypto::zeroize_for_security(actual_key, sizeof(actual_key));
      return -EIO;
    }
    auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
    aes->set_key(actual_key, AES_256_KEYSIZE);
    ::ceph::crypto::zeroize_for_security(actual_key, sizeof(actual_key));
    if (block_crypt) *block_crypt = std::move(aes);
    return 0;
  }

  if (stored_mode == "RGW-AUTO-GCM") {
    std::string master_encryption_key;
    try {
      master_encryption_key = from_base64(std::string(s->cct->_conf->rgw_crypt_default_encryption_key));
    } catch (...) {
      ldpp_dout(s, 5) << "ERROR: rgw_s3_prepare_decrypt invalid default encryption key "
                       << "which contains character that is not base64 encoded."
                       << dendl;
      s->err.message = "The default encryption key is not valid base64.";
      return -EINVAL;
    }

    if (master_encryption_key.size() != 256 / 8) {
      ldpp_dout(s, 0) << "ERROR: failed to decode 'rgw crypt default encryption key' to 256 bit string" << dendl;
      return -EIO;
    }

    std::string stored_salt = get_gcm_salt(s, s, attrs, "RGW-AUTO-GCM");
    if (stored_salt.empty()) return -EIO;

    auto gcm = std::make_unique<AES_256_GCM>(s, s->cct);
    gcm->set_salt(reinterpret_cast<const uint8_t*>(stored_salt.c_str()),
                   stored_salt.size());

    // Re-derive encryption key using HMAC-SHA256 with context binding
    // For CopyObject, use the SOURCE object's identity (not destination)
    std::string bucket_id;
    std::string object_name;
    pick_gcm_identity(s, copy_source, src_identity, bucket_id, object_name);

    if (!gcm->derive_object_key(
            reinterpret_cast<const uint8_t*>(master_encryption_key.c_str()),
            AES_256_KEYSIZE,
            bucket_id,
            object_name,
            part_number,
            "RGW-AUTO-GCM")) {
      ldpp_dout(s, 5) << "ERROR: RGW-AUTO-GCM key derivation failed for "
                       << bucket_id << "/" << object_name << dendl;
      s->err.message = "Failed to derive decryption key.";
      return -EIO;
    }

    if (block_crypt) *block_crypt = std::move(gcm);
    return 0;
  }

  /* SSE-S3 */
  if (stored_mode == "AES256") {
    /* try to retrieve actual key */
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    std::string actual_key;
    res = reconstitute_actual_key_from_sse_s3(s, attrs, y, actual_key);
    if (res != 0) {
      ldpp_dout(s, 10) << "ERROR: failed to retrieve actual key" << dendl;
      s->err.message = "Failed to retrieve the actual key";
      return res;
    }
    if (actual_key.size() != AES_256_KEYSIZE) {
      ldpp_dout(s, 0) << "ERROR: key obtained " <<
          "is not 256 bit size" << dendl;
      s->err.message = "SSE-S3 provided an invalid key for the given keyid.";
      return -EINVAL;
    }

    auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
    aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);
    ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
    if (block_crypt) *block_crypt = std::move(aes);

    if (crypt_http_responses) {
      crypt_http_responses->emplace("x-amz-server-side-encryption", "AES256");
    }

    return 0;
  }

  /* SSE-S3 with GCM */
  if (stored_mode == "AES256-GCM") {
    /* try to retrieve actual key */
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    std::string actual_key;
    res = reconstitute_actual_key_from_sse_s3(s, attrs, y, actual_key);
    if (res != 0) {
      ldpp_dout(s, 10) << "ERROR: failed to retrieve actual key" << dendl;
      s->err.message = "Failed to retrieve the actual key";
      return res;
    }
    if (actual_key.size() != AES_256_KEYSIZE) {
      ldpp_dout(s, 0) << "ERROR: key obtained " <<
          "is not 256 bit size" << dendl;
      s->err.message = "SSE-S3 provided an invalid key for the given keyid.";
      return -EINVAL;
    }

    std::string stored_salt = get_gcm_salt(s, s, attrs, "AES256-GCM");
    if (stored_salt.empty()) {
      ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
      return -EIO;
    }

    auto aes = AES_256_GCM_create(s, s->cct,
                                   reinterpret_cast<const uint8_t*>(actual_key.c_str()),
                                   AES_256_KEYSIZE,
                                   reinterpret_cast<const uint8_t*>(stored_salt.c_str()),
                                   stored_salt.size(),
                                   part_number);
    if (!aes) {
      ldpp_dout(s, 5) << "ERROR: Failed to create AES-256-GCM instance for decryption" << dendl;
      ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
      return -EIO;
    }
    std::string bucket_id, object_name;
    pick_gcm_identity(s, copy_source, src_identity, bucket_id, object_name);
    auto* gcm = dynamic_cast<AES_256_GCM*>(aes.get());
    if (!gcm || !gcm->derive_object_key(
            reinterpret_cast<const uint8_t*>(actual_key.c_str()),
            AES_256_KEYSIZE,
            bucket_id,
            object_name,
            part_number,
            "AES256-GCM")) {
      ldpp_dout(s, 5) << "ERROR: AES256-GCM key derivation failed" << dendl;
      ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
      return -EIO;
    }
    ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());
    if (block_crypt) *block_crypt = std::move(aes);

    if (crypt_http_responses) {
      crypt_http_responses->emplace("x-amz-server-side-encryption", "AES256");
    }

    return 0;
  }

  /*no decryption*/
  return 0;
}

int rgw_remove_sse_s3_bucket_key(req_state *s, optional_yield y)
{
  int res;
  auto key_id { expand_key_name(s, s->cct->_conf->rgw_crypt_sse_s3_key_template) };
  auto saved_key { fetch_bucket_key_id(s) };
  size_t i;

  if (key_id == cant_expand_key) {
    ldpp_dout(s, 5) << "ERROR: unable to expand key_id " <<
      s->cct->_conf->rgw_crypt_sse_s3_key_template << " on bucket" << dendl;
    s->err.message = "Server side error - unable to expand key_id";
    return -EINVAL;
  }

  if (saved_key == "") {
    return 0;
  } else if (saved_key != key_id) {
    ldpp_dout(s, 5) << "Found but will not delete strange KEK ID: " << saved_key << dendl;
    return 0;
  }
  i = s->cct->_conf->rgw_crypt_sse_s3_key_template.find("%bucket_id");
  if (i == std::string_view::npos) {
    ldpp_dout(s, 5) << "Kept valid KEK ID: " << saved_key << dendl;
    return 0;
  }
  ldpp_dout(s, 5) << "Removing valid KEK ID: " << saved_key << dendl;
  res = remove_sse_s3_bucket_key(s, saved_key, y);
  if (res != 0) {
    ldpp_dout(s, 0) << "ERROR: Unable to remove KEK ID: " << saved_key << " got " << res << dendl;
  }
  return res;
}

/*********************************************************************
*	"BOTTOM OF FILE"
*	I've left some commented out lines above.  They are there for
*	a reason, which I will explain.  The "canonical" json constructed
*	by the code above as a crypto context must take a json object and
*	turn it into a unique deterministic fixed form.  For most json
*	types this is easy.  The hardest problem that is handled above is
*	detailing with unicode strings; they must be turned into
*	NFC form and sorted in a fixed order.  Numbers, however,
*	are another story.  Json makes no distinction between integers
*	and floating point, and both types have their problems.
*	Integers can overflow, so very large numbers are a problem.
*	Floating point is even worse; not all floating point numbers
*	can be represented accurately in c++ data types, and there
*	are many quirks regarding how overflow, underflow, and loss
*	of significance are handled.
*
*	In this version of the code, I took the simplest answer, I
*	reject all numbers altogether.  This is not ideal, but it's
*	the only choice that is guaranteed to be future compatible.
*	AWS S3 does not guarantee to support numbers at all; but it
*	actually converts all numbers into strings right off.
*	This has the interesting property that 7 and 007 are different,
*	but that 007 and "007" are the same.  I would rather
*	treat numbers as a string of digits and have logic
*	to produce the "most compact" equivalent form.  This can
*	fix all the overflow/underflow problems, but it requires
*	fixing the json parser part, and I put that problem off.
*
*	The commented code above indicates places in this code that
*	will need to be revised depending on future work in this area.
*	Removing those comments makes that work harder.
*				February 25, 2021
*********************************************************************/
