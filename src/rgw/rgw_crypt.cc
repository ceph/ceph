// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
#include "include/function2.hpp"
#include "crypto/crypto_accel.h"
#include "crypto/crypto_plugin.h"
#include "rgw/rgw_kms.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/error/error.h"
#include "rapidjson/error/en.h"
#include <unicode/normalizer2.h>	// libicu

#include <openssl/evp.h>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

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
                                               optional_yield y)
    :
    RGWGetObj_Filter(next),
    dpp(dpp),
    cct(cct),
    crypt(std::move(crypt)),
    enc_begin_skip(0),
    ofs(0),
    end(0),
    cache(),
    y(y),
    parts_len(std::move(parts_len))
{
  block_size = this->crypt->get_block_size();
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
  ldpp_dout(this->dpp, 20) << "fixup_range [" << inp_ofs << "," << inp_end
      << "] => [" << bl_ofs << "," << bl_end << "]" << dendl;
  return 0;
}

int RGWGetObj_BlockDecrypt::process(bufferlist& in, size_t part_ofs, size_t size)
{
  bufferlist data;
  if (!crypt->decrypt(in, 0, size, data, part_ofs, y)) {
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

int RGWGetObj_BlockDecrypt::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
  ldpp_dout(this->dpp, 25) << "Decrypt " << bl_len << " bytes" << dendl;
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
  ldpp_dout(this->dpp, 25) << "Decrypt flushing " << cache.length() << " bytes" << dendl;
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

int rgw_s3_prepare_encrypt(req_state* s, optional_yield y,
                           std::map<std::string, ceph::bufferlist>& attrs,
                           std::unique_ptr<BlockCrypt>* block_crypt,
                           std::map<std::string, std::string>& crypt_http_responses)
{
  int res = 0;
  CryptAttributes crypt_attributes { s };
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

      set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-C-AES256");
      set_attr(attrs, RGW_ATTR_CRYPT_KEYMD5, keymd5_bin);

      if (block_crypt) {
        auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
        aes->set_key(reinterpret_cast<const uint8_t*>(key_bin.c_str()), AES_256_KEYSIZE);
        *block_crypt = std::move(aes);
      }

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
        std::string key_selector = create_random_key_selector(s->cct);
        set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-KMS");
        set_attr(attrs, RGW_ATTR_CRYPT_KEYID, key_id);
        set_attr(attrs, RGW_ATTR_CRYPT_KEYSEL, key_selector);
        set_attr(attrs, RGW_ATTR_CRYPT_CONTEXT, cooked_context);
        std::string actual_key;
        res = make_actual_key_from_kms(s, attrs, y, actual_key);
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

        if (block_crypt) {
          auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
          aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);
          *block_crypt = std::move(aes);
        }
        ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());

        crypt_http_responses["x-amz-server-side-encryption"] = "aws:kms";
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
      std::string key_selector = create_random_key_selector(s->cct);

      set_attr(attrs, RGW_ATTR_CRYPT_KEYSEL, key_selector);
      set_attr(attrs, RGW_ATTR_CRYPT_CONTEXT, cooked_context);
      set_attr(attrs, RGW_ATTR_CRYPT_MODE, "AES256");
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

      if (block_crypt) {
        auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
        aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);
        *block_crypt = std::move(aes);
      }
      ::ceph::crypto::zeroize_for_security(actual_key.data(), actual_key.length());

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

      set_attr(attrs, RGW_ATTR_CRYPT_MODE, "RGW-AUTO");
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
      if (block_crypt) {
        auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s, s->cct));
        aes->set_key(reinterpret_cast<const uint8_t*>(actual_key), AES_256_KEYSIZE);
        *block_crypt = std::move(aes);
      }
      ::ceph::crypto::zeroize_for_security(actual_key, sizeof(actual_key));
      return 0;
    }
  }
  return 0;
}


int rgw_s3_prepare_decrypt(req_state* s, optional_yield y,
                           map<string, bufferlist>& attrs,
                           std::unique_ptr<BlockCrypt>* block_crypt,
                           std::map<std::string, std::string>& crypt_http_responses)
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
    const char *req_cust_alg =
        s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM", NULL);

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

    std::string key_bin;
    try {
      key_bin = from_base64(s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY", ""));
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

    std::string keymd5 =
        s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5", "");
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

    crypt_http_responses["x-amz-server-side-encryption-customer-algorithm"] = "AES256";
    crypt_http_responses["x-amz-server-side-encryption-customer-key-MD5"] = keymd5;
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
    res = reconstitute_actual_key_from_kms(s, attrs, y, actual_key);
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
    actual_key.replace(0, actual_key.length(), actual_key.length(), '\000');
    if (block_crypt) *block_crypt = std::move(aes);

    crypt_http_responses["x-amz-server-side-encryption"] = "AES256";
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
