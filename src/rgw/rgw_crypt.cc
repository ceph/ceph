// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/**
 * Crypto filters for Put/Post/Get operations.
 */
#include <rgw/rgw_op.h>
#include <rgw/rgw_crypt.h>
#include <crypto++/cryptlib.h>
#include <crypto++/modes.h>
#include <crypto++/aes.h>
#include <auth/Crypto.h>
#include <rgw/rgw_b64.h>
#include <rgw/rgw_rest_s3.h>
#include "include/assert.h"
#include <boost/utility/string_ref.hpp>
#include <rgw/rgw_keystone.h>

#define dout_subsys ceph_subsys_rgw

using namespace CryptoPP;
using namespace rgw;

class AES_256_CTR_impl {
  static const size_t AES_256_KEYSIZE = 256 / 8;
  static const size_t AES_256_IVSIZE = 128 / 8;
  static const uint8_t IV[AES_256_IVSIZE];

  CephContext* cct;
  uint8_t key[AES_256_KEYSIZE];
  uint8_t nonce[AES_256_IVSIZE];
public:
  AES_256_CTR_impl(CephContext* cct): cct(cct) {
  }
  ~AES_256_CTR_impl() {
  }
  /**
   * Sets key and nonce.
   */
  bool set_key(const uint8_t* _key, size_t key_size) {
    if (key_size != AES_256_KEYSIZE) {
      return false;
    }
    memcpy(key, _key, AES_256_KEYSIZE);
    return true;
  }
  size_t get_block_size() {
    return AES_256_KEYSIZE;
  }
  bool encrypt(bufferlist& input, off_t in_ofs, size_t size, bufferlist& output, off_t stream_offset) {
    byte iv[AES_256_IVSIZE];
    ldout(cct, 20)
        << "encrypt in_ofs " << in_ofs
        << " size=" << size
        << " stream_offset=" << stream_offset
        << " input buffer #=" << input.buffers().size()
        << " input buffer 0=" << input.buffers().begin()->length()
        << dendl;
    if (input.length() < in_ofs + size) {
      return false;
    }

    if ((size % AES_256_KEYSIZE) == 0) {
      //uneven
    }
    output.clear();
    buffer::ptr buf((size + AES_256_KEYSIZE - 1) / AES_256_KEYSIZE * AES_256_KEYSIZE);
    /*create CTR mask*/
    prepare_iv(iv, stream_offset);
    CTR_Mode< AES >::Encryption e;
    e.SetKeyWithIV(key, AES_256_KEYSIZE, iv, AES_256_IVSIZE);
    buf.zero();
    e.ProcessData((byte*)buf.c_str(), (byte*)buf.c_str(), buf.length());
    buf.set_length(size);
    off_t plaintext_pos = in_ofs;
    off_t crypt_pos = 0;
    auto iter = input.buffers().begin();
    //skip unaffected begin
    while ((iter != input.buffers().end()) && (plaintext_pos >= iter->length())) {
      plaintext_pos -= iter->length();
      ++iter;
    }
    while (iter != input.buffers().end()) {
      off_t cnt = std::min((off_t)(iter->length() - plaintext_pos), (off_t)(size - crypt_pos));
      byte* src = (byte*)iter->c_str() + plaintext_pos;
      byte* dst = (byte*)buf.c_str() + crypt_pos;
      ldout(cct, 20)
              << "cnt= " << cnt
              << " plaintext_pos=" << plaintext_pos
              << " crypt_pos=" << crypt_pos
              << dendl;
      for (off_t i=0; i<cnt; i++) {
        dst[i] ^= src[i];
      }
      ++iter;
      plaintext_pos = 0;
      crypt_pos += cnt;
    }
    output.append(buf);
    return true;
  }
  bool decrypt(bufferlist& input, off_t in_ofs, size_t size, bufferlist& output, off_t stream_offset) {
    return encrypt(input, in_ofs, size, output, stream_offset);
  }
  void prepare_iv(byte iv[AES_256_IVSIZE], off_t offset) {
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

const uint8_t AES_256_CTR_impl::IV[AES_256_CTR_impl::AES_256_IVSIZE] =
    { 'a', 'e', 's', '2', '5', '6', 'i', 'v', '_', 'c', 't', 'r', '1', '3', '3', '7' };

AES_256_CTR::AES_256_CTR(CephContext* cct) {
  pimpl = new AES_256_CTR_impl(cct);
}
AES_256_CTR::~AES_256_CTR() {
  delete pimpl;
}
bool AES_256_CTR::set_key(const uint8_t* key, size_t key_size) {
  return pimpl->set_key(key, key_size);
}
size_t AES_256_CTR::get_block_size() {
  return pimpl->get_block_size();
}
bool AES_256_CTR::encrypt(bufferlist& input, off_t in_ofs, size_t size, bufferlist& output, off_t stream_offset) {
  return pimpl->encrypt(input, in_ofs, size, output, stream_offset);
}
bool AES_256_CTR::decrypt(bufferlist& input, off_t in_ofs, size_t size, bufferlist& output, off_t stream_offset) {
  return pimpl->decrypt(input, in_ofs, size, output, stream_offset);
}

bool AES_256_ECB_encrypt(uint8_t* key, size_t key_size, uint8_t* data_in, uint8_t* data_out, size_t data_size) {
  bool res = false;
  if (key_size == AES_256_KEYSIZE) {
    try {
      ECB_Mode< AES >::Encryption e;
      e.SetKey( key, key_size );
      e.ProcessData(data_out, data_in, data_size);
      res = true;
    } catch( CryptoPP::Exception& ex ) {
    }
  }
  return res;
}





RGWGetObj_BlockDecrypt::RGWGetObj_BlockDecrypt(CephContext* cct, RGWGetDataCB& next, BlockCrypt* crypt):
    RGWGetObj_Filter(next),
    cct(cct),
    crypt(crypt),
    enc_begin_skip(0), ofs(0), end(0), cache() {
  block_size = crypt->get_block_size();
  }
RGWGetObj_BlockDecrypt::~RGWGetObj_BlockDecrypt() {}

int RGWGetObj_BlockDecrypt::read_manifest(bufferlist& manifest_bl) {
  parts_len.clear();
  RGWObjManifest manifest;
  if (manifest_bl.length()) {
    bufferlist::iterator miter = manifest_bl.begin();
    try {
      ::decode(manifest, miter);
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
    if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
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
    off_t in_ofs=bl_ofs;
    off_t in_end=bl_end;

    size_t i = 0;
    while (i<parts_len.size() && (in_ofs > (off_t)parts_len[i])) {
      in_ofs -= parts_len[i];
      i++;
    }
    //in_ofs is inside block i
    size_t j = 0;
    while (j<parts_len.size() && (in_end > (off_t)parts_len[j])) {
      in_end -= parts_len[j];
      j++;
    }
    //in_end is inside block j

    size_t rounded_end;
    rounded_end = ( in_end & ~(block_size - 1) ) + (block_size - 1);
    if (rounded_end + 1 >= parts_len[j]) {
      rounded_end = parts_len[j] - 1;
    }

    enc_begin_skip = in_ofs & (block_size - 1);
    ofs = bl_ofs - enc_begin_skip;
    end = bl_end;
    bl_ofs = bl_ofs - enc_begin_skip;
    bl_end += rounded_end - in_end;
  }
  else
  {
    enc_begin_skip = bl_ofs & (block_size - 1);
    ofs=bl_ofs & ~(block_size - 1);
    end=bl_end;
    bl_ofs = bl_ofs & ~(block_size - 1);
    bl_end = ( bl_end & ~(block_size - 1) ) + (block_size - 1);
  }
  ldout(cct, 20) << "fixup_range [" << inp_ofs << "," << inp_end
      << "] => [" << bl_ofs << "," << bl_end << "]" << dendl;
  return 0;
}

int RGWGetObj_BlockDecrypt::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
  int res = 0;
  ldout(cct, 20) << "Decrypt " << bl_len << " bytes" << dendl;
  size_t part_ofs = ofs;
  size_t i = 0;
  while (i<parts_len.size() && (part_ofs >= parts_len[i])) {
    part_ofs -= parts_len[i];
    i++;
  }
  if (cache.length() > 0) {
    //append before operation.
    off_t append_size = block_size - cache.length();
    if (append_size > bl_len)
      append_size = bl_len;
    char *src = bl.get_contiguous(bl_ofs, append_size);
    cache.append(src,append_size);
    bl_ofs += append_size;
    bl_len -= append_size;

    if (cache.length() == block_size) {
      bufferlist data;
      crypt->decrypt(cache, 0, block_size, data, part_ofs);
      part_ofs += block_size;
      res = next.handle_data(data, enc_begin_skip, block_size - enc_begin_skip);
      enc_begin_skip = 0;
      cache.clear();
      ofs += block_size;
      if (res != 0)
        return res;
    }
  }
  if (bl_len > 0) {
    off_t aligned_size = bl_len & ~(block_size - 1);
    //save remainder
    off_t remainder = bl.length() - (bl_ofs + aligned_size);
    if(remainder > 0) {
      cache.append(bl.get_contiguous(bl_ofs + aligned_size, remainder), remainder);
    }

    if (aligned_size > 0) {
      bufferlist data;
      crypt->decrypt(bl, bl_ofs, aligned_size, data, part_ofs);
      part_ofs += aligned_size;
      off_t send_size = aligned_size - enc_begin_skip;
      if (ofs + enc_begin_skip + send_size > end + 1) {
        send_size = end + 1 - ofs - enc_begin_skip;
      }
      res = next.handle_data(data, enc_begin_skip, send_size);
      enc_begin_skip = 0;
      ofs += aligned_size;

      if (res != 0)
        return res;
    }
  }
  return 0;
}

/**
 * flush remainder of data to output
 */
int RGWGetObj_BlockDecrypt::flush() {
  int res = 0;
  size_t part_ofs = ofs;
  size_t i = 0;
  while (i<parts_len.size() && (part_ofs > parts_len[i])) {
    part_ofs -= parts_len[i];
    i++;
  }
  if (cache.length() > 0) {
    bufferlist data;
    crypt->decrypt(cache, 0, cache.length(), data, part_ofs);
    off_t send_size = cache.length() - enc_begin_skip;
    if (ofs + enc_begin_skip + send_size > end + 1) {
      send_size = end + 1 - ofs - enc_begin_skip;
    }
    res = next.handle_data(data, enc_begin_skip, send_size);
    enc_begin_skip = 0;
    ofs += send_size;
  }
  return res;
}

RGWPutObj_BlockEncrypt::RGWPutObj_BlockEncrypt(CephContext* cct, RGWPutObjDataProcessor& next, BlockCrypt* crypt):
      RGWPutObj_Filter(next), cct(cct), crypt(crypt),
      ofs(0), cache() {
  block_size = crypt->get_block_size();
}

RGWPutObj_BlockEncrypt::~RGWPutObj_BlockEncrypt() {
  delete crypt;
}
int RGWPutObj_BlockEncrypt::handle_data(bufferlist& bl, off_t in_ofs, void **phandle, rgw_obj *pobj, bool *again) {
  int res = 0;
  if (*again) {
    bufferlist no_data;
    res = next.handle_data(no_data, in_ofs, phandle, pobj, again);
    //if *again is not set to false, we will have endless loop
    //drop info on log
    if (*again) {
      ldout(cct, 0) << "*again==true" << dendl;
    }
    return res;
  }
  off_t bl_ofs = 0;
  if (cache.length() > 0) {
      //append before operation.
      off_t size = block_size - cache.length();
      if (size > bl.length())
        size = bl.length();
      if (size > 0) {
        char *src = bl.get_contiguous(0, size);
        cache.append(src,size);
        bl_ofs += size;
      }
      if (cache.length() == block_size) {
        bufferlist data;
        crypt->encrypt(cache, 0, block_size, data, ofs);
        res = next.handle_data(data, ofs, phandle, pobj, again);
        cache.clear();
        ofs += block_size;
        if (res != 0)
          return res;
      }
    }
  if (bl_ofs < bl.length()) {
    off_t aligned_size = (bl.length() - bl_ofs) & ~(block_size - 1);
    //save remainder
    off_t remainder = (bl.length() - bl_ofs) - aligned_size;
    if(remainder > 0) {
      cache.append(bl.get_contiguous(bl_ofs + aligned_size, remainder), remainder);
    }
    if (aligned_size > 0) {
      bufferlist data;
      crypt->encrypt(bl, bl_ofs, aligned_size, data, ofs);
      res=next.handle_data(data, ofs, phandle, pobj, again);
      ofs += aligned_size;
      if (res != 0)
        return res;
    }
  }
  if (bl.length() == 0) {
    if (cache.length() > 0) {
      /*flush cached data*/
      bufferlist data;
      crypt->encrypt(cache, 0, cache.length(), data, ofs);
      res=next.handle_data(data, ofs, phandle, pobj, again);
      ofs+=cache.length();
      cache.clear();
      if (res != 0)
        return res;
    }
    /*replicate 0-sized handle_data*/
    res=next.handle_data(cache, ofs, phandle, pobj, again);
  }
  return res;
}

int RGWPutObj_BlockEncrypt::throttle_data(void *handle, const rgw_obj& obj,
                                          uint64_t size, bool need_to_wait) {
  return next.throttle_data(handle, obj, size, need_to_wait);
}

std::string create_random_key_selector() {
  char random[AES_256_KEYSIZE];
  if (get_random_bytes(&random[0], sizeof(random)) != 0) {
    dout(0) << "ERROR: cannot get_random_bytes. " << dendl;
    for (char& v:random) v=rand();
  }
  return std::string(random, sizeof(random));
}

//-H "Accept: application/octet-stream" -H "X-Auth-Token: fa7067ae04e942fb879eaf37f46411a5"
//curl -v -H "Accept: application/octet-stream" -H "X-Auth-Token: fa7067ae04e942fb879eaf37f46411a5" http://localhost:9311/v1/secrets/5206dbad-7970-4a7a-82de-bd7df9a016db

int get_barbican_url(CephContext * const cct,
                     std::string& url)
{
  url = cct->_conf->rgw_barbican_url;
  if (url.empty()) {
    ldout(cct, 0) << "ERROR: conf rgw_barbican_url is not set" << dendl;
    return -EINVAL;
  }

  if (url[url.size() - 1] != '/') {
    url.append("/");
  }

  return 0;
}

int request_key_from_barbican(CephContext *cct,
                              boost::string_ref key_id,
                              boost::string_ref key_selector,
                              const std::string& barbican_token,
                              std::string& actual_key) {
  std::string secret_url;
  if (get_barbican_url(cct, secret_url) < 0) {
     return -EINVAL;
  }
  secret_url += "v1/secrets/" + std::string(key_id);

  int res;
  bufferlist secret_bl;
  RGWHTTPTransceiver secret_req(cct, &secret_bl);
  secret_req.append_header("Accept", "application/octet-stream");
  secret_req.append_header("X-Auth-Token", barbican_token);

  res = secret_req.process("GET", secret_url.c_str());
  if (res < 0) {
    return res;
  }
  if (secret_req.get_http_status() ==
      RGWHTTPTransceiver::HTTP_STATUS_UNAUTHORIZED) {
    return -EACCES;
  }

  if (secret_req.get_http_status() >=200 &&
      secret_req.get_http_status() < 300 &&
      secret_bl.length() == AES_256_KEYSIZE) {
    actual_key = std::string(secret_bl.c_str(), secret_bl.length());
    } else {
      res = -EACCES;
    }
  return res;
}


int get_actual_key_from_kms(CephContext *cct, boost::string_ref key_id, boost::string_ref key_selector, std::string& actual_key)
{
  int res = 0;
  ldout(cct, 20) << "Getting KMS encryption key for key=" << key_id << dendl;
  if (key_id.starts_with("testkey-")) {
    /* test keys for testing purposes */
    boost::string_ref key = key_id.substr(sizeof("testkey-")-1);
    std::string master_key;
    if (key == "1")       master_key = "012345678901234567890123456789012345";
    else if (key == "2")  master_key = "abcdefghijklmnopqrstuvwxyzabcdefghij";
    else {
      res = -EIO;
      return res;
    }
    uint8_t _actual_key[AES_256_KEYSIZE];
    if (AES_256_ECB_encrypt((uint8_t*)master_key.c_str(), AES_256_KEYSIZE,
                            (uint8_t*)key_selector.data(),
                            _actual_key, AES_256_KEYSIZE)) {
      actual_key = std::string((char*)&_actual_key[0], AES_256_KEYSIZE);
    } else {
      res = -EIO;
    }
  }
  else {
    std::string token;
    if (rgw::keystone::Service::get_keystone_barbican_token(cct, token) < 0) {
      ldout(cct, 20) << "Failed to retrieve token for barbican" << dendl;
      res = -EINVAL;
      return res;
    }

    res = request_key_from_barbican(cct, key_id, key_selector, token, actual_key);
    if (res != 0) {
      ldout(cct, 0) << "Failed to retrieve secret from barbican:" << key_id << dendl;
    }
  }
  return res;
}

static inline void set_attr(map<string, bufferlist>& attrs, const char* key, const std::string& value)
{
  bufferlist bl;
  ::encode(value,bl);
  attrs.emplace(key, std::move(bl));
}

static inline void set_attr(map<string, bufferlist>& attrs, const char* key, const char* value)
{
  bufferlist bl;
  ::encode(value,bl);
  attrs.emplace(key, std::move(bl));
}

static inline void set_attr(map<string, bufferlist>& attrs, const char* key, boost::string_ref value)
{
  bufferlist bl;
  __u32 len = value.length();
  encode(len, bl);
  if (len)
    bl.append(value.data(), len);
  attrs.emplace(key, std::move(bl));
}

static inline std::string get_str_attribute(map<string, bufferlist>& attrs, const std::string& name, const std::string& default_value="") {
  std::string value;
  auto iter = attrs.find(name);
  if (iter == attrs.end() ) {
    value = default_value;
  } else {
    try {
      ::decode(value, iter->second);
    } catch (buffer::error& err) {
      value = default_value;
      dout(0) << "ERROR: failed to decode attr:" << name << dendl;
    }
  }
  return value;
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

boost::string_ref rgw_trim_whitespace(const boost::string_ref& src)
{
  if (src.empty()) {
    return boost::string_ref();
  }

  int start = 0;
  for (; start != (int)src.size(); start++) {
    if (!isspace(src[start]))
      break;
  }

  int end = src.size() - 1;
  if (end < start) {
    return boost::string_ref();
  }

  for (; end > start; end--) {
    if (!isspace(src[end]))
      break;
  }

  return src.substr(start, end - start + 1);
}

static boost::string_ref get_crypt_attribute(RGWEnv* env,
                                       map<string, post_form_part, const ltstr_nocase>* parts,
                                       crypt_option_e option)
{
  static_assert(X_AMZ_SERVER_SIDE_ENCRYPTION_LAST == sizeof(crypt_options)/sizeof(*crypt_options), "Missing items in crypt_options");
  if (parts != nullptr) {
    map<string, struct post_form_part, ltstr_nocase>::iterator iter
      = parts->find(crypt_options[option].post_part_name);
    if (iter == parts->end())
      return boost::string_ref();
    bufferlist& data = iter->second.data;
    boost::string_ref str = boost::string_ref(data.c_str(), data.length());
    return rgw_trim_whitespace(str);
  } else {
    const char* hdr = env->get(crypt_options[option].http_header_name, nullptr);
    if (hdr != nullptr) {
      return boost::string_ref(hdr);
    } else {
      return boost::string_ref();
    }
  }
}

int s3_prepare_encrypt(struct req_state* s,
                       map<string, bufferlist>& attrs,
                       map<string, post_form_part, const ltstr_nocase>* parts,
                       BlockCrypt** block_crypt,
                       std::string& crypt_http_responses)
{
  int res = 0;
  crypt_http_responses = "";
  if (block_crypt) *block_crypt = nullptr;
  {
    boost::string_ref req_sse_ca =
        get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM);
    if (! req_sse_ca.empty()) {
      if (req_sse_ca != "AES256") {
        res = -ERR_INVALID_REQUEST;
        goto done;
      }
      std::string key_bin = from_base64(
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY) );
      if (key_bin.size() != AES_256_CTR::AES_256_KEYSIZE) {
        res = -ERR_INVALID_REQUEST;
        goto done;
      }
      boost::string_ref keymd5 =
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5);
      std::string keymd5_bin = from_base64(keymd5);
      if (keymd5_bin.size() != CEPH_CRYPTO_MD5_DIGESTSIZE) {
        res = -ERR_INVALID_DIGEST;
        goto done;
      }
      MD5 key_hash;
      uint8_t key_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
      key_hash.Update((uint8_t*)key_bin.c_str(), key_bin.size());
      key_hash.Final(key_hash_res);

      if (memcmp(key_hash_res, keymd5_bin.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) {
        res = -ERR_INVALID_DIGEST;
        goto done;
      }

      set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-C-AES256");
      set_attr(attrs, RGW_ATTR_CRYPT_KEYMD5, keymd5_bin);

      if (block_crypt) {
        AES_256_CTR* aes = new AES_256_CTR(s->cct);
        aes->set_key(reinterpret_cast<const uint8_t*>(key_bin.c_str()), AES_256_KEYSIZE);
        *block_crypt = aes;
      }

      crypt_http_responses =
          "x-amz-server-side-encryption-customer-algorithm: AES256\r\n"
          "x-amz-server-side-encryption-customer-key-MD5: " + std::string(keymd5) + "\r\n";
      goto done;
    }
    /* AMAZON server side encryption with KMS (key management service) */
    boost::string_ref req_sse =
        get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION);
    if (! req_sse.empty()) {
      if (req_sse != "aws:kms") {
        res = -ERR_INVALID_REQUEST;
        goto done;
      }
      boost::string_ref key_id =
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID);
      if (key_id.empty()) {
        /* TODO!!! retrieve key_id from bucket */
        res = -ERR_INVALID_ACCESS_KEY;
        goto done;
      }
      /* try to retrieve actual key */
      std::string key_selector = create_random_key_selector();
      std::string actual_key;
      res = get_actual_key_from_kms(s->cct, key_id, key_selector, actual_key);
      if (res != 0)
        goto done;
      if (actual_key.size() != AES_256_KEYSIZE) {
        ldout(s->cct, 0) << "ERROR: key obtained from key_id:" <<
            key_id << " is not 256 bit size" << dendl;
        res = -ERR_INVALID_ACCESS_KEY;
        goto done;
      }
      set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-KMS");
      set_attr(attrs, RGW_ATTR_CRYPT_KEYID, key_id);
      set_attr(attrs, RGW_ATTR_CRYPT_KEYSEL, key_selector);

      if (block_crypt) {
        AES_256_CTR* aes = new AES_256_CTR(s->cct);
        aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);
        *block_crypt = aes;
      }
      goto done;
    }

    /* no other encryption mode, check if default encryption is selected */
    if (s->cct->_conf->rgw_crypt_default_encryption_key != "") {
      std::string master_encryption_key = from_base64(std::string(s->cct->_conf->rgw_crypt_default_encryption_key));
      if (master_encryption_key.size() != 256 / 8) {
        ldout(s->cct, 0) << "ERROR: failed to decode 'rgw crypt default encryption key' to 256 bit string" << dendl;
        /* not an error to return; missing encryption does not inhibit processing */
        goto done;
      }

      set_attr(attrs, RGW_ATTR_CRYPT_MODE, "RGW-AUTO");
      std::string key_selector = create_random_key_selector();
      set_attr(attrs, RGW_ATTR_CRYPT_KEYSEL, key_selector);

      uint8_t actual_key[AES_256_KEYSIZE];
      if (AES_256_ECB_encrypt((uint8_t*)master_encryption_key.c_str(), AES_256_KEYSIZE,
                              (uint8_t*)key_selector.c_str(),
                              actual_key, AES_256_KEYSIZE) != true) {
        res = -EIO;
        goto done;
      }
      if (block_crypt) {
        AES_256_CTR* aes = new AES_256_CTR(s->cct);
        aes->set_key(actual_key, AES_256_KEYSIZE);
        *block_crypt = aes;
      }

      goto done;
    }
  }
  done:
  return res;
}

int s3_prepare_decrypt(
    struct req_state* s,
    map<string, bufferlist>& attrs,
    BlockCrypt** block_crypt,
    std::map<std::string, std::string>& crypt_http_responses)
{
  int res = 0;
  std::string stored_mode = get_str_attribute(attrs, RGW_ATTR_CRYPT_MODE);
  ldout(s->cct, 15) << "Encryption mode: " << stored_mode << dendl;

  if (stored_mode == "SSE-C-AES256") {
    const char *req_cust_alg = s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM", NULL);
    if ((nullptr == req_cust_alg) || (strcmp(req_cust_alg, "AES256") != 0)) {
      res = -ERR_INVALID_REQUEST;
      goto done;
    }

    std::string key_bin = from_base64(s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY", ""));
    if (key_bin.size() != AES_256_CTR::AES_256_KEYSIZE) {
      res = -ERR_INVALID_REQUEST;
      goto done;
    }

    std::string keymd5 = s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5", "");
    std::string keymd5_bin = from_base64(keymd5);
    if (keymd5_bin.size() != CEPH_CRYPTO_MD5_DIGESTSIZE) {
      res = -ERR_INVALID_DIGEST;
      goto done;
    }
    MD5 key_hash;
    uint8_t key_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
    key_hash.Update((uint8_t*)key_bin.c_str(), key_bin.size());
    key_hash.Final(key_hash_res);

    if ((memcmp(key_hash_res, keymd5_bin.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) ||
        (get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYMD5) != keymd5_bin)) {
      res = -ERR_INVALID_DIGEST;
      goto done;
    }
    AES_256_CTR* aes = new AES_256_CTR(s->cct);
    aes->set_key((uint8_t*)key_bin.c_str(), AES_256_CTR::AES_256_KEYSIZE);
    if (block_crypt) *block_crypt = aes;

    crypt_http_responses =
        "x-amz-server-side-encryption-customer-algorithm: AES256\r\n"
        "x-amz-server-side-encryption-customer-key-MD5: " + keymd5 + "\r\n";
    goto done;
  }

  if (stored_mode == "SSE-KMS") {
    /* try to retrieve actual key */
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    std::string key_selector = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYSEL);
    std::string actual_key;
    res = get_actual_key_from_kms(s->cct, key_id, key_selector, actual_key);
    if (res != 0) {
      ldout(s->cct, 10) << "No encryption key for key-id=" << key_id << dendl;
      goto done;
    }
    if (actual_key.size() != AES_256_KEYSIZE) {
      ldout(s->cct, 0) << "ERROR: key obtained from key_id:" <<
          key_id << " is not 256 bit size" << dendl;
      res = -ERR_INVALID_ACCESS_KEY;
      goto done;
    }

    AES_256_CTR* aes = new AES_256_CTR(s->cct);
    aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);

    if (block_crypt) *block_crypt = aes;

    crypt_http_responses =
        "x-amz-server-side-encryption: aws:kms\r\n"
        "x-amz-server-side-encryption-aws-kms-key-id: " + key_id + "\r\n";
    goto done;
  }

  if (stored_mode == "RGW-AUTO") {
    std::string master_encryption_key = from_base64(std::string(s->cct->_conf->rgw_crypt_default_encryption_key));
    if (master_encryption_key.size() != 256 / 8) {
      ldout(s->cct, 0) << "ERROR: failed to decode 'rgw crypt default encryption key' to 256 bit string" << dendl;
      res = -EIO;
      goto done;
    }
    std::string attr_key_selector = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYSEL);
    if (attr_key_selector.size() != AES_256_CTR::AES_256_KEYSIZE) {
      ldout(s->cct, 0) << "ERROR: missing or invalid " RGW_ATTR_CRYPT_KEYSEL << dendl;
      res = -EIO;
      goto done;
    }
    uint8_t actual_key[AES_256_KEYSIZE];
    if (AES_256_ECB_encrypt((uint8_t*)master_encryption_key.c_str(), AES_256_KEYSIZE,
                            (uint8_t*)attr_key_selector.c_str(),
                            actual_key, AES_256_KEYSIZE) != true) {
      res = -EIO;
      goto done;
    }
    AES_256_CTR* aes = new AES_256_CTR(s->cct);
    aes->set_key(actual_key, AES_256_KEYSIZE);

    if (block_crypt) *block_crypt = aes;
    goto done;
  }

  done:
  return res;
}

