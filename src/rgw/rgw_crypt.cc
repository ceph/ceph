// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/**
 * Crypto filters for Put/Post/Get operations.
 */
#include <rgw/rgw_op.h>
#include <rgw/rgw_crypt.h>
#include <auth/Crypto.h>
#include <rgw/rgw_b64.h>
#include <rgw/rgw_rest_s3.h>
#include "include/assert.h"
#include <boost/utility/string_view.hpp>
#include <rgw/rgw_keystone.h>
#include "include/str_map.h"
#include "crypto/crypto_accel.h"
#include "crypto/crypto_plugin.h"
#ifdef USE_NSS
# include <nspr.h>
# include <nss.h>
# include <pk11pub.h>
#endif

#ifdef USE_CRYPTOPP
#include <cryptopp/cryptlib.h>
#include <cryptopp/modes.h>
#include <cryptopp/aes.h>

/*
 * patching for https://github.com/openssl/openssl/issues/1437
 * newer versions of civetweb do not experience problems
 */
extern "C"
{
bool SSL_CTX_set_ecdh_auto(void* dummy, int onoff)
{
  return onoff!=0;
}
}

using namespace CryptoPP;
#endif

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
  AES_256_CBC(CephContext* cct): cct(cct) {
  }
  ~AES_256_CBC() {
    memset(key, 0, AES_256_KEYSIZE);
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

  bool cbc_transform(bufferlist& out,
                     bufferlist& in,
                     bufferptr& iv,
                     bufferptr& key,
                     bool encrypt)
  {
    CryptoHandler* ch = CryptoHandler::create(CEPH_CRYPTO_AES_256_CBC);
    string error;

    size_t out_len = out.length();
    CryptoKeyHandler* ckh = ch->get_key_handler(key, iv, error);
    if (encrypt) {
      ckh->encrypt(in, out, &error);
    } else {
      ckh->decrypt(in, out, &error);
    }
    assert(out.length() == out_len+in.length());
    delete ckh;
    delete ch;
    return true;
  }

  bool cbc_transform(bufferlist& output,
                     bufferlist& input,
                     off_t stream_offset,
                     bufferptr& key,
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
    if (key.length() != AES_256_KEYSIZE) {
      return false;
    }
    unsigned char iv_arr[AES_256_IVSIZE];
    unsigned char key_arr[AES_256_KEYSIZE];
    if (crypto_accel != nullptr) {
      memcpy(key_arr, key.c_str(), AES_256_KEYSIZE);
    }
    bool result = true;
    size_t size = input.length();
    for (size_t offset = 0; result && (offset < size); offset += CHUNK_SIZE) {
      size_t process_size = offset + CHUNK_SIZE <= size ? CHUNK_SIZE : size - offset;
      bufferptr iv = prepare_iv(stream_offset + offset);
      assert(iv.length() == AES_256_IVSIZE);
      if (crypto_accel != nullptr) {
        memcpy(key_arr, key.c_str(), AES_256_KEYSIZE);
        memcpy(iv_arr, iv.c_str(), AES_256_IVSIZE);
        bufferptr out_buf(process_size);
        unsigned char* out_ptr =
            reinterpret_cast<unsigned char*>(out_buf.c_str());
        const unsigned char* in_ptr =
          reinterpret_cast<unsigned char*>(input.get_contiguous(offset, process_size));
        if (encrypt) {
          result = crypto_accel->cbc_encrypt(out_ptr, in_ptr,
                                             process_size, iv_arr, key_arr);
        } else {
          result = crypto_accel->cbc_decrypt(out_ptr, in_ptr,
                                             process_size, iv_arr, key_arr);
        }
        output.append(out_buf);
      } else {
        bufferlist _tmp;
        _tmp.substr_of(input, offset, process_size);

        result = cbc_transform(output, _tmp, iv, key, encrypt);
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
    bufferptr _key((char*)key, AES_256_KEYSIZE);

    /* encrypt main bulk of data */
    bufferlist _tmp;
    _tmp.substr_of(input, in_ofs, aligned_size);
    result = cbc_transform(output, _tmp, stream_offset, _key, true);

    if (result && (unaligned_rest_size > 0)) {
      /* remainder to encrypt */
      bufferlist unaligned_chunk;
      if (aligned_size % CHUNK_SIZE > 0) {
        /* use last chunk for unaligned part */
        bufferptr iv(AES_256_IVSIZE);
        iv.zero();
        bufferlist _tmp;
        _tmp.substr_of(output, aligned_size - AES_256_IVSIZE, AES_256_IVSIZE);
        result = cbc_transform(unaligned_chunk, _tmp, iv, _key, true);
      } else {
        /* 0 full blocks in current chunk, use IV as base for unaligned part */
        bufferptr iv(AES_256_IVSIZE);
        iv.zero();
        bufferptr data = prepare_iv(stream_offset + aligned_size);
        bufferlist _data;
        _data.append(data);
        result = cbc_transform(unaligned_chunk, _data, iv, _key, true);
      }
      if (result) {
        char* unaligned_chunk_bytes = unaligned_chunk.c_str();
        const char* unaligned_rest_bytes = input.get_contiguous(in_ofs + aligned_size, unaligned_rest_size);
        for (size_t i = 0 ; i < unaligned_rest_size ; i++) {
          unaligned_chunk_bytes[i] ^= unaligned_rest_bytes[i];
        }
        output.append(unaligned_chunk_bytes, unaligned_rest_size);
      }
    }
    if (result) {
      ldout(cct, 25) << "Encrypted " << size << " bytes"<< dendl;
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
    /* decrypt main bulk of data */
    bufferptr _key((char*)key, AES_256_KEYSIZE);

    bufferlist _tmp;
    _tmp.substr_of(input, in_ofs, aligned_size);
    result = cbc_transform(output,
                           _tmp,
                           stream_offset, _key, false);

    if (result && unaligned_rest_size > 0) {
      /* remainder to decrypt */
      bufferlist unaligned_chunk;
      if (aligned_size % CHUNK_SIZE > 0) {
        /* use last chunk for unaligned part */
        bufferptr iv(AES_256_IVSIZE);
        iv.zero();
        bufferlist _tmp;
        _tmp.substr_of(input, in_ofs + aligned_size - AES_256_IVSIZE, AES_256_IVSIZE);
        result = cbc_transform(unaligned_chunk, _tmp, iv, _key, true);
      } else {
        /* 0 full blocks in current chunk, use IV as base for unaligned part */
        bufferptr iv(AES_256_IVSIZE);
        iv.zero();
        bufferptr data = prepare_iv(stream_offset + aligned_size);
        bufferlist _data;
        _data.append(data);
        result = cbc_transform(unaligned_chunk, _data, iv, _key, true);
      }
      if (result) {
        char* unaligned_chunk_bytes = unaligned_chunk.c_str();
        const char* unaligned_rest_bytes = input.get_contiguous(in_ofs + aligned_size, unaligned_rest_size);
        for (size_t i = 0 ; i < unaligned_rest_size ; i++) {
          unaligned_chunk_bytes[i] ^= unaligned_rest_bytes[i];
        }
        output.append(unaligned_chunk_bytes, unaligned_rest_size);
      }
    }
    if (result) {
      ldout(cct, 25) << "Decrypted " << size << " bytes"<< dendl;
    } else {
      ldout(cct, 5) << "Failed to decrypt" << dendl;
    }
    return result;
  }

  void prepare_iv(byte (&iv)[AES_256_IVSIZE], off_t offset) {
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

  bufferptr prepare_iv(off_t offset) {
    bufferptr iv(AES_256_IVSIZE);
    unsigned char* _iv = (unsigned char*) iv.c_str();
    off_t index = offset / AES_256_IVSIZE;
    off_t i = AES_256_IVSIZE - 1;
    unsigned int val;
    unsigned int carry = 0;
    while (i>=0) {
      val = (index & 0xff) + IV[i] + carry;
      _iv[i] = val;
      carry = val >> 8;
      index = index >> 8;
      i--;
    }
    return iv;
  }

};


std::unique_ptr<BlockCrypt> AES_256_CBC_create(CephContext* cct, const uint8_t* key, size_t len)
{
  auto cbc = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(cct));
  cbc->set_key(key, AES_256_KEYSIZE);
  return std::move(cbc);
}


const uint8_t AES_256_CBC::IV[AES_256_CBC::AES_256_IVSIZE] =
    { 'a', 'e', 's', '2', '5', '6', 'i', 'v', '_', 'c', 't', 'r', '1', '3', '3', '7' };


bool AES_256_ECB_encrypt(CephContext* cct,
                         const bufferptr& key,
                         const bufferlist& in,
                         bufferlist& out)
{
  CryptoHandler* ch = CryptoHandler::create(CEPH_CRYPTO_AES_256_ECB);
  if (!ch) {
    ldout(cct, 5) << "Cannot create CEPH_CRYPTO_AES_256_ECB" << dendl;
    return false;
  }
  string error;
  int result;
  size_t out_len = out.length();
  CryptoKeyHandler* ckh = ch->get_key_handler(key, error);
  result = ckh->encrypt(in, out, &error);

  if (result != 0) {
    ldout(cct, 5) << "Error in ECB encryption:" << error << dendl;
  }
  assert(out.length() == out_len+in.length());
  delete ckh;
  delete ch;
  return (result == 0);
}


RGWGetObj_BlockDecrypt::RGWGetObj_BlockDecrypt(CephContext* cct,
                                               RGWGetDataCB* next,
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
    off_t in_ofs = bl_ofs;
    off_t in_end = bl_end;

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
    ofs = bl_ofs & ~(block_size - 1);
    end = bl_end;
    bl_ofs = bl_ofs & ~(block_size - 1);
    bl_end = ( bl_end & ~(block_size - 1) ) + (block_size - 1);
  }
  ldout(cct, 20) << "fixup_range [" << inp_ofs << "," << inp_end
      << "] => [" << bl_ofs << "," << bl_end << "]" << dendl;
  return 0;
}


int RGWGetObj_BlockDecrypt::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
  int res = 0;
  ldout(cct, 25) << "Decrypt " << bl_len << " bytes" << dendl;
  size_t part_ofs = ofs;
  size_t i = 0;
  while (i<parts_len.size() && (part_ofs >= parts_len[i])) {
    part_ofs -= parts_len[i];
    i++;
  }
  bl.copy(bl_ofs, bl_len, cache);
  off_t aligned_size = cache.length() & ~(block_size - 1);
  if (aligned_size > 0) {
    bufferlist data;
    if (! crypt->decrypt(cache, 0, aligned_size, data, part_ofs) ) {
      return -ERR_INTERNAL_ERROR;
    }
    off_t send_size = aligned_size - enc_begin_skip;
    if (ofs + enc_begin_skip + send_size > end + 1) {
      send_size = end + 1 - ofs - enc_begin_skip;
    }
    res = next->handle_data(data, enc_begin_skip, send_size);
    enc_begin_skip = 0;
    ofs += aligned_size;
    cache.splice(0, aligned_size);
  }
  return res;
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
    if (! crypt->decrypt(cache, 0, cache.length(), data, part_ofs) ) {
      return -ERR_INTERNAL_ERROR;
    }
    off_t send_size = cache.length() - enc_begin_skip;
    if (ofs + enc_begin_skip + send_size > end + 1) {
      send_size = end + 1 - ofs - enc_begin_skip;
    }
    res = next->handle_data(data, enc_begin_skip, send_size);
    enc_begin_skip = 0;
    ofs += send_size;
  }
  return res;
}

RGWPutObj_BlockEncrypt::RGWPutObj_BlockEncrypt(CephContext* cct,
                                               RGWPutObjDataProcessor* next,
                                               std::unique_ptr<BlockCrypt> crypt):
    RGWPutObj_Filter(next),
    cct(cct),
    crypt(std::move(crypt)),
    ofs(0),
    cache()
{
  block_size = this->crypt->get_block_size();
}

RGWPutObj_BlockEncrypt::~RGWPutObj_BlockEncrypt() {
}

int RGWPutObj_BlockEncrypt::handle_data(bufferlist& bl,
                                        off_t in_ofs,
                                        void **phandle,
                                        rgw_raw_obj *pobj,
                                        bool *again) {
  int res = 0;
  ldout(cct, 25) << "Encrypt " << bl.length() << " bytes" << dendl;

  if (*again) {
    bufferlist no_data;
    res = next->handle_data(no_data, in_ofs, phandle, pobj, again);
    //if *again is not set to false, we will have endless loop
    //drop info on log
    if (*again) {
      ldout(cct, 20) << "*again==true" << dendl;
    }
    return res;
  }

  cache.append(bl);
  off_t proc_size = cache.length() & ~(block_size - 1);
  if (bl.length() == 0) {
    proc_size = cache.length();
  }
  if (proc_size > 0) {
    bufferlist data;
    if (! crypt->encrypt(cache, 0, proc_size, data, ofs) ) {
      return -ERR_INTERNAL_ERROR;
    }
    res = next->handle_data(data, ofs, phandle, pobj, again);
    ofs += proc_size;
    cache.splice(0, proc_size);
    if (res < 0)
      return res;
  }

  if (bl.length() == 0) {
    /*replicate 0-sized handle_data*/
    res = next->handle_data(bl, ofs, phandle, pobj, again);
  }
  return res;
}

int RGWPutObj_BlockEncrypt::throttle_data(void *handle,
                                          const rgw_raw_obj& obj,
                                          uint64_t size,
                                          bool need_to_wait) {
  return next->throttle_data(handle, obj, size, need_to_wait);
}

std::string create_random_key_selector(CephContext * const cct) {
  char random[AES_256_KEYSIZE];
  if (get_random_bytes(&random[0], sizeof(random)) != 0) {
    ldout(cct, 0) << "ERROR: cannot get_random_bytes. " << dendl;
    for (char& v:random) v=rand();
  }
  return std::string(random, sizeof(random));
}

static int get_barbican_url(CephContext * const cct,
                     std::string& url)
{
  url = cct->_conf->rgw_barbican_url;
  if (url.empty()) {
    ldout(cct, 0) << "ERROR: conf rgw_barbican_url is not set" << dendl;
    return -EINVAL;
  }
  if (url.back() != '/') {
    url.append("/");
  }
  return 0;
}

static int request_key_from_barbican(CephContext *cct,
                              boost::string_view key_id,
                              boost::string_view key_selector,
                              const std::string& barbican_token,
                              bufferlist& secret_bl) {
  std::string secret_url;
  int res;
  res = get_barbican_url(cct, secret_url);
  if (res < 0) {
     return res;
  }
  secret_url += "v1/secrets/" + std::string(key_id);

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

  if ( !(secret_req.get_http_status() >=200 &&
      secret_req.get_http_status() < 300 &&
      secret_bl.length() == AES_256_KEYSIZE) ) {
      memset(secret_bl.c_str(), 0, secret_bl.length());
      secret_bl.clear();
      res = -EACCES;
    }
  return res;
}

static map<string,string> get_str_map(const string &str) {
  map<string,string> m;
  get_str_map(str, &m, ";, \t");
  return m;
}

static int get_actual_key_from_kms(CephContext *cct,
                            boost::string_view key_id,
                            boost::string_view key_selector,
                            bufferlist& actual_key)
{
  int res = 0;
  ldout(cct, 20) << "Getting KMS encryption key for key=" << key_id << dendl;
  static map<string,string> str_map = get_str_map(
      cct->_conf->rgw_crypt_s3_kms_encryption_keys);

  map<string, string>::iterator it = str_map.find(std::string(key_id));
  if (it != str_map.end() ) {
    std::string master_key = from_base64((*it).second);
    if (master_key.length() == AES_256_KEYSIZE) {
      bufferptr _master_key(master_key.c_str(), master_key.length());
      bufferlist _key_selector;
      _key_selector.append( bufferptr(key_selector.data(), key_selector.length()) );
      actual_key.clear();
      if ( !AES_256_ECB_encrypt(cct,
          _master_key,
          _key_selector,
          actual_key) ) {
        res = -EIO;
      }
    } else {
      ldout(cct, 20) << "Wrong size for key=" << key_id << dendl;
      res = -EIO;
    }
  } else {
    std::string token;
    if (rgw::keystone::Service::get_keystone_barbican_token(cct, token) < 0) {
      ldout(cct, 5) << "Failed to retrieve token for barbican" << dendl;
      res = -EINVAL;
      return res;
    }

    res = request_key_from_barbican(cct, key_id, key_selector, token, actual_key);
    if (res != 0) {
      ldout(cct, 5) << "Failed to retrieve secret from barbican:" << key_id << dendl;
    }
  }
  return res;
}

static inline void set_attr(map<string, bufferlist>& attrs,
                            const char* key,
                            boost::string_view value)
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

static boost::string_view get_crypt_attribute(
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
      return boost::string_view();
    bufferlist& data = iter->second.data;
    boost::string_view str = boost::string_view(data.c_str(), data.length());
    return rgw_trim_whitespace(str);
  } else {
    const char* hdr = env->get(crypt_options[option].http_header_name, nullptr);
    if (hdr != nullptr) {
      return boost::string_view(hdr);
    } else {
      return boost::string_view();
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
    boost::string_view req_sse_ca =
        get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM);
    if (! req_sse_ca.empty()) {
      if (req_sse_ca != "AES256") {
        ldout(s->cct, 5) << "ERROR: Invalid value for header "
                         << "x-amz-server-side-encryption-customer-algorithm"
                         << dendl;
        return -ERR_INVALID_REQUEST;
      }
      if (s->cct->_conf->rgw_crypt_require_ssl &&
          !s->info.env->exists("SERVER_PORT_SECURE")) {
        ldout(s->cct, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
        return -ERR_INVALID_REQUEST;
      }
      std::string key_bin = from_base64(
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY) );
      if (key_bin.size() != AES_256_CBC::AES_256_KEYSIZE) {
        ldout(s->cct, 5) << "ERROR: invalid encryption key size" << dendl;
        return -ERR_INVALID_REQUEST;
      }
      boost::string_view keymd5 =
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5);
      std::string keymd5_bin = from_base64(keymd5);
      if (keymd5_bin.size() != CEPH_CRYPTO_MD5_DIGESTSIZE) {
        ldout(s->cct, 5) << "ERROR: Invalid key md5 size" << dendl;
        return -ERR_INVALID_DIGEST;
      }
      MD5 key_hash;
      byte key_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
      key_hash.Update(reinterpret_cast<const byte*>(key_bin.c_str()), key_bin.size());
      key_hash.Final(key_hash_res);

      if (memcmp(key_hash_res, keymd5_bin.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) {
        ldout(s->cct, 5) << "ERROR: Invalid key md5 hash" << dendl;
        return -ERR_INVALID_DIGEST;
      }

      set_attr(attrs, RGW_ATTR_CRYPT_MODE, "SSE-C-AES256");
      set_attr(attrs, RGW_ATTR_CRYPT_KEYMD5, keymd5_bin);

      if (block_crypt) {
        auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s->cct));
        aes->set_key(reinterpret_cast<const uint8_t*>(key_bin.c_str()), AES_256_KEYSIZE);
        *block_crypt = std::move(aes);
      }

      crypt_http_responses["x-amz-server-side-encryption-customer-algorithm"] = "AES256";
      crypt_http_responses["x-amz-server-side-encryption-customer-key-MD5"] = keymd5.to_string();
      return 0;
    }
    /* AMAZON server side encryption with KMS (key management service) */
    boost::string_view req_sse =
        get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION);
    if (! req_sse.empty()) {
      if (req_sse != "aws:kms") {
        ldout(s->cct, 5) << "ERROR: Invalid value for header x-amz-server-side-encryption"
                         << dendl;
        return -ERR_INVALID_REQUEST;
      }
      if (s->cct->_conf->rgw_crypt_require_ssl &&
          !s->info.env->exists("SERVER_PORT_SECURE")) {
        ldout(s->cct, 5) << "ERROR: insecure request, rgw_crypt_require_ssl is set" << dendl;
        return -ERR_INVALID_REQUEST;
      }
      boost::string_view key_id =
          get_crypt_attribute(s->info.env, parts, X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID);
      if (key_id.empty()) {
        return -ERR_INVALID_ACCESS_KEY;
      }
      /* try to retrieve actual key */
      std::string key_selector = create_random_key_selector(s->cct);
      bufferlist actual_key;
      res = get_actual_key_from_kms(s->cct, key_id, key_selector, actual_key);
      if (res != 0)
        return res;
      if (actual_key.length() != AES_256_KEYSIZE) {
        ldout(s->cct, 5) << "ERROR: key obtained from key_id:" <<
            key_id << " is not 256 bit size" << dendl;
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
      memset(actual_key.c_str(), 0, actual_key.length());
      return 0;
    }

    /* no other encryption mode, check if default encryption is selected */
    if (s->cct->_conf->rgw_crypt_default_encryption_key != "") {
      std::string master_encryption_key =
          from_base64(s->cct->_conf->rgw_crypt_default_encryption_key);
      if (master_encryption_key.size() != 256 / 8) {
        ldout(s->cct, 0) << "ERROR: failed to decode 'rgw crypt default encryption key' to 256 bit string" << dendl;
        /* not an error to return; missing encryption does not inhibit processing */
        return 0;
      }

      set_attr(attrs, RGW_ATTR_CRYPT_MODE, "RGW-AUTO");
      std::string key_selector = create_random_key_selector(s->cct);
      set_attr(attrs, RGW_ATTR_CRYPT_KEYSEL, key_selector);

      bufferptr _master_encryption_key( master_encryption_key.c_str(),
                                        master_encryption_key.length());
      bufferlist _key_selector;
      _key_selector.append(bufferptr( key_selector.c_str(),
                                      key_selector.length()) );
      bufferlist _actual_key;
      if (AES_256_ECB_encrypt(s->cct,
                              _master_encryption_key,
                              _key_selector,
                              _actual_key) != true) {
        memset(_actual_key.c_str(), 0, _actual_key.length());
        return -EIO;
      }
      if (block_crypt) {
        auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s->cct));
        aes->set_key(reinterpret_cast<const uint8_t*>(_actual_key.c_str()), AES_256_KEYSIZE);
        *block_crypt = std::move(aes);
      }
      memset(_actual_key.c_str(), 0, _actual_key.length());
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
  if (stored_mode == "SSE-C-AES256") {
    if (s->cct->_conf->rgw_crypt_require_ssl &&
        !s->info.env->exists("SERVER_PORT_SECURE")) {
      ldout(s->cct, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
      return -ERR_INVALID_REQUEST;
    }
    const char *req_cust_alg =
        s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM", NULL);

    if ((nullptr == req_cust_alg) || (strcmp(req_cust_alg, "AES256") != 0)) {
      ldout(s->cct, 5) << "ERROR: Invalid value for header "
                       << "x-amz-server-side-encryption-customer-algorithm"
                       << dendl;
      return -ERR_INVALID_REQUEST;
    }

    std::string key_bin =
        from_base64(s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY", ""));
    if (key_bin.size() != AES_256_CBC::AES_256_KEYSIZE) {
      ldout(s->cct, 5) << "ERROR: Invalid encryption key size" << dendl;
      return -ERR_INVALID_REQUEST;
    }

    std::string keymd5 =
        s->info.env->get("HTTP_X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5", "");
    std::string keymd5_bin = from_base64(keymd5);
    if (keymd5_bin.size() != CEPH_CRYPTO_MD5_DIGESTSIZE) {
      ldout(s->cct, 5) << "ERROR: Invalid key md5 size " << dendl;
      return -ERR_INVALID_DIGEST;
    }

    MD5 key_hash;
    uint8_t key_hash_res[CEPH_CRYPTO_MD5_DIGESTSIZE];
    key_hash.Update(reinterpret_cast<const byte*>(key_bin.c_str()), key_bin.size());
    key_hash.Final(key_hash_res);

    if ((memcmp(key_hash_res, keymd5_bin.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE) != 0) ||
        (get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYMD5) != keymd5_bin)) {
      return -ERR_INVALID_DIGEST;
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
        !s->info.env->exists("SERVER_PORT_SECURE")) {
      ldout(s->cct, 5) << "ERROR: Insecure request, rgw_crypt_require_ssl is set" << dendl;
      return -ERR_INVALID_REQUEST;
    }
    /* try to retrieve actual key */
    std::string key_id = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYID);
    std::string key_selector = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYSEL);
    bufferlist actual_key;
    res = get_actual_key_from_kms(s->cct, key_id, key_selector, actual_key);
    if (res != 0) {
      ldout(s->cct, 10) << "No encryption key for key-id=" << key_id << dendl;
      return res;
    }
    if (actual_key.length() != AES_256_KEYSIZE) {
      ldout(s->cct, 0) << "ERROR: key obtained from key_id:" <<
          key_id << " is not 256 bit size" << dendl;
      return -ERR_INVALID_ACCESS_KEY;
    }

    auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s->cct));
    aes->set_key(reinterpret_cast<const uint8_t*>(actual_key.c_str()), AES_256_KEYSIZE);
    memset(actual_key.c_str(), 0, actual_key.length());
    if (block_crypt) *block_crypt = std::move(aes);

    crypt_http_responses["x-amz-server-side-encryption"] = "aws:kms";
    crypt_http_responses["x-amz-server-side-encryption-aws-kms-key-id"] = key_id;
    return 0;
  }

  if (stored_mode == "RGW-AUTO") {
    std::string master_encryption_key =
        from_base64(std::string(s->cct->_conf->rgw_crypt_default_encryption_key));
    if (master_encryption_key.size() != 256 / 8) {
      ldout(s->cct, 0) << "ERROR: failed to decode 'rgw crypt default encryption key' to 256 bit string" << dendl;
      return -EIO;
    }
    std::string attr_key_selector = get_str_attribute(attrs, RGW_ATTR_CRYPT_KEYSEL);
    if (attr_key_selector.size() != AES_256_CBC::AES_256_KEYSIZE) {
      ldout(s->cct, 0) << "ERROR: missing or invalid " RGW_ATTR_CRYPT_KEYSEL << dendl;
      return -EIO;
    }
    bufferptr _master_encryption_key( master_encryption_key.c_str(),
                                      master_encryption_key.length());
    bufferlist _key_selector;
    _key_selector.append(bufferptr( attr_key_selector.c_str(),
                                    attr_key_selector.length()) );
    bufferlist _actual_key;
    if (AES_256_ECB_encrypt(s->cct,
                            _master_encryption_key,
                            _key_selector,
                            _actual_key) != true) {
      memset(_actual_key.c_str(), 0, _actual_key.length());
      return -EIO;
    }
    auto aes = std::unique_ptr<AES_256_CBC>(new AES_256_CBC(s->cct));
    aes->set_key(reinterpret_cast<const uint8_t*>(_actual_key.c_str()), AES_256_KEYSIZE);
    memset(_actual_key.c_str(), 0, _actual_key.length());
    if (block_crypt) *block_crypt = std::move(aes);
    return 0;
  }
  /*no decryption*/
  return 0;
}
