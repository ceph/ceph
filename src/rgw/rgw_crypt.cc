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

#define dout_subsys ceph_subsys_rgw

using namespace CryptoPP;

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
  bool set_key(uint8_t* _key, size_t key_size) {
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
bool AES_256_CTR::set_key(uint8_t* key, size_t key_size) {
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

int RGWPutObj_BlockEncrypt::throttle_data(void *handle, const rgw_obj& obj, bool need_to_wait) {
  return next.throttle_data(handle, obj, need_to_wait);
}



