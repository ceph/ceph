// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/**
 * Crypto filters for Put/Post/Get operations.
 */
#include <rgw/rgw_op.h>
#include <rgw/rgw_crypt.h>
#define dout_subsys ceph_subsys_rgw




AES_256_CTR::AES_256_CTR() {

}
AES_256_CTR::~AES_256_CTR() {

}
bool AES_256_CTR::set_key(uint8_t* key, uint8_t* nonce, size_t key_len)
{
  return true;
}
size_t AES_256_CTR::get_block_size() {
  return 4096;
}
bool AES_256_CTR::encrypt(const void *src, size_t size, void *dst, off_t offset) {
  return true;
}
bool AES_256_CTR::decrypt(const void *src, size_t size, void *dst, off_t offset) {
  return true;
}

RGWGetObj_BlockDecrypt::RGWGetObj_BlockDecrypt(CephContext* cct, RGWGetDataCB& next, BlockCrypt* crypt):
    RGWGetObj_Filter(next), cct(cct), crypt(crypt),
    enc_begin_skip(0), ofs(0), end(0), cache() {
  block_size = crypt->get_block_size();
  }
RGWGetObj_BlockDecrypt::~RGWGetObj_BlockDecrypt() {}


void RGWGetObj_BlockDecrypt::fixup_range(off_t& bl_ofs, off_t& bl_end) {
  off_t in_ofs=bl_ofs;
  off_t in_end=bl_end;
  enc_begin_skip = bl_ofs & (block_size - 1);
  ofs=bl_ofs & ~(block_size - 1);
  end=bl_end;
  bl_ofs = bl_ofs & ~(block_size - 1);
  bl_end = ( bl_end & ~(block_size - 1) ) + (block_size - 1);
  ldout(cct, 20) << "fixup_range in_ofs: " << in_ofs << " in_end " << in_end << dendl;
  ldout(cct, 20) << " enc_skip: " << enc_begin_skip << " ofs: " << ofs << " end: " << end << dendl;
  ldout(cct, 20) << " bl_ofs: " << bl_ofs << " bl_end: " << bl_end << dendl;
}

int RGWGetObj_BlockDecrypt::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
  int res = 0;
  ldout(cct, 20)
  << "HANDLE DATA bl_ofs " << bl_ofs
  << " bl_len " << bl_len
  << " ofs: " << ofs
  << " end: " << end
  << " cached: " << cache.length()
  << dendl;
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
      crypt->decrypt(cache.c_str(), block_size, cache.c_str(), ofs);
      res = next.handle_data(cache, enc_begin_skip, block_size - enc_begin_skip);
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
      //TODO - avoid forcing long conignous areas
      char *src = bl.get_contiguous(bl_ofs, aligned_size);
      crypt->decrypt(src, aligned_size, src, ofs);
      //content of bl changes
      //res = op->send_response_data(bl, bl_ofs + enc_begin_skip, aligned_size - enc_begin_skip);
      off_t send_size = aligned_size - enc_begin_skip;
      if (ofs + enc_begin_skip + send_size > end + 1) {
        send_size = end + 1 - ofs - enc_begin_skip;
      }
      ldout(cct, 20)
      << "XX ofs: " << ofs
      << " end: " << end
      << " bl_ofs: " << bl_ofs
      << " bl_len: " << bl_len
      << " aligned_size: " << aligned_size
      << " send_size: "<< send_size << dendl;
      res = next.handle_data(bl, bl_ofs + enc_begin_skip, send_size);
      enc_begin_skip = 0;
      ofs += aligned_size;

      if (res != 0)
        return res;
    }
  }
  return 0;
  //op->send_response_data(bl, ofs, end);
}
//off_t get_
/**
 * flush remainder of data to output
 */
int RGWGetObj_BlockDecrypt::flush() {
  int res;
  if (cache.length() > 0){
    crypt->decrypt(cache.c_str(), cache.length(), cache.c_str(), ofs);
    //return op->send_response_data(cache, 0, cache.length());
    off_t send_size = cache.length() - enc_begin_skip;
    if (ofs + enc_begin_skip + send_size > end + 1){
      send_size = end + 1 - ofs - enc_begin_skip;
    }
    ldout(cct, 20)
    << "flush ofs: " << ofs
    << " end: " << end
    << " send_size: "<< send_size << dendl;
    res = next.handle_data(cache, enc_begin_skip, send_size);
    enc_begin_skip = 0;
    ofs += send_size;
    return res; //cb->handle_data(cache, 0, send_size);
  } else {
    return 0;
  }
}

RGWPutObj_BlockEncrypt::RGWPutObj_BlockEncrypt(CephContext* cct, RGWPutObjDataProcessor& next, BlockCrypt* crypt):
      RGWPutObj_Filter(next), cct(cct), crypt(crypt),
      ofs(0), cache() {
  block_size = crypt->get_block_size();
}
RGWPutObj_BlockEncrypt::~RGWPutObj_BlockEncrypt() {}
int RGWPutObj_BlockEncrypt::handle_data(bufferlist& bl, off_t in_ofs, void **phandle, bool *again) {
  int res = 0;
  ldout(cct, 20)
    << "PUT HANDLE DATA " << bl.length()
    << " in_ofs = " << in_ofs
    << dendl;
  if (*again) {
    bufferlist lll;
    res = next.handle_data(lll, in_ofs, phandle, again);
    ldout(cct, 20)
        << "AGAIN =  " << *again
        << dendl;
    *again = false;
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
        char* src = cache.get_contiguous(0, block_size);
        crypt->encrypt(src, block_size, src, ofs);
        res = next.handle_data(cache, ofs, phandle, again);
        ldout(cct, 20)
        << " cache.len=" << cache.length()
        << " pos=" << 0
        << " again=" << *again
        << "res=" << res
        << dendl;
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
      ldout(cct, 20)
      << " added to cache len=" << remainder
      << dendl;
    }
    if (aligned_size > 0) {
      bufferlist toadd;
      char* src = bl.get_contiguous(bl_ofs, aligned_size);
      toadd.append(src,aligned_size);
      crypt->encrypt(src, aligned_size, src, ofs);
      //res=next.handle_data(bl, ofs, phandle, again);
      res=next.handle_data(toadd, ofs, phandle, again);
      ldout(cct, 20)
      << " toadd.len=" << toadd.length()
      << " pos=" << ofs
      << " phandle=" << *phandle
      << " again=" << *again
      << " res=" << res
      << dendl;
      //if(*again == false)
      ofs += aligned_size;
      if (res != 0)
        return res;
    }
  }
  if (bl.length() == 0) {
    if (cache.length() > 0) {
      /*flush cached data*/
      char* src = cache.get_contiguous(0, cache.length());
      crypt->encrypt(src, cache.length(), src, ofs);
      res=next.handle_data(cache, ofs, phandle, again);
      ofs+=cache.length();
      ldout(cct, 20)
      << " cache.len=" << cache.length()
      << " pos=" << 0
      << " again=" << *again
      << " res=" << res
      << dendl;
      cache.clear();
      if (res != 0)
        return res;
    }
    /*replicate 0-sized handle_data*/
    res=next.handle_data(cache, ofs, phandle, again);
    ldout(cct, 20)
    << " cache.len=" << cache.length()
    << " pos=" << 0
    << " again=" << *again
    << " res=" << res
    << dendl;
  }
  return res;
}

int RGWPutObj_BlockEncrypt::throttle_data(void *handle, bool need_to_wait) {
  return next.throttle_data(handle, need_to_wait);
}



