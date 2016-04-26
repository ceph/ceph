// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/**
 * Crypto filters for Put/Post/Get operations.
 */
#include <rgw/rgw_op.h>
#include <rgw/rgw_crypt.h>
#define dout_subsys ceph_subsys_rgw




AES_256_CTR::AES_256_CTR(CephContext* cct):
cct(cct) {

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
bool AES_256_CTR::encrypt(bufferlist& input, off_t in_ofs, size_t size, bufferlist& output, off_t stream_offset) {

  ldout(cct, 20)
    << "ENC: offset= " << stream_offset
    << " size=" << size
    << dendl;

  output.append(input.get_contiguous(in_ofs, size), size);
  char* dst = output.get_contiguous(0,size);
  for (size_t i=0; i<size; i++) {
    *(dst+i)=*(dst+i)^((stream_offset+i)*111+((stream_offset+i)>>8)*117);
  }
  return true;
}
bool AES_256_CTR::decrypt(bufferlist& input, off_t in_ofs, size_t size, bufferlist& output, off_t stream_offset) {

  ldout(cct, 20)
    << "DEC: offset= " << stream_offset
    << " size=" << size
    << dendl;

  output.append(input.get_contiguous(in_ofs, size), size);
  char* dst = output.get_contiguous(0,size);
  for (size_t i=0; i<size; i++) {
    *(dst+i)=*(dst+i)^((stream_offset+i)*111+((stream_offset+i)>>8)*117);
  }
  return true;
}

RGWGetObj_BlockDecrypt::RGWGetObj_BlockDecrypt(RGWObjState* s,req_state* req, RGWGetDataCB& next, BlockCrypt* crypt):
    RGWGetObj_Filter(next), s(s), req(req), crypt(crypt),
    enc_begin_skip(0), ofs(0), end(0), cache() {
  block_size = crypt->get_block_size();
  }
RGWGetObj_BlockDecrypt::~RGWGetObj_BlockDecrypt() {}

int RGWGetObj_BlockDecrypt::read_manifest() {
  CephContext* cct = req->cct;

  parts_len.clear();
  bufferlist manifest_bl = s->attrset[RGW_ATTR_MANIFEST];
  if (manifest_bl.length()) {
    bufferlist::iterator miter = manifest_bl.begin();
    try {
      ::decode(s->manifest, miter);
      s->has_manifest = true;
      s->size = s->manifest.get_obj_size();
    } catch (buffer::error& err) {
      ldout(cct, 0) << "ERROR: couldn't decode manifest" << dendl;
      return -EIO;
    }

    RGWObjManifest::obj_iterator mi;
    for (mi = s->manifest.obj_begin(); mi != s->manifest.obj_end(); ++mi) {
      if (mi.get_cur_stripe() == 0) {
        parts_len.push_back(0);
      }
      parts_len.back() += mi.get_stripe_size();
    }

    for (size_t i = 0; i<parts_len.size(); i++) {
      ldout(cct, 0) << "part " << i << " size=" << parts_len[i] << dendl;
    }
  }
  return 0;
}
int RGWGetObj_BlockDecrypt::fixup_range(off_t& bl_ofs, off_t& bl_end) {
  off_t inp_ofs = bl_ofs;
  off_t inp_end = bl_end;
  int res = read_manifest();
  if (res != 0) {
    return res;
  }
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
  ldout(req->cct, 20) << "fixup_range [" << inp_ofs << "," << inp_end
      << "] => [" << bl_ofs << "," << bl_end << "]" << dendl;
  return 0;
}

int RGWGetObj_BlockDecrypt::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
  int res = 0;
  ldout(req->cct, 20) << "Decrypt " << bl_len << " bytes" << dendl;
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
RGWPutObj_BlockEncrypt::~RGWPutObj_BlockEncrypt() {}
int RGWPutObj_BlockEncrypt::handle_data(bufferlist& bl, off_t in_ofs, void **phandle, bool *again) {
  int res = 0;
  if (*again) {
    bufferlist no_data;
    res = next.handle_data(no_data, in_ofs, phandle, again);
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
        res = next.handle_data(data, ofs, phandle, again);
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
      res=next.handle_data(data, ofs, phandle, again);
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
      res=next.handle_data(data, ofs, phandle, again);
      ofs+=cache.length();
      cache.clear();
      if (res != 0)
        return res;
    }
    /*replicate 0-sized handle_data*/
    res=next.handle_data(cache, ofs, phandle, again);
  }
  return res;
}

int RGWPutObj_BlockEncrypt::throttle_data(void *handle, bool need_to_wait) {
  return next.throttle_data(handle, need_to_wait);
}



