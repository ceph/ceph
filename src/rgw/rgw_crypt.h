// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/**
 * Crypto filters for Put/Post/Get operations.
 */
#ifndef CEPH_RGW_CRYPT_H
#define CEPH_RGW_CRYPT_H

#include <rgw/rgw_op.h>

class BlockCrypt {
public:
  BlockCrypt(){};
  virtual ~BlockCrypt(){};
  /**
    * Determines size of encryption block.
    * This usually is multiply of key size.
    * It determines size of chunks that should be passed to \ref encrypt and \ref decrypt.
    */
  virtual size_t get_block_size() = 0;
  /**
   * Encrypts packet of data.
   * This is basic encryption of wider stream of data.
   * Offset shows location of <src,src+size) in stream. Offset must be multiply of basic block size \ref get_block_size.
   * Usually size is also multiply of \ref get_block_size, unless encrypting last part of stream.
   * Src and dst may be equal (in place encryption), but otherwise <src,src+size) and <dst,dst+size) may not overlap.
   *
   * \params
   * src - source of data
   * size - size of data
   * dst - destination to encrypt to
   * offset - location of <src,src+size) chunk in data stream
   */
  virtual bool encrypt(bufferlist& input, off_t in_ofs, size_t size, bufferlist& output, off_t stream_offset) = 0;
  /**
   * Decrypts packet of data.
   * This is basic decryption of wider stream of data.
   * Offset shows location of <src,src+size) in stream. Offset must be multiply of basic block size \ref get_block_size.
   * Usually size is also multiply of \ref get_block_size, unless decrypting last part of stream.
   * Src and dst may be equal (in place encryption), but otherwise <src,src+size) and <dst,dst+size) may not overlap.
   *
   * \params
   * src - source of data
   * size - size of data
   * dst - destination to decrypt to
   * offset - location of <src,src+size) chunk in data stream
   */
  virtual bool decrypt(bufferlist& input, off_t in_ofs, size_t size, bufferlist& output, off_t stream_offset) = 0;
};

/**
 * Encryption works in CTR mode. nonce + offset/block_size is used as IV for block starting at offset.
 */
class AES_256_CTR : public BlockCrypt {
  CephContext* cct;
public:
  AES_256_CTR(CephContext* cct);
  virtual ~AES_256_CTR();
  /**
   * Sets key and nonce.
   */
  bool set_key(uint8_t* key, uint8_t* nonce, size_t key_len);
  virtual size_t get_block_size() override;
  virtual bool encrypt(bufferlist& input, off_t in_ofs, size_t size, bufferlist& output, off_t stream_offset) override;
  virtual bool decrypt(bufferlist& input, off_t in_ofs, size_t size, bufferlist& output, off_t stream_offset) override;
};


class RGWGetObj_BlockDecrypt : public RGWGetObj_Filter {
  RGWObjState* s;
  req_state* req;
  BlockCrypt* crypt;
  off_t enc_begin_skip;
  off_t ofs;
  off_t end;
  bufferlist cache;
  size_t block_size;
  std::vector<size_t> parts_len;
public:
  RGWGetObj_BlockDecrypt(RGWObjState* s, req_state* req, RGWGetDataCB& next, BlockCrypt* crypt);
  virtual ~RGWGetObj_BlockDecrypt();

  virtual int fixup_range(off_t& bl_ofs, off_t& bl_end) override;
  virtual int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override;
  virtual int flush() override;
private:
  int read_manifest();
}; /* RGWGetObj_BlockDecrypt */

class RGWPutObj_BlockEncrypt : public RGWPutObj_Filter
{
  CephContext* cct;
  BlockCrypt* crypt;
  off_t ofs;
  bufferlist cache;
  size_t block_size;
public:
  RGWPutObj_BlockEncrypt(CephContext* cct, RGWPutObjDataProcessor& next, BlockCrypt* crypt);
  virtual ~RGWPutObj_BlockEncrypt();
  virtual int handle_data(bufferlist& bl, off_t ofs, void **phandle, bool *again) override;
  virtual int throttle_data(void *handle, bool need_to_wait) override;
}; /* RGWPutObj_BlockEncrypt */

#endif
