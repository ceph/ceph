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
  virtual bool encrypt(const void *src, size_t size, void *dst, off_t offset) = 0;
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
  virtual bool decrypt(const void *src, size_t size, void *dst, off_t offset) = 0;
};

/**
 * Encryption works in CTR mode. nonce + offset/block_size is used as IV for block starting at offset.
 */
class AES_256_CTR : public BlockCrypt {
public:
  /**
   * Sets key and nonce.
   */
  AES_256_CTR();
  virtual ~AES_256_CTR();
  bool set_key(uint8_t* key, uint8_t* nonce, size_t key_len);
  virtual size_t get_block_size() override;
  virtual bool encrypt(const void *src, size_t size, void *dst, off_t offset) override;
  virtual bool decrypt(const void *src, size_t size, void *dst, off_t offset) override;
};


class RGWGetObj_BlockDecrypt : public RGWGetObj_Filter {
  CephContext* cct;
  BlockCrypt* crypt;
  off_t enc_begin_skip;
  off_t ofs;
  off_t end;
  bufferlist cache;
  size_t block_size;
public:
  RGWGetObj_BlockDecrypt(CephContext* cct, RGWGetDataCB& next, BlockCrypt* crypt);
  virtual ~RGWGetObj_BlockDecrypt();

  virtual void fixup_range(off_t& bl_ofs, off_t& bl_end) override;
  virtual int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override;
  virtual int flush() override;
};

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
