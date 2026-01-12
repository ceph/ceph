#ifndef CEPH_HRAC_COMPRESSOR_H
#define CEPH_HRAC_COMPRESSOR_H

#include "compressor/Compressor.h"
#include "include/buffer.h"
#include "hrac.h"

class HracCompressor : public Compressor {
  CephContext* cct;

 public:
  // 【修改点】这里换成了 Compressor::COMP_ALG_HRAC，不再借用 LZ4 了
  HracCompressor(CephContext* cct) : Compressor(Compressor::COMP_ALG_HRAC, "hrac"), cct(cct) {}

  int compress(const ceph::buffer::list &src, ceph::buffer::list &dst, boost::optional<int32_t> &compressor_message) override;
  
  int decompress(const ceph::buffer::list &src, ceph::buffer::list &dst, boost::optional<int32_t> compressor_message) override;

  int decompress(ceph::buffer::list::const_iterator &p,
		 size_t compressed_len,
		 ceph::buffer::list &dst,
		 boost::optional<int32_t> compressor_message) override {
    ceph::buffer::list tmp;
    p.copy(compressed_len, tmp);
    return decompress(tmp, dst, compressor_message);
  }
};

#endif