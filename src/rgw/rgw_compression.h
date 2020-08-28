// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_COMPRESSION_H
#define CEPH_RGW_COMPRESSION_H

#include <vector>

#include "compressor/Compressor.h"
#include "rgw_putobj.h"
#include "rgw_op.h"
#include "rgw_compression_types.h"

int rgw_compression_info_from_attrset(map<string, bufferlist>& attrs, bool& need_decompress, RGWCompressionInfo& cs_info);

class RGWGetObj_Decompress : public RGWGetObj_Filter
{
  CephContext* cct;
  CompressorRef compressor;
  RGWCompressionInfo* cs_info;
  bool partial_content;
  vector<compression_block>::iterator first_block, last_block;
  off_t q_ofs, q_len;
  uint64_t cur_ofs;
  bufferlist waiting;
public:
  RGWGetObj_Decompress(CephContext* cct_, 
                       RGWCompressionInfo* cs_info_, 
                       bool partial_content_,
                       RGWGetObj_Filter* next);
  ~RGWGetObj_Decompress() override {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len, const Span& parent_span = nullptr) override;
  int fixup_range(off_t& ofs, off_t& end) override;

};

class RGWPutObj_Compress : public rgw::putobj::Pipe
{
  CephContext* cct;
  bool compressed{false};
  CompressorRef compressor;
  boost::optional<int32_t> compressor_message;
  std::vector<compression_block> blocks;
public:
  RGWPutObj_Compress(CephContext* cct_, CompressorRef compressor,
                     rgw::putobj::DataProcessor *next)
    : Pipe(next), cct(cct_), compressor(compressor) {}

  int process(bufferlist&& data, uint64_t logical_offset) override;

  bool is_compressed() { return compressed; }
  vector<compression_block>& get_compression_blocks() { return blocks; }
  boost::optional<int32_t> get_compressor_message() { return compressor_message; }

}; /* RGWPutObj_Compress */

#endif /* CEPH_RGW_COMPRESSION_H */
