// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <vector>

#include "compressor/Compressor.h"
#include "rgw_putobj.h"
#include "rgw_op.h"
#include "rgw_compression_types.h"

int rgw_compression_info_from_attr(const bufferlist& attr,
                                   bool& need_decompress,
                                   RGWCompressionInfo& cs_info);
int rgw_compression_info_from_attrset(const std::map<std::string, bufferlist>& attrs,
                                      bool& need_decompress,
                                      RGWCompressionInfo& cs_info);

class RGWGetObj_Decompress : public RGWGetObj_Filter
{
  CephContext* cct;
  CompressorRef compressor;
  RGWCompressionInfo* cs_info;
  bool partial_content;
  std::vector<compression_block>::iterator first_block, last_block;
  off_t q_ofs, q_len;
  uint64_t cur_ofs;
  bufferlist waiting;
public:
  RGWGetObj_Decompress(CephContext* cct_, 
                       RGWCompressionInfo* cs_info_, 
                       bool partial_content_,
                       RGWGetObj_Filter* next);
  virtual ~RGWGetObj_Decompress() override {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override;
  int fixup_range(off_t& ofs, off_t& end) override;

};

class RGWPutObj_Compress : public rgw::putobj::Pipe
{
  CephContext* cct;
  bool compressed{false};
  CompressorRef compressor;
  std::optional<int32_t> compressor_message;
  std::vector<compression_block> blocks;
  uint64_t compressed_ofs{0};
public:
  RGWPutObj_Compress(CephContext* cct_, CompressorRef compressor,
                     rgw::sal::DataProcessor *next)
    : Pipe(next), cct(cct_), compressor(compressor) {}
  virtual ~RGWPutObj_Compress() override {};

  int process(bufferlist&& data, uint64_t logical_offset) override;

  bool is_compressed() { return compressed; }
  std::vector<compression_block>& get_compression_blocks() { return blocks; }
  std::optional<int32_t> get_compressor_message() { return compressor_message; }

}; /* RGWPutObj_Compress */

/*
 * Helper: read-side decompression for lifecycle transitions.
 * Owns the decompression filter chain and the logical object size.
 */
class RGWDecompressHelper {
  CephContext* cct;
  uint64_t orig_size;
  RGWCompressionInfo decompress_info;
  std::optional<RGWGetObj_Decompress> decompress;

public:
  RGWDecompressHelper(CephContext* cct_, uint64_t obj_size,
                      RGWCompressionInfo cs_info)
    : cct(cct_), orig_size(cs_info.orig_size),
      decompress_info(std::move(cs_info)) {}

  void setup_filter(RGWGetObj_Filter*& filter, bool partial_content = false) {
    decompress.emplace(cct, &decompress_info,
                       partial_content, filter);
    filter = &*decompress;
  }

  void fixup_range(off_t& ofs, off_t& end) {
    if (decompress) {
      decompress->fixup_range(ofs, end);
    }
  }

  uint64_t get_orig_size() const { return orig_size; }
};

/*
 * Helper: write-side compression for lifecycle transitions.
 * Owns the compressor plugin and compression metadata.
 */
class RGWCompressHelper {
  CephContext* cct;
  std::string compression_type;
  std::optional<RGWPutObj_Compress> compressor;
  CompressorRef compressor_plugin;

public:
  RGWCompressHelper(CephContext* cct_, std::string compression_type_)
    : cct(cct_), compression_type(std::move(compression_type_)) {}

  rgw::sal::DataProcessor* setup_writer(rgw::sal::DataProcessor* writer,
                                        const DoutPrefixProvider* dpp) {
    if (compression_type == "none") {
      return writer;
    }
    compressor_plugin = Compressor::create(cct, compression_type);
    if (!compressor_plugin) {
      ldpp_dout(dpp, 1) << "WARNING: failed to load compressor for type "
          << compression_type << dendl;
      return writer;
    }
    compressor.emplace(cct, compressor_plugin, writer);
    return &*compressor;
  }

  void finalize_attrs(rgw::sal::Attrs& attrs, uint64_t orig_size) {
    if (compressor && compressor->is_compressed()) {
      bufferlist tmp;
      RGWCompressionInfo cs_info;
      cs_info.compression_type = compressor_plugin->get_type_name();
      cs_info.orig_size = orig_size;
      cs_info.compressor_message = compressor->get_compressor_message();
      cs_info.blocks = std::move(compressor->get_compression_blocks());
      encode(cs_info, tmp);
      attrs[RGW_ATTR_COMPRESSION] = tmp;
    }
  }

  bool is_compressed() {
    return compressor && compressor->is_compressed();
  }
};
