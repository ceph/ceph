// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_compression.h"
#include "rgw_range_projection.h"
#include "rgw_zone_types.h"
#include "common/split.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

int rgw_compression_info_from_attr(const bufferlist& attr,
                                   bool& need_decompress,
                                   RGWCompressionInfo& cs_info)
{
  auto bliter = attr.cbegin();
  try {
    decode(cs_info, bliter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  if (cs_info.blocks.size() == 0) {
    return -EIO;
  }
  if (cs_info.compression_type != "none")
    need_decompress = true;
  else
    need_decompress = false;
  return 0;
}

int rgw_compression_info_from_attrset(const map<string, bufferlist>& attrs,
                                      bool& need_decompress,
                                      RGWCompressionInfo& cs_info)
{
  auto value = attrs.find(RGW_ATTR_COMPRESSION);
  if (value == attrs.end()) {
    need_decompress = false;
    return 0;
  }
  return rgw_compression_info_from_attr(value->second, need_decompress, cs_info);
}

bool rgw_compression_below_min_size(uint64_t min_size,
                                    std::optional<uint64_t> size)
{
  return size && *size < min_size;
}

static bool sync_compression_type_usable(CephContext* cct, const std::string& type)
{
  // there's nothing to match without a fixed type
  if (type.empty() || type == "none" || type == "random") {
    return false;
  }
  // plugins don't come and go at runtime, so whether each one loads is
  // checked once per process
  static ceph::mutex lock = ceph::make_mutex("rgw_sync_compression_types");
  static std::map<std::string, bool, std::less<>> loadable;
  std::lock_guard l{lock};
  auto [i, inserted] = loadable.try_emplace(type, false);
  if (inserted) {
    i->second = static_cast<bool>(Compressor::create(cct, type));
  }
  return i->second;
}

std::string rgw_sync_compression_types(CephContext* cct,
                                       const RGWZonePlacementInfo& placement,
                                       const std::string& default_storage_class,
                                       const std::optional<std::string>& storage_class)
{
  std::string types;
  auto add = [cct, &types] (std::string_view sc, const std::string& type) {
    if (!sync_compression_type_usable(cct, type)) {
      return;
    }
    if (!types.empty()) {
      types += ',';
    }
    types.append(sc).append(":").append(type);
  };
  if (storage_class) {
    // an empty storage class from sync policy means the bucket's default
    add("*", placement.get_compression_type(storage_class->empty() ?
                                            default_storage_class : *storage_class));
    return types;
  }
  add("", placement.get_compression_type(default_storage_class));
  for (const auto& [sc, storage] : placement.storage_classes.get_all()) {
    // the separators can't be escaped, and "*" stands for every class
    if (sc.empty() || sc == "*" || sc.find_first_of(",:") != sc.npos) {
      continue;
    }
    add(sc, storage.compression_type.value_or(""));
  }
  return types;
}

bool rgw_sync_compression_type_matches(std::string_view types,
                                       std::string_view storage_class,
                                       std::string_view compression_type)
{
  for (std::string_view entry : ceph::split(types, ",")) {
    const auto pos = entry.find(':');
    if (pos == entry.npos) {
      continue;
    }
    const auto sc = entry.substr(0, pos);
    if ((sc == storage_class || sc == "*") &&
        entry.substr(pos + 1) == compression_type) {
      return true;
    }
  }
  return false;
}

//------------RGWPutObj_Compress---------------

int RGWPutObj_Compress::process(bufferlist&& in, uint64_t logical_offset)
{
  bufferlist out;
  compressed_ofs = logical_offset;

  if (in.length() > 0) {
    // compression stuff
    if ((logical_offset > 0 && compressed) || // if previous part was compressed
        (logical_offset == 0)) {              // or it's the first part
      ldout(cct, 10) << "Compression for rgw is enabled, compress part " << in.length() << dendl;
      int cr = compressor->compress(in, out, compressor_message);
      if (cr < 0) {
        if (logical_offset > 0) {
          lderr(cct) << "Compression failed with exit code " << cr
              << " for next part, compression process failed" << dendl;
          return -EIO;
        }
        compressed = false;
        ldout(cct, 5) << "Compression failed with exit code " << cr
            << " for first part, storing uncompressed" << dendl;
        out = std::move(in);
      } else {
        compressed = true;
    
        compression_block newbl;
        size_t bs = blocks.size();
        newbl.old_ofs = logical_offset;
        newbl.new_ofs = bs > 0 ? blocks[bs-1].len + blocks[bs-1].new_ofs : 0;
        newbl.len = out.length();
        blocks.push_back(newbl);

	compressed_ofs = newbl.new_ofs;
      }
    } else {
      compressed = false;
      out = std::move(in);
    }
    // end of compression stuff
  } else {
    size_t bs = blocks.size();
    compressed_ofs = bs > 0 ? blocks[bs-1].len + blocks[bs-1].new_ofs : logical_offset;
  }

  return Pipe::process(std::move(out), compressed_ofs);
}

//----------------RGWGetObj_Decompress---------------------
RGWGetObj_Decompress::RGWGetObj_Decompress(CephContext* cct_, 
                                           RGWCompressionInfo* cs_info_, 
                                           bool partial_content_,
                                           RGWGetObj_Filter* next): RGWGetObj_Filter(next),
                                                                cct(cct_),
                                                                cs_info(cs_info_),
                                                                partial_content(partial_content_),
                                                                q_ofs(0),
                                                                q_len(0),
                                                                cur_ofs(0)
{
  compressor = Compressor::create(cct, cs_info->compression_type);
  if (!compressor.get())
    lderr(cct) << "Cannot load compressor of type " << cs_info->compression_type << dendl;
}

int RGWGetObj_Decompress::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  ldout(cct, 10) << "Compression for rgw is enabled, decompress part "
      << "bl_ofs=" << bl_ofs << ", bl_len=" << bl_len << dendl;

  if (!compressor.get()) {
    // if compressor isn't available - error, because cannot return decompressed data?
    lderr(cct) << "Cannot load compressor of type " << cs_info->compression_type << dendl;
    return -EIO;
  }
  bufferlist out_bl, in_bl, temp_in_bl;
  bl.begin(bl_ofs).copy(bl_len, temp_in_bl);
  bl_ofs = 0;
  int r = 0;
  if (waiting.length() != 0) {
    in_bl.append(waiting);
    in_bl.append(temp_in_bl);        
    waiting.clear();
  } else {
    in_bl = std::move(temp_in_bl);
  }
  bl_len = in_bl.length();
  
  auto iter_in_bl = in_bl.cbegin();
  while (first_block <= last_block) {
    bufferlist tmp;
    off_t ofs_in_bl = first_block->new_ofs - cur_ofs;
    if (ofs_in_bl + (off_t)first_block->len > bl_len) {
      // not complete block, put it to waiting
      unsigned tail = bl_len - ofs_in_bl;
      if (iter_in_bl.get_off() != ofs_in_bl) {
        iter_in_bl.seek(ofs_in_bl);
      }
      iter_in_bl.copy(tail, waiting);
      cur_ofs -= tail;
      break;
    }
    if (iter_in_bl.get_off() != ofs_in_bl) {
      iter_in_bl.seek(ofs_in_bl);
    }
    iter_in_bl.copy(first_block->len, tmp);
    int cr = compressor->decompress(tmp, out_bl, cs_info->compressor_message);
    if (cr < 0) {
      lderr(cct) << "Decompression failed with exit code " << cr << dendl;
      return cr;
    }
    ++first_block;
    while (out_bl.length() - q_ofs >=
	   static_cast<off_t>(cct->_conf->rgw_max_chunk_size)) {
      off_t ch_len = std::min<off_t>(cct->_conf->rgw_max_chunk_size, q_len);
      q_len -= ch_len;
      r = next->handle_data(out_bl, q_ofs, ch_len);
      if (r < 0) {
        lsubdout(cct, rgw, 0) << "handle_data failed with exit code " << r << dendl;
        return r;
      }
      out_bl.splice(0, q_ofs + ch_len);
      q_ofs = 0;
    }
  }

  cur_ofs += bl_len;
  off_t ch_len = std::min<off_t>(out_bl.length() - q_ofs, q_len);
  if (ch_len > 0) {
    r = next->handle_data(out_bl, q_ofs, ch_len);
    if (r < 0) {
      lsubdout(cct, rgw, 0) << "handle_data failed with exit code " << r << dendl;
      return r;
    }
    out_bl.splice(0, q_ofs + ch_len);
    q_len -= ch_len;
    q_ofs = 0;
  }
  return r;
}

int RGWGetObj_Decompress::fixup_range(off_t& ofs, off_t& end)
{
  auto result = project_compress_range(ofs, end, *cs_info, partial_content);

  first_block = cs_info->blocks.begin() + result.first_block_idx;
  last_block = cs_info->blocks.begin() + result.last_block_idx;
  q_ofs = result.q_ofs;
  q_len = result.q_len;
  cur_ofs = result.ofs;
  waiting.clear();

  ofs = result.ofs;
  end = result.end;

  return next->fixup_range(ofs, end);
}

void compression_block::dump(Formatter *f) const
{
  f->dump_unsigned("old_ofs", old_ofs);
  f->dump_unsigned("new_ofs", new_ofs);
  f->dump_unsigned("len", len);
}

void RGWCompressionInfo::dump(Formatter *f) const
{
  f->dump_string("compression_type", compression_type);
  f->dump_unsigned("orig_size", orig_size);
  if (compressor_message) {
    f->dump_int("compressor_message", *compressor_message);
  }
  ::encode_json("blocks", blocks, f);
}

list<RGWCompressionInfo> RGWCompressionInfo::generate_test_instances()
{
  list<RGWCompressionInfo> o;
  RGWCompressionInfo i;
  i.compression_type = "type";
  i.orig_size = 1024;
  i.blocks.push_back(compression_block());
  i.blocks.back().old_ofs = 0;
  i.blocks.back().new_ofs = 0;
  i.blocks.back().len = 1024;
  o.push_back(std::move(i));
  return o;
}
