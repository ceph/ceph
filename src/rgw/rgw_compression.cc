// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_compression.h"

#define dout_subsys ceph_subsys_rgw

//------------RGWPutObj_Compress---------------

int RGWPutObj_Compress::process(bufferlist&& in, uint64_t logical_offset)
{
  bufferlist out;
  if (in.length() > 0) {
    // compression stuff
    if ((logical_offset > 0 && compressed) || // if previous part was compressed
        (logical_offset == 0)) {              // or it's the first part
      ldout(cct, 10) << "Compression for rgw is enabled, compress part " << in.length() << dendl;
      int cr = compressor->compress(in, out);
      if (cr < 0) {
        if (logical_offset > 0) {
          lderr(cct) << "Compression failed with exit code " << cr
              << " for next part, compression process failed" << dendl;
          return -EIO;
        }
        compressed = false;
        ldout(cct, 5) << "Compression failed with exit code " << cr
            << " for first part, storing uncompressed" << dendl;
        out.claim(in);
      } else {
        compressed = true;
    
        compression_block newbl;
        size_t bs = blocks.size();
        newbl.old_ofs = logical_offset;
        newbl.new_ofs = bs > 0 ? blocks[bs-1].len + blocks[bs-1].new_ofs : 0;
        newbl.len = out.length();
        blocks.push_back(newbl);
      }
    } else {
      compressed = false;
      out.claim(in);
    }
    // end of compression stuff
  }
  return Pipe::process(std::move(out), logical_offset);
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
      << "bl_ofs="<< bl_ofs << bl_len << dendl;

  if (!compressor.get()) {
    // if compressor isn't available - error, because cannot return decompressed data?
    lderr(cct) << "Cannot load compressor of type " << cs_info->compression_type << dendl;
    return -EIO;
  }
  bufferlist out_bl, in_bl, temp_in_bl;
  bl.copy(bl_ofs, bl_len, temp_in_bl); 
  bl_ofs = 0;
  int r = 0;
  if (waiting.length() != 0) {
    in_bl.append(waiting);
    in_bl.append(temp_in_bl);        
    waiting.clear();
  } else {
    in_bl.claim(temp_in_bl);
  }
  bl_len = in_bl.length();
  
  while (first_block <= last_block) {
    bufferlist tmp;
    off_t ofs_in_bl = first_block->new_ofs - cur_ofs;
    if (ofs_in_bl + (off_t)first_block->len > bl_len) {
      // not complete block, put it to waiting
      unsigned tail = bl_len - ofs_in_bl;
      in_bl.copy(ofs_in_bl, tail, waiting);
      cur_ofs -= tail;
      break;
    }
    in_bl.copy(ofs_in_bl, first_block->len, tmp);
    int cr = compressor->decompress(tmp, out_bl);
    if (cr < 0) {
      lderr(cct) << "Compression failed with exit code " << cr << dendl;
      return cr;
    }
    ++first_block;
    while (out_bl.length() - q_ofs >= cct->_conf->rgw_max_chunk_size)
    {
      off_t ch_len = std::min<off_t>(cct->_conf->rgw_max_chunk_size, q_len);
      q_len -= ch_len;
      r = next->handle_data(out_bl, q_ofs, ch_len);
      if (r < 0) {
        lderr(cct) << "handle_data failed with exit code " << r << dendl;
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
      lderr(cct) << "handle_data failed with exit code " << r << dendl;
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
  if (partial_content) {
    // if user set range, we need to calculate it in decompressed data
    first_block = cs_info->blocks.begin(); last_block = cs_info->blocks.begin();
    if (cs_info->blocks.size() > 1) {
      vector<compression_block>::iterator fb, lb;
      // not bad to use auto for lambda, I think
      auto cmp_u = [] (off_t ofs, const compression_block& e) { return (uint64_t)ofs < e.old_ofs; };
      auto cmp_l = [] (const compression_block& e, off_t ofs) { return e.old_ofs <= (uint64_t)ofs; };
      fb = upper_bound(cs_info->blocks.begin()+1,
                       cs_info->blocks.end(),
                       ofs,
                       cmp_u);
      first_block = fb - 1;
      lb = lower_bound(fb,
                       cs_info->blocks.end(),
                       end,
                       cmp_l);
      last_block = lb - 1;
    }
  } else {
    first_block = cs_info->blocks.begin(); last_block = cs_info->blocks.end() - 1;
  }

  q_ofs = ofs - first_block->old_ofs;
  q_len = end + 1 - ofs;

  ofs = first_block->new_ofs;
  end = last_block->new_ofs + last_block->len - 1;

  cur_ofs = ofs;
  waiting.clear();

  return next->fixup_range(ofs, end);
}
