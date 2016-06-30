
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_compression.h"

#define dout_subsys ceph_subsys_rgw

//------------RGWPutObj_Compress---------------

int RGWPutObj_Compress::handle_data(bufferlist& bl, off_t ofs, void **phandle, rgw_obj *pobj, bool *again)
{
  bufferlist in_bl;

  if (bl.length() > 0) {
    // compression stuff
    if ((ofs > 0 && compressed) ||                                // if previous part was compressed
        (ofs == 0)) {                                             // or it's the first part
      ldout(cct, 10) << "Compression for rgw is enabled, compress part " << bl.length() << dendl;
      CompressorRef compressor = Compressor::create(cct, cct->_conf->rgw_compression_type);
      if (!compressor.get()) {
        if (ofs > 0 && compressed) {
          lderr(cct) << "Cannot load compressor of type " << cct->_conf->rgw_compression_type
                              << " for next part, compression process failed" << dendl;
          return -EIO;
        }
        // if compressor isn't available - just do not use it with log warning?
        ldout(cct, 5) << "Cannot load compressor of type " << cct->_conf->rgw_compression_type 
                      << " for rgw, check rgw_compression_type config option" << dendl;
        compressed = false;
        in_bl.claim(bl);
      } else {
        int cr = compressor->compress(bl, in_bl);
        if (cr < 0) {
          if (ofs > 0 && compressed) {
            lderr(cct) << "Compression failed with exit code " << cr
                       << " for next part, compression process failed" << dendl;
            return -EIO;
          }
          ldout(cct, 5) << "Compression failed with exit code " << cr << dendl;
          compressed = false;
          in_bl.claim(bl);
        } else {
          compressed = true;
    
          compression_block newbl;
          int bs = blocks.size();
          newbl.old_ofs = ofs;
          newbl.new_ofs = bs > 0 ? blocks[bs-1].len + blocks[bs-1].new_ofs : 0;
          newbl.len = in_bl.length();
          blocks.push_back(newbl);
        }
      }
    } else {
      compressed = false;
      in_bl.claim(bl);
    }
    // end of compression stuff
  }

  return next->handle_data(in_bl, ofs, phandle, pobj, again);
}

//----------------RGWGetObj_Decompress---------------------
RGWGetObj_Decompress::RGWGetObj_Decompress(CephContext* cct_, 
                                           RGWCompressionInfo* cs_info_, 
                                           bool partial_content_,
                                           RGWGetDataCB* next): RGWGetObj_Filter(next),
                                                                cct(cct_),
                                                                cs_info(cs_info_),
                                                                partial_content(partial_content_),
                                                                q_ofs(0),
                                                                q_len(0),
                                                                first_data(true),
                                                                cur_ofs(0)
{
  compressor = Compressor::create(cct, cs_info->compression_type);
  if (!compressor.get())
    lderr(cct) << "Cannot load compressor of type " << cs_info->compression_type 
                     << " for rgw, check rgw_compression_type config option" << dendl;
}

int RGWGetObj_Decompress::handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len)
{
  ldout(cct, 10) << "Compression for rgw is enabled, decompress part " << bl_len << dendl;

  if (!compressor.get()) {
    // if compressor isn't available - error, because cannot return decompressed data?
    lderr(cct) << "Cannot load compressor of type " << cs_info->compression_type 
                     << " for rgw, check rgw_compression_type config option" << dendl;
    return -EIO;
  }
  bufferlist out_bl, in_bl;
  bl_ofs = 0;
  if (waiting.length() != 0) {
    in_bl.append(waiting);
    in_bl.append(bl);        
    waiting.clear();
  } else {
    in_bl.claim(bl);
  }
  bl_len = in_bl.length();
  
  while (first_block <= last_block) {
    bufferlist tmp, tmp_out;
    int ofs_in_bl = first_block->new_ofs - cur_ofs;
    if (ofs_in_bl + (unsigned)first_block->len > bl_len) {
      // not complete block, put it to waiting
      int tail = bl_len - ofs_in_bl;
      in_bl.copy(ofs_in_bl, tail, waiting);
      cur_ofs -= tail;
      break;
    }
    in_bl.copy(ofs_in_bl, first_block->len, tmp);
    int cr = compressor->decompress(tmp, tmp_out);
    if (cr < 0) {
      lderr(cct) << "Compression failed with exit code " << cr << dendl;
      return cr;
    }
    if (first_block == last_block && partial_content)
      tmp_out.copy(0, q_len, out_bl);
    else
      out_bl.append(tmp_out);
    first_block++;
  }

  if (first_data && partial_content && out_bl.length() != 0)
    bl_ofs =  q_ofs;

  if (first_data && out_bl.length() != 0)
    first_data = false;

  cur_ofs += bl_len;
  return next->handle_data(out_bl, bl_ofs, out_bl.length() - bl_ofs);
}

void RGWGetObj_Decompress::fixup_range(off_t& ofs, off_t& end)
{
  if (partial_content) {
    // if user set range, we need to calculate it in decompressed data
    first_block = cs_info->blocks.begin(); last_block = cs_info->blocks.begin();
    if (cs_info->blocks.size() > 1) {
      vector<compression_block>::iterator fb, lb;
      // not bad to use auto for lambda, I think
      auto cmp_u = [] (off_t ofs, const compression_block& e) { return (unsigned)ofs < e.old_ofs; };
      auto cmp_l = [] (const compression_block& e, off_t ofs) { return e.old_ofs < (unsigned)ofs; };
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
  q_len = end - last_block->old_ofs + 1;

  ofs = first_block->new_ofs;
  end = last_block->new_ofs + last_block->len;

  first_data = true;
  cur_ofs = ofs;
  waiting.clear();

  next->fixup_range(ofs, end);
}
