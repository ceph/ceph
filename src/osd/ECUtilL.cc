// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <errno.h>
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "include/encoding.h"
#include "ECUtilL.h"

using namespace std;
using ceph::bufferlist;
using ceph::ErasureCodeInterfaceRef;
using ceph::Formatter;

namespace ECLegacy {
  std::pair<uint64_t, uint64_t> ECUtilL::stripe_info_t::chunk_aligned_offset_len_to_chunk(
    std::pair<uint64_t, uint64_t> in) const {
    pair<uint64_t, uint64_t> tmp = offset_len_to_stripe_bounds(in);
    return std::make_pair(
      chunk_aligned_logical_offset_to_chunk_offset(tmp.first),
      chunk_aligned_logical_size_to_chunk_size(tmp.second));
  }

  int ECUtilL::decode(
    const stripe_info_t &sinfo,
    ErasureCodeInterfaceRef &ec_impl,
    const set<int> want_to_read,
    map<int, bufferlist> &to_decode,
    bufferlist *out)
  {
    ceph_assert(to_decode.size());

    uint64_t total_data_size = to_decode.begin()->second.length();
    ceph_assert(total_data_size % sinfo.get_chunk_size() == 0);

    ceph_assert(out);
    ceph_assert(out->length() == 0);

    for (map<int, bufferlist>::iterator i = to_decode.begin();
         i != to_decode.end();
         ++i) {
      ceph_assert(i->second.length() == total_data_size);
         }

    if (total_data_size == 0)
      return 0;

    for (uint64_t i = 0; i < total_data_size; i += sinfo.get_chunk_size()) {
      map<int, bufferlist> chunks;
      for (map<int, bufferlist>::iterator j = to_decode.begin();
           j != to_decode.end();
           ++j) {
        chunks[j->first].substr_of(j->second, i, sinfo.get_chunk_size());
           }
      bufferlist bl;
IGNORE_DEPRECATED
      int r = ec_impl->decode_concat(want_to_read, chunks, &bl);
END_IGNORE_DEPRECATED
      ceph_assert(r == 0);
      ceph_assert(bl.length() % sinfo.get_chunk_size() == 0);
      out->claim_append(bl);
    }
    return 0;
  }

  int ECUtilL::decode(
    const stripe_info_t &sinfo,
    ErasureCodeInterfaceRef &ec_impl,
    map<int, bufferlist> &to_decode,
    map<int, bufferlist*> &out) {

    ceph_assert(to_decode.size());

    for (auto &&i : to_decode) {
      if(i.second.length() == 0)
        return 0;
    }

    set<int> need;
    for (map<int, bufferlist*>::iterator i = out.begin();
         i != out.end();
         ++i) {
      ceph_assert(i->second);
      ceph_assert(i->second->length() == 0);
      need.insert(i->first);
         }

    set<int> avail;
    for (auto &&i : to_decode) {
      ceph_assert(i.second.length() != 0);
      avail.insert(i.first);
    }

    map<int, vector<pair<int, int>>> min;
IGNORE_DEPRECATED
    int r = ec_impl->minimum_to_decode(need, avail, &min);
END_IGNORE_DEPRECATED
    ceph_assert(r == 0);

    int chunks_count = 0;
    int repair_data_per_chunk = 0;
    int subchunk_size = sinfo.get_chunk_size()/ec_impl->get_sub_chunk_count();

    for (auto &&i : to_decode) {
      auto found = min.find(i.first);
      if (found != min.end()) {
        int repair_subchunk_count = 0;
        for (auto& subchunks : min[i.first]) {
          repair_subchunk_count += subchunks.second;
        }
        repair_data_per_chunk = repair_subchunk_count * subchunk_size;
        chunks_count = (int)i.second.length() / repair_data_per_chunk;
        break;
      }
    }

    for (int i = 0; i < chunks_count; i++) {
      map<int, bufferlist> chunks;
      for (auto j = to_decode.begin();
           j != to_decode.end();
           ++j) {
        chunks[j->first].substr_of(j->second,
                                   i*repair_data_per_chunk,
                                   repair_data_per_chunk);
           }
      map<int, bufferlist> out_bls;
  IGNORE_DEPRECATED
      r = ec_impl->decode(need, chunks, &out_bls, sinfo.get_chunk_size());
  END_IGNORE_DEPRECATED
      ceph_assert(r == 0);
      for (auto j = out.begin(); j != out.end(); ++j) {
        ceph_assert(out_bls.count(j->first));
        ceph_assert(out_bls[j->first].length() == sinfo.get_chunk_size());
        j->second->claim_append(out_bls[j->first]);
      }
    }
    for (auto &&i : out) {
      ceph_assert(i.second->length() == chunks_count * sinfo.get_chunk_size());
    }
    return 0;
  }

  int ECUtilL::encode(
    const stripe_info_t &sinfo,
    ErasureCodeInterfaceRef &ec_impl,
    bufferlist &in,
    const set<int> &want,
    map<int, bufferlist> *out) {

    uint64_t logical_size = in.length();

    ceph_assert(logical_size % sinfo.get_stripe_width() == 0);
    ceph_assert(out);
    ceph_assert(out->empty());

    if (logical_size == 0)
      return 0;

    for (uint64_t i = 0; i < logical_size; i += sinfo.get_stripe_width()) {
      map<int, bufferlist> encoded;
      bufferlist buf;
      buf.substr_of(in, i, sinfo.get_stripe_width());
IGNORE_DEPRECATED
      int r = ec_impl->encode(want, buf, &encoded);
END_IGNORE_DEPRECATED
      ceph_assert(r == 0);
      for (map<int, bufferlist>::iterator i = encoded.begin();
           i != encoded.end();
           ++i) {
        ceph_assert(i->second.length() == sinfo.get_chunk_size());
        (*out)[i->first].claim_append(i->second);
           }
    }

    for (map<int, bufferlist>::iterator i = out->begin();
         i != out->end();
         ++i) {
      ceph_assert(i->second.length() % sinfo.get_chunk_size() == 0);
      ceph_assert(
        sinfo.aligned_chunk_offset_to_logical_offset(i->second.length()) ==
        logical_size);
         }
    return 0;
  }

  void ECUtilL::HashInfo::append(uint64_t old_size,
                                map<int, bufferlist> &to_append) {
    ceph_assert(old_size == total_chunk_size);
    uint64_t size_to_append = to_append.begin()->second.length();
    if (has_chunk_hash()) {
      ceph_assert(to_append.size() == cumulative_shard_hashes.size());
      for (map<int, bufferlist>::iterator i = to_append.begin();
           i != to_append.end();
           ++i) {
        ceph_assert(size_to_append == i->second.length());
        ceph_assert((unsigned)i->first < cumulative_shard_hashes.size());
        uint32_t new_hash = i->second.crc32c(cumulative_shard_hashes[i->first]);
        cumulative_shard_hashes[i->first] = new_hash;
           }
    }
    total_chunk_size += size_to_append;
  }

  void ECUtilL::HashInfo::encode(bufferlist &bl) const
  {
    ENCODE_START(1, 1, bl);
    encode(total_chunk_size, bl);
    encode(cumulative_shard_hashes, bl);
    ENCODE_FINISH(bl);
  }

  void ECUtilL::HashInfo::decode(bufferlist::const_iterator &bl)
  {
    DECODE_START(1, bl);
    decode(total_chunk_size, bl);
    decode(cumulative_shard_hashes, bl);
    projected_total_chunk_size = total_chunk_size;
    DECODE_FINISH(bl);
  }

  void ECUtilL::HashInfo::dump(Formatter *f) const
  {
    f->dump_unsigned("total_chunk_size", total_chunk_size);
    f->open_array_section("cumulative_shard_hashes");
    for (unsigned i = 0; i != cumulative_shard_hashes.size(); ++i) {
      f->open_object_section("hash");
      f->dump_unsigned("shard", i);
      f->dump_unsigned("hash", cumulative_shard_hashes[i]);
      f->close_section();
    }
    f->close_section();
  }

  namespace ECUtilL {
  std::ostream& operator<<(std::ostream& out, const HashInfo& hi)
  {
    ostringstream hashes;
    for (auto hash: hi.cumulative_shard_hashes)
      hashes << " " << hex << hash;
    return out << "tcs=" << hi.total_chunk_size << hashes.str();
  }
  }

  void ECUtilL::HashInfo::generate_test_instances(list<HashInfo*>& o)
  {
    o.push_back(new HashInfo(3));
    {
      bufferlist bl;
      bl.append_zero(20);
      map<int, bufferlist> buffers;
      buffers[0] = bl;
      buffers[1] = bl;
      buffers[2] = bl;
      o.back()->append(0, buffers);
      o.back()->append(20, buffers);
    }
    o.push_back(new HashInfo(4));
  }

  const string HINFO_KEY = "hinfo_key";

  bool ECUtilL::is_hinfo_key_string(const string &key)
  {
    return key == HINFO_KEY;
  }

  const string &ECUtilL::get_hinfo_key()
  {
    return HINFO_KEY;
  }
}