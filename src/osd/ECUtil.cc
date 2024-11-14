// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <errno.h>
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "include/encoding.h"
#include "ECUtil.h"

using namespace std;
using ceph::bufferlist;
using ceph::ErasureCodeInterfaceRef;
using ceph::Formatter;

// FIXME: We want this to be the same as the constant in ErasureCode.h, which
//        cannot be included here, for reasons I have not figured out yet.
static const unsigned SIMD_ALIGN = 32;


std::pair<uint64_t, uint64_t> ECUtil::stripe_info_t::chunk_aligned_offset_len_to_chunk(
  uint64_t _off, uint64_t _len) const {
  auto [off, len] = offset_len_to_stripe_bounds(_off, _len);
  return std::make_pair(
    chunk_aligned_logical_offset_to_chunk_offset(off),
    chunk_aligned_logical_size_to_chunk_size(len));
}

/*
ASCII Art describing the various variables in the following function:
                    start    end
                      |       |
                      |       |
                      |       |
           - - - - - -v- -+---+-----------+ - - - - - -
                 start_adj|   |           |      ^
to_read.offset - ->-------+   |           | chunk_size
                  |           |           |      v
           +------+ - - - - - + - - - - - + - - - - - -
           |                  |           |
           |                  v           |
           |              - - - - +-------+
           |               end_adj|
           |              +-------+
           |              |       |
           +--------------+       |
                          |       |
                          | shard |

Given an offset and size, this adds to a vector of extents describing the
minimal IO ranges on each shard.  If passed, this method will also populate
a superset of all extents required.
 */
void ECUtil::stripe_info_t::ro_range_to_shards(
    uint64_t ro_offset,
    uint64_t ro_size,
    map<int, extent_set> *shard_extent_set,
    extent_set *extent_superset,
    buffer::list *bl,
    shard_extent_map_t *shard_extent_map) const {

  // Some of the maths below assumes size not zero.
  if (ro_size == 0) return;

  uint64_t k  = get_data_chunk_count();

  // Aim is to minimise non-^2 divs (chunk_size is assumed to be a power of 2).
  // These should be the only non ^2 divs.
  uint64_t begin_div = ro_offset / stripe_width;
  uint64_t end_div = (ro_offset+ro_size+stripe_width-1)/stripe_width - 1;
  uint64_t start = begin_div*chunk_size;
  uint64_t end = end_div*chunk_size;

  uint64_t start_shard = (ro_offset - begin_div*stripe_width) / chunk_size;
  uint64_t chunk_count = (ro_offset + ro_size + chunk_size - 1)/chunk_size - ro_offset/chunk_size;;

  // The end_shard needs a modulus to calculate the actual shard, however
  // it is convenient to store it like this for the loop.
  auto end_shard = start_shard + std::min(chunk_count, k );

  // The last shard is the raw shard index which contains the last chunk.
  // Is it possible to calculate this without th e +%?
  uint64_t last_shard = (start_shard + chunk_count - 1) % k ;

  uint64_t buffer_shard_start_offset = 0;

  for (auto i = start_shard; i < end_shard; i++) {
    auto raw_shard = i >= k  ? i - k  : i;

    // Adjust the start and end blocks if needed.
    uint64_t start_adj = 0;
    uint64_t end_adj = 0;

    if (raw_shard<start_shard) {
      // Shards before the start, must start on the next chunk.
      start_adj = chunk_size;
    } else if (raw_shard == start_shard) {
      // The start shard itself needs to be moved a partial-chunk forward.
      start_adj = ro_offset % chunk_size;
    }

    // The end is similar to the start, but the end must be rounded up.
    if (raw_shard < last_shard) {
      end_adj = chunk_size;
    } else if (raw_shard == last_shard) {
      end_adj = (ro_offset + ro_size - 1) % chunk_size + 1;
    }

    auto shard = chunk_mapping.size() > raw_shard ?
             chunk_mapping[raw_shard] : static_cast<int>(raw_shard);

    uint64_t off = start + start_adj;
    uint64_t len =  end + end_adj - start - start_adj;
    if (shard_extent_set) {
      (*shard_extent_set)[shard].insert(off, len);
    }

    if (extent_superset) {
      extent_superset->insert(off, len);
    }

    if (shard_extent_map) {
      ceph_assert(bl);
      buffer::list shard_bl;

      uint64_t bl_offset = buffer_shard_start_offset;

      // Start with any partial chunks.
      if(chunk_size != start_adj) {
        shard_bl.substr_of(*bl, bl_offset, min((uint64_t)bl->length() - bl_offset, chunk_size-start_adj));
        buffer_shard_start_offset += chunk_size - start_adj;
        bl_offset += chunk_size - start_adj + (k - 1) * chunk_size;
      } else {
        buffer_shard_start_offset += chunk_size;
      }
      while (bl_offset < bl->length()) {
        buffer::list tmp;
        tmp.substr_of(*bl, bl_offset, min(chunk_size, bl->length() - bl_offset));
        shard_bl.append(tmp);
        bl_offset += k * chunk_size;
      }
      (*shard_extent_map).insert_in_shard(shard, off, shard_bl, ro_offset, ro_offset + ro_size);
    }
  }
}

void ECUtil::stripe_info_t::trim_shard_extent_set_for_ro_offset (uint64_t ro_offset,
  std::map<int, extent_set> &shard_extent_set) const {

  /* If the offset is within the first shard, then the remaining shards are
   * not written and we don't need to generated zeros for either */
  int ro_offset_shard = (ro_offset / chunk_size) % k;
  if (ro_offset_shard == 0) {
    uint64_t shard_offset = ro_offset_to_shard_offset(ro_offset, 0);
    for (auto &&[shard, eset] : shard_extent_set) {
      eset.erase(align_page_next(shard_offset), ((uint64_t)-1) >> 1);
    }
    std::erase_if(shard_extent_set, [](const auto& item)
    {
        auto const& [_, eset] = item;
        return eset.empty();
    });
  }
}

void ECUtil::stripe_info_t::ro_size_to_read_and_zero_mask(
  uint64_t ro_size,
  std::map<int, extent_set> &shard_extent_set) const {
  ro_range_to_shard_extent_set_with_parity(0, logical_to_next_stripe_offset(ro_size), shard_extent_set);
  trim_shard_extent_set_for_ro_offset(ro_size, shard_extent_set);
}

void ECUtil::stripe_info_t::ro_size_to_read_mask(
  uint64_t ro_size,
  std::map<int, extent_set> &shard_extent_set) const {
  ro_range_to_shard_extent_set_with_parity(0, align_page_next(ro_size), shard_extent_set);
}

void ECUtil::stripe_info_t::ro_size_to_zero_mask(
  uint64_t ro_size,
  std::map<int, extent_set> &shard_extent_set) const {
  // There should never be any zero padding on the parity.
  ro_range_to_shard_extent_set(align_page_next(ro_size),
    logical_to_next_stripe_offset(ro_size) - align_page_next(ro_size),
    shard_extent_set);
  trim_shard_extent_set_for_ro_offset(ro_size, shard_extent_set);
}

/** This variant of decode requires that the set of shards contained in
 * want_to_read is the same for every stripe. Unlike the previous decode, this
 * variant is able to take the entire buffer list of each shard in a single
 * buffer list.  If performance is not critical, this is a simpler interface
 * * and as such is suitable for test tools.
 */
int ECUtil::decode(
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
    int r = ec_impl->decode_concat(want_to_read, chunks, &bl);
    ceph_assert(r == 0);
    ceph_assert(bl.length() % sinfo.get_chunk_size() == 0);
    out->claim_append(bl);
  }
  return 0;
}

/* This variant of decode is used from recovery of an EC */
int ECUtil::decode(
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
  int r = ec_impl->minimum_to_decode(need, avail, &min);
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
    r = ec_impl->decode(need, chunks, &out_bls, sinfo.get_chunk_size());
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

namespace ECUtil {
  void shard_extent_map_t::erase_after_ro_offset(uint64_t ro_offset)
  {
    /* Ignore the null case */
    if (ro_offset >= ro_end)
      return;

    std::map<int, extent_set> ro_to_erase;
    sinfo->ro_range_to_shard_extent_set(ro_offset, ro_end - ro_start,
                              ro_to_erase);
    for (auto && [shard, eset] : ro_to_erase) {
      if (extent_maps.contains(shard)) {
        extent_maps[shard].erase(eset.range_start(), eset.range_end());
      }

      // If the result is empty, delete the extent map.
      if (extent_maps[shard].empty()) {
          extent_maps.erase(shard);
      }
    }

    compute_ro_range();
  }

  shard_extent_map_t shard_extent_map_t::intersect_ro_range(uint64_t ro_offset,
    uint64_t ro_length) const
  {    // Optimise (common) use case where the overlap is everything
    if (ro_offset <= ro_start &&
        ro_offset + ro_length >= ro_end) {
      return *this;
    }

    // Optimise (common) use cases where the overlap is nothing
    if (ro_offset >= ro_end ||
        ro_offset + ro_length <= ro_start) {
      return shard_extent_map_t(sinfo);
    }

    std::map<int, extent_set> ro_to_intersect;
    sinfo->ro_range_to_shard_extent_set(ro_offset, ro_length, ro_to_intersect);

    return intersect(ro_to_intersect);
  }

  shard_extent_map_t shard_extent_map_t::intersect(optional<map<int, extent_set>> const &other) const
  {
    if (!other)
      return shard_extent_map_t(sinfo);

    return intersect(*other);
  }

  shard_extent_map_t shard_extent_map_t::intersect(map<int, extent_set> const &other) const
  {
    shard_extent_map_t out(sinfo);

    for (auto && [shard, this_eset] : other) {
      if (extent_maps.contains(shard)) {
        extent_map tmp;
        extent_set eset;
        eset.intersection_of(extent_maps.at(shard).get_interval_set(), this_eset);

        for (auto [offset, len] : eset) {
          bufferlist bl;
          get_buffer(shard, offset, len, bl);
          tmp.insert(offset, len, bl);
        }
        if (!tmp.empty()) {
          out.extent_maps.emplace(shard, std::move(tmp));
        }
      }
    }

    // This is a fairly inefficient function, so there might be a better way
    // of keeping track here. However, any solution has to cope with holes
    // in the interval map around the start/end of the intersection range.
    out.compute_ro_range();

    return out;
  }

  void shard_extent_map_t::insert(shard_extent_map_t const &other)
  {
    for (auto && [shard, eset] : other.extent_maps)
    {
      extent_maps[shard].insert(eset);
    }

    if (ro_start == invalid_offset || other.ro_start < ro_start)
      ro_start = other.ro_start;
    if (ro_end == invalid_offset || other.ro_end > ro_end)
      ro_end = other.ro_end;
  }

  uint64_t shard_extent_map_t::size()
  {
    uint64_t size = 0;
    for (auto &i : extent_maps)
    {
      for (auto &j : i.second ) size += j.get_len();
    }

    return size;
  }

  void shard_extent_map_t::clear() {
    ro_start = ro_end = invalid_offset;
    extent_maps.clear();
  }


  /* Insert a buffer for a particular shard.
   * NOTE: DO NOT CALL sinfo->get_min_want_shards()
   */
  void shard_extent_map_t::insert_in_shard(int shard, uint64_t off,
    const buffer::list &bl)
  {
    if (bl.length() == 0)
      return;

    extent_maps[shard].insert(off, bl.length(), bl);
    uint64_t new_start = calc_ro_offset(sinfo->get_raw_shard(shard), off);
    uint64_t new_end = calc_ro_offset(sinfo->get_raw_shard(shard), off + bl.length() - 1) + 1;
    if (empty() || new_start < ro_start)
      ro_start = new_start;
    if (empty() || new_end > ro_end )
      ro_end = new_end;
  }

  /* Insert a buffer for a particular shard.
   * If the client knows the new start and end, use this interface to improve
   * performance.
   */
  void shard_extent_map_t::insert_in_shard(int shard, uint64_t off,
    const buffer::list &bl, uint64_t new_start, uint64_t new_end)
  {
    if (bl.length() == 0)
      return;

    extent_maps[shard].insert(off, bl.length(), bl);
    if (empty() || new_start < ro_start)
      ro_start = new_start;
    if (empty() || new_end > ro_end )
      ro_end = new_end;
  }

  /* Insert a region of zeros in rados object address space..
   */
  void shard_extent_map_t::insert_ro_zero_buffer( uint64_t ro_offset,
    uint64_t ro_length )
  {
    buffer::list zero_buffer;
    zero_buffer.append_zero(ro_length);
    sinfo->ro_range_to_shard_extent_map(ro_offset, ro_length, zero_buffer, *this);
  }

  /* Append zeros to the extent maps, such that all bytes from the current end
   * of the rados object range to the specified offset are zero.  Note that the
   * byte at ro_offset does NOT get populated, so that this works as an
   * addition to length.
   */
  void shard_extent_map_t::append_zeros_to_ro_offset( uint64_t ro_offset )
  {
    uint64_t _ro_end = ro_end == invalid_offset ? 0 : ro_end;
    if (ro_offset <= _ro_end)
      return;
    uint64_t append_offset = _ro_end;
    uint64_t append_length = ro_offset - _ro_end;
    insert_ro_zero_buffer(append_offset, append_length);
  }

  /* This method rearranges buffers from a rados object extent map into a shard
   * extent map.  Note that it is a simple transformation, it does NOT perform
   * any encoding of parity shards.
   */
  void shard_extent_map_t::insert_ro_extent_map(const extent_map &host_extent_map)
  {
    for (auto &&range = host_extent_map.begin();
        range != host_extent_map.end();
        ++range) {
      buffer::list bl = range.get_val();
      sinfo->ro_range_to_shard_extent_map(
        range.get_off(),
        range.get_len(),
        bl,
        *this);
    }
  }

  extent_set shard_extent_map_t::get_extent_superset() const {
    extent_set eset;

    for (auto &&[shard, emap] : extent_maps) {
      eset.union_of(emap.get_interval_set());
    }

    return eset;
  }

  void shard_extent_map_t::insert_parity_buffers()
  {
    extent_set encode_set = get_extent_superset();

    /* Invent buffers for the parity coding, if they were not provided.
     * e.g. appends will not provide parity buffers.
     * We should EITHER have no buffers, or have the right buffers.
     */
    for (int i=sinfo->get_k(); i<sinfo->get_k_plus_m(); i++) {
      int shard = sinfo->get_shard(i);
      for (auto &&[offset, length] : encode_set) {
        std::set<int> shards;
        std::map<int, buffer::list> chunk_buffers;
        bufferlist bl;
        bl.push_back(buffer::create_aligned(length, SIMD_ALIGN));
        extent_maps[shard].insert(offset, length, bl);
      }
    }
  }

  /* Encode parity chunks, using the encode_chunks interface into the
   * erasure coding.  This generates all parity.
   */
  int shard_extent_map_t::encode(ErasureCodeInterfaceRef& ecimpl,
    const HashInfoRef &hinfo,
    uint64_t before_ro_size) {

    extent_set encode_set = get_extent_superset();
    encode_set.align(CEPH_PAGE_SIZE);

    for (auto &&[offset, length] : encode_set) {
      std::set<int> shards;
      std::map<int, buffer::list> chunk_buffers;

      for (int raw_shard = 0; raw_shard< sinfo->get_k_plus_m(); raw_shard++) {
        int shard = sinfo->get_shard(raw_shard);
        zero_pad(shard, offset, length);
        get_buffer(shard, offset, length, chunk_buffers[shard]);

        ceph_assert(chunk_buffers[shard].length() == length);
        chunk_buffers[shard].rebuild_aligned_size_and_memory(sinfo->get_chunk_size(), SIMD_ALIGN);

        // The above can replace the buffer, which will not make it back into
        // the extent map, so re-insert this buffer back into the original
        // extent map. We can probably optimise this out much of the time.
        extent_maps[shard].insert(offset, length, chunk_buffers[shard]);

        if (raw_shard >= sinfo->get_k()) {
          shards.insert(raw_shard); // FIXME: Why raw shards??? Needs fix or comment.
        }
      }

      /* Eventually this will call a new API to allow for delta writes. For now
       * however, we call this interface, which will segfault if a full stripe
       * is not provided.
       */
      int r = ecimpl->encode_chunks(shards, &chunk_buffers);
      if (r) return r;

      /* NEEDS REVIEW:  The following calculates the new hinfo CRCs. This is
       *                 currently considering ALL the buffers, including the
       *                 parity buffers.  Is this really right?
       *                 Also, does this really belong here? Its convenient
       *                 because have just built the buffer list...
       */
      if (hinfo && ro_start >= before_ro_size) {
        ceph_assert(ro_start == before_ro_size);
        hinfo->append(
          offset,
          chunk_buffers);
      }
    }
    return 0;
  }

  int shard_extent_map_t::decode(ErasureCodeInterfaceRef& ecimpl,
    map<int, extent_set> want)
  {
    bool decoded = false;
    int r = 0;
    for (auto &&[shard, eset]: want) {
      /* We are assuming here that a shard that has been read does not need
       * to be decoding. The ECBackend::handle_sub_read_reply code will erase
       * buffers for any shards with missing reads, so this should be safe.
       */
      if (extent_maps.contains(shard))
        continue;

      /* Use encode to encode the parity */
      if (sinfo->get_raw_shard(shard) >= sinfo->get_k())
        continue;

      decoded = true;

      for (auto [offset, length]: eset) {
        /* Here we recover each missing shard independently. There may be
         * multiple missing shards and we could collect together all the
         * recoveries at one time. There may be some performance gains in
         * * that scenario if found necessary.
         */
        std::set<int> want_to_read;
        std::map<int, bufferlist> decoded;

        want_to_read.insert(shard);
        auto s = slice(offset, length);

        for (auto &&[_, bl] : s) {
          bl.rebuild_aligned_size_and_memory(sinfo->get_chunk_size(), SIMD_ALIGN);
        }

        /* Call the decode function.  This is not particularly efficient, as it
         * creates buffers for every shard, even if they are not needed.
         *
         * Currently, some plugins rely on this behaviour.
         *
         * The chunk size passed in is only used in the clay encoding. It is
         * NOT the size of the decode.
         */
        int r_i = ecimpl->decode(want_to_read, s, &decoded, sinfo->get_chunk_size());
        if (r_i) {
          r = r_i;
          break;
        }

        ceph_assert(decoded[shard].length() == length);
        insert_in_shard(shard, offset, decoded[shard], ro_start, ro_end);
      }
    }

    if (decoded) compute_ro_range();

    return r;
  }

  std::map<int, bufferlist> shard_extent_map_t::slice(int offset, int length) const
  {
    std::map<int, bufferlist> slice;

    for (auto &&[shard, emap]: extent_maps) {
      bufferlist shard_bl;
      get_buffer(shard, offset, length, shard_bl);
      if (shard_bl.length()) {
        shard_bl.rebuild_aligned_size_and_memory(length, SIMD_ALIGN);
        slice.emplace(shard, std::move(shard_bl));
      }
    }

    return slice;
  }

  void shard_extent_map_t::get_buffer(int shard, uint64_t offset, uint64_t length,
                                      buffer::list &append_to) const
  {
    const extent_map &emap = extent_maps.at(shard);
    auto &&[range, _] = emap.get_containing_range(offset, length);

    if(range == emap.end() || !range.contains(offset, length))
      return;

    if (range.get_len() == length) {
      buffer::list bl = range.get_val();
      // This should be asserted on extent map insertion.
      ceph_assert(bl.length() == length);
      append_to.append(bl);
    } else {
      buffer::list bl;
      bl.substr_of(range.get_val(), offset - range.get_off(), length);
      append_to.append(bl);
    }
  }

  void shard_extent_map_t::get_shard_first_buffer(int shard, buffer::list &append_to) const {

    if (!extent_maps.contains(shard))
      return;
    const extent_map &emap = extent_maps.at(shard);
    auto range = emap.begin();
    if (range == emap.end())
      return;

    append_to.append(range.get_val());
  }

  uint64_t shard_extent_map_t::get_shard_first_offset(int shard) const {

    if (!extent_maps.contains(shard))
      return invalid_offset;
    const extent_map &emap = extent_maps.at(shard);
    auto range = emap.begin();
    if (range == emap.end())
      return invalid_offset;

    return range.get_off();
  }

  void shard_extent_map_t::zero_pad(int shard, uint64_t offset, uint64_t length)
  {
    const extent_map &emap = extent_maps[shard];
    auto &&[range, _] = emap.get_containing_range(offset, length);

    if (range != emap.end() && range.contains(offset, length)) return;

    extent_set required;
    required.insert(offset, length);
    required.subtract(emap.get_interval_set());

    for (auto [z_off, z_len] : required) {
      bufferlist zeros;
      zeros.append_zero(z_len);
      insert_in_shard(shard, z_off, zeros);
    }
  }

  map <int, extent_set> shard_extent_map_t::get_extent_set_map()
  {
    map<int, extent_set> eset_map;
    for (auto &&[shard, emap] : extent_maps) {
      eset_map.emplace(shard, emap.get_interval_set());
    }

    return eset_map;
  }

  void shard_extent_map_t::erase_shard(int shard)
  {
    if (extent_maps.erase(shard)) {
      compute_ro_range();
    }
  }

  bufferlist shard_extent_map_t::get_ro_buffer(
    uint64_t ro_offset,
    uint64_t ro_length)
  {
    bufferlist bl;
    uint64_t chunk_size = sinfo->get_chunk_size();
    uint64_t stripe_size = sinfo->get_stripe_width();
    int data_chunk_count = sinfo->get_data_chunk_count();

    pair read_pair(ro_offset, ro_length);
    auto chunk_aligned_read = sinfo->offset_len_to_chunk_bounds(read_pair);

    int raw_shard = (ro_offset / chunk_size) % data_chunk_count;

    for (uint64_t chunk_offset = chunk_aligned_read.first;
        chunk_offset < chunk_aligned_read.first + chunk_aligned_read.second;
        chunk_offset += chunk_size, raw_shard++) {

      if (raw_shard == data_chunk_count) raw_shard = 0;

      uint64_t sub_chunk_offset = std::max(chunk_offset, ro_offset);
      uint64_t sub_chunk_shard_offset = (chunk_offset / stripe_size) * chunk_size + sub_chunk_offset - chunk_offset;
      uint64_t sub_chunk_len = std::min(ro_offset + ro_length, chunk_offset + chunk_size) - sub_chunk_offset;

      get_buffer(sinfo->get_shard(raw_shard), sub_chunk_shard_offset, sub_chunk_len, bl);
    }
    return bl;
  }

  std::string shard_extent_map_t::debug_string(uint64_t interval, uint64_t offset) const
  {
    std::stringstream str;
    str << "shard_extent_map_t: " << *this << " bufs: [";

    bool s_comma = false;
    for ( auto &&[shard, emap]: get_extent_maps()) {
      if (s_comma) str << ", ";
      s_comma = true;
      str << shard << ": [";

      bool comma = false;
      for ( auto &&extent: emap ) {
        bufferlist bl = extent.get_val();
        char *buf = bl.c_str();
        for (uint64_t i=0; i < extent.get_len(); i += interval) {
          int *seed = (int*)&buf[i + offset];
          if (comma) str << ", ";
          str << (i + extent.get_off()) << ":" << std::to_string(*seed);
          comma = true;
        }
      }
      str << "]";
    }
    str << "]";
    return str.str();
  }

  void shard_extent_map_t::erase_stripe(uint64_t offset, uint64_t length)
  {
    for ( auto iter = extent_maps.begin(); iter != extent_maps.end(); ) {
      auto && [shard, emap] = *iter;
      emap.erase(offset, length);
      if (emap.empty()) {
        iter = extent_maps.erase(iter);
      } else {
        ++iter;
      }
    }
    compute_ro_range();
  }

  bool shard_extent_map_t::contains(int shard) const
  {
    return extent_maps.contains(shard);
  }

  bool shard_extent_map_t::contains(optional<map<int, extent_set>> const &other) const
  {
    if (!other)
      return true;

    return contains(*other);
  }

  bool shard_extent_map_t::contains(map<int, extent_set> const &other) const
  {
    for ( auto &&[shard, other_eset]: other)
    {
      if (!extent_maps.contains(shard))
        return false;

      extent_set eset = extent_maps.at(shard).get_interval_set();
      if (!eset.contains(other_eset)) return false;
    }

    return true;
  }
}

int ECUtil::encode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  bufferlist &in,
  uint64_t offset,
  const set<int> &want,
  map<int, bufferlist> *out) {

  uint64_t logical_size = in.length();
  uint64_t stripe_width = sinfo.get_stripe_width();

  ceph_assert(logical_size % sinfo.get_stripe_width() == 0);
  ceph_assert(out);
  ceph_assert(out->empty());

  if (logical_size == 0)
    return 0;

  for (uint64_t i = 0, start = offset; i < logical_size;) {
    uint64_t to_end_of_stripe = (start/stripe_width + 1) * stripe_width - start;
    uint64_t to_end_of_buffer = logical_size - i;
    uint64_t buffer_size = min(to_end_of_buffer, to_end_of_stripe);

    map<int, bufferlist> encoded;
    bufferlist buf;
    buf.substr_of(in, i, buffer_size);
    int r = ec_impl->encode(want, buf, &encoded);
    ceph_assert(r == 0);
    for (map<int, bufferlist>::iterator i = encoded.begin();
	 i != encoded.end();
	 ++i) {
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

void ECUtil::HashInfo::append(uint64_t old_size,
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

void ECUtil::HashInfo::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(total_chunk_size, bl);
  encode(cumulative_shard_hashes, bl);
  ENCODE_FINISH(bl);
}

void ECUtil::HashInfo::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(total_chunk_size, bl);
  decode(cumulative_shard_hashes, bl);
  DECODE_FINISH(bl);
}

void ECUtil::HashInfo::dump(Formatter *f) const
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

namespace ECUtil {
std::ostream& operator<<(std::ostream& out, const HashInfo& hi)
{
  ostringstream hashes;
  for (auto hash: hi.cumulative_shard_hashes)
    hashes << " " << hex << hash;
  return out << "tcs=" << hi.total_chunk_size << hashes.str();
}
std::ostream& operator<<(std::ostream& out, const shard_extent_map_t& rhs)
{
  // sinfo not thought to be needed for debug, as it is constant.
  return out << "shard_extent_map: ({" << rhs.ro_start << "~"
    << rhs.ro_end << "}, maps=" << rhs.extent_maps << ")";
}

std::ostream& operator<<(std::ostream& out, const log_entry_t& rhs)
{
  switch(rhs.event) {
  case READ_REQUEST: out << "READ_REQUEST"; break;
  case ZERO_REQUEST: out << "ZERO_REQUEST"; break;
  case READ_DONE: out << "READ_DONE"; break;
  case ZERO_DONE: out << "ZERO_DONE"; break;
  case INJECT_EIO: out << "INJECT_EIO"; break;
  case CANCELLED: out << "CANCELLED"; break;
  case ERROR: out << "ERROR"; break;
  case REQUEST_MISSING: out << "REQUEST_MISSING"; break;
  case COMPLETE_ERROR: out << "COMPLETE_ERROR"; break;
  case ERROR_CLEAR: out << "ERROR_CLEAR"; break;
  case COMPLETE: out << "COMPLETE"; break;
  default:
    ceph_assert(false);
  }
  return out << "[" << rhs.shard << "]->" << rhs.io << "\n";
}
}

void ECUtil::HashInfo::generate_test_instances(list<HashInfo*>& o)
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

bool ECUtil::is_hinfo_key_string(const string &key)
{
  return key == HINFO_KEY;
}

const string &ECUtil::get_hinfo_key()
{
  return HINFO_KEY;
}
