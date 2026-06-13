// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_CRYPTO_CRYPTO_INTERFACE_H
#define CEPH_LIBRBD_CRYPTO_CRYPTO_INTERFACE_H

#include "include/buffer.h"
#include "include/ceph_assert.h"
#include "include/intarith.h"
#include "librbd/io/Types.h"

namespace librbd {
namespace crypto {

class CryptoInterface {

public:
  virtual ~CryptoInterface() = default;

  virtual int encrypt(ceph::bufferlist* data, uint64_t image_offset) = 0;
  virtual int decrypt(ceph::bufferlist* data, uint64_t image_offset) = 0;
  virtual uint64_t get_block_size() const = 0;
  virtual uint32_t get_meta_size() const = 0;
  virtual uint64_t get_data_offset() const = 0;
  virtual const unsigned char* get_key() const = 0;
  virtual int get_key_length() const = 0;

  inline std::pair<uint64_t, uint64_t> get_pre_and_post_align(
          uint64_t off, uint64_t len, bool meta_align=false) {
    if (len == 0) {
      return std::make_pair(0, 0);
    }
    auto block_size = !meta_align ? get_block_size() : get_meta_size();
    return std::make_pair(p2phase(off, block_size),
                          p2nphase(off + len, block_size));
  }

  inline std::pair<uint64_t, uint64_t> align(uint64_t off, uint64_t len) {
    auto aligns = get_pre_and_post_align(off, len);
    return std::make_pair(off - aligns.first,
                          len + aligns.first + aligns.second);
  }

  inline bool is_aligned(uint64_t off, uint64_t len) {
    auto aligns = get_pre_and_post_align(off, len);
    return aligns.first == 0 && aligns.second == 0;
  }

  inline bool is_meta_aligned(uint64_t off, uint64_t len) {
    auto meta_align = get_pre_and_post_align(off, len, true);
    return meta_align.first == 0 && meta_align.second == 0;
  }

  inline void get_physical_extends(const io::ReadExtents& extents,
                            io::ReadExtents* aligned_extents, const uint64_t& image_size) {
    const io::ReadExtents* source_extents = &extents;
    io::ReadExtents temp_aligned_extents;
    if (!is_aligned(extents)) { 
      align_extents(extents, &temp_aligned_extents);
      source_extents = &temp_aligned_extents;
    }
    for (const auto& extent: *source_extents) {
      aligned_extents->emplace_back(extent.offset, extent.length);
      size_t block_length = get_block_size();
      size_t meta_size = get_meta_size();
      size_t block_num = extent.offset / block_length;
      size_t blocks_cnt = extent.length / block_length;
      aligned_extents->emplace_back(image_size + meta_size * block_num, meta_size * blocks_cnt);
    }
  }

  inline bool is_aligned(const io::ReadExtents& extents) {
    for (const auto& extent: extents) {
      if (!is_aligned(extent.offset, extent.length)) {
        return false;
      }
    }
    return true;
  }

  inline bool is_physical_aligned(const io::ReadExtents& extents) {
    if (extents.size() % 2 != 0) {
      return false;
    }
    for (size_t i = 0; i < extents.size(); i += 2) {
      if (!is_aligned(extents[i].offset, extents[i].length) ||
          !is_meta_aligned(extents[i+1].offset, extents[i+1].length)) {
        return false;
      }
    }
    return true;
  }

  inline void align_extents(const io::ReadExtents& extents,
                            io::ReadExtents* aligned_extents) {
    for (const auto& extent: extents) {
      auto aligned = align(extent.offset, extent.length);
      aligned_extents->emplace_back(aligned.first, aligned.second);
    }
  }

  inline int decrypt_aligned_extent(io::ReadExtent& extent,
                                    uint64_t image_offset, io::ReadExtent* meta = nullptr) {
    if (extent.length == 0 || extent.bl.length() == 0) {
      return 0;
    }
    const uint32_t meta_size = get_meta_size();
    const bool has_meta = (meta_size != 0);
    unsigned int meta_block_off = 0;

    if (extent.extent_map.empty()) {
      ceph_assert(extent.bl.length() <= extent.length);
      extent.extent_map.emplace_back(extent.offset, extent.bl.length());
    } // else the sparse read case

    ceph::bufferlist result_bl;
    io::Extents result_extent_map;

    ceph::bufferlist curr_block_bl;
    auto curr_offset = extent.offset;
    auto curr_block_start_offset = curr_offset;
    auto curr_block_end_offset = curr_offset;

    // this will add a final loop iteration for decrypting the last extent
    extent.extent_map.emplace_back(
            extent.offset + extent.length + get_block_size(), 0);

    for (auto [off, len]: extent.extent_map) {
      auto [aligned_off, aligned_len] = align(off, len);
      if (aligned_off > curr_block_end_offset) {
        curr_block_bl.append_zero(curr_block_end_offset - curr_offset);
        auto curr_block_length = curr_block_bl.length();
        if (curr_block_length > 0) {
          if (has_meta) {
            size_t block_length = get_block_size();
            size_t meta_size = get_meta_size();
            size_t block_num = curr_block_bl.length() / block_length;
            // TODO: Do I need to do something with meta->extent_map or buffer_extents?
            ceph_assert(meta_block_off + block_num * meta_size <= meta->bl.length());
            bufferlist meta_block;
            // TODO: Splice instead of copy
            meta_block.substr_of(meta->bl, meta_block_off, block_num * meta_size);
            curr_block_bl.claim_append(std::move(meta_block));
            meta_block_off += block_num * meta_size;
          }
          auto r = decrypt(
                  &curr_block_bl,
                  image_offset + curr_block_start_offset - extent.offset);
          if (r != 0) {
            return r;
          }

          curr_block_bl.splice(0, curr_block_length, &result_bl);
          result_extent_map.emplace_back(
                  curr_block_start_offset, curr_block_length);
        }

        curr_block_start_offset = aligned_off;
        curr_block_end_offset = aligned_off + aligned_len;
        curr_offset = aligned_off;
      }

      curr_block_bl.append_zero(off - curr_offset);
      extent.bl.splice(0, len, &curr_block_bl);
      curr_offset = off + len;
      curr_block_end_offset = aligned_off + aligned_len;
    }

    extent.bl = std::move(result_bl);
    extent.extent_map = std::move(result_extent_map);

    return 0;
  }
};

} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_CRYPTO_INTERFACE_H
