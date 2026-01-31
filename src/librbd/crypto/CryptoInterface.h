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
          uint64_t off, uint64_t len) {
    if (len == 0) {
      return std::make_pair(0, 0);
    }
    auto block_size = get_block_size();
    return std::make_pair(p2phase(off, block_size),
                          p2nphase(off + len, block_size));
  }

  inline std::pair<uint64_t, uint64_t> map_logical_physical(uint64_t log_offset, uint64_t log_length) {
    const uint64_t meta_size = get_meta_size();
    if (meta_size == 0) {
      return {log_offset, log_length};
    }
    uint64_t phys_sector_size = get_block_size() + meta_size;
    uint64_t log_sector_size = get_block_size();
    uint64_t start_sector_idx = log_offset / log_sector_size;
    uint64_t last_byte_idx = log_offset + log_length;
    uint64_t end_sector_idx = (last_byte_idx + log_sector_size - 1) /
                              log_sector_size;
    uint64_t phys_offset = start_sector_idx * phys_sector_size;
    uint64_t num_sectors_to_read = end_sector_idx - start_sector_idx;
    uint64_t phys_length = num_sectors_to_read * phys_sector_size;
    return {phys_offset, phys_length};
  }

  inline std::pair<uint64_t, uint64_t> map_physical_logical(uint64_t phy_offset, 
                                        uint64_t phy_length) {
    const uint64_t meta_size = get_meta_size();
    if (meta_size == 0) {
      return {phy_offset, phy_length};
    }
    uint64_t phys_sector_size = get_block_size() + meta_size;
    uint64_t log_sector_size = get_block_size();
    uint64_t start_sector_idx = phy_offset / phys_sector_size;
    uint64_t last_byte_idx = phy_offset + phy_length;
    uint64_t end_sector_idx = (last_byte_idx + phys_sector_size - 1) / phys_sector_size;
    uint64_t log_offset = start_sector_idx * log_sector_size;
    uint64_t num_sectors_to_read = end_sector_idx - start_sector_idx;
    uint64_t log_length = num_sectors_to_read * log_sector_size;
    return {log_offset, log_length};
  }

  inline std::pair<uint64_t, uint64_t> get_pre_and_post_align_physical(
      uint64_t off, uint64_t len) {
    if (len == 0) {
      return std::make_pair(0, 0);
    }
    uint64_t block_size = get_block_size() + get_meta_size();
    uint64_t end = off + len;
    uint64_t pre = off % block_size;
    uint64_t end_rem = end % block_size;
    uint64_t post = (end_rem == 0) ? 0 : (block_size - end_rem);
    return std::make_pair(pre, post);
  }

  inline std::pair<uint64_t, uint64_t> align_physical(uint64_t off, uint64_t len) {
    auto aligns = get_pre_and_post_align_physical(off, len);
    return std::make_pair(off - aligns.first,
                          len + aligns.first + aligns.second);
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
  
  inline void align_extents_physical(const io::ReadExtents& extents,
                            io::ReadExtents* aligned_extents) {
    for (const auto& extent: extents) {
      auto physical_aligned = map_logical_physical(extent.offset, extent.length);
      aligned_extents->emplace_back(physical_aligned.first, physical_aligned.second);
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

  inline bool is_physical_aligned(uint64_t offset, uint64_t length) {
    uint64_t physical_size = get_block_size() + get_meta_size();
    return (length % physical_size == 0) && (offset % physical_size == 0);
  }

  inline bool is_physical_aligned(const io::ReadExtents& extents) {
    for (const auto& extent: extents) {
      if (!is_physical_aligned(extent.offset, extent.length)) {
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
                                    uint64_t image_offset) {
    if (extent.length == 0 || extent.bl.length() == 0) {
      return 0;
    }
    const uint32_t meta_size = get_meta_size();
    const bool has_meta = (meta_size != 0);

    if (extent.extent_map.empty()) {
      extent.extent_map.emplace_back(extent.offset, extent.bl.length());
    }

    ceph::bufferlist result_bl;
    io::Extents result_extent_map;

    ceph::bufferlist curr_block_bl;
    auto curr_offset = extent.offset;
    auto curr_block_start_offset = curr_offset;
    auto curr_block_end_offset = curr_offset;

    // this will add a final loop iteration for decrypting the last extent
    extent.extent_map.emplace_back(
            extent.offset + extent.length + get_block_size() + meta_size, 0);

    for (auto [off, len]: extent.extent_map) {
      uint64_t aligned_off, aligned_len;
      if (has_meta) {
        std::tie(aligned_off, aligned_len) = align_physical(off, len);
      } else {
        std::tie(aligned_off, aligned_len)= align(off, len);
      }
      if (aligned_off > curr_block_end_offset) {
        if (curr_block_end_offset > curr_offset ) {
          curr_block_bl.append_zero(curr_block_end_offset - curr_offset);
        }
        auto curr_block_length = curr_block_bl.length();
        if (curr_block_length > 0) {
          auto block_offset =
              image_offset +
              map_physical_logical(curr_block_start_offset - extent.offset, 0)
                  .first;
          auto r = decrypt(&curr_block_bl, block_offset);
          if (r != 0) {
            return r;
          }
          if (has_meta) {
            std::tie(curr_block_start_offset, curr_block_length) =
                map_physical_logical(
                    curr_block_start_offset, curr_block_bl.length());
          }
          ceph_assert(curr_block_bl.length() == curr_block_length);
          curr_block_bl.splice(0, curr_block_length, &result_bl);
          // if unalinged read then in remove_alignment_data()
          // this will be the aligned_extents.extent_map
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
