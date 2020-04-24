// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "frames_v2.h"

#include <ostream>

#include <fmt/format.h>

namespace ceph::msgr::v2 {

// Discards trailing empty segments, unless there is just one segment.
// A frame always has at least one (possibly empty) segment.
static size_t calc_num_segments(const bufferlist segment_bls[],
                                size_t segment_count) {
  ceph_assert(segment_count > 0 && segment_count <= MAX_NUM_SEGMENTS);
  for (size_t i = segment_count; i-- > 0; ) {
    if (segment_bls[i].length() > 0) {
      return i + 1;
    }
  }
  return 1;
}

void FrameAssembler::fill_preamble(Tag tag,
                                   preamble_block_t& preamble) const {
  // FIPS zeroization audit 20191115: this memset is not security related.
  ::memset(&preamble, 0, sizeof(preamble));

  preamble.tag = static_cast<__u8>(tag);
  for (size_t i = 0; i < m_descs.size(); i++) {
    preamble.segments[i].length = m_descs[i].logical_len;
    preamble.segments[i].alignment = m_descs[i].align;
  }
  preamble.num_segments = m_descs.size();
  preamble.crc = ceph_crc32c(
      0, reinterpret_cast<const unsigned char*>(&preamble),
      sizeof(preamble) - sizeof(preamble.crc));
}

uint64_t FrameAssembler::get_frame_logical_len() const {
  ceph_assert(!m_descs.empty());
  uint64_t logical_len = 0;
  for (size_t i = 0; i < m_descs.size(); i++) {
    logical_len += m_descs[i].logical_len;
  }
  return logical_len;
}

uint64_t FrameAssembler::get_frame_onwire_len() const {
  ceph_assert(!m_descs.empty());
  uint64_t onwire_len = get_preamble_onwire_len();
  for (size_t i = 0; i < m_descs.size(); i++) {
    onwire_len += get_segment_onwire_len(i);
  }
  onwire_len += get_epilogue_onwire_len();
  return onwire_len;
}

bufferlist FrameAssembler::asm_crc_rev0(const preamble_block_t& preamble,
                                        bufferlist segment_bls[]) const {
  epilogue_plain_block_t epilogue;
  // FIPS zeroization audit 20191115: this memset is not security related.
  ::memset(&epilogue, 0, sizeof(epilogue));

  bufferlist frame_bl(sizeof(preamble) + sizeof(epilogue));
  frame_bl.append(reinterpret_cast<const char*>(&preamble), sizeof(preamble));
  for (size_t i = 0; i < m_descs.size(); i++) {
    ceph_assert(segment_bls[i].length() == m_descs[i].logical_len);
    epilogue.crc_values[i] = segment_bls[i].crc32c(-1);
    if (segment_bls[i].length() > 0) {
      frame_bl.claim_append(segment_bls[i]);
    }
  }
  frame_bl.append(reinterpret_cast<const char*>(&epilogue), sizeof(epilogue));
  return frame_bl;
}

bufferlist FrameAssembler::asm_secure_rev0(const preamble_block_t& preamble,
                                           bufferlist segment_bls[]) const {
  bufferlist preamble_bl(sizeof(preamble));
  preamble_bl.append(reinterpret_cast<const char*>(&preamble),
                     sizeof(preamble));

  epilogue_secure_block_t epilogue;
  // FIPS zeroization audit 20191115: this memset is not security related.
  ::memset(&epilogue, 0, sizeof(epilogue));
  bufferlist epilogue_bl(sizeof(epilogue));
  epilogue_bl.append(reinterpret_cast<const char*>(&epilogue),
                     sizeof(epilogue));

  // preamble + MAX_NUM_SEGMENTS + epilogue
  uint32_t onwire_lens[MAX_NUM_SEGMENTS + 2];
  onwire_lens[0] = preamble_bl.length();
  for (size_t i = 0; i < m_descs.size(); i++) {
    onwire_lens[i + 1] = segment_bls[i].length();  // already padded
  }
  onwire_lens[m_descs.size() + 1] = epilogue_bl.length();
  m_crypto->tx->reset_tx_handler(onwire_lens,
                                 onwire_lens + m_descs.size() + 2);
  m_crypto->tx->authenticated_encrypt_update(preamble_bl);
  for (size_t i = 0; i < m_descs.size(); i++) {
    if (segment_bls[i].length() > 0) {
      m_crypto->tx->authenticated_encrypt_update(segment_bls[i]);
    }
  }
  m_crypto->tx->authenticated_encrypt_update(epilogue_bl);
  return m_crypto->tx->authenticated_encrypt_final();
}

bufferlist FrameAssembler::assemble_frame(Tag tag, bufferlist segment_bls[],
                                          const uint16_t segment_aligns[],
                                          size_t segment_count) {
  m_descs.resize(calc_num_segments(segment_bls, segment_count));
  for (size_t i = 0; i < m_descs.size(); i++) {
    m_descs[i].logical_len = segment_bls[i].length();
    m_descs[i].align = segment_aligns[i];
  }

  preamble_block_t preamble;
  fill_preamble(tag, preamble);

  if (m_crypto->rx) {
    for (size_t i = 0; i < m_descs.size(); i++) {
      ceph_assert(segment_bls[i].length() == m_descs[i].logical_len);
      // We're padding segments to biggest cipher's block size. Although
      // AES-GCM can live without that as it's a stream cipher, we don't
      // want to be fixed to stream ciphers only.
      uint32_t padded_len = get_segment_padded_len(i);
      if (padded_len > segment_bls[i].length()) {
        uint32_t pad_len = padded_len - segment_bls[i].length();
        segment_bls[i].reserve(pad_len);
        segment_bls[i].append_zero(pad_len);
      }
    }
    return asm_secure_rev0(preamble, segment_bls);
  }
  return asm_crc_rev0(preamble, segment_bls);
}

std::ostream& operator<<(std::ostream& os, const FrameAssembler& frame_asm) {
  if (!frame_asm.m_descs.empty()) {
    os << frame_asm.get_preamble_onwire_len();
    for (size_t i = 0; i < frame_asm.m_descs.size(); i++) {
      os << " + " << frame_asm.get_segment_onwire_len(i)
         << " (logical " << frame_asm.m_descs[i].logical_len
         << "/" << frame_asm.m_descs[i].align << ")";
    }
    os << " + " << frame_asm.get_epilogue_onwire_len() << " ";
  }
  os << "rx=" << frame_asm.m_crypto->rx.get()
     << " tx=" << frame_asm.m_crypto->tx.get();
  return os;
}

}  // namespace ceph::msgr::v2
