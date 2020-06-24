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

// Unpads bufferlist to unpadded_len.
static void unpad_zero(bufferlist& bl, uint32_t unpadded_len) {
  ceph_assert(bl.length() >= unpadded_len);
  if (bl.length() > unpadded_len) {
    bl.splice(unpadded_len, bl.length() - unpadded_len);
  }
}

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

static void check_segment_crc(const bufferlist& segment_bl,
                              uint32_t expected_crc) {
  uint32_t crc = segment_bl.crc32c(-1);
  if (crc != expected_crc) {
    throw FrameError(fmt::format(
        "bad segment crc calculated={} expected={}", crc, expected_crc));
  }
}

// Returns true if the frame is ready for dispatching, or false if
// it was aborted by the sender and must be dropped.
static bool check_epilogue_late_status(__u8 late_status) {
  __u8 aborted = late_status & FRAME_LATE_STATUS_ABORTED_MASK;
  if (aborted != FRAME_LATE_STATUS_ABORTED &&
      aborted != FRAME_LATE_STATUS_COMPLETE) {
    throw FrameError(fmt::format("bad late_status"));
  }
  return aborted == FRAME_LATE_STATUS_COMPLETE;
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
  epilogue_crc_rev0_block_t epilogue;
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

  epilogue_secure_rev0_block_t epilogue;
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

bufferlist FrameAssembler::asm_crc_rev1(const preamble_block_t& preamble,
                                        bufferlist segment_bls[]) const {
  epilogue_crc_rev1_block_t epilogue;
  // FIPS zeroization audit 20191115: this memset is not security related.
  ::memset(&epilogue, 0, sizeof(epilogue));
  epilogue.late_status |= FRAME_LATE_STATUS_COMPLETE;

  bufferlist frame_bl(sizeof(preamble) + FRAME_CRC_SIZE + sizeof(epilogue));
  frame_bl.append(reinterpret_cast<const char*>(&preamble), sizeof(preamble));

  ceph_assert(segment_bls[0].length() == m_descs[0].logical_len);
  if (segment_bls[0].length() > 0) {
    uint32_t crc = segment_bls[0].crc32c(-1);
    frame_bl.claim_append(segment_bls[0]);
    encode(crc, frame_bl);
  }
  if (m_descs.size() == 1) {
    return frame_bl;  // no epilogue if only one segment
  }

  for (size_t i = 1; i < m_descs.size(); i++) {
    ceph_assert(segment_bls[i].length() == m_descs[i].logical_len);
    epilogue.crc_values[i - 1] = segment_bls[i].crc32c(-1);
    if (segment_bls[i].length() > 0) {
      frame_bl.claim_append(segment_bls[i]);
    }
  }
  frame_bl.append(reinterpret_cast<const char*>(&epilogue), sizeof(epilogue));
  return frame_bl;
}

bufferlist FrameAssembler::asm_secure_rev1(const preamble_block_t& preamble,
                                           bufferlist segment_bls[]) const {
  bufferlist preamble_bl;
  if (segment_bls[0].length() > FRAME_PREAMBLE_INLINE_SIZE) {
    // first segment is partially inlined, inline buffer is full
    preamble_bl.reserve(sizeof(preamble));
    preamble_bl.append(reinterpret_cast<const char*>(&preamble),
                       sizeof(preamble));
    segment_bls[0].splice(0, FRAME_PREAMBLE_INLINE_SIZE, &preamble_bl);
  } else {
    // first segment is fully inlined, inline buffer may need padding
    uint32_t pad_len = FRAME_PREAMBLE_INLINE_SIZE - segment_bls[0].length();
    preamble_bl.reserve(sizeof(preamble) + pad_len);
    preamble_bl.append(reinterpret_cast<const char*>(&preamble),
                       sizeof(preamble));
    preamble_bl.claim_append(segment_bls[0]);
    if (pad_len > 0) {
      preamble_bl.append_zero(pad_len);
    }
  }

  m_crypto->tx->reset_tx_handler({preamble_bl.length()});
  m_crypto->tx->authenticated_encrypt_update(preamble_bl);
  auto frame_bl = m_crypto->tx->authenticated_encrypt_final();

  if (segment_bls[0].length() > 0) {
    m_crypto->tx->reset_tx_handler({segment_bls[0].length()});
    m_crypto->tx->authenticated_encrypt_update(segment_bls[0]);
    frame_bl.claim_append(m_crypto->tx->authenticated_encrypt_final());
  }
  if (m_descs.size() == 1) {
    return frame_bl;  // no epilogue if only one segment
  }

  epilogue_secure_rev1_block_t epilogue;
  // FIPS zeroization audit 20191115: this memset is not security related.
  ::memset(&epilogue, 0, sizeof(epilogue));
  epilogue.late_status |= FRAME_LATE_STATUS_COMPLETE;
  bufferlist epilogue_bl(sizeof(epilogue));
  epilogue_bl.append(reinterpret_cast<const char*>(&epilogue),
                     sizeof(epilogue));

  // MAX_NUM_SEGMENTS - 1 + epilogue
  uint32_t onwire_lens[MAX_NUM_SEGMENTS];
  for (size_t i = 1; i < m_descs.size(); i++) {
    onwire_lens[i - 1] = segment_bls[i].length();  // already padded
  }
  onwire_lens[m_descs.size() - 1] = epilogue_bl.length();
  m_crypto->tx->reset_tx_handler(onwire_lens, onwire_lens + m_descs.size());
  for (size_t i = 1; i < m_descs.size(); i++) {
    if (segment_bls[i].length() > 0) {
      m_crypto->tx->authenticated_encrypt_update(segment_bls[i]);
    }
  }
  m_crypto->tx->authenticated_encrypt_update(epilogue_bl);
  frame_bl.claim_append(m_crypto->tx->authenticated_encrypt_final());
  return frame_bl;
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
    if (m_is_rev1) {
      return asm_secure_rev1(preamble, segment_bls);
    }
    return asm_secure_rev0(preamble, segment_bls);
  }
  if (m_is_rev1) {
    return asm_crc_rev1(preamble, segment_bls);
  }
  return asm_crc_rev0(preamble, segment_bls);
}

Tag FrameAssembler::disassemble_preamble(bufferlist& preamble_bl) {
  if (m_crypto->rx) {
    m_crypto->rx->reset_rx_handler();
    if (m_is_rev1) {
      ceph_assert(preamble_bl.length() == FRAME_PREAMBLE_WITH_INLINE_SIZE +
                                          get_auth_tag_len());
      m_crypto->rx->authenticated_decrypt_update_final(preamble_bl);
    } else {
      ceph_assert(preamble_bl.length() == sizeof(preamble_block_t));
      m_crypto->rx->authenticated_decrypt_update(preamble_bl);
    }
  } else {
    ceph_assert(preamble_bl.length() == sizeof(preamble_block_t));
  }

  // I expect ceph_le32 will make the endian conversion for me. Passing
  // everything through ::Decode is unnecessary.
  auto preamble = reinterpret_cast<const preamble_block_t*>(
      preamble_bl.c_str());
  // check preamble crc before any further processing
  uint32_t crc = ceph_crc32c(
      0, reinterpret_cast<const unsigned char*>(preamble),
      sizeof(*preamble) - sizeof(preamble->crc));
  if (crc != preamble->crc) {
    throw FrameError(fmt::format(
        "bad preamble crc calculated={} expected={}", crc, preamble->crc));
  }

  // see calc_num_segments()
  if (preamble->num_segments < 1 ||
      preamble->num_segments > MAX_NUM_SEGMENTS) {
    throw FrameError(fmt::format(
        "bad number of segments num_segments={}", preamble->num_segments));
  }
  if (preamble->num_segments > 1 &&
      preamble->segments[preamble->num_segments - 1].length == 0) {
    throw FrameError("last segment empty");
  }

  m_descs.resize(preamble->num_segments);
  for (size_t i = 0; i < m_descs.size(); i++) {
    m_descs[i].logical_len = preamble->segments[i].length;
    m_descs[i].align = preamble->segments[i].alignment;
  }
  return static_cast<Tag>(preamble->tag);
}

bool FrameAssembler::disasm_all_crc_rev0(bufferlist segment_bls[],
                                         bufferlist& epilogue_bl) const {
  ceph_assert(epilogue_bl.length() == sizeof(epilogue_crc_rev0_block_t));
  auto epilogue = reinterpret_cast<const epilogue_crc_rev0_block_t*>(
      epilogue_bl.c_str());

  for (size_t i = 0; i < m_descs.size(); i++) {
    ceph_assert(segment_bls[i].length() == m_descs[i].logical_len);
    check_segment_crc(segment_bls[i], epilogue->crc_values[i]);
  }
  return !(epilogue->late_flags & FRAME_LATE_FLAG_ABORTED);
}

bool FrameAssembler::disasm_all_secure_rev0(bufferlist segment_bls[],
                                            bufferlist& epilogue_bl) const {
  for (size_t i = 0; i < m_descs.size(); i++) {
    ceph_assert(segment_bls[i].length() == get_segment_padded_len(i));
    if (segment_bls[i].length() > 0) {
      m_crypto->rx->authenticated_decrypt_update(segment_bls[i]);
      unpad_zero(segment_bls[i], m_descs[i].logical_len);
    }
  }

  ceph_assert(epilogue_bl.length() == sizeof(epilogue_secure_rev0_block_t) +
                                      get_auth_tag_len());
  m_crypto->rx->authenticated_decrypt_update_final(epilogue_bl);
  auto epilogue = reinterpret_cast<const epilogue_secure_rev0_block_t*>(
      epilogue_bl.c_str());
  return !(epilogue->late_flags & FRAME_LATE_FLAG_ABORTED);
}

void FrameAssembler::disasm_first_crc_rev1(bufferlist& preamble_bl,
                                           bufferlist& segment_bl) const {
  ceph_assert(preamble_bl.length() == sizeof(preamble_block_t));
  if (m_descs[0].logical_len > 0) {
    ceph_assert(segment_bl.length() == m_descs[0].logical_len +
                                       FRAME_CRC_SIZE);
    bufferlist::const_iterator it(&segment_bl, m_descs[0].logical_len);
    uint32_t expected_crc;
    decode(expected_crc, it);
    segment_bl.splice(m_descs[0].logical_len, FRAME_CRC_SIZE);
    check_segment_crc(segment_bl, expected_crc);
  } else {
    ceph_assert(segment_bl.length() == 0);
  }
}

bool FrameAssembler::disasm_remaining_crc_rev1(bufferlist segment_bls[],
                                               bufferlist& epilogue_bl) const {
  ceph_assert(epilogue_bl.length() == sizeof(epilogue_crc_rev1_block_t));
  auto epilogue = reinterpret_cast<const epilogue_crc_rev1_block_t*>(
      epilogue_bl.c_str());

  for (size_t i = 1; i < m_descs.size(); i++) {
    ceph_assert(segment_bls[i].length() == m_descs[i].logical_len);
    check_segment_crc(segment_bls[i], epilogue->crc_values[i - 1]);
  }
  return check_epilogue_late_status(epilogue->late_status);
}

void FrameAssembler::disasm_first_secure_rev1(bufferlist& preamble_bl,
                                              bufferlist& segment_bl) const {
  ceph_assert(preamble_bl.length() == FRAME_PREAMBLE_WITH_INLINE_SIZE);
  uint32_t padded_len = get_segment_padded_len(0);
  if (padded_len > FRAME_PREAMBLE_INLINE_SIZE) {
    ceph_assert(segment_bl.length() == padded_len + get_auth_tag_len() -
                                       FRAME_PREAMBLE_INLINE_SIZE);
    m_crypto->rx->reset_rx_handler();
    m_crypto->rx->authenticated_decrypt_update_final(segment_bl);
    // prepend the inline buffer (already decrypted) to segment_bl
    bufferlist tmp;
    segment_bl.swap(tmp);
    preamble_bl.splice(sizeof(preamble_block_t), FRAME_PREAMBLE_INLINE_SIZE,
                       &segment_bl);
    segment_bl.claim_append(std::move(tmp));
  } else {
    ceph_assert(segment_bl.length() == 0);
    preamble_bl.splice(sizeof(preamble_block_t), FRAME_PREAMBLE_INLINE_SIZE,
                       &segment_bl);
  }
  unpad_zero(segment_bl, m_descs[0].logical_len);
  ceph_assert(segment_bl.length() == m_descs[0].logical_len);
}

bool FrameAssembler::disasm_remaining_secure_rev1(
    bufferlist segment_bls[], bufferlist& epilogue_bl) const {
  m_crypto->rx->reset_rx_handler();
  for (size_t i = 1; i < m_descs.size(); i++) {
    ceph_assert(segment_bls[i].length() == get_segment_padded_len(i));
    if (segment_bls[i].length() > 0) {
      m_crypto->rx->authenticated_decrypt_update(segment_bls[i]);
      unpad_zero(segment_bls[i], m_descs[i].logical_len);
    }
  }

  ceph_assert(epilogue_bl.length() == sizeof(epilogue_secure_rev1_block_t) +
                                      get_auth_tag_len());
  m_crypto->rx->authenticated_decrypt_update_final(epilogue_bl);
  auto epilogue = reinterpret_cast<const epilogue_secure_rev1_block_t*>(
      epilogue_bl.c_str());
  return check_epilogue_late_status(epilogue->late_status);
}

void FrameAssembler::disassemble_first_segment(bufferlist& preamble_bl,
                                               bufferlist& segment_bl) const {
  ceph_assert(!m_descs.empty());
  if (m_is_rev1) {
    if (m_crypto->rx) {
      disasm_first_secure_rev1(preamble_bl, segment_bl);
    } else {
      disasm_first_crc_rev1(preamble_bl, segment_bl);
    }
  } else {
    // noop, everything is handled in disassemble_remaining_segments()
  }
}

bool FrameAssembler::disassemble_remaining_segments(
    bufferlist segment_bls[], bufferlist& epilogue_bl) const {
  ceph_assert(!m_descs.empty());
  if (m_is_rev1) {
    if (m_descs.size() == 1) {
      // no epilogue if only one segment
      ceph_assert(epilogue_bl.length() == 0);
      return true;
    }
    if (m_crypto->rx) {
      return disasm_remaining_secure_rev1(segment_bls, epilogue_bl);
    }
    return disasm_remaining_crc_rev1(segment_bls, epilogue_bl);
  }
  if (m_crypto->rx) {
    return disasm_all_secure_rev0(segment_bls, epilogue_bl);
  }
  return disasm_all_crc_rev0(segment_bls, epilogue_bl);
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
  os << "rev1=" << frame_asm.m_is_rev1
     << " rx=" << frame_asm.m_crypto->rx.get()
     << " tx=" << frame_asm.m_crypto->tx.get();
  return os;
}

}  // namespace ceph::msgr::v2
