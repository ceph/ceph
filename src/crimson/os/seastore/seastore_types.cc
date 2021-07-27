// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

std::ostream &segment_to_stream(std::ostream &out, const segment_id_t &t)
{
  if (t == NULL_SEG_ID)
    return out << "NULL_SEG";
  else if (t == BLOCK_REL_SEG_ID)
    return out << "BLOCK_REL_SEG";
  else if (t == RECORD_REL_SEG_ID)
    return out << "RECORD_REL_SEG";
  else if (t == ZERO_SEG_ID)
    return out << "ZERO_SEG";
  else if (t == FAKE_SEG_ID)
    return out << "FAKE_SEG";
  else if (t == DELAYED_TEMP_SEG_ID)
    return out << "DELAYED_TEMP_SEG";
  else
    return out << t;
}

std::ostream &offset_to_stream(std::ostream &out, const segment_off_t &t)
{
  if (t == NULL_SEG_OFF)
    return out << "NULL_OFF";
  else
    return out << t;
}

std::ostream &operator<<(std::ostream &out, const paddr_t &rhs)
{
  out << "paddr_t<";
  segment_to_stream(out, rhs.segment);
  out << ", ";
  offset_to_stream(out, rhs.offset);
  return out << ">";
}

std::ostream &operator<<(std::ostream &out, const journal_seq_t &seq)
{
  return out << "journal_seq_t(segment_seq="
	     << seq.segment_seq << ", offset="
	     << seq.offset
	     << ")";
}

std::ostream &operator<<(std::ostream &out, extent_types_t t)
{
  switch (t) {
  case extent_types_t::ROOT:
    return out << "ROOT";
  case extent_types_t::LADDR_INTERNAL:
    return out << "LADDR_INTERNAL";
  case extent_types_t::LADDR_LEAF:
    return out << "LADDR_LEAF";
  case extent_types_t::ONODE_BLOCK_STAGED:
    return out << "ONODE_BLOCK_STAGED";
  case extent_types_t::OMAP_INNER:
    return out << "OMAP_INNER";
  case extent_types_t::OMAP_LEAF:
    return out << "OMAP_LEAF";
  case extent_types_t::COLL_BLOCK:
    return out << "COLL_BLOCK";
  case extent_types_t::OBJECT_DATA_BLOCK:
    return out << "OBJECT_DATA_BLOCK";
  case extent_types_t::RETIRED_PLACEHOLDER:
    return out << "RETIRED_PLACEHOLDER";
  case extent_types_t::TEST_BLOCK:
    return out << "TEST_BLOCK";
  case extent_types_t::TEST_BLOCK_PHYSICAL:
    return out << "TEST_BLOCK_PHYSICAL";
  case extent_types_t::NONE:
    return out << "NONE";
  default:
    return out << "UNKNOWN";
  }
}

std::ostream &operator<<(std::ostream &out, const laddr_list_t &rhs)
{
  bool first = false;
  for (auto &i: rhs) {
    out << (first ? '[' : ',') << '(' << i.first << ',' << i.second << ')';
    first = true;
  }
  return out << ']';
}
std::ostream &operator<<(std::ostream &out, const paddr_list_t &rhs)
{
  bool first = false;
  for (auto &i: rhs) {
    out << (first ? '[' : ',') << '(' << i.first << ',' << i.second << ')';
    first = true;
  }
  return out << ']';
}

std::ostream &operator<<(std::ostream &lhs, const delta_info_t &rhs)
{
  return lhs << "delta_info_t("
	     << "type: " << rhs.type
	     << ", paddr: " << rhs.paddr
	     << ", laddr: " << rhs.laddr
	     << ", prev_crc: " << rhs.prev_crc
	     << ", final_crc: " << rhs.final_crc
	     << ", length: " << rhs.length
	     << ", pversion: " << rhs.pversion
	     << ")";
}

extent_len_t get_encoded_record_raw_mdlength(
  const record_t &record,
  size_t block_size) {
  extent_len_t metadata =
    (extent_len_t)ceph::encoded_sizeof_bounded<record_header_t>();
  metadata += sizeof(checksum_t) /* crc */;
  metadata += record.extents.size() *
    ceph::encoded_sizeof_bounded<extent_info_t>();
  for (const auto &i: record.deltas) {
    metadata += ceph::encoded_sizeof(i);
  }
  return metadata;
}

record_size_t get_encoded_record_length(
  const record_t &record,
  size_t block_size) {
  extent_len_t metadata =
    get_encoded_record_raw_mdlength(record, block_size);
  extent_len_t data = 0;
  for (const auto &i: record.extents) {
    data += i.bl.length();
  }
  metadata = p2roundup(metadata, (extent_len_t)block_size);
  return record_size_t{metadata, data};
}

ceph::bufferlist encode_record(
  record_size_t rsize,
  record_t &&record,
  size_t block_size,
  segment_off_t committed_to,
  segment_nonce_t current_segment_nonce)
{
  bufferlist data_bl;
  for (auto &i: record.extents) {
    data_bl.append(i.bl);
  }

  bufferlist bl;
  record_header_t header{
    rsize.mdlength,
    rsize.dlength,
    (uint32_t)record.deltas.size(),
    (uint32_t)record.extents.size(),
    current_segment_nonce,
    committed_to,
    data_bl.crc32c(-1)
  };
  encode(header, bl);

  auto metadata_crc_filler = bl.append_hole(sizeof(uint32_t));

  for (const auto &i: record.extents) {
    encode(extent_info_t(i), bl);
  }
  for (const auto &i: record.deltas) {
    encode(i, bl);
  }
  if (bl.length() % block_size != 0) {
    bl.append_zero(
      block_size - (bl.length() % block_size));
  }
  ceph_assert(bl.length() == rsize.mdlength);


  auto bliter = bl.cbegin();
  auto metadata_crc = bliter.crc32c(
    ceph::encoded_sizeof_bounded<record_header_t>(),
    -1);
  bliter += sizeof(checksum_t); /* crc hole again */
  metadata_crc = bliter.crc32c(
    bliter.get_remaining(),
    metadata_crc);
  ceph_le32 metadata_crc_le;
  metadata_crc_le = metadata_crc;
  metadata_crc_filler.copy_in(
    sizeof(checksum_t),
    reinterpret_cast<const char *>(&metadata_crc_le));

  bl.claim_append(data_bl);
  ceph_assert(bl.length() == (rsize.dlength + rsize.mdlength));

  return bl;
}

bool need_delayed_allocation(device_type_t type) {
  return type <= RANDOM_BLOCK;
}

}
