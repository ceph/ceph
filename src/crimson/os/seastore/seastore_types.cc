// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/common/log.h"

namespace {

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_seastore);
}

}

namespace crimson::os::seastore {

std::ostream &segment_to_stream(std::ostream &out, const segment_id_t &t)
{
  if (t == NULL_SEG_ID)
    return out << "NULL_SEG";
  else if (t == FAKE_SEG_ID)
    return out << "FAKE_SEG";
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

std::ostream &operator<<(std::ostream &out, const segment_id_t& segment)
{
  return out << "[" << (uint64_t)segment.device_id() << ","
    << segment.device_segment_id() << "]";
}

std::ostream &operator<<(std::ostream &out, const paddr_t &rhs)
{
  out << "paddr_t<";
  if (rhs == P_ADDR_NULL) {
    out << "NULL_PADDR";
  } else if (rhs == P_ADDR_MIN) {
    out << "MIN_PADDR";
  } else if (rhs.is_block_relative()) {
    out << "BLOCK_REG";
  } else if (rhs.is_record_relative()) {
    out << "RECORD_REG";
  } else if (rhs.get_device_id() == DEVICE_ID_DELAYED) {
    out << "DELAYED_TEMP";
  } else if (rhs.get_addr_type() == addr_types_t::SEGMENT) {
    const seg_paddr_t& s = rhs.as_seg_paddr();
    segment_to_stream(out, s.get_segment_id());
    out << ", ";
    offset_to_stream(out, s.get_segment_off());
  } else {
    out << "INVALID";
  }
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

void record_size_t::account_extent(extent_len_t extent_len)
{
  assert(extent_len);
  plain_mdlength += ceph::encoded_sizeof_bounded<extent_info_t>();
  dlength += extent_len;
}

void record_size_t::account(const delta_info_t& delta)
{
  assert(delta.bl.length());
  plain_mdlength += ceph::encoded_sizeof(delta);
}

extent_len_t record_size_t::get_raw_mdlength() const
{
  return plain_mdlength +
         sizeof(checksum_t) +
         ceph::encoded_sizeof_bounded<record_header_t>();
}

void record_group_size_t::account(
    const record_size_t& rsize,
    extent_len_t _block_size)
{
  // FIXME: prevent submitting empty records
  // assert(!rsize.is_empty());
  assert(_block_size > 0);
  assert(rsize.dlength % _block_size == 0);
  assert(block_size == 0 || block_size == _block_size);
  raw_mdlength += rsize.get_raw_mdlength();
  mdlength += rsize.get_mdlength(_block_size);
  dlength += rsize.dlength;
  block_size = _block_size;
}

ceph::bufferlist encode_record(
  record_t&& record,
  extent_len_t block_size,
  const journal_seq_t& committed_to,
  segment_nonce_t current_segment_nonce)
{
  bufferlist data_bl;
  for (auto &i: record.extents) {
    data_bl.append(i.bl);
  }

  bufferlist bl;
  record_header_t header{
    record.size.get_mdlength(block_size),
    record.size.dlength,
    (extent_len_t)record.deltas.size(),
    (extent_len_t)record.extents.size(),
    current_segment_nonce,
    committed_to,
    data_bl.crc32c(-1)
  };
  encode(header, bl);

  auto metadata_crc_filler = bl.append_hole(sizeof(checksum_t));

  for (const auto &i: record.extents) {
    encode(extent_info_t(i), bl);
  }
  for (const auto &i: record.deltas) {
    encode(i, bl);
  }
  ceph_assert(bl.length() == record.size.get_raw_mdlength());

  auto aligned_mdlength = record.size.get_mdlength(block_size);
  if (bl.length() != aligned_mdlength) {
    assert(bl.length() < aligned_mdlength);
    bl.append_zero(aligned_mdlength - bl.length());
  }

  auto bliter = bl.cbegin();
  auto metadata_crc = bliter.crc32c(
    ceph::encoded_sizeof_bounded<record_header_t>(),
    -1);
  bliter += sizeof(checksum_t); /* metadata crc hole */
  metadata_crc = bliter.crc32c(
    bliter.get_remaining(),
    metadata_crc);
  ceph_le32 metadata_crc_le;
  metadata_crc_le = metadata_crc;
  metadata_crc_filler.copy_in(
    sizeof(checksum_t),
    reinterpret_cast<const char *>(&metadata_crc_le));

  bl.claim_append(data_bl);
  ceph_assert(bl.length() == (record.size.get_encoded_length(block_size)));

  return bl;
}

ceph::bufferlist encode_records(
  record_group_t& record_group,
  const journal_seq_t& committed_to,
  segment_nonce_t current_segment_nonce)
{
  assert(record_group.size.block_size > 0);
  assert(record_group.records.size() > 0);

  bufferlist bl;
  for (auto& r: record_group.records) {
    bl.claim_append(
        encode_record(
          std::move(r),
          record_group.size.block_size,
          committed_to,
          current_segment_nonce));
  }
  ceph_assert(bl.length() == record_group.size.get_encoded_length());

  record_group.clear();
  return bl;
}

std::optional<record_header_t>
try_decode_record_header(
    const ceph::bufferlist& header_bl,
    segment_nonce_t expected_nonce)
{
  auto bp = header_bl.cbegin();
  record_header_t header;
  try {
    decode(header, bp);
  } catch (ceph::buffer::error &e) {
    logger().debug(
        "try_decode_record_header: failed, "
        "cannot decode record_header_t, got {}.",
        e);
    return std::nullopt;
  }
  if (header.segment_nonce != expected_nonce) {
    logger().debug(
        "try_decode_record_header: failed, record_header nonce mismatch, "
        "read {}, expected {}!",
        header.segment_nonce,
        expected_nonce);
    return std::nullopt;
  }
  return header;
}

bool validate_record_metadata(
    const ceph::bufferlist& md_bl)
{
  auto bliter = md_bl.cbegin();
  auto test_crc = bliter.crc32c(
    ceph::encoded_sizeof_bounded<record_header_t>(),
    -1);
  ceph_le32 recorded_crc_le;
  decode(recorded_crc_le, bliter);
  uint32_t recorded_crc = recorded_crc_le;
  test_crc = bliter.crc32c(
    bliter.get_remaining(),
    test_crc);
  bool success = (test_crc == recorded_crc);
  if (!success) {
    logger().debug("validate_record_metadata: failed, metadata crc mismatch.");
  }
  return success;
}

bool validate_record_data(
    const record_header_t& header,
    const ceph::bufferlist& data_bl)
{
  bool success = (data_bl.crc32c(-1) == header.data_crc);
  if (!success) {
    logger().debug("validate_record_data: failed, data crc mismatch!");
  }
  return success;
}

std::optional<record_extent_infos_t>
try_decode_extent_info(
    const record_header_t& header,
    const ceph::bufferlist& md_bl)
{
  auto bliter = md_bl.cbegin();
  bliter += ceph::encoded_sizeof_bounded<record_header_t>();
  bliter += sizeof(checksum_t) /* metadata crc hole */;

  record_extent_infos_t record_extent_info;
  record_extent_info.extent_infos.resize(header.extents);
  for (auto& i: record_extent_info.extent_infos) {
    try {
      decode(i, bliter);
    } catch (ceph::buffer::error &e) {
      logger().debug(
          "try_decode_extent_infos: failed, "
          "cannot decode extent_info_t, got {}.",
          e);
      return std::nullopt;
    }
  }
  return record_extent_info;
}

std::optional<record_deltas_t>
try_decode_deltas(
    const record_header_t& header,
    const ceph::bufferlist& md_bl)
{
  auto bliter = md_bl.cbegin();
  bliter += ceph::encoded_sizeof_bounded<record_header_t>();
  bliter += sizeof(checksum_t) /* metadata crc hole */;
  bliter += header.extents  * ceph::encoded_sizeof_bounded<extent_info_t>();

  record_deltas_t record_delta;
  record_delta.deltas.resize(header.deltas);
  for (auto& i: record_delta.deltas) {
    try {
      decode(i, bliter);
    } catch (ceph::buffer::error &e) {
      logger().debug(
          "try_decode_deltas: failed, "
          "cannot decode delta_info_t, got {}.",
          e);
      return std::nullopt;
    }
  }
  return record_delta;
}

bool can_delay_allocation(device_type_t type) {
  // Some types of device may not support delayed allocation, for example PMEM.
  return type <= RANDOM_BLOCK;
}

device_type_t string_to_device_type(std::string type) {
  if (type == "segmented") {
    return device_type_t::SEGMENTED;
  }
  if (type == "random_block") {
    return device_type_t::RANDOM_BLOCK;
  }
  if (type == "pmem") {
    return device_type_t::PMEM;
  }
  return device_type_t::NONE;
}

std::string device_type_to_string(device_type_t dtype) {
  switch (dtype) {
  case device_type_t::SEGMENTED:
    return "segmented";
  case device_type_t::RANDOM_BLOCK:
    return "random_block";
  case device_type_t::PMEM:
    return "pmem";
  default:
    ceph_assert(0 == "impossible");
  }
}
paddr_t convert_blk_paddr_to_paddr(blk_paddr_t addr, size_t block_size,
    uint32_t blocks_per_segment, device_id_t d_id)
{
  segment_id_t id = segment_id_t {
    d_id,
    (device_segment_id_t)(addr / (block_size * blocks_per_segment))
  };
  segment_off_t off = addr % (block_size * blocks_per_segment);
  return paddr_t::make_seg_paddr(id, off);
}

blk_paddr_t convert_paddr_to_blk_paddr(paddr_t addr, size_t block_size,
    uint32_t blocks_per_segment)
{
  seg_paddr_t& s = addr.as_seg_paddr();
  return (blk_paddr_t)(s.get_segment_id().device_segment_id() *
	  (block_size * blocks_per_segment) + s.get_segment_off());
}


}
