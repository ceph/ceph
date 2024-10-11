// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/seastore_types.h"

#include <utility>

#include "crimson/common/log.h"

namespace {

seastar::logger& journal_logger() {
  return crimson::get_logger(ceph_subsys_seastore_journal);
}

}

namespace crimson::os::seastore {

bool is_aligned(uint64_t offset, uint64_t alignment)
{
  return (offset % alignment) == 0;
}

std::ostream& operator<<(std::ostream &out, const omap_root_t &root)
{
  return out << "omap_root{addr=" << root.addr
	      << ", depth=" << root.depth
	      << ", hint=" << root.hint
	      << ", mutated=" << root.mutated
	      << "}";
}

std::ostream& operator<<(std::ostream& out, const seastore_meta_t& meta)
{
  return out << meta.seastore_id;
}

std::ostream &operator<<(std::ostream &out, const device_id_printer_t &id)
{
  auto _id = id.id;
  if (_id == DEVICE_ID_NULL) {
    return out << "Dev(NULL)";
  } else if (_id == DEVICE_ID_RECORD_RELATIVE) {
    return out << "Dev(RR)";
  } else if (_id == DEVICE_ID_BLOCK_RELATIVE) {
    return out << "Dev(BR)";
  } else if (_id == DEVICE_ID_DELAYED) {
    return out << "Dev(DELAYED)";
  } else if (_id == DEVICE_ID_FAKE) {
    return out << "Dev(FAKE)";
  } else if (_id == DEVICE_ID_ZERO) {
    return out << "Dev(ZERO)";
  } else if (_id == DEVICE_ID_ROOT) {
    return out << "Dev(ROOT)";
  } else {
    return out << "Dev(0x"
               << std::hex << (unsigned)_id << std::dec
               << ")";
  }
}

std::ostream &operator<<(std::ostream &out, const segment_id_t &segment)
{
  if (segment == NULL_SEG_ID) {
    return out << "Seg[NULL]";
  } else {
    return out << "Seg[" << device_id_printer_t{segment.device_id()}
               << ",0x" << std::hex << segment.device_segment_id() << std::dec
               << "]";
  }
}

std::ostream& operator<<(std::ostream& out, segment_type_t t)
{
  switch(t) {
  case segment_type_t::JOURNAL:
    return out << "JOURNAL";
  case segment_type_t::OOL:
    return out << "OOL";
  case segment_type_t::NULL_SEG:
    return out << "NULL_SEG";
  default:
    return out << "INVALID_SEGMENT_TYPE!";
  }
}

std::ostream& operator<<(std::ostream& out, segment_seq_printer_t seq)
{
  if (seq.seq == NULL_SEG_SEQ) {
    return out << "sseq(NULL)";
  } else {
    return out << "sseq(" << seq.seq << ")";
  }
}

std::ostream &operator<<(std::ostream &out, const laddr_t &laddr) {
  return out << "L0x" << std::hex << laddr.value << std::dec;
}

std::ostream &operator<<(std::ostream &out, const laddr_offset_t &laddr_offset) {
  return out << laddr_offset.get_aligned_laddr()
	     << "+0x" << std::hex << laddr_offset.get_offset() << std::dec;
}

std::ostream &operator<<(std::ostream &out, const pladdr_t &pladdr)
{
  if (pladdr.is_laddr()) {
    return out << pladdr.get_laddr();
  } else {
    return out << pladdr.get_paddr();
  }
}

std::ostream &operator<<(std::ostream &out, const paddr_t &rhs)
{
  auto id = rhs.get_device_id();
  out << "paddr<";
  if (rhs == P_ADDR_NULL) {
    out << "NULL";
  } else if (rhs == P_ADDR_MIN) {
    out << "MIN";
  } else if (rhs == P_ADDR_ZERO) {
    out << "ZERO";
  } else if (has_device_off(id)) {
    auto &s = rhs.as_res_paddr();
    out << device_id_printer_t{id}
        << ",0x"
        << std::hex << s.get_device_off() << std::dec;
  } else if (rhs.get_addr_type() == paddr_types_t::SEGMENT) {
    auto &s = rhs.as_seg_paddr();
    out << s.get_segment_id()
        << ",0x"
        << std::hex << s.get_segment_off() << std::dec;
  } else if (rhs.get_addr_type() == paddr_types_t::RANDOM_BLOCK) {
    auto &s = rhs.as_blk_paddr();
    out << device_id_printer_t{s.get_device_id()}
        << ",0x"
        << std::hex << s.get_device_off() << std::dec;
  } else {
    out << "INVALID!";
  }
  return out << ">";
}

journal_seq_t journal_seq_t::add_offset(
      backend_type_t type,
      device_off_t off,
      device_off_t roll_start,
      device_off_t roll_size) const
{
  assert(offset.is_absolute());
  assert(off <= DEVICE_OFF_MAX && off >= DEVICE_OFF_MIN);
  assert(roll_start >= 0);
  assert(roll_size > 0);

  segment_seq_t jseq = segment_seq;
  device_off_t joff;
  if (type == backend_type_t::SEGMENTED) {
    joff = offset.as_seg_paddr().get_segment_off();
  } else {
    assert(type == backend_type_t::RANDOM_BLOCK);
    auto boff = offset.as_blk_paddr().get_device_off();
    joff = boff;
  }
  auto roll_end = roll_start + roll_size;
  assert(joff >= roll_start);
  assert(joff <= roll_end);

  if (off >= 0) {
    device_off_t new_jseq = jseq + (off / roll_size);
    joff += (off % roll_size);
    if (joff >= roll_end) {
      ++new_jseq;
      joff -= roll_size;
    }
    assert(std::cmp_less(new_jseq, MAX_SEG_SEQ));
    jseq = static_cast<segment_seq_t>(new_jseq);
  } else {
    device_off_t mod = (-off) / roll_size;
    joff -= ((-off) % roll_size);
    if (joff < roll_start) {
      ++mod;
      joff += roll_size;
    }
    if (std::cmp_greater_equal(jseq, mod)) {
      jseq -= mod;
    } else {
      return JOURNAL_SEQ_MIN;
    }
  }
  assert(joff >= roll_start);
  assert(joff < roll_end);
  return journal_seq_t{jseq, make_block_relative_paddr(joff)};
}

device_off_t journal_seq_t::relative_to(
      backend_type_t type,
      const journal_seq_t& r,
      device_off_t roll_start,
      device_off_t roll_size) const
{
  assert(offset.is_absolute());
  assert(r.offset.is_absolute());
  assert(roll_start >= 0);
  assert(roll_size > 0);

  device_off_t ret = static_cast<device_off_t>(segment_seq) - r.segment_seq;
  ret *= roll_size;
  if (type == backend_type_t::SEGMENTED) {
    ret += (static_cast<device_off_t>(offset.as_seg_paddr().get_segment_off()) -
            static_cast<device_off_t>(r.offset.as_seg_paddr().get_segment_off()));
  } else {
    assert(type == backend_type_t::RANDOM_BLOCK);
    ret += offset.as_blk_paddr().get_device_off() -
           r.offset.as_blk_paddr().get_device_off();
  }
  assert(ret <= DEVICE_OFF_MAX && ret >= DEVICE_OFF_MIN);
  return ret;
}

std::ostream &operator<<(std::ostream &out, const journal_seq_t &seq)
{
  if (seq == JOURNAL_SEQ_NULL) {
    return out << "JOURNAL_SEQ_NULL";
  } else if (seq == JOURNAL_SEQ_MIN) {
    return out << "JOURNAL_SEQ_MIN";
  } else {
    return out << "jseq("
               << segment_seq_printer_t{seq.segment_seq}
               << ", " << seq.offset
               << ")";
  }
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
  case extent_types_t::DINK_LADDR_LEAF:
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
  case extent_types_t::ALLOC_INFO:
    return out << "ALLOC_INFO";
  case extent_types_t::JOURNAL_TAIL:
    return out << "JOURNAL_TAIL";
  case extent_types_t::TEST_BLOCK:
    return out << "TEST_BLOCK";
  case extent_types_t::TEST_BLOCK_PHYSICAL:
    return out << "TEST_BLOCK_PHYSICAL";
  case extent_types_t::BACKREF_INTERNAL:
    return out << "BACKREF_INTERNAL";
  case extent_types_t::BACKREF_LEAF:
    return out << "BACKREF_LEAF";
  case extent_types_t::NONE:
    return out << "NONE";
  default:
    return out << "UNKNOWN(" << (unsigned)t << ")";
  }
}

std::ostream &operator<<(std::ostream &out, rewrite_gen_printer_t gen)
{
  if (gen.gen == NULL_GENERATION) {
    return out << "GEN_NULL";
  } else if (gen.gen == INIT_GENERATION) {
    return out << "GEN_INIT";
  } else if (gen.gen == INLINE_GENERATION) {
    return out << "GEN_INL";
  } else if (gen.gen == OOL_GENERATION) {
    return out << "GEN_OOL";
  } else if (gen.gen > REWRITE_GENERATIONS) {
    return out << "GEN_INVALID(" << (unsigned)gen.gen << ")!";
  } else {
    return out << "GEN(" << (unsigned)gen.gen << ")";
  }
}

std::ostream &operator<<(std::ostream &out, data_category_t c)
{
  switch (c) {
    case data_category_t::METADATA:
      return out << "MD";
    case data_category_t::DATA:
      return out << "DATA";
    default:
      return out << "INVALID_CATEGORY!";
  }
}

bool can_inplace_rewrite(extent_types_t type) {
  return is_data_type(type);
}

std::ostream &operator<<(std::ostream &out, sea_time_point_printer_t tp)
{
  if (tp.tp == NULL_TIME) {
    return out << "tp(NULL)";
  }
  auto time = seastar::lowres_system_clock::to_time_t(tp.tp);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&time));
  return out << "tp(" << buf << ")";
}

std::ostream &operator<<(std::ostream &out, mod_time_point_printer_t tp) {
  auto time = mod_to_timepoint(tp.tp);
  return out << "mod_" << sea_time_point_printer_t{time};
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

std::ostream &operator<<(std::ostream &out, const delta_info_t &delta)
{
  return out << "delta_info_t("
	     << "type: " << delta.type
	     << ", paddr: " << delta.paddr
	     << ", laddr: " << delta.laddr
	     << ", prev_crc: " << delta.prev_crc
	     << ", final_crc: " << delta.final_crc
	     << ", length: " << delta.length
	     << ", pversion: " << delta.pversion
	     << ", ext_seq: " << delta.ext_seq
	     << ", seg_type: " << delta.seg_type
	     << ")";
}

std::ostream &operator<<(std::ostream &out, const journal_tail_delta_t &delta)
{
  return out << "journal_tail_delta_t("
             << "alloc_tail=" << delta.alloc_tail
             << ", dirty_tail=" << delta.dirty_tail
             << ")";
}

std::ostream &operator<<(std::ostream &out, const extent_info_t &info)
{
  return out << "extent_info_t("
	     << "type: " << info.type
	     << ", addr: " << info.addr
	     << ", len: " << info.len
	     << ")";
}

std::ostream &operator<<(std::ostream &out, const segment_header_t &header)
{
  return out << "segment_header_t("
             << header.physical_segment_id
             << " " << header.type
             << " " << segment_seq_printer_t{header.segment_seq}
             << " " << header.category
             << " " << rewrite_gen_printer_t{header.generation}
             << ", dirty_tail=" << header.dirty_tail
             << ", alloc_tail=" << header.alloc_tail
             << ", segment_nonce=" << header.segment_nonce
	     << ", modify_time=" << mod_time_point_printer_t{header.modify_time}
             << ")";
}

std::ostream &operator<<(std::ostream &out, const segment_tail_t &tail)
{
  return out << "segment_tail_t("
             << tail.physical_segment_id
             << " " << tail.type
             << " " << segment_seq_printer_t{tail.segment_seq}
             << ", segment_nonce=" << tail.segment_nonce
             << ", modify_time=" << mod_time_point_printer_t{tail.modify_time}
             << ", num_extents=" << tail.num_extents
             << ")";
}

extent_len_t record_size_t::get_raw_mdlength() const
{
  assert(record_type < record_type_t::MAX);
  // empty record is allowed to submit
  extent_len_t ret = plain_mdlength;
  if (record_type == record_type_t::JOURNAL) {
    ret += ceph::encoded_sizeof_bounded<record_header_t>();
  } else {
    // OOL won't contain metadata
    assert(ret == 0);
  }
  return ret;
}

void record_size_t::account_extent(extent_len_t extent_len)
{
  assert(extent_len);
  if (record_type == record_type_t::JOURNAL) {
    plain_mdlength += ceph::encoded_sizeof_bounded<extent_info_t>();
  } else {
    // OOL won't contain metadata
  }
  dlength += extent_len;
}

void record_size_t::account(const delta_info_t& delta)
{
  assert(record_type == record_type_t::JOURNAL);
  assert(delta.bl.length());
  plain_mdlength += ceph::encoded_sizeof(delta);
}

std::ostream &operator<<(std::ostream &os, transaction_type_t type)
{
  switch (type) {
  case transaction_type_t::MUTATE:
    return os << "MUTATE";
  case transaction_type_t::READ:
    return os << "READ";
  case transaction_type_t::TRIM_DIRTY:
    return os << "TRIM_DIRTY";
  case transaction_type_t::TRIM_ALLOC:
    return os << "TRIM_ALLOC";
  case transaction_type_t::CLEANER_MAIN:
    return os << "CLEANER_MAIN";
  case transaction_type_t::CLEANER_COLD:
    return os << "CLEANER_COLD";
  case transaction_type_t::MAX:
    return os << "TRANS_TYPE_NULL";
  default:
    return os << "INVALID_TRANS_TYPE("
              << static_cast<std::size_t>(type)
              << ")";
  }
}

std::ostream &operator<<(std::ostream& out, const record_size_t& rsize)
{
  return out << "record_size_t("
             << "record_type=" << rsize.record_type
             << "raw_md=" << rsize.get_raw_mdlength()
             << ", data=" << rsize.dlength
             << ")";
}

std::ostream &operator<<(std::ostream& out, const record_type_t& type)
{
  switch (type) {
  case record_type_t::JOURNAL:
    return out << "JOURNAL";
  case record_type_t::OOL:
    return out << "OOL";
  case record_type_t::MAX:
    return out << "NULL";
  default:
    return out << "INVALID_RECORD_TYPE("
               << static_cast<std::size_t>(type)
               << ")";
  }
}

std::ostream &operator<<(std::ostream& out, const record_t& r)
{
  return out << "record_t("
             << "trans_type=" << r.trans_type
             << ", num_extents=" << r.extents.size()
             << ", num_deltas=" << r.deltas.size()
             << ", modify_time=" << sea_time_point_printer_t{r.modify_time}
             << ")";
}

std::ostream &operator<<(std::ostream& out, const record_header_t& r)
{
  return out << "record_header_t("
             << "type=" << r.type
             << ", num_extents=" << r.extents
             << ", num_deltas=" << r.deltas
             << ", modify_time=" << mod_time_point_printer_t{r.modify_time}
             << ")";
}

std::ostream& operator<<(std::ostream& out, const record_group_header_t& h)
{
  return out << "record_group_header_t("
             << "num_records=" << h.records
             << ", mdlength=" << h.mdlength
             << ", dlength=" << h.dlength
             << ", nonce=" << h.segment_nonce
             << ", committed_to=" << h.committed_to
             << ", data_crc=" << h.data_crc
             << ")";
}

extent_len_t record_group_size_t::get_raw_mdlength() const
{
  assert(record_type < record_type_t::MAX);
  extent_len_t ret = plain_mdlength;
  if (record_type == record_type_t::JOURNAL) {
    ret += sizeof(checksum_t);
    ret += ceph::encoded_sizeof_bounded<record_group_header_t>();
  } else {
    // OOL won't contain metadata
    assert(ret == 0);
  }
  return ret;
}

void record_group_size_t::account(
    const record_size_t& rsize,
    extent_len_t _block_size)
{
  // empty record is allowed to submit
  assert(_block_size > 0);
  assert(rsize.dlength % _block_size == 0);
  assert(block_size == 0 || block_size == _block_size);
  assert(record_type == RECORD_TYPE_NULL ||
         record_type == rsize.record_type);
  block_size = _block_size;
  record_type = rsize.record_type;
  if (record_type == record_type_t::JOURNAL) {
    plain_mdlength += rsize.get_raw_mdlength();
  } else {
    // OOL won't contain metadata
    assert(rsize.get_raw_mdlength() == 0);
  }
  dlength += rsize.dlength;
}

std::ostream& operator<<(std::ostream& out, const record_group_size_t& size)
{
  return out << "record_group_size_t("
             << "record_type=" << size.record_type
             << "raw_md=" << size.get_raw_mdlength()
             << ", data=" << size.dlength
             << ", block_size=" << size.block_size
             << ", fullness=" << size.get_fullness()
             << ")";
}

std::ostream& operator<<(std::ostream& out, const record_group_t& rg)
{
  return out << "record_group_t("
             << "num_records=" << rg.records.size()
             << ", " << rg.size
             << ")";
}

ceph::bufferlist encode_record(
  record_t&& record,
  extent_len_t block_size,
  const journal_seq_t& committed_to,
  segment_nonce_t current_segment_nonce)
{
  record_group_t record_group(std::move(record), block_size);
  return encode_records(
      record_group,
      committed_to,
      current_segment_nonce);
}

ceph::bufferlist encode_records(
  record_group_t& record_group,
  const journal_seq_t& committed_to,
  segment_nonce_t current_segment_nonce)
{
  assert(record_group.size.record_type < record_type_t::MAX);
  assert(record_group.size.block_size > 0);
  assert(record_group.records.size() > 0);

  bufferlist data_bl;
  for (auto& r: record_group.records) {
    for (auto& i: r.extents) {
      assert(i.bl.length());
      data_bl.append(i.bl);
    }
  }

  if (record_group.size.record_type == record_type_t::OOL) {
    // OOL won't contain metadata
    assert(record_group.size.get_mdlength() == 0);
    ceph_assert(data_bl.length() ==
                record_group.size.get_encoded_length());
    record_group.clear();
    return data_bl;
  }
  // JOURNAL
  bufferlist bl;
  record_group_header_t header{
    static_cast<extent_len_t>(record_group.records.size()),
    record_group.size.get_mdlength(),
    record_group.size.dlength,
    current_segment_nonce,
    committed_to,
    data_bl.crc32c(-1)
  };
  encode(header, bl);

  auto metadata_crc_filler = bl.append_hole(sizeof(checksum_t));

  for (auto& r: record_group.records) {
    record_header_t rheader{
      r.trans_type,
      (extent_len_t)r.deltas.size(),
      (extent_len_t)r.extents.size(),
      timepoint_to_mod(r.modify_time)
    };
    encode(rheader, bl);
  }
  for (auto& r: record_group.records) {
    for (const auto& i: r.extents) {
      encode(extent_info_t(i), bl);
    }
  }
  for (auto& r: record_group.records) {
    for (const auto& i: r.deltas) {
      encode(i, bl);
    }
  }
  ceph_assert(bl.length() == record_group.size.get_raw_mdlength());

  auto aligned_mdlength = record_group.size.get_mdlength();
  if (bl.length() != aligned_mdlength) {
    assert(bl.length() < aligned_mdlength);
    bl.append_zero(aligned_mdlength - bl.length());
  }

  auto bliter = bl.cbegin();
  auto metadata_crc = bliter.crc32c(
    ceph::encoded_sizeof_bounded<record_group_header_t>(),
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
  ceph_assert(bl.length() == record_group.size.get_encoded_length());

  record_group.clear();
  return bl;
}

std::optional<record_group_header_t>
try_decode_records_header(
    const ceph::bufferlist& header_bl,
    segment_nonce_t expected_nonce)
{
  auto bp = header_bl.cbegin();
  record_group_header_t header;
  try {
    decode(header, bp);
  } catch (ceph::buffer::error &e) {
    journal_logger().debug(
        "try_decode_records_header: failed, "
        "cannot decode record_group_header_t, got {}.",
        e.what());
    return std::nullopt;
  }
  if (header.segment_nonce != expected_nonce) {
    journal_logger().debug(
        "try_decode_records_header: failed, record_group_header nonce mismatch, "
        "read {}, expected {}!",
        header.segment_nonce,
        expected_nonce);
    return std::nullopt;
  }
  return header;
}

bool validate_records_metadata(
    const ceph::bufferlist& md_bl)
{
  auto bliter = md_bl.cbegin();
  auto test_crc = bliter.crc32c(
    ceph::encoded_sizeof_bounded<record_group_header_t>(),
    -1);
  ceph_le32 recorded_crc_le;
  decode(recorded_crc_le, bliter);
  uint32_t recorded_crc = recorded_crc_le;
  test_crc = bliter.crc32c(
    bliter.get_remaining(),
    test_crc);
  bool success = (test_crc == recorded_crc);
  if (!success) {
    journal_logger().debug(
        "validate_records_metadata: failed, metadata crc mismatch.");
  }
  return success;
}

bool validate_records_data(
    const record_group_header_t& header,
    const ceph::bufferlist& data_bl)
{
  bool success = (data_bl.crc32c(-1) == header.data_crc);
  if (!success) {
    journal_logger().debug(
        "validate_records_data: failed, data crc mismatch!");
  }
  return success;
}

std::optional<std::vector<record_header_t>>
try_decode_record_headers(
    const record_group_header_t& header,
    const ceph::bufferlist& md_bl)
{
  auto bliter = md_bl.cbegin();
  bliter += ceph::encoded_sizeof_bounded<record_group_header_t>();
  bliter += sizeof(checksum_t); /* metadata crc hole */
  std::vector<record_header_t> record_headers(header.records);
  for (auto &&i: record_headers) {
    try {
      decode(i, bliter);
    } catch (ceph::buffer::error &e) {
      journal_logger().debug(
          "try_decode_record_headers: failed, "
          "cannot decode record_header_t, got {}.",
          e.what());
      return std::nullopt;
    }
  }
  return record_headers;
}

std::optional<std::vector<record_extent_infos_t> >
try_decode_extent_infos(
    const record_group_header_t& header,
    const ceph::bufferlist& md_bl)
{
  auto maybe_headers = try_decode_record_headers(header, md_bl);
  if (!maybe_headers) {
    return std::nullopt;
  }

  auto bliter = md_bl.cbegin();
  bliter += ceph::encoded_sizeof_bounded<record_group_header_t>();
  bliter += sizeof(checksum_t); /* metadata crc hole */
  bliter += (ceph::encoded_sizeof_bounded<record_header_t>() *
             maybe_headers->size());

  std::vector<record_extent_infos_t> record_extent_infos(
      maybe_headers->size());
  auto result_iter = record_extent_infos.begin();
  for (auto& h: *maybe_headers) {
    result_iter->header = h;
    result_iter->extent_infos.resize(h.extents);
    for (auto& i: result_iter->extent_infos) {
      try {
        decode(i, bliter);
      } catch (ceph::buffer::error &e) {
        journal_logger().debug(
            "try_decode_extent_infos: failed, "
            "cannot decode extent_info_t, got {}.",
            e.what());
        return std::nullopt;
      }
    }
    ++result_iter;
  }
  return record_extent_infos;
}

std::optional<std::vector<record_deltas_t> >
try_decode_deltas(
    const record_group_header_t& header,
    const ceph::bufferlist& md_bl,
    paddr_t record_block_base)
{
  auto maybe_record_extent_infos = try_decode_extent_infos(header, md_bl);
  if (!maybe_record_extent_infos) {
    return std::nullopt;
  }

  auto bliter = md_bl.cbegin();
  bliter += ceph::encoded_sizeof_bounded<record_group_header_t>();
  bliter += sizeof(checksum_t); /* metadata crc hole */
  bliter += (ceph::encoded_sizeof_bounded<record_header_t>() *
             maybe_record_extent_infos->size());
  for (auto& r: *maybe_record_extent_infos) {
    bliter += (ceph::encoded_sizeof_bounded<extent_info_t>() *
               r.extent_infos.size());
  }

  std::vector<record_deltas_t> record_deltas(
      maybe_record_extent_infos->size());
  auto result_iter = record_deltas.begin();
  for (auto& r: *maybe_record_extent_infos) {
    result_iter->record_block_base = record_block_base;
    result_iter->deltas.resize(r.header.deltas);
    for (auto& i: result_iter->deltas) {
      try {
        decode(i.second, bliter);
        i.first = mod_to_timepoint(r.header.modify_time);
      } catch (ceph::buffer::error &e) {
        journal_logger().debug(
            "try_decode_deltas: failed, "
            "cannot decode delta_info_t, got {}.",
            e.what());
        return std::nullopt;
      }
    }
    for (auto& i: r.extent_infos) {
      record_block_base = record_block_base.add_offset(i.len);
    }
    ++result_iter;
  }
  return record_deltas;
}

std::ostream& operator<<(std::ostream& out, placement_hint_t h)
{
  switch (h) {
  case placement_hint_t::HOT:
    return out << "Hint(HOT)";
  case placement_hint_t::COLD:
    return out << "Hint(COLD)";
  case placement_hint_t::REWRITE:
    return out << "Hint(REWRITE)";
  case PLACEMENT_HINT_NULL:
    return out << "Hint(NULL)";
  default:
    return out << "INVALID_PLACEMENT_HINT_TYPE!";
  }
}

bool can_delay_allocation(device_type_t type) {
  // Some types of device may not support delayed allocation, for example PMEM.
  // All types of device currently support delayed allocation.
  return true;
}

device_type_t string_to_device_type(std::string type) {
  if (type == "HDD") {
    return device_type_t::HDD;
  }
  if (type == "SSD") {
    return device_type_t::SSD;
  }
  if (type == "ZBD") {
    return device_type_t::ZBD;
  }
  if (type == "RANDOM_BLOCK_SSD") {
    return device_type_t::RANDOM_BLOCK_SSD;
  }
  return device_type_t::NONE;
}

std::ostream& operator<<(std::ostream& out, device_type_t t)
{
  switch (t) {
  case device_type_t::NONE:
    return out << "NONE";
  case device_type_t::HDD:
    return out << "HDD";
  case device_type_t::SSD:
    return out << "SSD";
  case device_type_t::ZBD:
    return out << "ZBD";
  case device_type_t::EPHEMERAL_COLD:
    return out << "EPHEMERAL_COLD";
  case device_type_t::EPHEMERAL_MAIN:
    return out << "EPHEMERAL_MAIN";
  case device_type_t::RANDOM_BLOCK_SSD:
    return out << "RANDOM_BLOCK_SSD";
  case device_type_t::RANDOM_BLOCK_EPHEMERAL:
    return out << "RANDOM_BLOCK_EPHEMERAL";
  default:
    return out << "INVALID_DEVICE_TYPE!";
  }
}

std::ostream& operator<<(std::ostream& out, backend_type_t btype) {
  if (btype == backend_type_t::SEGMENTED) {
    return out << "SEGMENTED";
  } else {
    return out << "RANDOM_BLOCK";
  }
}

std::ostream& operator<<(std::ostream& out, const write_result_t& w)
{
  return out << "write_result_t("
             << "start=" << w.start_seq
             << ", length=" << w.length
             << ")";
}

std::ostream& operator<<(std::ostream& out, const record_locator_t& l)
{
  return out << "record_locator_t("
             << "block_base=" << l.record_block_base
             << ", " << l.write_result
             << ")";
}

void scan_valid_records_cursor::emplace_record_group(
    const record_group_header_t& header, ceph::bufferlist&& md_bl)
{
  auto new_committed_to = header.committed_to;
  ceph_assert(last_committed == JOURNAL_SEQ_NULL ||
              last_committed <= new_committed_to);
  last_committed = new_committed_to;
  pending_record_groups.emplace_back(
    seq.offset,
    header,
    std::move(md_bl));
  increment_seq(header.dlength + header.mdlength);
  ceph_assert(new_committed_to == JOURNAL_SEQ_NULL ||
              new_committed_to < seq);
}

std::ostream& operator<<(std::ostream& out, const scan_valid_records_cursor& c)
{
  return out << "cursor(last_valid_header_found=" << c.last_valid_header_found
             << ", seq=" << c.seq
             << ", last_committed=" << c.last_committed
             << ", pending_record_groups=" << c.pending_record_groups.size()
             << ", num_consumed_records=" << c.num_consumed_records
             << ")";
}

std::ostream& operator<<(std::ostream& out, const tw_stats_printer_t& p)
{
  constexpr const char* dfmt = "{:.2f}";
  double d_num_records = static_cast<double>(p.stats.num_records);
  out << "rps="
      << fmt::format(dfmt, d_num_records/p.seconds)
      << ",bwMiB="
      << fmt::format(dfmt, p.stats.get_total_bytes()/p.seconds/(1<<20))
      << ",sizeB="
      << fmt::format(dfmt, p.stats.get_total_bytes()/d_num_records)
      << "("
      << fmt::format(dfmt, p.stats.data_bytes/d_num_records)
      << ","
      << fmt::format(dfmt, p.stats.metadata_bytes/d_num_records)
      << ")";
  return out;
}

std::ostream& operator<<(std::ostream& out, const writer_stats_printer_t& p)
{
  constexpr const char* dfmt = "{:.2f}";
  auto d_num_io = static_cast<double>(p.stats.io_depth_stats.num_io);
  out << "iops="
      << fmt::format(dfmt, d_num_io/p.seconds)
      << ",depth="
      << fmt::format(dfmt, p.stats.io_depth_stats.average())
      << ",batch="
      << fmt::format(dfmt, p.stats.record_batch_stats.average())
      << ",bwMiB="
      << fmt::format(dfmt, p.stats.get_total_bytes()/p.seconds/(1<<20))
      << ",sizeB="
      << fmt::format(dfmt, p.stats.get_total_bytes()/d_num_io)
      << "("
      << fmt::format(dfmt, p.stats.data_bytes/d_num_io)
      << ","
      << fmt::format(dfmt, p.stats.record_group_metadata_bytes/d_num_io)
      << ","
      << fmt::format(dfmt, p.stats.record_group_padding_bytes/d_num_io)
      << ")";
  return out;
}

std::ostream& operator<<(std::ostream& out, const cache_size_stats_t& p)
{
  constexpr const char* dfmt = "{:.2f}";
  out << "("
      << fmt::format(dfmt, p.get_mb())
      << "MiB,"
      << fmt::format(dfmt, p.get_avg_kb())
      << "KiB,"
      << p.num_extents
      << ")";
  return out;
}

std::ostream& operator<<(std::ostream& out, const cache_size_stats_printer_t& p)
{
  constexpr const char* dfmt = "{:.2f}";
  out << "("
      << fmt::format(dfmt, p.stats.get_mb()/p.seconds)
      << "MiB/s,"
      << fmt::format(dfmt, p.stats.get_avg_kb())
      << "KiB,"
      << fmt::format(dfmt, p.stats.num_extents/p.seconds)
      << "ps)";
  return out;
}

std::ostream& operator<<(std::ostream& out, const cache_io_stats_printer_t& p)
{
  out << "in"
      << cache_size_stats_printer_t{p.seconds, p.stats.in_sizes}
      << " out"
      << cache_size_stats_printer_t{p.seconds, p.stats.out_sizes};
  return out;
}

std::ostream& operator<<(std::ostream& out, const dirty_io_stats_printer_t& p)
{
  constexpr const char* dfmt = "{:.2f}";
  out << "in"
      << cache_size_stats_printer_t{p.seconds, p.stats.in_sizes}
      << " replaces="
      << fmt::format(dfmt, p.stats.num_replace/p.seconds)
      << "ps out"
      << cache_size_stats_printer_t{p.seconds, p.stats.out_sizes}
      << " outv="
      << fmt::format(dfmt, p.stats.get_avg_out_version());
  return out;
}

std::ostream& operator<<(std::ostream& out, const extent_access_stats_printer_t& p)
{
  constexpr const char* dfmt = "{:.2f}";
  double est_total_access = static_cast<double>(p.stats.get_estimated_total_access());
  out << "(~";
  if (est_total_access > 1000000) {
    out << fmt::format(dfmt, est_total_access/1000000)
        << "M, ";
  } else {
    out << fmt::format(dfmt, est_total_access/1000)
        << "K, ";
  }
  double trans_hit = static_cast<double>(p.stats.get_trans_hit());
  double cache_hit = static_cast<double>(p.stats.get_cache_hit());
  double est_cache_access = static_cast<double>(p.stats.get_estimated_cache_access());
  double load_absent = static_cast<double>(p.stats.load_absent);
  out << "trans-hit=~"
      << fmt::format(dfmt, trans_hit/est_total_access*100)
      << "%(p"
      << fmt::format(dfmt, p.stats.trans_pending/trans_hit)
      << ",d"
      << fmt::format(dfmt, p.stats.trans_dirty/trans_hit)
      << ",l"
      << fmt::format(dfmt, p.stats.trans_lru/trans_hit)
      << "), cache-hit=~"
      << fmt::format(dfmt, cache_hit/est_cache_access*100)
      << "%(d"
      << fmt::format(dfmt, p.stats.cache_dirty/cache_hit)
      << ",l"
      << fmt::format(dfmt, p.stats.cache_lru/cache_hit)
      <<"), load-present/absent="
      << fmt::format(dfmt, p.stats.load_present/load_absent)
      << ")";
  return out;
}

std::ostream& operator<<(std::ostream& out, const cache_access_stats_printer_t& p)
{
  constexpr const char* dfmt = "{:.2f}";
  double total_access = static_cast<double>(p.stats.get_total_access());
  out << "(";
  if (total_access > 1000000) {
    out << fmt::format(dfmt, total_access/1000000)
        << "M, ";
  } else {
    out << fmt::format(dfmt, total_access/1000)
        << "K, ";
  }
  double trans_hit = static_cast<double>(p.stats.s.get_trans_hit());
  double cache_hit = static_cast<double>(p.stats.s.get_cache_hit());
  double cache_access = static_cast<double>(p.stats.get_cache_access());
  double load_absent = static_cast<double>(p.stats.s.load_absent);
  out << "trans-hit="
      << fmt::format(dfmt, trans_hit/total_access*100)
      << "%(p"
      << fmt::format(dfmt, p.stats.s.trans_pending/trans_hit)
      << ",d"
      << fmt::format(dfmt, p.stats.s.trans_dirty/trans_hit)
      << ",l"
      << fmt::format(dfmt, p.stats.s.trans_lru/trans_hit)
      << "), cache-hit="
      << fmt::format(dfmt, cache_hit/cache_access*100)
      << "%(d"
      << fmt::format(dfmt, p.stats.s.cache_dirty/cache_hit)
      << ",l"
      << fmt::format(dfmt, p.stats.s.cache_lru/cache_hit)
      <<"), load/absent="
      << fmt::format(dfmt, load_absent/p.stats.cache_absent*100)
      << "%, load-present/absent="
      << fmt::format(dfmt, p.stats.s.load_present/load_absent)
      << ")";
  return out;
}

} // namespace crimson::os::seastore
