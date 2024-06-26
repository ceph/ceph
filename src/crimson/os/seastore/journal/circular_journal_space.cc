// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "circular_journal_space.h"

#include <fmt/format.h>
#include <fmt/os.h>

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/journal/circular_bounded_journal.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore::journal {

std::ostream &operator<<(std::ostream &out,
    const CircularJournalSpace::cbj_header_t &header)
{
  return out << "cbj_header_t(" 
	     << "dirty_tail=" << header.dirty_tail
	     << ", alloc_tail=" << header.alloc_tail
	     << ", magic=" << header.magic
             << ")";
}

CircularJournalSpace::CircularJournalSpace(RBMDevice * device) : device(device) {}
  
bool CircularJournalSpace::needs_roll(std::size_t length) const {
  if (length + get_rbm_addr(get_written_to()) > get_journal_end()) {
    return true;
  }
  return false;
}

extent_len_t CircularJournalSpace::get_block_size() const {
  return device->get_block_size();
}

CircularJournalSpace::roll_ertr::future<> CircularJournalSpace::roll() {
  paddr_t paddr = convert_abs_addr_to_paddr(
    get_records_start(),
    get_device_id());
  auto seq = get_written_to();
  seq.segment_seq++;
  assert(seq.segment_seq < MAX_SEG_SEQ);
  set_written_to(
    journal_seq_t{seq.segment_seq, paddr});
  return roll_ertr::now();
}

CircularJournalSpace::write_ret
CircularJournalSpace::write(ceph::bufferlist&& to_write) {
  LOG_PREFIX(CircularJournalSpace::write);
  assert(get_written_to().segment_seq != NULL_SEG_SEQ);
  auto encoded_size = to_write.length();
  if (encoded_size > get_records_available_size()) {
    ceph_abort("should be impossible with EPM reservation");
  }
  assert(encoded_size + get_rbm_addr(get_written_to())
	 < get_journal_end());

  journal_seq_t j_seq = get_written_to();
  auto target = get_rbm_addr(get_written_to());
  auto new_written_to = target + encoded_size;
  assert(new_written_to < get_journal_end());
  paddr_t paddr = convert_abs_addr_to_paddr(
    new_written_to,
    get_device_id());
  set_written_to(
    journal_seq_t{get_written_to().segment_seq, paddr});
  DEBUG("{}, target {}", to_write.length(), target);

  auto write_result = write_result_t{
    j_seq,
    encoded_size
  };
  return device_write_bl(target, to_write
  ).safe_then([this, target,
    length=encoded_size,
    write_result,
    FNAME] {
    DEBUG("commit target {} used_size {} written length {}",
          target, get_records_used_size(), length);
    return write_result;
  }).handle_error(
    base_ertr::pass_further{},
    crimson::ct_error::assert_all{ "Invalid error" }
  );
}

segment_nonce_t calc_new_nonce(
  uint32_t crc,
  unsigned char const *data,
  unsigned length)
{
  crc &= std::numeric_limits<uint32_t>::max() >> 1;
  return ceph_crc32c(crc, data, length);
}

CircularJournalSpace::open_ret CircularJournalSpace::open(bool is_mkfs) {
  std::ostringstream oss;
  oss << device_id_printer_t{get_device_id()};
  print_name = oss.str();

  if (is_mkfs) {
    LOG_PREFIX(CircularJournalSpace::open);
    assert(device);
    ceph::bufferlist bl;
    CircularJournalSpace::cbj_header_t head;
    assert(device->get_journal_size());
    head.dirty_tail =
      journal_seq_t{0,
	convert_abs_addr_to_paddr(
	  get_records_start(),
	  device->get_device_id())};
    head.alloc_tail = head.dirty_tail;
    auto meta = device->get_meta();
    head.magic = calc_new_nonce(
      std::rand() % std::numeric_limits<uint32_t>::max(),
      reinterpret_cast<const unsigned char *>(meta.seastore_id.bytes()),
      sizeof(meta.seastore_id.uuid));
    encode(head, bl);
    header = head;
    set_written_to(head.dirty_tail);
    initialized = true;
    DEBUG(
      "initialize header block in CircularJournalSpace length {}, head: {}",
      bl.length(), header);
    return write_header(
    ).safe_then([this]() {
      return open_ret(
	open_ertr::ready_future_marker{},
	get_written_to());
    }).handle_error(
      open_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error write_header"
      }
    );
  }
  ceph_assert(initialized);
  if (written_to.segment_seq == NULL_SEG_SEQ) {
    written_to.segment_seq = 0;
  }
  return open_ret(
    open_ertr::ready_future_marker{},
    get_written_to());
}

ceph::bufferlist CircularJournalSpace::encode_header()
{
  bufferlist bl;
  encode(header, bl);
  auto header_crc_filler = bl.append_hole(sizeof(checksum_t));
  auto bliter = bl.cbegin();
  auto header_crc = bliter.crc32c(
    ceph::encoded_sizeof_bounded<cbj_header_t>(),
    -1);
  ceph_le32 header_crc_le;
  header_crc_le = header_crc;
  header_crc_filler.copy_in(
    sizeof(checksum_t),
    reinterpret_cast<const char *>(&header_crc_le));
  return bl;
}

CircularJournalSpace::write_ertr::future<> CircularJournalSpace::device_write_bl(
    rbm_abs_addr offset, bufferlist &bl)
{
  LOG_PREFIX(CircularJournalSpace::device_write_bl);
  auto length = bl.length();
  if (offset + length > get_journal_end()) {
    return crimson::ct_error::erange::make();
  }
  DEBUG(
    "overwrite in CircularJournalSpace, offset {}, length {}",
    offset,
    length);
  return device->writev(offset, bl
  ).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all{ "Invalid error device->write" }
  );
}

CircularJournalSpace::read_header_ret
CircularJournalSpace::read_header()
{
  LOG_PREFIX(CircularJournalSpace::read_header);
  assert(device);
  auto bptr = bufferptr(ceph::buffer::create_page_aligned(
			device->get_block_size()));
  DEBUG("reading {}", device->get_shard_journal_start());
  return device->read(device->get_shard_journal_start(), bptr
  ).safe_then([bptr, FNAME]() mutable
    -> read_header_ret {
    bufferlist bl;
    bl.append(bptr);
    auto bp = bl.cbegin();
    cbj_header_t cbj_header;
    try {
      decode(cbj_header, bp);
    } catch (ceph::buffer::error &e) {
      ERROR("unable to read header block");
      return crimson::ct_error::enoent::make();
    }
    auto bliter = bl.cbegin();
    auto test_crc = bliter.crc32c(
      ceph::encoded_sizeof_bounded<cbj_header_t>(),
      -1);
    ceph_le32 recorded_crc_le;
    decode(recorded_crc_le, bliter);
    uint32_t recorded_crc = recorded_crc_le;
    if (test_crc != recorded_crc) {
      ERROR("error, header crc mismatch.");
      return read_header_ret(
	read_header_ertr::ready_future_marker{},
	std::nullopt);
    }
    return read_header_ret(
      read_header_ertr::ready_future_marker{},
      std::make_pair(cbj_header, bl)
    );
  });
}

CircularJournalSpace::write_ertr::future<>
CircularJournalSpace::write_header()
{
  LOG_PREFIX(CircularJournalSpace::write_header);
  ceph::bufferlist bl = encode_header();
  ceph_assert(bl.length() <= get_block_size());
  DEBUG(
    "sync header of CircularJournalSpace, length {}",
    bl.length());
  assert(device);
  auto iter = bl.begin();
  assert(bl.length() < get_block_size());
  bufferptr bp = bufferptr(ceph::buffer::create_page_aligned(get_block_size()));
  iter.copy(bl.length(), bp.c_str());
  return device->write(device->get_shard_journal_start(), std::move(bp)
  ).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all{ "Invalid error device->write" }
  );
}

}
