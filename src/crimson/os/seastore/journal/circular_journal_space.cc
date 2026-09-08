// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab expandtab

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
  if (length + get_rbm_addr(get_written_to()) >= get_journal_end()) {
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

CircularJournalSpace::write_ertr::future<>
CircularJournalSpace::write(ceph::bufferlist&& to_write) {
  LOG_PREFIX(CircularJournalSpace::write);
  assert(get_written_to().segment_seq != NULL_SEG_SEQ);
  auto encoded_size = to_write.length();
  if (encoded_size > get_records_available_size()) {
    ceph_abort_msg("should be impossible with EPM reservation");
  }

  auto target = get_rbm_addr(get_written_to());
  auto new_written_to = target + encoded_size;
  assert(new_written_to < get_journal_end());
  paddr_t paddr = convert_abs_addr_to_paddr(
    new_written_to,
    get_device_id());
  set_written_to(
    journal_seq_t{get_written_to().segment_seq, paddr});
  DEBUG("length {}, commit target {}, used_size {}",
        encoded_size, target, get_records_used_size());

  return device_write_bl(target, to_write
  ).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all( "Invalid error" )
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
    co_await write_header(
    ).handle_error(
      open_ertr::pass_further{},
      crimson::ct_error::assert_all(
        "Invalid error write_header"
      )
    );

    co_return get_written_to();
  }
  ceph_assert(initialized);
  if (written_to.segment_seq == NULL_SEG_SEQ) {
    written_to.segment_seq = 0;
  }
  co_return get_written_to();
}

ceph::bufferlist CircularJournalSpace::encode_header()
{
  bufferlist bl;
  encode(header, bl);
  if (!device->is_end_to_end_data_protection()) {
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
  }
  return bl;
}

CircularJournalSpace::submit_ertr::future<>
CircularJournalSpace::device_write_bl(
    rbm_abs_addr offset, bufferlist &bl)
{
  LOG_PREFIX(CircularJournalSpace::device_write_bl);
  auto length = bl.length();
  if (offset + length >= get_journal_end()) {
    co_return co_await submit_ertr::future<>(crimson::ct_error::erange::make());
  }
  DEBUG(
    "overwrite in CircularJournalSpace, offset {}, length {}",
    offset,
    length);
  co_await device->writev(offset, bl
  ).handle_error(
    submit_ertr::pass_further{},
    crimson::ct_error::assert_all( "Invalid error device->write" )
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
  co_await device->read(device->get_shard_journal_start(), bptr);
  
  bufferlist bl;
  bl.append(bptr);
  auto bp = bl.cbegin();
  cbj_header_t cbj_header;
  bool decode_failed = false;
  try {
    decode(cbj_header, bp);
  } catch (ceph::buffer::error &e) {
    ERROR("unable to read header block");
    decode_failed = true;
  }
  if (decode_failed) {
    co_return co_await read_header_ret(crimson::ct_error::enoent::make());
  }
  if (!device->is_end_to_end_data_protection()) {
    auto bliter = bl.cbegin();
    auto test_crc = bliter.crc32c(
      ceph::encoded_sizeof_bounded<cbj_header_t>(),
      -1);
    ceph_le32 recorded_crc_le;
    decode(recorded_crc_le, bliter);
    uint32_t recorded_crc = recorded_crc_le;
    if (test_crc != recorded_crc) {
      ERROR("error, header crc mismatch.");
      co_return std::nullopt;
    }
  }
  co_return std::make_pair(cbj_header, bl);
}

CircularJournalSpace::submit_ertr::future<>
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
  co_await device->write(device->get_shard_journal_start(), std::move(bp)
  ).handle_error(
    submit_ertr::pass_further{},
    crimson::ct_error::assert_all( "Invalid error device->write" )
  );
}

}
