// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
#include "include/interval_set.h"
#include "include/buffer.h"
#include <list>
#include <map>
#include <set>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "Object.h"

void ContDesc::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(objnum, bl);
  encode(cursnap, bl);
  encode(seqnum, bl);
  encode(prefix, bl);
  encode(oid, bl);
  ENCODE_FINISH(bl);
}

void ContDesc::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(objnum, bl);
  decode(cursnap, bl);
  decode(seqnum, bl);
  decode(prefix, bl);
  decode(oid, bl);
  DECODE_FINISH(bl);
}

std::ostream &operator<<(std::ostream &out, const ContDesc &rhs)
{
  return out << "(ObjNum " << rhs.objnum
	     << " snap " << rhs.cursnap
	     << " seq_num " << rhs.seqnum
	     << ")";
}

void AppendGenerator::get_ranges_map(
  const ContDesc &cont, std::map<uint64_t, uint64_t> &out) {
  RandWrap rand(cont.seqnum);
  uint64_t pos = off;
  uint64_t limit = off + get_append_size(cont);
  while (pos < limit) {
    uint64_t segment_length = round_up(
      rand() % (max_append_size - min_append_size),
      alignment) + min_append_size;
    ceph_assert(segment_length >= min_append_size);
    if (segment_length + pos > limit) {
      segment_length = limit - pos;
    }
    if (alignment)
      ceph_assert(segment_length % alignment == 0);
    out.insert(std::pair<uint64_t, uint64_t>(pos, segment_length));
    pos += segment_length;
  }
}

void VarLenGenerator::get_ranges_map(
  const ContDesc &cont, std::map<uint64_t, uint64_t> &out) {
  RandWrap rand(cont.seqnum);
  uint64_t pos = 0;
  uint64_t limit = get_length(cont);
  bool include = false;
  while (pos < limit) {
    uint64_t segment_length = (rand() % (max_stride_size - min_stride_size)) + min_stride_size;
    ceph_assert(segment_length < max_stride_size);
    ceph_assert(segment_length >= min_stride_size);
    if (segment_length + pos > limit) {
      segment_length = limit - pos;
    }
    if (include) {
      out.insert(std::pair<uint64_t, uint64_t>(pos, segment_length));
      include = false;
    } else {
      include = true;
    }
    pos += segment_length;
  }
}

void ObjectDesc::iterator::adjust_stack() {
  while (!stack.empty() && pos >= stack.top().second.next) {
    ceph_assert(pos == stack.top().second.next);
    size = stack.top().second.size;
    current = stack.top().first;
    stack.pop();
  }

  if (stack.empty()) {
    cur_valid_till = std::numeric_limits<uint64_t>::max();
  } else {
    cur_valid_till = stack.top().second.next;
  }

  while (current != layers.end() && !current->covers(pos)) {
    uint64_t next = current->next(pos);
    if (next < cur_valid_till) {
      stack.emplace(current, StackState{next, size});
      cur_valid_till = next;
    }

    ++current;
  }

  if (current == layers.end()) {
    size = 0;
  } else {
    current->iter.seek(pos);
    size = std::min(size, current->get_size());
    cur_valid_till = std::min(
      current->valid_till(pos),
      cur_valid_till);
  }
}

const ContDesc &ObjectDesc::most_recent() {
  return layers.begin()->second;
}

void ObjectDesc::update(ContentsGenerator *gen, const ContDesc &next) {
  layers.push_front(std::pair<std::shared_ptr<ContentsGenerator>, ContDesc>(std::shared_ptr<ContentsGenerator>(gen), next));
  return;
}

bool ObjectDesc::check(bufferlist &to_check,
		       const std::pair<uint64_t, uint64_t>& offlen) {
  iterator objiter = begin();
  const auto [offset, size] = offlen;
  objiter.seek(offset);
  std::cout << "seeking to " << offset << std::endl;
  uint64_t error_at = 0;
  if (!objiter.check_bl_advance(to_check, &error_at)) {
    std::cout << "incorrect buffer at pos " << error_at << std::endl;
    return false;
  }

  if (to_check.length() < size) {
    std::cout << "only read " << to_check.length()
	      << " out of size " << size << std::endl;
    return false;
  }
  return true;
}

// Dump a hex+ASCII diff of expected vs actual bytes for a bufferlist region.
// Only mismatched rows are shown; runs of matching rows are collapsed to a
// single range summary line, e.g. "  [14078, 14222): 144 bytes match".
// Mismatched bytes within a row are flagged with '!' in the hex columns.
static void dump_extent_diff(std::ostream &out,
                             uint64_t obj_offset,
                             bufferlist &actual,
                             bufferlist &expected)
{
  static const int ROW = 16;
  uint64_t len = std::max(actual.length(), expected.length());

  // Header is printed lazily — only when the first mismatched row is about
  // to be emitted, so extents that somehow have no mismatches stay silent.
  bool header_printed = false;
  auto print_header = [&]() {
    if (!header_printed) {
      out << "  offset   | expected bytes (hex)                             "
          << "| actual bytes (hex)                              "
          << "| expected ASCII  | actual ASCII\n";
      out << "  ---------+-------------------------------------------------"
          << "+-------------------------------------------------"
          << "+-----------------+-----------------\n";
      header_printed = true;
    }
  };

  // Track runs of matching rows so they can be collapsed into one summary.
  bool in_match_run = false;
  uint64_t match_run_start = 0;  // absolute object offset of the run start
  uint64_t match_run_bytes = 0;  // number of bytes accumulated in the run

  auto flush_match_run = [&]() {
    if (in_match_run && match_run_bytes > 0) {
      out << "  [" << match_run_start << ", "
          << (match_run_start + match_run_bytes) << "): "
          << match_run_bytes << " bytes match\n";
    }
    in_match_run = false;
    match_run_bytes = 0;
  };

  for (uint64_t row_start = 0; row_start < len; row_start += ROW) {
    std::ostringstream exp_hex, act_hex, exp_asc, act_asc;
    bool row_has_mismatch = false;

    // How many valid bytes are in this row (may be < ROW on the last row).
    uint64_t row_bytes = std::min((uint64_t)ROW, len - row_start);

    for (int col = 0; col < ROW; ++col) {
      uint64_t i = row_start + col;
      uint8_t exp_byte = (i < expected.length()) ?
                           (uint8_t)expected[i] : 0;
      uint8_t act_byte = (i < actual.length()) ?
                           (uint8_t)actual[i] : 0;
      bool mismatch = (i < len) && (exp_byte != act_byte);
      if (mismatch)
        row_has_mismatch = true;

      // hex columns: mark mismatches with '!' prefix, else ' '
      if (i < len) {
        exp_hex << (mismatch ? '!' : ' ')
                << std::hex << std::setw(2) << std::setfill('0')
                << (unsigned)exp_byte << ' ';
        act_hex << (mismatch ? '!' : ' ')
                << std::hex << std::setw(2) << std::setfill('0')
                << (unsigned)act_byte << ' ';
      } else {
        exp_hex << "    ";
        act_hex << "    ";
      }

      // ASCII columns
      if (i < expected.length())
        exp_asc << (std::isprint(exp_byte) ? (char)exp_byte : '.');
      else
        exp_asc << ' ';
      if (i < actual.length())
        act_asc << (std::isprint(act_byte) ? (char)act_byte : '.');
      else
        act_asc << ' ';
    }

    if (!row_has_mismatch) {
      // Accumulate into the current match run.
      if (!in_match_run) {
        in_match_run = true;
        match_run_start = obj_offset + row_start;
        match_run_bytes = 0;
      }
      match_run_bytes += row_bytes;
    } else {
      // Flush any preceding match run, then print the mismatched row.
      flush_match_run();
      print_header();
      out << " >"
          << std::setw(8) << std::dec << (obj_offset + row_start)
          << " | " << std::setw(48) << std::left << exp_hex.str()
          << "| " << std::setw(48) << std::left << act_hex.str()
          << "| " << std::setw(16) << std::left << exp_asc.str()
          << " | " << act_asc.str() << "\n";
    }
  }

  // Flush any trailing match run.
  flush_match_run();
}

bool ObjectDesc::check_sparse(const std::map<uint64_t, uint64_t>& extents,
			      bufferlist &to_check,
			      const std::pair<uint64_t, uint64_t>& offlen)
{
  const auto [offset_to_skip, read_length] = offlen;
  uint64_t pos = offset_to_skip;
  uint64_t off = 0;
  auto objiter = begin();
  objiter.seek(pos);

  for (auto &&extiter : extents) {
    // verify hole
    {
      bufferlist bl;
      bl.append_zero(extiter.first - pos);
      uint64_t error_at = 0;
      if (!objiter.check_bl_advance(bl, &error_at)) {
	std::cout << "sparse read omitted non-zero data at "
		  << error_at << std::endl;
	return false;
      }
    }

    ceph_assert(off <= to_check.length());
    pos = extiter.first;
    objiter.seek(pos);

    {
      bufferlist bl;
      bl.substr_of(
	to_check,
	off,
	std::min(to_check.length() - off, extiter.second));
      uint64_t error_at = 0;

      if (!objiter.check_bl_advance(bl, &error_at)) {
        std::cout << "incorrect buffer at pos " << error_at
                  << " (object offset " << (pos + error_at) << ")"
                  << " in extent [" << extiter.first
                  << "+" << extiter.second << ")\n";

        // regenerate expected bytes for this extent by seeking a fresh iterator
        auto expected_iter = begin();
        expected_iter.seek(pos);
        bufferlist expected = expected_iter.gen_bl_advance(bl.length());

        std::cout << "  extent object_offset=" << extiter.first
                  << "  length=" << bl.length()
                  << "  first_mismatch_in_extent=" << error_at << "\n";
        dump_extent_diff(std::cout, pos, bl, expected);
        std::cout << std::flush;
        return false;
      }
      off += extiter.second;
      pos += extiter.second;
    }

    if (pos < extiter.first + extiter.second) {
      std::cout << "reached end of iterator first" << std::endl;
      return false;
    }
  }

  // final hole: validate from end of last returned extent to end of requested range
  bufferlist bl;
  uint64_t end = offset_to_skip + read_length;
  if (end > pos) {
    bl.append_zero(end - pos);
    uint64_t error_at;
    if (!objiter.check_bl_advance(bl, &error_at)) {
      std::cout << "sparse read omitted non-zero data at "
                << error_at << std::endl;
      return false;
    }
  }
  return true;
}

interval_set<uint64_t> ObjectDesc::get_min_written_extents(uint64_t alignment) const
{
  interval_set<uint64_t> written = get_written_extents(alignment);

  // alignment == 1 means replicated pool — no 4k-block concept, nothing to drop.
  if (alignment <= 1) {
    return written;
  }

  interval_set<uint64_t> min_written;

  for (auto it = written.begin(); it != written.end(); ++it) {
    const uint64_t ext_start = it.get_start();
    const uint64_t ext_end   = ext_start + it.get_len();

    // First aligned block boundary at or after ext_start.
    const uint64_t first_full_block =
      (ext_start + alignment - 1) / alignment * alignment;
    // Last aligned block boundary at or before ext_end.
    const uint64_t last_full_block = ext_end / alignment * alignment;

    // Scan [scan_start, scan_end) in the object model and return true
    // if every byte is '\0'.
    auto is_all_zero = [&](uint64_t scan_start, uint64_t scan_end) -> bool {
      ObjectDesc *mutable_this = const_cast<ObjectDesc *>(this);
      iterator objiter = mutable_this->begin();
      objiter.seek(scan_start);
      for (uint64_t i = 0; i < (scan_end - scan_start) && !objiter.end();
           ++i, ++objiter) {
        if (*objiter != '\0')
          return false;
      }
      return true;
    };

    // A head fragment that falls inside a prior zero-op hole is all-zero. 
    // The OSD may leave it as a hole rather than allocating an extent, 
    // so it must not be required in min.
    if (ext_start < first_full_block && first_full_block <= ext_end) {
      if (!is_all_zero(ext_start, first_full_block)) {
        min_written.insert(ext_start, first_full_block - ext_start);
      }
    } else if (first_full_block > ext_end) {
      // The whole extent is smaller than one alignment unit.
      if (!is_all_zero(ext_start, ext_end)) {
        min_written.insert(ext_start, ext_end - ext_start);
      }
      continue;
    }

    // Full aligned blocks [first_full_block, last_full_block).
    // Keep a block only if it contains at least one non-zero byte.
    for (uint64_t blk = first_full_block; blk < last_full_block; blk += alignment) {
      if (!is_all_zero(blk, blk + alignment)) {
        min_written.insert(blk, alignment);
      }
    }

    // Keep the tail's partial fragment only if it contains at least 
    // one non-zero byte, for the same reason as the head.
    if (last_full_block < ext_end) {
      if (!is_all_zero(last_full_block, ext_end)) {
        min_written.insert(last_full_block, ext_end - last_full_block);
      }
    }
  }

  return min_written;
}
