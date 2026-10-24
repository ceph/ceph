// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// SPDX-License-Identifier: LGPL-2.1-or-later
#include "ECWriteJournal.h"

#include <algorithm>
#include <cerrno>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <utility>

#include "include/ceph_assert.h"
#include "include/intarith.h"

namespace ECWriteJournal {

static constexpr uint64_t record_header_key_bytes = 1 + 2 * sizeof(uint64_t);

std::string record_header_key(uint64_t slot_offset)
{
  return fmt::format("r{:016x}", slot_offset);
}

bool is_slot_object(const hobject_t& object)
{
  return object.is_internal_pg_local() &&
    object.oid.name.starts_with(slot_name_prefix);
}

void Record::encode_header(ceph::bufferlist& out, uint64_t slot_offset) const
{
  ceph::bufferlist header;
  ENCODE_START(3, 2, header);
  encode(slot_offset, header);
  encode(object, header);
  encode(version, header);
  encode(object_size, header);
  encode(offset, header);
  encode(uint32_t(data.length()), header);
  encode(mtime, header);
  encode(reqid, header);
  encode(user_version, header);
  ENCODE_FINISH(header);
  ceph::encode(detached_record_magic, out);
  ceph::encode(data.crc32c(header.crc32c(-1)), out);
  ceph::encode(header, out);
}

namespace {
struct HeaderFields {
  uint64_t slot_offset = 0;
  uint32_t length = 0;
  uint32_t checksum = 0;
  ceph::bufferlist header; // what the crc covers besides the payload
};

HeaderFields parse_header(const ceph::bufferlist& encoded, Record& record)
{
  HeaderFields parsed;
  auto cursor = encoded.cbegin();
  uint32_t magic = 0;
  ceph::decode(magic, cursor);
  if (magic != detached_record_magic) {
    throw ceph::buffer::malformed_input("EC journal header: bad magic");
  }
  ceph::decode(parsed.checksum, cursor);
  ceph::decode(parsed.header, cursor);
  if (!cursor.end()) {
    throw ceph::buffer::malformed_input("EC journal header: trailing bytes");
  }
  auto fields = parsed.header.cbegin();
  DECODE_START(3, fields);
  decode(parsed.slot_offset, fields);
  decode(record.object, fields);
  decode(record.version, fields);
  decode(record.object_size, fields);
  decode(record.offset, fields);
  decode(parsed.length, fields);
  decode(record.mtime, fields);
  if (struct_v >= 3) {
    decode(record.reqid, fields);
    decode(record.user_version, fields);
  } else {
    record.reqid = osd_reqid_t();
    record.user_version = 0;
  }
  DECODE_FINISH(fields);
  if (!parsed.length || parsed.length % block_size ||
      record.offset % block_size || record.offset > record.object_size ||
      parsed.length > record.object_size - record.offset ||
      parsed.slot_offset % record_alignment) {
    throw ceph::buffer::malformed_input("EC journal header: invalid payload range");
  }
  return parsed;
}
} // namespace

std::pair<uint64_t, uint32_t> Record::decode_fields(const ceph::bufferlist& header)
{
  const auto parsed = parse_header(header, *this);
  data.clear();
  return {parsed.slot_offset, parsed.length};
}

uint64_t Record::decode_header(const ceph::bufferlist& encoded,
                               const ceph::bufferlist& payload,
                               uint64_t payload_offset)
{
  const auto parsed = parse_header(encoded, *this);
  const uint64_t slot_offset = parsed.slot_offset;
  const uint64_t padded = p2roundup<uint64_t>(parsed.length, record_alignment);
  if (slot_offset < payload_offset ||
      slot_offset - payload_offset > payload.length() ||
      padded > payload.length() - (slot_offset - payload_offset)) {
    throw ceph::buffer::malformed_input("EC journal header: invalid payload range");
  }
  data.substr_of(payload, slot_offset - payload_offset, parsed.length);
  if (data.crc32c(parsed.header.crc32c(-1)) != parsed.checksum) {
    throw ceph::buffer::malformed_input("EC journal record: bad crc");
  }
  return slot_offset;
}

std::vector<SlotEntry> decode_slot_headers(uint64_t slot,
                                           std::optional<uint64_t> slot_bytes,
                                           const SlotHeaders& headers,
                                           pg_t pgid, unsigned split_bits)
{
  std::vector<SlotEntry> entries;
  uint64_t expected = 0;
  for (const auto& [key, header] : headers) {
    SlotEntry entry;
    const auto [offset, length] = entry.header.decode_fields(header);
    if (offset != expected || key != record_header_key(offset)) {
      throw ceph::buffer::malformed_input("EC journal slot: missing or misplaced header");
    }
    const uint64_t padded = p2roundup<uint64_t>(length, record_alignment);
    expected += padded;
    if (slot_bytes && expected > *slot_bytes) {
      throw ceph::buffer::malformed_input("EC journal slot: short payload");
    }
    if (pgid.contains(split_bits, entry.header.object)) {
      entry.locator = RecordLocator{slot, offset, padded, entry.header.offset,
                                    header};
      entries.push_back(std::move(entry));
    }
  }
  if (slot_bytes && expected != *slot_bytes) {
    throw ceph::buffer::malformed_input("EC journal slot: unindexed payload");
  }
  return entries;
}

std::vector<Record> decode_slot_records(const ceph::bufferlist& data,
                                        const SlotHeaders& headers,
                                        pg_t pgid, unsigned split_bits)
{
  std::vector<Record> records;
  for (const auto& entry : decode_slot_headers(0, data.length(), headers,
                                               pgid, split_bits)) {
    ceph::bufferlist payload;
    payload.substr_of(data, entry.locator.slot_offset, entry.locator.length);
    records.push_back(entry.locator.decode_record(payload));
  }
  return records;
}

Record RecordLocator::decode_record(const ceph::bufferlist& payload) const
{
  if (payload.length() != length) {
    throw ceph::buffer::malformed_input("EC journal fetch: short payload");
  }
  Record record;
  if (record.decode_header(header, payload, slot_offset) != slot_offset) {
    throw ceph::buffer::malformed_input("EC journal fetch: wrong slot offset");
  }
  if (record.offset != offset) {
    throw ceph::buffer::malformed_input("EC journal fetch: wrong object offset");
  }
  record.slot = slot;
  return record;
}

bool Materialized::covers(uint64_t stripe_offset, eversion_t version) const
{
  if (version <= base) {
    return true;
  }
  auto i = stripes.find(stripe_offset);
  return i != stripes.end() && version <= i->second;
}

void Materialized::merge(const Materialized& previous)
{
  base = std::max(base, previous.base);
  for (const auto& [offset, version] : previous.stripes) {
    auto& stripe = stripes[offset];
    stripe = std::max(stripe, version);
  }
  std::erase_if(stripes, [this](const auto& entry) { return entry.second <= base; });
}

eversion_t just_below(eversion_t v)
{
  ceph_assert(v.version > 0);
  return eversion_t(v.epoch, v.version - 1);
}

namespace {
// LEB128, and zigzag for the signed differences.
void put_varint(uint64_t v, ceph::bufferlist& out)
{
  while (v >= 0x80) {
    out.append(char(v | 0x80));
    v >>= 7;
  }
  out.append(char(v));
}

uint64_t get_varint(ceph::bufferlist::const_iterator& in)
{
  uint64_t v = 0;
  for (unsigned shift = 0; shift < 64; shift += 7) {
    uint8_t byte;
    in.copy(1, reinterpret_cast<char*>(&byte));
    v |= uint64_t(byte & 0x7f) << shift;
    if (!(byte & 0x80)) {
      return v;
    }
  }
  throw ceph::buffer::malformed_input("EC journal marker: varint too long");
}

uint64_t zigzag(int64_t v)
{
  return (uint64_t(v) << 1) ^ uint64_t(v >> 63);
}

int64_t unzigzag(uint64_t v)
{
  return int64_t(v >> 1) ^ -int64_t(v & 1);
}
} // namespace

void Materialized::encode(ceph::bufferlist& out) const
{
  if (stripes.empty()) {
    ENCODE_START(1, 1, out);
    encode(base, out);
    encode(stripes, out);
    ENCODE_FINISH(out);
    return;
  }
  ENCODE_START(2, 2, out);
  encode(base, out);
  uint64_t unit = 0;
  for (const auto& [offset, version] : stripes) {
    unit = std::gcd(unit, offset);
  }
  unit = std::max<uint64_t>(unit, 1);
  put_varint(unit, out);
  put_varint(stripes.size(), out);
  uint64_t previous = 0;
  for (const auto& [offset, version] : stripes) {
    put_varint((offset - previous) / unit, out);
    previous = offset;
    put_varint(zigzag(int64_t(version.epoch) - int64_t(base.epoch)), out);
    put_varint(zigzag(int64_t(version.version - base.version)), out);
  }
  ENCODE_FINISH(out);
}

void Materialized::decode(ceph::bufferlist::const_iterator& in)
{
  DECODE_START(2, in);
  decode(base, in);
  if (struct_v < 2) {
    decode(stripes, in);
  } else {
    stripes.clear();
    const uint64_t unit = get_varint(in);
    uint64_t offset = 0;
    for (uint64_t count = get_varint(in); count; --count) {
      offset += get_varint(in) * unit;
      eversion_t version;
      version.epoch = epoch_t(int64_t(base.epoch) + unzigzag(get_varint(in)));
      version.version = base.version + uint64_t(unzigzag(get_varint(in)));
      stripes.emplace_hint(stripes.end(), offset, version);
    }
  }
  DECODE_FINISH(in);
}

void ReplayFetches::add(const hobject_t& object, eversion_t version,
                        RecordLocator locator)
{
  entries.push_back({object, version, std::move(locator)});
  objects.insert(object);
}

bool ReplayFetches::complete(const hobject_t& object, eversion_t version)
{
  const auto pending = std::find_if(entries.begin(), entries.end(),
    [&](const Pending& entry) {
      return entry.object == object && entry.version == version;
    });
  if (pending == entries.end()) {
    return false;
  }
  entries.erase(pending);
  objects.erase(objects.find(object));
  if (entries.empty() && !unreadable) {
    failed = 0;
  }
  return true;
}

void ReplayFetches::fail(int result, bool all_objects)
{
  ceph_assert(result < 0);
  failed = result;
  unreadable |= all_objects;
}

void ReplayFetches::clear()
{
  entries.clear();
  objects.clear();
  failed = 0;
  unreadable = false;
}

std::vector<std::pair<uint64_t, uint64_t>> stripe_fragments(
  uint64_t offset, uint64_t length, uint64_t stripe_width)
{
  std::vector<std::pair<uint64_t, uint64_t>> pieces;
  while (length) {
    const uint64_t piece = std::min(length,
                                    stripe_width - offset % stripe_width);
    pieces.emplace_back(offset, piece);
    offset += piece;
    length -= piece;
  }
  return pieces;
}

std::map<uint64_t, ceph::bufferlist> absorb_runs(
  const std::vector<Stripe>& stripes, const interval_set<uint64_t>& written,
  uint64_t* dropped)
{
  std::map<uint64_t, ceph::bufferlist> runs;
  auto last = runs.end();
  uint64_t run_end = 0;
  for (const auto& stripe : stripes) {
    bool added = false;
    // A stripe the write covers up to its last byte adds nothing.
    if (!written.contains(stripe.offset, stripe.valid_bytes)) {
      for (const auto& [off, data] : stripe.blocks) {
        const uint64_t at = stripe.offset + off;
        interval_set<uint64_t> keep, covered;
        keep.insert(at, data.length());
        covered.intersection_of(keep, written);
        keep.subtract(covered);
        for (auto i = keep.begin(); i != keep.end(); ++i) {
          ceph::bufferlist piece;
          piece.substr_of(data, i.get_start() - at, i.get_len());
          if (last != runs.end() && run_end == i.get_start()) {
            last->second.claim_append(piece);
          } else {
            last = runs.emplace(i.get_start(), std::move(piece)).first;
          }
          run_end = i.get_start() + i.get_len();
          added = true;
        }
      }
    }
    if (!added && dropped) {
      ++*dropped;
    }
  }
  return runs;
}

bool Overlay::covers(uint64_t offset, uint64_t length) const
{
  if (!length) {
    return false;
  }
  uint64_t at = offset / block_size * block_size;
  for (auto i = blocks.find(at); at < offset + length; at += block_size, ++i) {
    if (i == blocks.end() || i->first != at) {
      return false;
    }
  }
  return true;
}

void Overlay::apply(uint64_t offset, uint64_t length,
                    ceph::bufferlist& data) const
{
  const uint64_t end = offset + length;
  ceph::bufferlist out;
  // Adjacent pieces of one buffer become one again: a record of several
  // blocks is that many slices of the same buffer.
  const auto take = [&out](const ceph::bufferlist& from, uint64_t off,
                           uint64_t len) {
    ceph::bufferlist piece;
    piece.substr_of(from, off, len);
    for (const auto& ptr : piece.buffers()) {
      out.append(ptr, 0, ptr.length());
    }
  };
  uint64_t pos = offset;
  for (auto i = blocks.lower_bound(offset / block_size * block_size);
       i != blocks.end() && i->first < end; ++i) {
    const uint64_t from = std::max(i->first, offset);
    const uint64_t to = std::min(i->first + block_size, end);
    if (pos < from) {
      take(data, pos - offset, from - pos);
    }
    take(i->second, from - i->first, to - from);
    pos = to;
  }
  if (pos == offset) {
    return; // no block in the range
  }
  if (pos < end) {
    take(data, pos - offset, end - pos);
  }
  data = std::move(out);
}

bool Limits::valid() const
{
  return stripe_width && stripe_width % 4096 == 0 &&
    segment_bytes >= record_alignment && segment_bytes <= max_bytes &&
    max_segments;
}

Journal::Journal(ghobject_t prefix, Limits limits)
  : prefix(std::move(prefix)), limits(limits)
{
  if (!limits.valid()) {
    throw std::invalid_argument("invalid EC journal geometry or capacity");
  }
  for (uint64_t slot = 0; slot < limits.max_segments; ++slot) {
    free_slots.insert(slot);
  }
}

ghobject_t Journal::slot_object(uint64_t slot, shard_id_t shard) const
{
  auto object = prefix;
  object.hobj.oid.name += "." + std::to_string(slot);
  object.shard_id = shard;
  return object;
}

ghobject_t Journal::segment_object(uint64_t segment) const
{
  for (const auto& s : segments) {
    if (s.id == segment) {
      return slot_object(s.slot, prefix.shard_id);
    }
  }
  throw std::out_of_range("EC journal segment is not resident");
}

Journal::Segment* Journal::find_segment(uint64_t id)
{
  for (auto& segment : segments) {
    if (segment.id == id) {
      return &segment;
    }
  }
  return nullptr;
}

Journal::StripeKey Journal::key_of(const hobject_t& object, uint64_t offset) const
{
  return {object, offset / limits.stripe_width * limits.stripe_width};
}

void Journal::release(Entry& entry, Block& block)
{
  auto segment = find_segment(block.segment);
  ceph_assert(segment && segment->live_blocks);
  --segment->live_blocks;
  --live;
  if (!block.durable) {
    --entry.undurable;
  }
}

void Journal::reindex(const StripeKey& key, Entry& entry)
{
  if (auto i = by_age.find(entry.oldest); i != by_age.end() && i->second == key) {
    by_age.erase(i);
  }
  ready.erase(key);
  if (entry.blocks.empty()) {
    return; // the caller drops the entry
  }
  auto oldest = entry.blocks.begin();
  entry.oldest_version.reset();
  for (auto b = entry.blocks.begin(); b != entry.blocks.end(); ++b) {
    if (b->second.sequence < oldest->second.sequence) {
      oldest = b;
    }
    // A snapshot in flight holds the blocks up to entry.taken.
    if ((!entry.flushing || b->second.sequence > entry.taken) &&
        (!entry.oldest_version || b->second.version < *entry.oldest_version)) {
      entry.oldest_version = b->second.version;
    }
  }
  entry.oldest = oldest->second.sequence;
  if (entry.flushing) {
    return;
  }
  by_age.emplace(entry.oldest, key);
  if (entry.undurable == 0 && entry.full()) {
    ready.insert(key);
  }
}

void Journal::emit(const Placement& placement, const coll_t& coll,
                   shard_id_t shard, ceph::os::Transaction& t) const
{
  const auto object = slot_object(placement.slot, shard);
  if (placement.reset) {
    t.remove(coll, object); // absent is fine (BlueStore ignores -ENOENT)
  }
  t.write(coll, object, placement.offset, placement.bytes.length(),
          placement.bytes);
  t.omap_setkeys(coll, object, placement.header_updates);
}

int Journal::copy_slots_for_split(spg_t child, const ReadSlot& read,
                                  ceph::os::Transaction& transaction) const
{
  ceph::os::Transaction copies;
  for (uint64_t slot = 0; slot < limits.max_segments; ++slot) {
    // The child's slot may already exist: a merge leaves the source PG's
    // slots behind (unread, under their old hash) and the split hands them
    // back to the child. Its stale tail and headers must not survive.
    auto destination = slot_object(slot, child.shard);
    destination.hobj.set_hash(child.pgid.ps());
    copies.remove(coll_t(child), destination); // absent is fine
    const auto source = slot_object(slot, prefix.shard_id);
    ceph::bufferlist data;
    SlotHeaders headers;
    const int result = read(source, data, headers);
    if (result == -ENOENT) {
      continue;
    }
    if (result < 0) {
      return result;
    }
    copies.write(coll_t(child), destination, 0, data.length(), data);
    copies.omap_setkeys(coll_t(child), destination, headers);
  }
  transaction.append(copies);
  return 0;
}

uint64_t Journal::record_bytes(const hobject_t& object, uint64_t length) const
{
  // The header's encoded length depends on the object only.
  Record header{object, eversion_t(), 0, 0, {}};
  ceph::bufferlist encoded;
  header.encode_header(encoded, 0);
  return p2roundup<uint64_t>(length, record_alignment) + encoded.length() +
    record_header_key_bytes;
}

int Journal::room_for(uint64_t bytes, bool* rotate) const
{
  if (failed) {
    return failed;
  }
  if (bytes > limits.segment_bytes) {
    return -E2BIG;
  }
  const bool next = segments.empty() || segments.back().state != State::open ||
    bytes > limits.segment_bytes - segments.back().bytes;
  // A dead segment that keeps its slot only for requests in the window
  // gives both up when the room is needed (append: unpin_oldest).
  const auto pinned = reclaimable();
  if (bytes > limits.max_bytes - (used_bytes - pinned.bytes)) {
    return -EAGAIN;
  }
  if (next && (segments.size() - pinned.count == limits.max_segments ||
               free_slots.size() + pinned.count == 0)) {
    return -EAGAIN;
  }
  if (rotate) {
    *rotate = next;
  }
  return 0;
}

Journal::Reclaimable Journal::reclaimable() const
{
  Reclaimable r;
  for (const auto& segment : segments) {
    if (segment.state == State::closed && segment.live_blocks == 0 &&
        segment.durable_records == segment.entries.size() &&
        segment.window_ids > 0) {
      r.bytes += segment.bytes;
      ++r.count;
    }
  }
  return r;
}

uint64_t Journal::pinned_segments() const
{
  return reclaimable().count;
}

bool Journal::unpin_oldest()
{
  for (auto& segment : segments) {
    if (segment.state == State::closed && segment.live_blocks == 0 &&
        segment.durable_records == segment.entries.size() &&
        segment.window_ids > 0) {
      // Its requests are forgotten as if they had left the window.
      for (const auto& [sequence, pending] : segment.entries) {
        drop_request(pending.reqid, sequence);
      }
      ceph_assert(segment.window_ids == 0);
      collect();
      return true;
    }
  }
  return false;
}

int Journal::check_append(const hobject_t& object, uint64_t length) const
{
  if (failed) {
    return failed;
  }
  if (!length || length % block_size || length > limits.stripe_width) {
    return -EINVAL;
  }
  return room_for(record_bytes(object, length));
}

int Journal::check_append(const hobject_t& object,
                          const std::vector<uint64_t>& lengths) const
{
  if (failed) {
    return failed;
  }
  // As append would place them, one after the other: a record that does
  // not fit the open segment opens the next one, which takes a free slot.
  // (append can only find more room than this: sealing a segment and
  // superseding blocks may release dead segments on the way.)
  bool open = !segments.empty() && segments.back().state == State::open;
  uint64_t open_bytes = open ? segments.back().bytes : 0;
  const auto pinned = reclaimable();
  uint64_t used = used_bytes - pinned.bytes;
  uint64_t count = segments.size() - pinned.count;
  uint64_t slots = free_slots.size() + pinned.count;
  for (const uint64_t length : lengths) {
    if (!length || length % block_size || length > limits.stripe_width) {
      return -EINVAL;
    }
    const uint64_t bytes = record_bytes(object, length);
    if (bytes > limits.segment_bytes) {
      return -E2BIG;
    }
    const bool next = !open || bytes > limits.segment_bytes - open_bytes;
    if (bytes > limits.max_bytes - used) {
      return -EAGAIN;
    }
    if (next) {
      if (count == limits.max_segments || slots == 0) {
        return -EAGAIN;
      }
      ++count;
      --slots;
      open = true;
      open_bytes = 0;
    }
    open_bytes += bytes;
    used += bytes;
  }
  return 0;
}

int Journal::append(const hobject_t& object, eversion_t version,
                    uint64_t object_size, uint64_t offset,
                    const ceph::bufferlist& data, Placement* placement,
                    Ticket* ticket, utime_t mtime,
                    const osd_reqid_t& reqid, version_t user_version)
{
  if (failed) {
    return failed;
  }
  const uint64_t len = data.length();
  if (!ticket || !placement || !len || len % block_size || offset % block_size ||
      offset > object_size || len > object_size - offset ||
      len > limits.stripe_width - offset % limits.stripe_width) {
    return -EINVAL;
  }
  if (next_sequence == std::numeric_limits<uint64_t>::max() ||
      next_segment == std::numeric_limits<uint64_t>::max()) {
    return -EOVERFLOW;
  }

  const uint64_t padded = p2roundup<uint64_t>(len, record_alignment);
  const uint64_t bytes = record_bytes(object, len);
  bool rotate = false;
  if (const int r = room_for(bytes, &rotate); r < 0) {
    return r;
  }
  if (rotate) {
    seal();
    if (free_slots.empty()) {
      ceph_assert(unpin_oldest()); // room_for counted it
    }
    const uint64_t slot = *free_slots.begin();
    free_slots.erase(free_slots.begin());
    segments.push_back(Segment{next_segment++, slot});
  }
  auto& segment = segments.back();
  const uint64_t sequence = next_sequence++;
  const Record record{object, version, object_size, offset, data, mtime,
                      no_slot, reqid, user_version};
  ceph::bufferlist header;
  record.encode_header(header, segment.payload_bytes);
  ceph_assert(padded + header.length() + record_header_key_bytes == bytes);
  ceph::bufferlist payload = data;
  payload.append_zero(padded - len);
  *placement = Placement{segment.slot, segment.payload_bytes,
                         segment.payload_bytes == 0, sequence,
                         std::move(payload), std::move(header), {}};
  const SlotHeaders headers{{record_header_key(placement->offset), placement->header}};
  ceph::encode(headers, placement->header_updates);
  segment.entries.emplace(sequence, Pending{object, offset, uint32_t(len),
                                            false, reqid});
  segment.bytes += bytes;
  segment.payload_bytes += padded;
  used_bytes += bytes;
  used_payload_bytes += padded;
  ++used_records;

  const auto key = key_of(object, offset);
  auto [i, inserted] = index.try_emplace(key);
  auto& entry = i->second;
  if (inserted) {
    entry.object_size = object_size;
    entry.valid_bytes = std::min(limits.stripe_width, object_size - key.second);
  } else {
    // Admission drains the journal before any size change reaches the
    // object, so every live record of a stripe sees the same size.
    ceph_assert(entry.object_size == object_size);
  }
  for (uint64_t off = 0; off < len; off += block_size) {
    ceph::bufferlist block;
    block.substr_of(data, off, block_size);
    auto [b, fresh] = entry.blocks.try_emplace(offset - key.second + off);
    if (!fresh) {
      release(entry, b->second); // latest append wins, the older is dead
      ++superseded;
    }
    b->second = Block{std::move(block), version, sequence, segment.id,
                      false, mtime};
    ++entry.undurable;
    ++live;
    ++segment.live_blocks;
  }
  add_request(entry, key, reqid, user_version, sequence, segment.id, false);
  note_meta(object, user_version, mtime);
  reindex(key, entry);
  *ticket = Ticket{segment.id, sequence};
  collect(); // a superseded block can leave an older segment dead
  return 0;
}

int Journal::committed(Ticket ticket, int result)
{
  auto segment = find_segment(ticket.segment);
  if (!segment) {
    return -ENOENT;
  }
  auto i = segment->entries.find(ticket.sequence);
  if (i == segment->entries.end()) {
    return -ENOENT;
  }
  if (result < 0) {
    if (!failed) {
      failed = result;
    }
    return result;
  }
  auto& pending = i->second;
  if (pending.durable) {
    return -EALREADY;
  }
  pending.durable = true;
  ++segment->durable_records;
  if (auto r = requests.find(pending.reqid); r != requests.end()) {
    r->second.durable = true;
  }
  const auto key = key_of(pending.object, pending.offset);
  if (auto e = index.find(key); e != index.end()) {
    auto& entry = e->second;
    for (uint64_t off = 0; off < pending.length; off += block_size) {
      auto b = entry.blocks.find(pending.offset - key.second + off);
      if (b != entry.blocks.end() && b->second.sequence == ticket.sequence) {
        b->second.durable = true;
        --entry.undurable;
      }
    }
    reindex(key, entry);
  }
  collect();
  return failed;
}

void Journal::seal()
{
  if (!segments.empty() && segments.back().state == State::open) {
    segments.back().state = State::closed;
    collect();
  }
}

void Journal::collect()
{
  for (auto i = segments.begin(); i != segments.end();) {
    if (i->state == State::closed && i->live_blocks == 0 &&
        i->durable_records == i->entries.size() && i->window_ids == 0) {
      used_bytes -= i->bytes;
      used_payload_bytes -= i->payload_bytes;
      used_records -= i->entries.size();
      // A slot replay still has records to place in stays out of use.
      if (!holds.contains(i->slot)) {
        free_slots.insert(i->slot);
      }
      i = segments.erase(i);
    } else {
      ++i;
    }
  }
}

bool Journal::adopting() const
{
  return std::any_of(segments.begin(), segments.end(),
    [](const Segment& s) { return s.adopted; });
}

uint64_t Journal::adopted_segments() const
{
  return std::count_if(segments.begin(), segments.end(),
    [](const Segment& s) { return s.adopted; });
}

bool Journal::adopted_segment(uint64_t id) const
{
  return std::any_of(segments.begin(), segments.end(),
    [id](const Segment& s) { return s.id == id && s.adopted; });
}

bool Journal::adopted_stripe(const Entry& entry) const
{
  return std::any_of(entry.blocks.begin(), entry.blocks.end(),
    [this](const auto& block) { return adopted_segment(block.second.segment); });
}

void Journal::hold_slot(uint64_t slot)
{
  if (slot >= limits.max_segments) {
    return;
  }
  ++holds[slot];
  free_slots.erase(slot);
}

void Journal::release_hold(uint64_t slot)
{
  const auto i = holds.find(slot);
  if (i == holds.end()) {
    return;
  }
  if (--i->second == 0) {
    holds.erase(i);
    if (std::none_of(segments.begin(), segments.end(),
          [slot](const Segment& s) { return s.slot == slot; })) {
      free_slots.insert(slot);
    }
  }
}

int Journal::adopt(const Record& record)
{
  if (failed) {
    return failed;
  }
  const uint64_t len = record.data.length();
  const uint64_t offset = record.offset;
  if (!len || len % block_size || offset % block_size ||
      offset > record.object_size || len > record.object_size - offset ||
      len > limits.stripe_width - offset % limits.stripe_width ||
      record.slot >= limits.max_segments) {
    return -EINVAL;
  }
  if (next_sequence == std::numeric_limits<uint64_t>::max() ||
      next_segment == std::numeric_limits<uint64_t>::max()) {
    return -EOVERFLOW;
  }
  // The record stays in its slot: that slot's adopted segment, created ahead
  // of every segment of this interval (reclaim and a drain take the front
  // first; a record adopted late, after a fetch or a recovery, gets a later
  // sequence, which begin_flush allows for). A slot a segment of this
  // interval already uses was never held, which replay prevents; the record
  // then joins that segment and flushes from memory.
  auto i = std::find_if(segments.begin(), segments.end(),
    [&record](const Segment& s) { return s.slot == record.slot; });
  if (i == segments.end()) {
    free_slots.erase(record.slot);
    const auto before = std::find_if(segments.begin(), segments.end(),
      [](const Segment& s) { return !s.adopted; });
    i = segments.emplace(before, next_segment++, record.slot);
    i->state = State::closed;
    i->adopted = true;
  }
  release_hold(record.slot); // the segment keeps the slot from here on
  auto& segment = *i;
  const uint64_t sequence = next_sequence++;
  const uint64_t padded = p2roundup<uint64_t>(len, record_alignment);
  const uint64_t bytes = record_bytes(record.object, len);
  const auto key = key_of(record.object, offset);
  auto [e, inserted] = index.try_emplace(key);
  auto& entry = e->second;
  if (inserted) {
    entry.object_size = record.object_size;
    entry.valid_bytes = std::min(limits.stripe_width,
                                 record.object_size - key.second);
  } else {
    // A size change is never journaled and bumps the object's base, which
    // kills every earlier record: live records of a stripe agree on size.
    ceph_assert(entry.object_size == record.object_size);
  }
  segment.entries.emplace(sequence, Pending{record.object, offset,
                                            uint32_t(len), true,
                                            record.reqid});
  ++segment.durable_records;
  segment.bytes += bytes;
  segment.payload_bytes += padded;
  used_bytes += bytes;
  used_payload_bytes += padded;
  ++used_records;
  for (uint64_t off = 0; off < len; off += block_size) {
    auto [b, fresh] = entry.blocks.try_emplace(offset - key.second + off);
    if (!fresh) {
      if (b->second.version > record.version) {
        continue; // this block has a newer record
      }
      release(entry, b->second);
      ++superseded;
    }
    ceph::bufferlist block;
    block.substr_of(record.data, off, block_size);
    b->second = Block{std::move(block), record.version, sequence, segment.id,
                      true, record.mtime};
    ++live;
    ++segment.live_blocks;
  }
  if (entry.blocks.empty()) {
    index.erase(e);
  } else {
    add_request(entry, key, record.reqid, record.user_version, sequence,
                segment.id, true);
    note_meta(record.object, record.user_version, record.mtime);
    reindex(key, entry);
  }
  collect();
  return 0;
}

void Journal::note_meta(const hobject_t& object, version_t user_version,
                        utime_t mtime)
{
  // The write with the highest user version is the object's last write
  // (records without one, header version 2, fall back to arrival order).
  auto& meta = metas[object];
  if (user_version >= meta.user_version) {
    meta.user_version = user_version;
    meta.mtime = mtime;
  }
  newest_user_version_ = std::max(newest_user_version_, user_version);
}

std::optional<Journal::ObjectMeta> Journal::object_meta(
  const hobject_t& object) const
{
  const auto i = metas.find(object);
  if (i == metas.end()) {
    return std::nullopt;
  }
  return i->second;
}

void Journal::add_request(Entry& entry, const StripeKey& key,
                          const osd_reqid_t& reqid, version_t user_version,
                          uint64_t sequence, uint64_t segment, bool durable)
{
  if (reqid == osd_reqid_t() || limits.request_window == 0) {
    return; // a record without one (header version 2, or a test)
  }
  while (window.size() >= limits.request_window) {
    forget_oldest_request();
  }
  entry.reqids.push_back(Reqid{reqid, user_version, sequence, segment});
  window.push_back(Remembered{reqid, sequence, key});
  // A write across a stripe boundary is one request in two records (they
  // commit together), and a resend older than the window runs again as a
  // new record: the newest record's entry stands for the request.
  if (auto r = requests.find(reqid); r != requests.end()) {
    if (auto s = find_segment(r->second.segment)) {
      --s->window_ids;
    }
    r->second = Request{user_version, durable, segment, sequence};
  } else {
    requests.emplace(reqid, Request{user_version, durable, segment, sequence});
  }
  find_segment(segment)->window_ids++;
}

void Journal::forget_oldest_request()
{
  const Remembered old = window.front();
  window.pop_front();
  // The stripe's list is a subsequence of the window in the same order.
  if (auto e = index.find(old.stripe); e != index.end() &&
      !e->second.reqids.empty() &&
      e->second.reqids.front().sequence == old.sequence) {
    e->second.reqids.pop_front();
  }
  drop_request(old.reqid, old.sequence);
}

void Journal::drop_request(const osd_reqid_t& reqid, uint64_t sequence)
{
  auto r = requests.find(reqid);
  if (r == requests.end() || r->second.sequence != sequence) {
    return; // logged already, or a newer record of the same request
  }
  if (auto s = find_segment(r->second.segment)) {
    ceph_assert(s->window_ids > 0);
    --s->window_ids;
  }
  requests.erase(r);
}

std::optional<Journal::Request> Journal::find_request(
  const osd_reqid_t& reqid) const
{
  const auto i = requests.find(reqid);
  if (i == requests.end()) {
    return std::nullopt;
  }
  return i->second;
}

// By value: the caller's key is a reference into ready/by_age, which
// reindex() erases below.
Stripe Journal::take(StripeKey key)
{
  auto& entry = index.at(key);
  entry.flushing = true;
  entry.taken = 0;
  for (const auto& [off, block] : entry.blocks) {
    entry.taken = std::max(entry.taken, block.sequence);
  }
  reindex(key, entry); // leaves the resident structures
  Stripe stripe;
  stripe.object = key.first;
  stripe.object_size = entry.object_size;
  stripe.offset = key.second;
  stripe.width = limits.stripe_width;
  stripe.valid_bytes = entry.valid_bytes;
  for (const auto& [off, block] : entry.blocks) {
    stripe.blocks.emplace(off, block.data);
    // Appends come in version order, adopted records need not: the flush
    // covers the newest version among its blocks, whatever its sequence.
    stripe.newest_sequence = std::max(stripe.newest_sequence, block.sequence);
    stripe.newest_version = std::max(stripe.newest_version, block.version);
  }
  for (const auto& r : entry.reqids) {
    if (r.sequence <= entry.taken) {
      stripe.reqids.emplace_back(r.reqid, r.user_version);
    }
  }
  return stripe;
}

std::optional<Stripe> Journal::begin_flush(
  bool every_partial,
  const Skip& skip, bool* deferred, std::optional<uint64_t> partial_upto)
{
  if (deferred) {
    *deferred = false;
  }
  if (failed) {
    return std::nullopt;
  }
  for (const auto& key : ready) {
    if ((skip && skip(key)) || readers.contains(key)) {
      if (deferred) {
        *deferred = true;
      }
      continue;
    }
    return take(key);
  }
  // Adopted stripes (replay) flush regardless of pressure. One
  // adopted after a fetch or a recovery has a younger sequence than fresh
  // appends, so while any is resident the whole order is scanned.
  const bool replaying = adopting();
  if (!every_partial && !partial_upto && !replaying) {
    return std::nullopt;
  }
  for (const auto& [sequence, key] : by_age) {
    auto& entry = index.at(key);
    // by_age is in admission order, so the reclaim bound cuts it at one
    // point: a stripe past it is younger than everything after it. Strictly
    // oldest first, too, so a stripe still waiting for an append commit
    // holds the newer ones back (milliseconds).
    if (!must_flush(entry, every_partial, partial_upto, replaying) ||
        entry.undurable) {
      if (replaying) {
        continue;
      }
      break;
    }
    if ((skip && skip(key)) || readers.contains(key)) {
      if (deferred) {
        *deferred = true;
      }
      continue;
    }
    return take(key);
  }
  return std::nullopt;
}

std::pair<std::map<Journal::StripeKey, Journal::Entry>::const_iterator,
          std::map<Journal::StripeKey, Journal::Entry>::const_iterator>
Journal::stripes_of(const hobject_t& object, uint64_t offset,
                    uint64_t length) const
{
  const uint64_t first = offset / limits.stripe_width * limits.stripe_width;
  const uint64_t end = length > whole_object - offset ? whole_object
                                                      : offset + length;
  auto begin = index.lower_bound(StripeKey{object, first});
  auto stop = begin;
  while (stop != index.end() && stop->first.first == object &&
         stop->first.second < end) {
    ++stop;
  }
  return {begin, stop};
}

bool Journal::must_flush(const Entry& entry, bool every_partial,
                         std::optional<uint64_t> partial_upto,
                         bool adopting) const
{
  return every_partial || (partial_upto && entry.oldest <= *partial_upto) ||
    (adopting && adopted_stripe(entry));
}

bool Journal::must_flush(const StripeKey& key, bool every_partial,
                         std::optional<uint64_t> partial_upto) const
{
  const auto i = index.find(key);
  return i != index.end() &&
    must_flush(i->second, every_partial, partial_upto, adopting());
}

std::optional<Stripe> Journal::begin_flush_object(const hobject_t& object,
  const Skip& skip, bool* deferred, uint64_t offset, uint64_t length,
  bool adopted_only)
{
  if (deferred) {
    *deferred = false;
  }
  if (failed || (adopted_only && !adopting())) {
    return std::nullopt;
  }
  const auto [begin, end] = stripes_of(object, offset, length);
  for (auto i = begin; i != end; ++i) {
    const auto& entry = i->second;
    if (entry.flushing || entry.undurable) {
      continue; // wait for the flush or the append commit
    }
    if (adopted_only && !adopted_stripe(entry)) {
      continue;
    }
    if ((skip && skip(i->first)) || readers.contains(i->first)) {
      if (deferred) {
        *deferred = true;
      }
      continue;
    }
    return take(i->first);
  }
  return std::nullopt;
}

int Journal::take_range(const hobject_t& object, uint64_t offset,
                        uint64_t length, std::vector<Stripe>* taken)
{
  if (failed) {
    return failed;
  }
  if (!taken) {
    return -EINVAL;
  }
  const auto state = write_state(object, offset, length);
  if (state == ReadState::in_flight || state == ReadState::flushing) {
    return -EBUSY;
  }
  // take() moves the stripes out of the resident structures, not the index.
  const auto [begin, end] = stripes_of(object, offset, length);
  for (auto i = begin; i != end; ++i) {
    taken->push_back(take(i->first));
  }
  return 0;
}

int Journal::finish_flush(const Stripe& flushed, int result)
{
  auto i = index.find(StripeKey{flushed.object, flushed.offset});
  if (i == index.end()) {
    return -ENOENT;
  }
  auto& entry = i->second;
  if (!entry.flushing) {
    return -EINVAL;
  }
  entry.flushing = false;
  if (result < 0) {
    reindex(i->first, entry);
    return result;
  }
  for (auto b = entry.blocks.begin(); b != entry.blocks.end();) {
    if (b->second.sequence <= flushed.newest_sequence) {
      release(entry, b->second);
      b = entry.blocks.erase(b);
    } else {
      ++b;
    }
  }
  // The write that materialized the snapshot logged its requests
  // (Stripe::reqids): the PG log answers for them from here on.
  while (!entry.reqids.empty() &&
         entry.reqids.front().sequence <= flushed.newest_sequence) {
    drop_request(entry.reqids.front().reqid, entry.reqids.front().sequence);
    entry.reqids.pop_front();
  }
  reindex(i->first, entry);
  if (entry.blocks.empty()) {
    index.erase(i);
    // The write that materialized the object's last records persisted its
    // metadata (the overlay was in the object info it wrote).
    if (!dirty(flushed.object)) {
      metas.erase(flushed.object);
    }
  }
  collect();
  return 0;
}

void Journal::on_reset()
{
  index.clear();
  by_age.clear();
  ready.clear();
  window.clear();
  segments.clear();
  used_bytes = 0;
  used_payload_bytes = 0;
  used_records = 0;
  live = 0;
  failed = 0;
  holds.clear();
  requests.clear();
  metas.clear();
  newest_user_version_ = 0;
  // The reads in flight are dropped with the interval and never end.
  readers.clear();
  free_slots.clear();
  for (uint64_t slot = 0; slot < limits.max_segments; ++slot) {
    free_slots.insert(slot);
  }
}

bool Journal::resident(uint64_t segment) const
{
  return std::any_of(segments.begin(), segments.end(),
    [segment](const Segment& s) { return s.id == segment; });
}

bool Journal::has_open_segment() const
{
  return !segments.empty() && segments.back().state == State::open;
}

bool Journal::flush_ready() const
{
  return !failed && !ready.empty();
}

bool Journal::dirty(const hobject_t& object, uint64_t offset,
                    uint64_t length) const
{
  const auto [begin, end] = stripes_of(object, offset, length);
  return begin != end;
}

std::optional<eversion_t> Journal::oldest_version(const hobject_t& object,
  std::optional<uint64_t> skip_stripe) const
{
  // One entry per resident or flushing stripe, never empty (an entry is
  // dropped with its last block), each keeping its own lowest version.
  std::optional<eversion_t> oldest;
  const auto [begin, end] = stripes_of(object, 0, whole_object);
  for (auto i = begin; i != end; ++i) {
    if (skip_stripe && i->first.second == *skip_stripe) {
      continue;
    }
    if (i->second.oldest_version &&
        (!oldest || *i->second.oldest_version < *oldest)) {
      oldest = i->second.oldest_version;
    }
  }
  return oldest;
}

std::optional<eversion_t> Journal::oldest_version_outside(
  const hobject_t& object, uint64_t offset, uint64_t length) const
{
  const uint64_t first = offset / limits.stripe_width * limits.stripe_width;
  const uint64_t stop = length > whole_object - offset ? whole_object
                                                       : offset + length;
  std::optional<eversion_t> oldest;
  const auto [begin, end] = stripes_of(object, 0, whole_object);
  for (auto i = begin; i != end; ++i) {
    const uint64_t stripe = i->first.second;
    if (length && stripe >= first && stripe < stop) {
      continue;
    }
    if (i->second.oldest_version &&
        (!oldest || *i->second.oldest_version < *oldest)) {
      oldest = i->second.oldest_version;
    }
  }
  return oldest;
}

bool Journal::object_ready(const hobject_t& object, uint64_t offset,
                           uint64_t length) const
{
  if (failed) {
    return false;
  }
  const auto [begin, end] = stripes_of(object, offset, length);
  for (auto i = begin; i != end; ++i) {
    if (!i->second.flushing && i->second.undurable == 0) {
      return true;
    }
  }
  return false;
}

ReadState Journal::read_state(const hobject_t& object, uint64_t offset,
                              uint64_t length) const
{
  ReadState state = ReadState::clean;
  if (!length) {
    return state;
  }
  const bool replayed = adopting();
  const auto [begin, end] = stripes_of(object, offset, length);
  for (auto i = begin; i != end; ++i) {
    const auto& entry = i->second;
    state = std::max(state,
      replayed && adopted_stripe(entry) ? ReadState::adopted :
      entry.flushing ? ReadState::flushing :
      entry.undurable ? ReadState::in_flight : ReadState::ready);
  }
  return state;
}

ReadState Journal::write_state(const hobject_t& object, uint64_t offset,
                               uint64_t length) const
{
  ReadState state = ReadState::clean;
  if (!length) {
    return state;
  }
  const auto [begin, end] = stripes_of(object, offset, length);
  for (auto i = begin; i != end; ++i) {
    const auto& entry = i->second;
    state = std::max(state,
      entry.flushing ? ReadState::flushing :
      entry.undurable ? ReadState::in_flight : ReadState::ready);
  }
  return state;
}

void Journal::read_overlay(const hobject_t& object, uint64_t offset,
                           uint64_t length, Overlay* overlay) const
{
  if (!length) {
    return;
  }
  const uint64_t first = offset / block_size * block_size;
  const auto [begin, end] = stripes_of(object, offset, length);
  for (auto i = begin; i != end; ++i) {
    const uint64_t stripe = i->first.second;
    const auto& blocks = i->second.blocks;
    for (auto b = blocks.lower_bound(first > stripe ? first - stripe : 0);
         b != blocks.end() && stripe + b->first < offset + length; ++b) {
      overlay->blocks.try_emplace(stripe + b->first, b->second.data);
    }
  }
}

void Journal::read_begin(const hobject_t& object, uint64_t offset,
                         uint64_t length)
{
  if (!length) {
    return;
  }
  for (uint64_t stripe = offset / limits.stripe_width * limits.stripe_width;
       stripe < offset + length; stripe += limits.stripe_width) {
    ++readers[StripeKey{object, stripe}];
  }
}

void Journal::read_end(const hobject_t& object, uint64_t offset,
                       uint64_t length)
{
  if (!length) {
    return;
  }
  for (uint64_t stripe = offset / limits.stripe_width * limits.stripe_width;
       stripe < offset + length; stripe += limits.stripe_width) {
    const auto i = readers.find(StripeKey{object, stripe});
    ceph_assert(i != readers.end());
    if (--i->second == 0) {
      readers.erase(i);
    }
  }
}

bool Journal::partial_ready() const
{
  return !failed && !by_age.empty() &&
    index.at(by_age.begin()->second).undurable == 0;
}

bool Journal::adopted_ready() const
{
  if (failed || !adopting()) {
    return false;
  }
  return std::any_of(by_age.begin(), by_age.end(), [this](const auto& aged) {
    const auto& entry = index.at(aged.second);
    return entry.undurable == 0 && adopted_stripe(entry);
  });
}

bool Journal::pressure() const
{
  const auto pinned = reclaimable();
  return !segments.empty() &&
    (segments.size() - pinned.count >= limits.max_segments ||
     used_bytes - pinned.bytes + limits.segment_bytes > limits.max_bytes);
}

std::optional<uint64_t> Journal::reclaim_bound() const
{
  if (failed) {
    return std::nullopt;
  }
  // A closed segment without live records needs no flush to go, only a
  // pending append commit. Count its space as returned, or every pass would
  // reach past the segment it frees.
  uint64_t bytes = used_bytes;
  uint64_t count = segments.size();
  const Segment* pinned = nullptr;
  for (const auto& segment : segments) {
    if (segment.state != State::open && segment.live_blocks == 0) {
      bytes -= segment.bytes;
      --count;
    } else if (!pinned && segment.live_blocks) {
      pinned = &segment;
    }
  }
  const bool short_of_room = count >= limits.max_segments ||
    bytes + limits.segment_bytes > limits.max_bytes;
  if (!short_of_room || !pinned || pinned->entries.empty()) {
    return std::nullopt;
  }
  // Nothing older is live, so a stripe pins this segment exactly when its
  // oldest live record is in it.
  return pinned->entries.rbegin()->first;
}

bool Journal::reclaim_ready() const
{
  if (failed || by_age.empty()) {
    return false;
  }
  const auto bound = reclaim_bound();
  const auto& [oldest, key] = *by_age.begin();
  return bound && oldest <= *bound && index.at(key).undurable == 0;
}

} // namespace ECWriteJournal
