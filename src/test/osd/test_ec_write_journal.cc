// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// SPDX-License-Identifier: LGPL-2.1-or-later
#include <algorithm>
#include <cerrno>
#include <deque>
#include <limits>
#include <numeric>
#include <random>
#include <set>
#include <stdexcept>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "osd/ECWriteJournal.h"

using namespace ECWriteJournal;
using Transaction = ceph::os::Transaction;

namespace {
ceph::bufferlist bytes(size_t n, char c)
{
  ceph::bufferlist bl;
  bl.append(std::string(n, c));
  return bl;
}

// RBD-style names are longer than the small-string buffer: a dangling
// reference to a destroyed key would read freed heap memory, not a copy.
hobject_t object(const std::string& name = "data")
{
  return hobject_t(object_t("rbd_data.7f1cb2a3d4e5.000000000000" + name), "",
                   CEPH_NOSNAP, 0, 1, "");
}

ghobject_t prefix()
{
  return ghobject_t(object("ec-journal-test-run"), ghobject_t::NO_GEN,
                   shard_id_t(0));
}

// One holder's copy of an append, as the backend writes it for each holder:
// place the record, then emit it into t for prefix()'s shard.
int append_copy(Journal& journal, const hobject_t& object, eversion_t version,
                uint64_t object_size, uint64_t offset,
                const ceph::bufferlist& data, Transaction& t, Ticket* ticket,
                utime_t mtime = {})
{
  Placement placement;
  const int r = journal.append(object, version, object_size, offset, data,
                               &placement, ticket, mtime);
  if (r == 0) {
    journal.emit(placement, coll_t(), prefix().shard_id, t);
  }
  return r;
}

// Logical reference model of a stripe, kept out of libosd: the OSD assembles
// through ECJournalFlush (see test_ec_journal_flush.cc). Object ranges that
// still need base data (not rounded device reads).
std::vector<std::pair<uint64_t, uint64_t>> holes(const Stripe& stripe)
{
  std::vector<std::pair<uint64_t, uint64_t>> result;
  uint64_t pos = 0;
  for (const auto& [off, data] : stripe.blocks) {
    if (pos < off) {
      result.emplace_back(stripe.offset + pos, off - pos);
    }
    pos = off + block_size;
  }
  if (pos < stripe.valid_bytes) {
    result.emplace_back(stripe.offset + pos, stripe.valid_bytes - pos);
  }
  return result;
}

// A complete stripe needs no base. Otherwise base must contain valid_bytes
// from the pre-existing stripe. Only the known range beyond EOF is zeroed.
ceph::bufferlist assemble(const Stripe& stripe,
                          const ceph::bufferlist* base = nullptr)
{
  if (!stripe.full() && (!base || base->length() != stripe.valid_bytes)) {
    throw std::invalid_argument("incomplete EC journal stripe requires base data");
  }
  ceph::bufferlist out;
  for (uint64_t off = 0; off < stripe.valid_bytes; off += block_size) {
    auto i = stripe.blocks.find(off);
    if (i != stripe.blocks.end()) {
      out.append(i->second);
    } else {
      ceph::bufferlist original;
      original.substr_of(*base, off, std::min(block_size, stripe.valid_bytes - off));
      out.append(original);
    }
  }
  out.append_zero(stripe.width - stripe.valid_bytes);
  return out;
}

class JournalTest : public testing::TestWithParam<unsigned> {
 protected:
  Limits limits() const {
    Limits limits;
    limits.stripe_width = GetParam() * 4096;
    return limits;
  }
  Journal journal() const { return Journal(prefix(), limits()); }
  uint64_t width() const { return limits().stripe_width; }
  // Encoded size of one one-block record, to size segments in records.
  uint64_t record_bytes() const {
    auto j = journal();
    Transaction t;
    Ticket ticket;
    EXPECT_EQ(0, append_copy(j, object(), eversion_t(1, 1), width(), 0,
                             bytes(block_size, 'x'), t, &ticket));
    return j.bytes();
  }
  Ticket append(Journal& journal, uint64_t off, uint64_t len, char c = 'x',
                const hobject_t& oid = object(), bool durable = true) {
    Transaction t;
    Ticket ticket;
    EXPECT_EQ(0, append_copy(journal, oid, eversion_t(1, next_version++),
                             16 * width(), off, bytes(len, c), t, &ticket));
    seen.insert(ticket.segment);
    if (durable) {
      // Simulated ObjectStore commit. These unit tests do NOT test fsync.
      EXPECT_EQ(0, journal.committed(ticket, 0));
    }
    return ticket;
  }
  // An append that carries its client request (as the backend's do): the
  // request id names the client (entity 1) and the tid.
  static osd_reqid_t reqid(ceph_tid_t tid, int64_t client = 1) {
    return osd_reqid_t(entity_name_t::CLIENT(client), 0, tid);
  }
  Ticket append_request(Journal& journal, uint64_t off, uint64_t len,
                        ceph_tid_t tid, version_t user_version = 7,
                        bool durable = true, const hobject_t& oid = object()) {
    Transaction t;
    Placement placement;
    Ticket ticket;
    EXPECT_EQ(0, journal.append(oid, eversion_t(1, next_version++), 16 * width(),
                                off, bytes(len, 'q'), &placement, &ticket, {},
                                reqid(tid), user_version));
    journal.emit(placement, coll_t(), prefix().shard_id, t);
    seen.insert(ticket.segment);
    if (durable) {
      EXPECT_EQ(0, journal.committed(ticket, 0));
    }
    return ticket;
  }
  // Flush any durable stripe, oldest first (the drain policy).
  static constexpr bool any() { return true; }
  // Materialize one stripe: begin, simulated durable shard writes, finish.
  std::optional<Stripe> flush(Journal& journal, bool every_partial = any()) {
    auto stripe = journal.begin_flush(every_partial);
    if (stripe) {
      EXPECT_EQ(0, journal.finish_flush(*stripe, 0));
    }
    return stripe;
  }
  // The oldest segment appended to since the last call that has since been
  // released (dead and durable: its budget and slot are free). A released
  // segment needs no transaction of its own: the next segment placed in its
  // slot removes the old contents.
  std::optional<uint64_t> trim(Journal& journal) {
    for (auto i = seen.begin(); i != seen.end(); ++i) {
      if (!journal.resident(*i)) {
        const auto segment = *i;
        seen.erase(i);
        return segment;
      }
    }
    return std::nullopt;
  }
  // Flush everything, as a drain does: every segment is released.
  void drain(Journal& journal) {
    journal.seal();
    while (flush(journal)) {
    }
    while (trim(journal)) {
    }
    EXPECT_FALSE(journal.dirty());
    EXPECT_TRUE(journal.empty());
  }
  std::set<uint64_t> seen; // segments appended to, for trim()
 private:
  uint64_t next_version = 1;
};

TEST_P(JournalTest, AppendVersionsFollowTheSequence)
{
  // A record's journal version is (interval epoch, the sequence its append
  // takes): the caller reads next_append_sequence first, so versions and
  // sequences order alike. version_now covers every record appended so far,
  // and the snapshot carries the newest version and client mtime of its
  // blocks for the flush.
  auto j = journal();
  EXPECT_EQ(eversion_t(7, 0), j.version_now(7));
  for (unsigned n = 1; n <= 3; ++n) {
    const eversion_t version(7, j.next_append_sequence());
    EXPECT_EQ(n, version.version);
    Transaction t;
    Ticket ticket;
    ASSERT_EQ(0, append_copy(j, object(), version, 16 * width(),
                             (n - 1) * block_size, bytes(block_size, 'v'), t,
                             &ticket, utime_t(100 + n, 0)));
    EXPECT_EQ(version.version, ticket.sequence);
    EXPECT_EQ(version, j.version_now(7));
    EXPECT_EQ(0, j.committed(ticket, 0));
  }
  j.seal();
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(eversion_t(7, 3), stripe->newest_version);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  // Records adopted from an earlier interval keep their versions, below
  // anything this interval appends or marks.
  auto replayed = journal();
  const Record old{object(), eversion_t(5, 40), 16 * width(), 0,
                   bytes(block_size, 'a'), utime_t(50, 0), 0};
  ASSERT_EQ(0, replayed.adopt(old));
  EXPECT_LT(old.version, replayed.version_now(8));
  auto adopted = replayed.begin_flush(any());
  ASSERT_TRUE(adopted);
  EXPECT_EQ(eversion_t(5, 40), adopted->newest_version);
}

TEST_P(JournalTest, EmptySealAndDrain)
{
  auto j = journal();
  j.seal();
  EXPECT_FALSE(j.begin_flush(any()));
  EXPECT_TRUE(j.empty());
  EXPECT_FALSE(j.dirty());
  EXPECT_FALSE(j.pressure());
  EXPECT_EQ(0u, j.bytes());
}

TEST(JournalReplay, MissingRecordsKeepObjectsBlocked)
{
  ReplayFetches fetches;
  const auto first = object("first");
  const auto second = object("second");
  fetches.add(first, eversion_t(1, 1), RecordLocator{0, 0, 4096, 0});
  fetches.add(first, eversion_t(1, 2), RecordLocator{0, 4096, 4096, block_size});
  fetches.add(second, eversion_t(1, 3), RecordLocator{1, 0, 4096, 0});
  fetches.fail(-EIO);
  EXPECT_EQ(-EIO, fetches.error());
  EXPECT_FALSE(fetches.empty());
  EXPECT_EQ(3u, fetches.pending().size());
  EXPECT_TRUE(fetches.blocked(first));
  EXPECT_TRUE(fetches.blocked(second));
  EXPECT_FALSE(fetches.blocked(object("unrelated")));
  EXPECT_FALSE(fetches.complete(first, eversion_t(1, 3)));
  EXPECT_EQ(3u, fetches.pending().size());
  EXPECT_TRUE(fetches.complete(first, eversion_t(1, 1)));
  EXPECT_TRUE(fetches.blocked(first));
  EXPECT_TRUE(fetches.complete(first, eversion_t(1, 2)));
  EXPECT_FALSE(fetches.blocked(first));
  EXPECT_TRUE(fetches.blocked(second));
  EXPECT_EQ(-EIO, fetches.error());
  EXPECT_TRUE(fetches.complete(second, eversion_t(1, 3)));
  EXPECT_TRUE(fetches.empty());
  EXPECT_EQ(0, fetches.error());
}

TEST(JournalReplay, UnreadableSlotBlocksEveryObject)
{
  ReplayFetches fetches;
  fetches.add(object(), eversion_t(1, 1), {});
  fetches.fail(-EIO, true);
  EXPECT_TRUE(fetches.blocked(object("other")));
  EXPECT_TRUE(fetches.complete(object(), eversion_t(1, 1)));
  EXPECT_TRUE(fetches.empty());
  EXPECT_EQ(-EIO, fetches.error());
  EXPECT_TRUE(fetches.blocked(object()));
  fetches.clear();
  EXPECT_EQ(0, fetches.error());
  EXPECT_FALSE(fetches.blocked(object()));
}

TEST(JournalReplay, IntervalChangeResetsFailedFetches)
{
  ReplayFetches fetches;
  fetches.add(object(), eversion_t(1, 1), {});
  fetches.fail(-EIO);
  fetches.clear();
  EXPECT_TRUE(fetches.empty());
  EXPECT_EQ(0, fetches.error());
  EXPECT_FALSE(fetches.blocked(object()));
  fetches.add(object(), eversion_t(2, 2), {});
  EXPECT_FALSE(fetches.complete(object(), eversion_t(1, 1)));
  EXPECT_TRUE(fetches.blocked(object()));
}

TEST_P(JournalTest, ReplayDoesNotFlushAnObjectWithMissingRecords)
{
  auto journal = this->journal();
  const auto pending_object = object("pending");
  const auto other_object = object("other");
  ASSERT_EQ(0, journal.adopt(Record{pending_object, eversion_t(1, 2),
    16 * width(), block_size, bytes(block_size, 'b'), {}, 0}));
  ASSERT_EQ(0, journal.adopt(Record{other_object, eversion_t(1, 3),
    16 * width(), 0, bytes(block_size, 'c'), {}, 1}));
  ReplayFetches fetches;
  fetches.add(pending_object, eversion_t(1, 1), {});
  const auto blocked = [&](const Journal::StripeKey& stripe) {
    return fetches.blocked(stripe.first);
  };
  auto stripe = journal.begin_flush(any(), blocked);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(other_object, stripe->object);
  ASSERT_EQ(0, journal.finish_flush(*stripe, 0));
  fetches.fail(-EIO);
  EXPECT_FALSE(journal.begin_flush(any(), blocked));
  EXPECT_TRUE(journal.dirty(pending_object));
  EXPECT_FALSE(fetches.empty());
  Record record{pending_object, eversion_t(1, 1), 16 * width(),
                0, bytes(block_size, 'a'), {}, 0};
  ASSERT_EQ(0, journal.adopt(record));
  ASSERT_TRUE(fetches.complete(record.object, record.version));
  stripe = journal.begin_flush(any(), blocked);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(pending_object, stripe->object);
  EXPECT_EQ("a", stripe->blocks.at(0).to_str().substr(0, 1));
  EXPECT_EQ("b", stripe->blocks.at(block_size).to_str().substr(0, 1));
}

TEST_P(JournalTest, SplitCopiesSlotsWithoutChangingRecordLocations)
{
  const shard_id_t shard(GetParam() + 2);
  auto slot_prefix = prefix();
  slot_prefix.shard_id = shard;
  slot_prefix.hobj.nspace = std::string(hobject_t::INTERNAL_PG_LOCAL_NS);
  const spg_t parent(pg_t(0, 1), shard);
  const spg_t child(pg_t(2, 1), shard);
  Journal journal(slot_prefix, limits());
  auto child_object = object("child");
  child_object.set_hash(2);
  Placement first, second;
  Ticket ticket;
  ASSERT_EQ(0, journal.append(object(), eversion_t(1, 1), width(), 0,
    bytes(block_size, 'a'), &first, &ticket));
  ASSERT_EQ(0, journal.append(child_object, eversion_t(1, 2), width(),
    block_size, bytes(2 * block_size, 'b'), &second, &ticket));
  ASSERT_EQ(first.slot, second.slot);
  ceph::bufferlist slot_data = first.bytes;
  slot_data.append(second.bytes);
  SlotHeaders headers;
  for (const auto& placement : {first, second}) {
    SlotHeaders updates;
    auto encoded = placement.header_updates.cbegin();
    ceph::decode(updates, encoded);
    headers.merge(updates);
  }
  Transaction transaction;
  size_t reads = 0;
  ASSERT_EQ(0, journal.copy_slots_for_split(child,
    [&](const ghobject_t& source, ceph::bufferlist& data,
      SlotHeaders& copied_headers) {
      ++reads;
      if (source != journal.slot_object(first.slot, shard)) {
        return -ENOENT;
      }
      data = slot_data;
      copied_headers = headers;
      return 0;
    }, transaction));
  EXPECT_EQ(limits().max_segments, reads);
  auto cursor = transaction.begin();
  // Every child slot is removed first, present in the parent or not: a merge
  // leaves the source PG's slots behind, and a split hands them back.
  for (uint64_t slot = 0; slot < first.slot; ++slot) {
    ASSERT_TRUE(cursor.have_op());
    const auto remove = cursor.decode_op();
    ASSERT_EQ(Transaction::OP_REMOVE, remove->op);
    auto removed = journal.slot_object(slot, shard);
    removed.hobj.set_hash(child.pgid.ps());
    EXPECT_EQ(removed, cursor.get_oid(remove->oid));
  }
  auto expected = journal.slot_object(first.slot, shard);
  expected.hobj.set_hash(child.pgid.ps());
  ASSERT_TRUE(cursor.have_op());
  const auto remove = cursor.decode_op();
  ASSERT_EQ(Transaction::OP_REMOVE, remove->op);
  EXPECT_EQ(coll_t(child), cursor.get_cid(remove->cid));
  EXPECT_EQ(expected, cursor.get_oid(remove->oid));
  ASSERT_TRUE(cursor.have_op());
  const auto write = cursor.decode_op();
  ASSERT_EQ(Transaction::OP_WRITE, write->op);
  EXPECT_EQ(coll_t(child), cursor.get_cid(write->cid));
  EXPECT_EQ(expected, cursor.get_oid(write->oid));
  EXPECT_EQ(0u, write->off);
  ceph::bufferlist copied;
  cursor.decode_bl(copied);
  EXPECT_TRUE(copied.contents_equal(slot_data));
  ASSERT_TRUE(cursor.have_op());
  const auto metadata = cursor.decode_op();
  ASSERT_EQ(Transaction::OP_OMAP_SETKEYS, metadata->op);
  SlotHeaders copied_headers;
  cursor.decode_attrset(copied_headers);
  EXPECT_EQ(headers, copied_headers);
  const auto child_records = decode_slot_records(copied, copied_headers, child.pgid,
    child.pgid.get_split_bits(4));
  ASSERT_EQ(1u, child_records.size());
  EXPECT_EQ(child_object, child_records.front().object);
  const auto parent_records = decode_slot_records(slot_data, headers, parent.pgid,
    parent.pgid.get_split_bits(4));
  ASSERT_EQ(1u, parent_records.size());
  EXPECT_EQ(object(), parent_records.front().object);
  for (const auto& placement : {first, second}) {
    Record record;
    EXPECT_EQ(placement.offset, record.decode_header(placement.header, copied));
    EXPECT_EQ(eversion_t(1, placement.sequence), record.version);
    EXPECT_EQ(placement.sequence == first.sequence ? object() : child_object,
              record.object);
    EXPECT_EQ(placement.sequence == second.sequence,
              child.pgid.contains(child.pgid.get_split_bits(4), record.object));
  }
  for (uint64_t slot = first.slot + 1; slot < limits().max_segments; ++slot) {
    ASSERT_TRUE(cursor.have_op());
    const auto later = cursor.decode_op();
    ASSERT_EQ(Transaction::OP_REMOVE, later->op);
    auto removed = journal.slot_object(slot, shard);
    removed.hobj.set_hash(child.pgid.ps());
    EXPECT_EQ(removed, cursor.get_oid(later->oid));
  }
  EXPECT_FALSE(cursor.have_op());
}

TEST_P(JournalTest, SplitReplayFiltersForeignVersionsAndRejectsPartialTails)
{
  auto child_object = object("child");
  child_object.set_hash(2);
  const eversion_t version(1, 1);
  Record parent_record{object(), version, width(), 0, bytes(block_size, 'a')};
  Record child_record{child_object, version, width(), 0, bytes(block_size, 'b')};
  ceph::bufferlist slot;
  SlotHeaders headers;
  for (const auto& record : {parent_record, child_record}) {
    record.encode_header(headers[record_header_key(slot.length())], slot.length());
    slot.append(record.data);
    slot.append_zero(record_alignment - record.data.length());
  }
  const pg_t child(2, 1);
  const auto records = decode_slot_records(slot, headers, child, child.get_split_bits(4));
  ASSERT_EQ(1u, records.size());
  EXPECT_EQ(child_object, records.front().object);
  EXPECT_EQ(version, records.front().version);
  EXPECT_TRUE(records.front().data.contents_equal(child_record.data));
  for (const auto& tail : {bytes(record_alignment, '\0'), bytes(100, 'x')}) {
    auto data = slot;
    data.append(tail);
    EXPECT_THROW(decode_slot_records(data, headers, child, child.get_split_bits(4)),
                 ceph::buffer::error);
  }
}

TEST_P(JournalTest, SplitReadErrorDoesNotEmitPartialCopies)
{
  auto journal = this->journal();
  Transaction transaction;
  size_t reads = 0;
  EXPECT_EQ(-EIO, journal.copy_slots_for_split(spg_t(pg_t(2, 1), shard_id_t(0)),
    [&](const ghobject_t&, ceph::bufferlist& data, SlotHeaders&) {
      if (++reads == 1) {
        data = bytes(record_alignment, 'a');
        return 0;
      }
      return -EIO;
    }, transaction));
  EXPECT_EQ(2u, reads);
  EXPECT_TRUE(transaction.empty());
}

TEST_P(JournalTest, ExactPayloadInAppendOnlyObjectStoreRecords)
{
  auto j = journal();
  Transaction t;
  Ticket first, second;
  // A one-block record and a two-block one.
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 1), width(), 0,
                           bytes(block_size, 'a'), t, &first));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 2), width(), 2 * block_size,
                           bytes(2 * block_size, 'b'), t, &second));
  EXPECT_EQ(first.segment, second.segment);
  auto it = t.begin();
  uint64_t end = 0;
  uint64_t metadata_bytes = 0;
  // The segment's first record clears its slot, whatever an older segment
  // (or an older run) left there.
  ASSERT_TRUE(it.have_op());
  auto remove = it.decode_op();
  EXPECT_EQ(Transaction::OP_REMOVE, remove->op);
  EXPECT_EQ(j.segment_object(first.segment), it.get_oid(remove->oid));
  for (auto [ticket, off, len, value] : {
         std::tuple{first, uint64_t(0), block_size, 'a'},
         std::tuple{second, 2 * block_size, 2 * block_size, 'b'}}) {
    ASSERT_TRUE(it.have_op());
    auto op = it.decode_op();
    EXPECT_EQ(Transaction::OP_WRITE, op->op);
    EXPECT_EQ(j.segment_object(ticket.segment), it.get_oid(op->oid));
    EXPECT_EQ(end, op->off);
    // Whole-block appends into never-written space: no BlueStore head read
    // or deferred rewrite of a block shared with the previous record.
    EXPECT_EQ(0u, op->off % record_alignment);
    EXPECT_EQ(0u, op->len % record_alignment);
    end += op->len;
    ceph::bufferlist payload;
    it.decode_bl(payload);
    // Whole blocks are already a multiple of record_alignment: no padding.
    EXPECT_EQ(len, op->len);
    EXPECT_EQ(op->len, payload.length());
    ASSERT_TRUE(it.have_op());
    const auto metadata = it.decode_op();
    ASSERT_EQ(Transaction::OP_OMAP_SETKEYS, metadata->op);
    EXPECT_EQ(it.get_oid(op->oid), it.get_oid(metadata->oid));
    SlotHeaders headers;
    it.decode_attrset(headers);
    EXPECT_EQ(1u, headers.size()); // the record's own header, nothing else
    for (const auto& [key, header] : headers) {
      metadata_bytes += key.length() + header.length();
    }
    Record record;
    EXPECT_EQ(op->off, record.decode_header(headers.at(record_header_key(op->off)),
                                            payload, op->off));
    EXPECT_EQ(object(), record.object);
    EXPECT_EQ(eversion_t(1, ticket.sequence), record.version);
    EXPECT_EQ(width(), record.object_size);
    EXPECT_EQ(off, record.offset);
    EXPECT_EQ(std::string(len, value), record.data.to_str());
  }
  EXPECT_FALSE(it.have_op());
  EXPECT_EQ(end + metadata_bytes, j.bytes());
  EXPECT_EQ(3 * record_alignment, j.payload_bytes());
  EXPECT_EQ(3u, j.live_blocks());
  EXPECT_FALSE(j.begin_flush(any())); // preparing transactions isn't durability
  ASSERT_EQ(0, j.committed(second, 0));
  EXPECT_FALSE(j.begin_flush(any())); // the oldest live record is not durable
  ASSERT_EQ(0, j.committed(first, 0));
  EXPECT_TRUE(j.begin_flush(any()));
}

TEST_P(JournalTest, CompleteStripeIsReadyOnlyOnceDurableAndWithoutPolicy)
{
  // A complete stripe needs no base reads, so it may flush at any time, but
  // only after every record it consists of has committed to the store.
  auto j = journal();
  std::vector<Ticket> tickets;
  for (uint64_t off = 0; off < width(); off += block_size) {
    tickets.push_back(append(j, off, block_size, 'a' + off / block_size,
                             object(), false));
  }
  EXPECT_FALSE(j.flush_ready());
  EXPECT_FALSE(j.begin_flush(false));
  for (size_t n = 0; n + 1 < tickets.size(); ++n) {
    EXPECT_EQ(0, j.committed(tickets[n], 0));
    EXPECT_FALSE(j.flush_ready());
  }
  EXPECT_EQ(0, j.committed(tickets.back(), 0));
  EXPECT_TRUE(j.flush_ready());
  auto stripe = j.begin_flush(false); // no partial policy needed
  ASSERT_TRUE(stripe);
  EXPECT_TRUE(stripe->full());
  EXPECT_TRUE(holes(*stripe).empty());
  EXPECT_EQ(tickets.back().sequence, stripe->newest_sequence);
  EXPECT_FALSE(j.flush_ready()); // flushing, no longer resident
  EXPECT_FALSE(j.begin_flush(any()));
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.dirty());
  EXPECT_EQ(0u, j.live_blocks());
  EXPECT_EQ(width() / block_size, j.records()); // the open segment keeps them
  EXPECT_FALSE(trim(j)); // still open
  j.seal();
  EXPECT_TRUE(trim(j)); // sealed and dead: released at once
  EXPECT_TRUE(j.empty());
  EXPECT_EQ(0u, j.records());
}

TEST_P(JournalTest, IncompleteStripeWaitsForReclaimOrDrain)
{
  auto j = journal();
  append(j, 0, block_size);
  EXPECT_TRUE(j.dirty());
  EXPECT_TRUE(j.partial_ready());
  EXPECT_FALSE(j.flush_ready());
  EXPECT_FALSE(j.pressure());
  EXPECT_FALSE(j.begin_flush(false)); // no reclaim bound, no drain
  auto stripe = j.begin_flush(true); // a drain takes it
  ASSERT_TRUE(stripe);
  EXPECT_FALSE(stripe->full());
  EXPECT_EQ(block_size, stripe->dirty_bytes());
  EXPECT_FALSE(j.partial_ready()); // nothing resident while it flushes
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.dirty());
}

TEST_P(JournalTest, WakeOnlyWhenTheOldestResidentStripeBecomesDurable)
{
  auto j = journal();
  auto first = append(j, 0, block_size, 'a', object(), false);
  auto second = append(j, width(), block_size, 'b', object(), false);
  EXPECT_TRUE(j.has_open_segment());
  EXPECT_FALSE(j.partial_ready());
  EXPECT_EQ(0, j.committed(second, 0));
  EXPECT_FALSE(j.partial_ready()); // the oldest is still in flight
  EXPECT_FALSE(j.begin_flush(any()));
  EXPECT_EQ(0, j.committed(first, 0));
  EXPECT_TRUE(j.partial_ready());
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0u, stripe->offset); // oldest first
  EXPECT_TRUE(j.partial_ready()); // the second one is durable too
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(width(), stripe->offset);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.partial_ready());
}

TEST_P(JournalTest, RandomOrderDistinctWritesCompleteStripeWithoutBaseReads)
{
  // One-block and two-block writes (k is even on every geometry).
  for (uint64_t size : {block_size, 2 * block_size}) {
    auto j = journal();
    std::vector<unsigned> slots(width() / size);
    std::iota(slots.begin(), slots.end(), 0);
    std::mt19937 rng(83);
    std::shuffle(slots.begin(), slots.end(), rng);
    std::string expected(width(), '?');
    for (unsigned slot : slots) {
      const char value = 'a' + slot % 26;
      EXPECT_FALSE(j.flush_ready());
      append(j, slot * size, size, value);
      expected.replace(slot * size, size, std::string(size, value));
    }
    EXPECT_TRUE(j.flush_ready());
    auto stripe = j.begin_flush(false);
    ASSERT_TRUE(stripe);
    EXPECT_TRUE(stripe->full());
    EXPECT_TRUE(holes(*stripe).empty());
    EXPECT_EQ(width(), stripe->dirty_bytes());
    EXPECT_EQ(expected, assemble(*stripe).to_str());
    EXPECT_EQ(width() / size, j.records());
    EXPECT_EQ(0, j.finish_flush(*stripe, 0));
    drain(j);
    EXPECT_EQ(0u, j.bytes());
    EXPECT_EQ(0u, j.records());
  }
}

TEST_P(JournalTest, AdjacentBlocksOneShortOfTheStripeLeaveItIncomplete)
{
  // Adjacent writes complete a stripe only once they cover every block: one
  // short of that, the stripe still needs base data for the block left.
  auto j = journal();
  const uint64_t blocks = width() / block_size;
  for (uint64_t slot = 0; slot + 1 < blocks; ++slot) {
    append(j, slot * block_size, block_size);
  }
  EXPECT_FALSE(j.flush_ready());
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_FALSE(stripe->full());
  EXPECT_EQ(width() - block_size, stripe->dirty_bytes());
  ASSERT_EQ(1u, holes(*stripe).size());
  EXPECT_EQ(std::make_pair(width() - block_size, block_size), holes(*stripe)[0]);
  EXPECT_THROW(assemble(*stripe), std::invalid_argument);
}

TEST_P(JournalTest, DuplicateWritesSupersedeWithoutCreatingCoverage)
{
  auto j = journal();
  const unsigned n = width() / block_size;
  for (unsigned i = 0; i < n; ++i) {
    append(j, block_size, block_size, 'a' + i % 26);
  }
  EXPECT_EQ(n, j.records());       // the log holds every append
  EXPECT_EQ(1u, j.live_blocks());  // memory holds the latest only
  EXPECT_EQ(n - 1, j.superseded_blocks());
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(block_size, stripe->dirty_bytes());
  EXPECT_FALSE(stripe->full());
  EXPECT_EQ(std::string(block_size, 'a' + (n - 1) % 26),
            stripe->blocks.at(block_size).to_str());
}

TEST_P(JournalTest, OverlapsUseAppendNotCommitOrderAndPreserveHoles)
{
  // A two-block record, a newer one-block write over its second block, and
  // one next to it; the stripe's last block stays a hole on every geometry.
  auto j = journal();
  auto first = append(j, width(), 2 * block_size, 'a', object(), false);
  auto second = append(j, width() + block_size, block_size, 'b', object(), false);
  auto third = append(j, width() + 2 * block_size, block_size, 'c', object(), false);
  EXPECT_EQ(0, j.committed(third, 0));
  EXPECT_EQ(0, j.committed(second, 0));
  EXPECT_FALSE(j.begin_flush(any()));
  EXPECT_EQ(0, j.committed(first, 0));
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(width(), stripe->offset);
  EXPECT_EQ(3 * block_size, stripe->dirty_bytes());
  auto base = bytes(width(), 'z');
  std::string expected(width(), 'z');
  expected.replace(0, 2 * block_size, std::string(2 * block_size, 'a'));
  expected.replace(block_size, block_size, std::string(block_size, 'b'));
  expected.replace(2 * block_size, block_size, std::string(block_size, 'c'));
  EXPECT_EQ(expected, assemble(*stripe, &base).to_str());
  auto short_base = bytes(block_size, 'z');
  EXPECT_THROW(assemble(*stripe, &short_base), std::invalid_argument);
}

TEST_P(JournalTest, ObjectsAndStripesAreIndependent)
{
  auto j = journal();
  append(j, 0, block_size, 'a', object("one"));
  append(j, width(), block_size, 'b', object("one"));
  append(j, 0, block_size, 'c', object("two"));
  unsigned flushed = 0;
  while (auto stripe = flush(j)) {
    EXPECT_EQ(block_size, stripe->dirty_bytes());
    EXPECT_FALSE(stripe->full());
    ++flushed;
  }
  EXPECT_EQ(3u, flushed);
}

TEST_P(JournalTest, NewerWriteToFlushingStripeStaysJournaled)
{
  auto j = journal();
  append(j, 0, block_size, 'a');
  auto old = j.begin_flush(any());
  ASSERT_TRUE(old);
  auto newer = append(j, 0, block_size, 'b'); // supersedes the copy being written
  EXPECT_EQ(1u, j.live_blocks());
  EXPECT_FALSE(j.begin_flush(any())); // the stripe is busy
  auto base = bytes(width(), 'z');
  auto old_data = assemble(*old, &base);
  EXPECT_EQ(std::string(block_size, 'a'), old_data.to_str().substr(0, block_size));
  EXPECT_EQ(0, j.finish_flush(*old, 0));
  EXPECT_TRUE(j.dirty()); // 'b' has a newer sequence than the snapshot
  EXPECT_EQ(1u, j.live_blocks());
  auto next = j.begin_flush(any());
  ASSERT_TRUE(next);
  EXPECT_EQ(newer.sequence, next->newest_sequence);
  EXPECT_EQ(std::string(block_size, 'b'),
            assemble(*next, &old_data).to_str().substr(0, block_size));
  EXPECT_EQ(0, j.finish_flush(*next, 0));
  EXPECT_FALSE(j.dirty());
}

TEST_P(JournalTest, CoalescesAcrossSegmentsAndKillsOldSegmentBySupersede)
{
  // Records of one stripe may live in several segments; the flush covers
  // them all. A segment dies when its records are superseded or flushed,
  // whichever comes first, and is removed in either order.
  auto l = limits();
  l.segment_bytes = record_bytes() * 5 / 2; // two one-block records per segment
  l.max_bytes = 16 * l.segment_bytes;
  l.max_segments = 16;
  Journal j(prefix(), l);
  auto a = append(j, 0, block_size, 'a');
  auto b = append(j, block_size, block_size, 'b');
  auto c = append(j, 2 * block_size, block_size, 'c');
  EXPECT_NE(a.segment, c.segment);
  auto a2 = append(j, 0, block_size, 'A'); // kills a, in the first segment
  EXPECT_EQ(3u, j.live_blocks());
  EXPECT_EQ(1u, j.superseded_blocks());
  EXPECT_FALSE(trim(j)); // b is still live in the first segment
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(3 * block_size, stripe->dirty_bytes());
  EXPECT_EQ(std::string(block_size, 'A'), stripe->blocks.at(0).to_str());
  EXPECT_EQ(a2.sequence, stripe->newest_sequence);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.dirty());
  auto first = trim(j);
  ASSERT_TRUE(first);
  EXPECT_EQ(a.segment, *first);
  EXPECT_EQ(2u, j.records()); // c and A remain in the open segment
  EXPECT_FALSE(trim(j));
  j.seal();
  EXPECT_TRUE(trim(j));
  EXPECT_TRUE(j.empty());
  (void)b;
}

TEST_P(JournalTest, PressureWhenTheLastSegmentOpensAndFlushesOldestFirst)
{
  auto l = limits();
  l.segment_bytes = record_bytes() * 5 / 2; // two one-block records per segment
  l.max_bytes = 2 * l.segment_bytes;
  l.max_segments = 2;
  Journal j(prefix(), l);
  append(j, 0, block_size, 'a');
  append(j, width(), block_size, 'b');
  EXPECT_EQ(1u, j.segment_count());
  EXPECT_FALSE(j.pressure());
  append(j, 2 * width(), block_size, 'c'); // rotates into the last allowed segment
  EXPECT_EQ(2u, j.segment_count());
  EXPECT_TRUE(j.pressure());
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0u, stripe->offset);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(trim(j)); // 'b' still keeps the first segment alive
  stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(width(), stripe->offset);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_TRUE(trim(j));
  EXPECT_FALSE(j.pressure());
  EXPECT_TRUE(j.dirty()); // 'c' stays journaled: no reclaim needed any more
  EXPECT_EQ(1u, j.segment_count());
}

TEST_P(JournalTest, BackpressureBudgetReleasedWhenTheSegmentDies)
{
  auto l = limits();
  // Room for one record: its padded payload, header and key take more than
  // half of the single 8 KiB segment.
  l.segment_bytes = l.max_bytes = 2 * record_alignment;
  l.max_segments = 1;
  Journal j(prefix(), l);
  append(j, 0, block_size);
  EXPECT_TRUE(j.pressure());
  auto used = j.bytes();
  Transaction t;
  Ticket rejected;
  EXPECT_EQ(-EAGAIN, append_copy(j, object(), eversion_t(1, 2), width(),
                                 block_size, bytes(block_size, 'b'), t,
                                 &rejected));
  EXPECT_TRUE(t.empty());
  EXPECT_EQ(used, j.bytes());
  ASSERT_TRUE(flush(j));
  EXPECT_EQ(used, j.bytes());
  EXPECT_EQ(1u, j.records());
  EXPECT_FALSE(trim(j)); // an open segment is never released
  j.seal();
  EXPECT_TRUE(trim(j));
  EXPECT_EQ(0u, j.bytes());
  EXPECT_EQ(0u, j.records());
  EXPECT_FALSE(j.pressure());
  append(j, block_size, block_size);
}

TEST_P(JournalTest, FullyMaterializedOnlyWhileNothingIsLive)
{
  // A PG reset may happen at any point. Only a journal whose records are
  // all durable on the shards is safe to reset over; anything with live
  // blocks is not.
  auto j = journal();
  EXPECT_FALSE(j.fully_materialized()); // empty is not "materialized"
  append(j, 0, block_size);
  EXPECT_FALSE(j.fully_materialized());
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_FALSE(j.fully_materialized()); // still being written
  ASSERT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_TRUE(j.fully_materialized()); // the open segment is still resident
  j.seal();
  EXPECT_TRUE(j.empty()); // released, nothing left to materialize
  EXPECT_FALSE(j.fully_materialized());
  // New records in a new segment are dirty again.
  append(j, block_size, block_size);
  EXPECT_FALSE(j.fully_materialized());
  EXPECT_TRUE(j.dirty());
}

TEST_P(JournalTest, ResetDropsSegmentsAndTheNextOneResetsItsSlot)
{
  auto j = journal();
  append(j, 0, block_size);
  ASSERT_TRUE(flush(j));
  j.seal();
  EXPECT_TRUE(j.empty());
  auto open = append(j, block_size, block_size, 'x', object(), false);
  EXPECT_TRUE(j.has_open_segment());
  ASSERT_EQ(0, j.committed(open, 0));
  ASSERT_TRUE(flush(j));
  // Commits registered before a PG reset never run; a clean journal drops
  // every segment, and each slot's leftovers stay until it is used again.
  j.on_reset();
  EXPECT_FALSE(j.has_open_segment());
  EXPECT_TRUE(j.empty());
  EXPECT_EQ(0u, j.bytes());
  EXPECT_EQ(0u, j.records());
  Placement placement;
  Ticket fresh;
  ASSERT_EQ(0, j.append(object(), eversion_t(2, 1), 16 * width(), 0,
                        bytes(block_size, 'y'), &placement, &fresh));
  EXPECT_NE(open.segment, fresh.segment);
  EXPECT_EQ(0u, placement.slot);
  EXPECT_EQ(0u, placement.offset);
  EXPECT_TRUE(placement.reset);
}

TEST_P(JournalTest, ByteLimitCountsPaddedPayloadsAndHeaders)
{
  auto l = limits();
  l.segment_bytes = l.max_bytes = record_bytes();
  Journal j(prefix(), l);
  append(j, 0, block_size);
  EXPECT_EQ(l.max_bytes, j.bytes());
  EXPECT_EQ(record_alignment, j.payload_bytes());
  Transaction t;
  Ticket ignored;
  EXPECT_EQ(-EAGAIN, append_copy(j, object(), eversion_t(1, 2), width(),
                                 block_size, bytes(block_size, 'x'), t,
                                 &ignored));
  EXPECT_TRUE(t.empty());
  Journal big(prefix(), l);
  EXPECT_EQ(-E2BIG, append_copy(big, object(), eversion_t(1, 1), width(), 0,
                                bytes(2 * block_size, 'x'), t, &ignored));
  EXPECT_TRUE(t.empty());
  EXPECT_TRUE(big.empty());
  EXPECT_EQ(0, append_copy(big, object(), eversion_t(1, 1), width(), 0,
                           bytes(block_size, 'x'), t, &ignored));
  EXPECT_EQ(record_alignment, big.payload_bytes());
  EXPECT_EQ(l.max_bytes, big.bytes());
  // The header's size depends on the object alone: a block more of payload
  // is charged a block more, nothing else.
  auto two = journal();
  EXPECT_EQ(0, append_copy(two, object(), eversion_t(1, 1), width(), 0,
                           bytes(2 * block_size, 'x'), t, &ignored));
  EXPECT_EQ(2 * record_alignment, two.payload_bytes());
  EXPECT_EQ(record_bytes() + block_size, two.bytes());
}

TEST_P(JournalTest, RecordsOfBothSizesStayBlockAligned)
{
  auto j = journal();
  std::mt19937 rng(7);
  Transaction t;
  uint64_t expected = 0;
  for (unsigned n = 0; n < 40; ++n) {
    // One or two blocks, at any block of the stripe they fit in.
    const uint64_t len = rng() % 2 ? block_size : 2 * block_size;
    const uint64_t off = (rng() % ((width() - len) / block_size + 1)) * block_size;
    Ticket ticket;
    ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, n + 1), width(), off,
                             bytes(len, 'x'), t, &ticket));
    expected += p2roundup<uint64_t>(len, record_alignment);
  }
  EXPECT_EQ(expected, j.payload_bytes());
  uint64_t next = 0;
  uint64_t metadata_bytes = 0;
  for (auto it = t.begin(); it.have_op();) {
    auto op = it.decode_op();
    if (op->op == Transaction::OP_REMOVE) {
      EXPECT_EQ(0u, next); // only ahead of the segment's first record
      continue;
    }
    if (op->op == Transaction::OP_OMAP_SETKEYS) {
      SlotHeaders headers;
      it.decode_attrset(headers);
      for (const auto& [key, header] : headers) {
        metadata_bytes += key.length() + header.length();
      }
      continue;
    }
    ASSERT_EQ(Transaction::OP_WRITE, op->op);
    EXPECT_EQ(next, op->off);
    EXPECT_EQ(0u, op->len % record_alignment);
    ceph::bufferlist bl;
    it.decode_bl(bl);
    next += op->len;
  }
  EXPECT_EQ(expected, next);
  EXPECT_EQ(expected + metadata_bytes, j.bytes());
}

TEST_P(JournalTest, ConcurrentFlushesTakeDistinctStripesAndFinishInAnyOrder)
{
  // The backend keeps several flushes outstanding per PG: reclaiming a sealed
  // segment means materializing every live stripe in it, and one round trip
  // at a time bounds the PG's whole write rate.
  auto j = journal();
  std::vector<hobject_t> objects;
  for (int i = 0; i < 4; ++i) {
    objects.push_back(object("00" + std::to_string(i)));
  }
  for (const auto& oid : objects) {
    for (uint64_t off : {uint64_t{0}, width()}) {
      append(j, off, block_size, 'a', oid); // incomplete stripes: one block
    }
  }
  const auto live = j.live_blocks();
  EXPECT_EQ(8u, live);

  // Every begin_flush hands out a stripe nobody else is flushing.
  std::vector<Stripe> taken;
  while (auto stripe = j.begin_flush(any())) {
    for (const auto& earlier : taken) {
      EXPECT_FALSE(earlier.object == stripe->object &&
                   earlier.offset == stripe->offset);
    }
    taken.push_back(*stripe);
  }
  EXPECT_EQ(8u, taken.size());
  // All of them are outstanding at once, and none is resident meanwhile.
  EXPECT_EQ(live, j.live_blocks());
  EXPECT_TRUE(j.dirty());
  EXPECT_FALSE(j.flush_ready());

  // Completions arrive in whatever order the EC writes commit.
  std::reverse(taken.begin(), taken.end());
  for (size_t i = 0; i < taken.size(); ++i) {
    EXPECT_EQ(0, j.finish_flush(taken[i], 0));
    EXPECT_EQ(live - (i + 1), j.live_blocks());
  }
  EXPECT_FALSE(j.dirty());
  EXPECT_EQ(0u, j.live_blocks());
}

TEST_P(JournalTest, AFailedConcurrentFlushReturnsOnlyItsOwnStripe)
{
  auto j = journal();
  append(j, 0, block_size, 'a', object("0001"));
  append(j, 0, block_size, 'b', object("0002"));
  auto first = j.begin_flush(any());
  auto second = j.begin_flush(any());
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  EXPECT_FALSE(j.begin_flush(any())); // both are outstanding
  EXPECT_EQ(-EIO, j.finish_flush(*first, -EIO));
  EXPECT_EQ(2u, j.live_blocks()); // a failed flush releases nothing
  auto retry = j.begin_flush(any());
  ASSERT_TRUE(retry); // only the failed stripe came back to the resident set
  EXPECT_EQ(first->object, retry->object);
  EXPECT_EQ(first->offset, retry->offset);
  EXPECT_FALSE(j.begin_flush(any()));
  EXPECT_EQ(0, j.finish_flush(*second, 0)); // the other one still completes
  EXPECT_EQ(1u, j.live_blocks());
  EXPECT_EQ(0, j.finish_flush(*retry, 0));
  EXPECT_EQ(0u, j.live_blocks());
}

TEST_P(JournalTest, SegmentRotationIsBoundedAndOldestFlushesFirst)
{
  auto l = limits();
  l.segment_bytes = record_bytes(); // one one-block record per segment
  l.max_segments = 2;
  Journal j(prefix(), l);
  auto first = append(j, 0, block_size, 'a', object(), false);
  auto second = append(j, width(), block_size, 'b');
  EXPECT_NE(first.segment, second.segment);
  EXPECT_TRUE(j.pressure());
  // Strictly oldest first: the durable newer stripe waits for the older
  // one's commit rather than being materialized early.
  EXPECT_FALSE(j.begin_flush(any()));
  Transaction t;
  Ticket ignored;
  EXPECT_EQ(-EAGAIN, append_copy(j, object(), eversion_t(1, 3), width(),
                                 2 * block_size, bytes(block_size, 'c'), t,
                                 &ignored));
  EXPECT_TRUE(t.empty());
  EXPECT_EQ(0, j.committed(first, 0));
  auto oldest = flush(j);
  ASSERT_TRUE(oldest);
  EXPECT_EQ(0u, oldest->offset);
  // The closed segment dies before the open one's record is even flushed.
  auto removed = trim(j);
  ASSERT_TRUE(removed);
  EXPECT_EQ(first.segment, *removed);
  EXPECT_FALSE(j.pressure());
  ASSERT_TRUE(flush(j));
  EXPECT_FALSE(trim(j)); // the second segment is still open
  j.seal();
  EXPECT_EQ(second.segment, trim(j));
  EXPECT_TRUE(j.empty());
}

TEST_P(JournalTest, ReclaimFlushesOnlyStripesPinningTheOldestSegment)
{
  auto l = limits();
  l.segment_bytes = 2 * record_bytes(); // two one-block records per segment
  l.max_bytes = 3 * l.segment_bytes;
  l.max_segments = 3;
  Journal j(prefix(), l);
  // One incomplete stripe per record: segments A = {0, 1}, B = {2, 3}, C = {4}.
  std::vector<Ticket> t;
  for (uint64_t i = 0; i < 5; ++i) {
    t.push_back(append(j, i * width(), block_size));
  }
  ASSERT_EQ(t[0].segment, t[1].segment);
  ASSERT_EQ(t[2].segment, t[3].segment);
  ASSERT_NE(t[1].segment, t[2].segment);
  ASSERT_NE(t[3].segment, t[4].segment);
  ASSERT_TRUE(j.pressure());
  auto bound = j.reclaim_bound();
  ASSERT_TRUE(bound);
  EXPECT_EQ(t[1].sequence, *bound); // the newest record of segment A
  EXPECT_TRUE(j.reclaim_ready());

  // Exactly the two stripes pinning A, oldest first, then nothing, although
  // three younger incomplete stripes are durable and pressure() still holds.
  auto a0 = j.begin_flush(false, {}, nullptr, bound);
  auto a1 = j.begin_flush(false, {}, nullptr, bound);
  ASSERT_TRUE(a0);
  ASSERT_TRUE(a1);
  EXPECT_EQ(0u, a0->offset);
  EXPECT_EQ(width(), a1->offset);
  EXPECT_FALSE(j.begin_flush(false, {}, nullptr, bound));
  EXPECT_FALSE(j.reclaim_ready()); // the completions wake the flusher
  EXPECT_TRUE(j.pressure());

  // Once A holds nothing live it is released, which relieves the log: no
  // more reclaim and no more pressure.
  EXPECT_EQ(0, j.finish_flush(*a0, 0));
  EXPECT_TRUE(j.reclaim_bound()); // a1 still pins A
  EXPECT_EQ(0, j.finish_flush(*a1, 0));
  EXPECT_FALSE(j.reclaim_bound());
  EXPECT_FALSE(j.reclaim_ready());
  EXPECT_EQ(t[0].segment, trim(j));
  EXPECT_FALSE(j.pressure());

  // The next cycle targets the new oldest segment, B, and still not C.
  append(j, 5 * width(), block_size); // fills C
  append(j, 6 * width(), block_size); // opens D: B, C, D
  bound = j.reclaim_bound();
  ASSERT_TRUE(bound);
  EXPECT_EQ(t[3].sequence, *bound);
  auto b0 = j.begin_flush(false, {}, nullptr, bound);
  auto b1 = j.begin_flush(false, {}, nullptr, bound);
  ASSERT_TRUE(b0);
  ASSERT_TRUE(b1);
  EXPECT_EQ(2 * width(), b0->offset);
  EXPECT_EQ(3 * width(), b1->offset);
  EXPECT_FALSE(j.begin_flush(false, {}, nullptr, bound));
  // What flushing by pressure() alone takes next: a stripe still gathering
  // records in C, flushed incomplete for no capacity gain.
  auto overreach = j.begin_flush(any());
  ASSERT_TRUE(overreach);
  EXPECT_EQ(4 * width(), overreach->offset);
}

TEST_P(JournalTest, ReclaimFollowsEachStripesOldestLiveRecord)
{
  auto l = limits();
  l.segment_bytes = 2 * record_bytes();
  l.max_bytes = 3 * l.segment_bytes;
  l.max_segments = 3;
  Journal j(prefix(), l);
  append(j, 0, block_size, 'a');           // A: stripe 0
  append(j, width(), block_size, 'b');     // A: stripe 1
  append(j, 2 * width(), block_size, 'c'); // B: stripe 2
  append(j, width(), block_size, 'B');     // B: supersedes stripe 1's record in A
  append(j, block_size, block_size, 'd');  // C: a second block for stripe 0
  ASSERT_TRUE(j.pressure());
  // Stripe 1 no longer pins A, its record there is dead. Stripe 0 does, and
  // goes with the block it has gathered since.
  auto bound = j.reclaim_bound();
  ASSERT_TRUE(bound);
  auto stripe = j.begin_flush(false, {}, nullptr, bound);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0u, stripe->offset);
  EXPECT_EQ(2u, stripe->blocks.size());
  EXPECT_FALSE(j.begin_flush(false, {}, nullptr, bound));
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.reclaim_bound()); // A is dead and released
  EXPECT_EQ(2u, j.segment_count());
}

TEST_P(JournalTest, FailedFlushDoesNotLoseJournal)
{
  auto j = journal();
  append(j, 0, block_size);
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  const auto used = j.bytes();
  EXPECT_EQ(-EIO, j.finish_flush(*stripe, -EIO));
  EXPECT_TRUE(j.dirty());
  EXPECT_EQ(1u, j.live_blocks());
  auto retry = j.begin_flush(any());
  ASSERT_TRUE(retry);
  EXPECT_EQ(stripe->newest_sequence, retry->newest_sequence);
  EXPECT_EQ(used, j.bytes());
  EXPECT_EQ(0, j.finish_flush(*retry, 0));
  j.seal();
  EXPECT_TRUE(trim(j));
  EXPECT_TRUE(j.empty());
}

TEST_P(JournalTest, FailedAppendStopsAdmissionAndFlush)
{
  auto j = journal();
  auto ticket = append(j, 0, block_size, 'x', object(), false);
  EXPECT_EQ(-EIO, j.committed(ticket, -EIO));
  EXPECT_EQ(-EIO, j.error());
  EXPECT_FALSE(j.flush_ready());
  EXPECT_FALSE(j.partial_ready());
  EXPECT_FALSE(j.begin_flush(any()));
  Transaction t;
  Ticket ignored;
  EXPECT_EQ(-EIO, append_copy(j, object(), eversion_t(1, 2), width(),
                              block_size, bytes(block_size, 'y'), t, &ignored));
  EXPECT_TRUE(t.empty());
  EXPECT_EQ(1u, j.records());
}

TEST_P(JournalTest, InvalidTransitionsDoNotReclaimRecords)
{
  auto j = journal();
  auto ticket = append(j, 0, block_size);
  EXPECT_EQ(-EALREADY, j.committed(ticket, 0));
  EXPECT_EQ(-ENOENT, j.committed(Ticket{ticket.segment, 999}, 0));
  EXPECT_EQ(-ENOENT, j.committed(Ticket{999, ticket.sequence}, 0));
  Stripe stray;
  stray.object = object("stray");
  EXPECT_EQ(-ENOENT, j.finish_flush(stray, 0));
  Stripe resident;
  resident.object = object();
  EXPECT_EQ(-EINVAL, j.finish_flush(resident, 0)); // not flushing
  EXPECT_TRUE(j.resident(ticket.segment)); // open, and live
  EXPECT_EQ(1u, j.records());
  EXPECT_EQ(1u, j.live_blocks());
}

TEST_P(JournalTest, SkippedCandidatesAreReportedAsDeferred)
{
  auto j = journal();
  for (uint64_t off = 0; off < width(); off += block_size) {
    append(j, off, block_size, 'a', object("busy"));
  }
  append(j, 0, block_size, 'b', object("free"));
  bool deferred = false;
  auto busy = [](const Journal::StripeKey& key) {
    return key.first == object("busy");
  };
  auto stripe = j.begin_flush(false, busy, &deferred);
  EXPECT_FALSE(stripe); // the complete stripe is blocked, no partial policy
  EXPECT_TRUE(deferred);
  stripe = j.begin_flush(any(), busy, &deferred);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(object("free"), stripe->object);
  EXPECT_TRUE(deferred);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  stripe = j.begin_flush(any(), {}, &deferred);
  ASSERT_TRUE(stripe);
  EXPECT_TRUE(stripe->full());
  EXPECT_FALSE(deferred);
}

TEST_P(JournalTest, InvalidWritesHaveNoSideEffects)
{
  auto j = journal();
  for (auto [off, len] : {
         std::pair{uint64_t(0), uint64_t(0)},
         std::pair{uint64_t(0), uint64_t(512)},
         std::pair{uint64_t(0), uint64_t(1024)},             // less than a block
         std::pair{uint64_t(0), block_size + 2048},          // not whole blocks
         std::pair{uint64_t(1), block_size},
         std::pair{uint64_t(1024), block_size},              // 1 KiB aligned
         std::pair{width() - block_size, 2 * block_size},    // crosses stripe/EOF
         std::pair{width(), block_size},
         std::pair{std::numeric_limits<uint64_t>::max() - (block_size - 1),
                   block_size}}) {
    Transaction t;
    Ticket ignored;
    EXPECT_EQ(-EINVAL, append_copy(j, object(), eversion_t(1, 1), width(), off,
                                   bytes(len, 'x'), t, &ignored));
    EXPECT_TRUE(t.empty());
    EXPECT_TRUE(j.empty());
    EXPECT_FALSE(j.dirty());
    EXPECT_EQ(0u, j.bytes());
  }
}

TEST_P(JournalTest, RandomOverwritesMatchReferenceAfterDrain)
{
  // Mechanism test, not an IOPS benchmark: random duplicates, both write sizes,
  // nonzero holes, multiple objects/stripes, deliberately reversed callbacks,
  // small segments and flushes interleaved with appends. Every flushed stripe
  // is applied to the reference "physical" data in flush order.
  auto l = limits();
  l.segment_bytes = 64 * 1024;
  l.max_bytes = 4 * l.segment_bytes;
  l.max_segments = 4;
  Journal j(prefix(), l);
  std::mt19937 rng(123);
  std::map<hobject_t, std::string> expected, physical;
  for (const auto& name : {"one", "two"}) {
    expected.emplace(object(name), std::string(width() * 3, 'z'));
    physical.emplace(object(name), std::string(width() * 3, 'z'));
  }
  auto apply = [&](const Stripe& stripe) {
    auto& data = physical.at(stripe.object);
    for (const auto& [off, block] : stripe.blocks) {
      data.replace(stripe.offset + off, block.length(), block.to_str());
    }
  };
  std::vector<Ticket> tickets;
  for (unsigned n = 0; n < 600; ++n) {
    // One or two blocks at any block of one of the three stripes, inside it.
    const uint64_t size = rng() % 2 ? block_size : 2 * block_size;
    const uint64_t stripe = rng() % 3;
    const uint64_t off = stripe * width() +
      (rng() % ((width() - size) / block_size + 1)) * block_size;
    auto oid = object(rng() % 2 ? "one" : "two");
    const char value = 'a' + rng() % 26;
    Transaction t;
    Ticket ticket;
    int r = append_copy(j, oid, eversion_t(1, n + 1), width() * 3, off,
                        bytes(size, value), t, &ticket);
    if (r == 0) {
      seen.insert(ticket.segment);
    }
    if (r == -EAGAIN) {
      // Reclaim as the backend does: commit everything outstanding, seal,
      // flush oldest first until a released segment makes room.
      for (auto i = tickets.rbegin(); i != tickets.rend(); ++i) {
        EXPECT_EQ(0, j.committed(*i, 0));
      }
      tickets.clear();
      j.seal();
      while ((r = append_copy(j, oid, eversion_t(1, n + 1), width() * 3, off,
                              bytes(size, value), t, &ticket)) == -EAGAIN) {
        auto stripe = j.begin_flush(any());
        ASSERT_TRUE(stripe);
        apply(*stripe);
        ASSERT_EQ(0, j.finish_flush(*stripe, 0));
      }
      if (r == 0) {
        seen.insert(ticket.segment);
      }
    }
    ASSERT_EQ(0, r);
    tickets.push_back(ticket);
    expected.at(oid).replace(off, size, std::string(size, value));
    if (n % 7 == 0) {
      // Complete stripes go out eagerly, whatever the age policy.
      if (auto stripe = j.begin_flush(false)) {
        EXPECT_TRUE(stripe->full());
        apply(*stripe);
        ASSERT_EQ(0, j.finish_flush(*stripe, 0));
      }
    }
  }
  for (auto i = tickets.rbegin(); i != tickets.rend(); ++i) {
    EXPECT_EQ(0, j.committed(*i, 0));
  }
  j.seal();
  while (auto stripe = j.begin_flush(any())) {
    apply(*stripe);
    ASSERT_EQ(0, j.finish_flush(*stripe, 0));
  }
  EXPECT_EQ(expected, physical);
  while (trim(j)) {
  }
  EXPECT_TRUE(j.empty());
  EXPECT_EQ(0u, j.live_blocks());
}

TEST_P(JournalTest, ObjectTailPreservesDataAndPadsOnlyBeyondEOF)
{
  auto j = journal();
  // A 4 MiB RBD object is not a multiple of a 12+3/4K stripe. Also exercise
  // an EOF inside a block: no full-stripe read may be requested beyond it.
  // The block between two holes is journaled; the one EOF cuts short is not.
  const uint64_t tail = 2 * block_size + 513;
  const uint64_t size = width() + tail;
  Transaction t;
  Ticket ticket;
  EXPECT_EQ(-EINVAL, append_copy(j, object(), eversion_t(1, 1), size,
                                 width() + 2 * block_size, bytes(block_size, 'x'),
                                 t, &ticket));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 1), size,
                           width() + block_size, bytes(block_size, 'x'), t,
                           &ticket));
  ASSERT_EQ(0, j.committed(ticket, 0));
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(tail, stripe->valid_bytes);
  EXPECT_FALSE(stripe->full());
  EXPECT_EQ(2u, holes(*stripe).size());
  for (const auto& [off, len] : holes(*stripe)) {
    EXPECT_LE(off + len, size);
  }
  auto base = bytes(tail, 'z');
  std::string expected(tail, 'z');
  expected.replace(block_size, block_size, std::string(block_size, 'x'));
  expected.resize(width(), '\0');
  EXPECT_EQ(expected, assemble(*stripe, &base).to_str());
}

TEST_P(JournalTest, FullyCoveredTailNeedsNoBaseRead)
{
  auto j = journal();
  Transaction t;
  Ticket ticket;
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 1), width() + block_size,
                           width(), bytes(block_size, 'x'), t, &ticket));
  ASSERT_EQ(0, j.committed(ticket, 0));
  EXPECT_TRUE(j.flush_ready()); // the whole tail is covered
  auto stripe = j.begin_flush(false);
  ASSERT_TRUE(stripe);
  EXPECT_TRUE(stripe->full());
  EXPECT_TRUE(holes(*stripe).empty());
  std::string expected(block_size, 'x');
  expected.resize(width(), '\0');
  EXPECT_EQ(expected, assemble(*stripe).to_str());
}

TEST_P(JournalTest, AFourKiBWriteIsOneIndexBlock)
{
  // The index keeps one block (one version, one sequence, one buffer
  // reference) per 4 KiB: a 4 KiB write is one block, an 8 KiB write two.
  ASSERT_EQ(4096u, block_size);
  auto j = journal();
  append(j, 4096, 4096, 'a');
  EXPECT_EQ(1u, j.live_blocks());
  EXPECT_EQ(4096u, j.live_bytes());
  auto k = journal();
  append(k, 4096, 8192, 'b');
  EXPECT_EQ(2u, k.live_blocks());
  EXPECT_EQ(8192u, k.live_bytes());
  // A flush takes the same blocks.
  auto one = j.begin_flush(any());
  ASSERT_TRUE(one);
  ASSERT_EQ(1u, one->blocks.size());
  EXPECT_EQ(4096u, one->blocks.at(4096).length());
  auto two = k.begin_flush(any());
  ASSERT_TRUE(two);
  ASSERT_EQ(2u, two->blocks.size());
  EXPECT_EQ(8192u, two->dirty_bytes());
}

TEST_P(JournalTest, MergedWriteFillsEveryBlockItCovers)
{
  // librbd merges adjacent 4 KiB writes to one object into one request: one
  // record, one block per 4 KiB, latest append wins per block as usual.
  auto j = journal();
  append(j, block_size, 2 * block_size, 'm');
  EXPECT_EQ(2u, j.live_blocks());
  EXPECT_EQ(2 * block_size, j.payload_bytes());
  EXPECT_LT(2 * block_size, j.bytes());
  append(j, 2 * block_size, block_size, 'n');
  EXPECT_EQ(1u, j.superseded_blocks());
  EXPECT_EQ(2u, j.live_blocks());
  EXPECT_FALSE(j.flush_ready());
  append(j, 0, block_size, 'a');
  append(j, 3 * block_size, width() - 3 * block_size, 'z'); // the rest, merged again
  EXPECT_TRUE(j.flush_ready());
  auto stripe = j.begin_flush(false);
  ASSERT_TRUE(stripe);
  EXPECT_TRUE(stripe->full());
  EXPECT_EQ(std::string(block_size, 'a') + std::string(block_size, 'm') +
            std::string(block_size, 'n') +
            std::string(width() - 3 * block_size, 'z'),
            assemble(*stripe).to_str());
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  // A whole stripe in one record needs no second write to be complete.
  append(j, width(), width(), 'w');
  EXPECT_TRUE(j.flush_ready());
}

TEST_P(JournalTest, ObjectFlushTakesOnlyThatObjectsStripesAtAnyAge)
{
  // An unsupported mutation of one object waits for that object's buffered
  // data only; every other stripe keeps gathering records.
  auto j = journal();
  const auto a = object("a");
  const auto b = object("b");
  append(j, 0, block_size, 'b', b);
  append(j, 0, block_size, 'a', a);
  append(j, width(), block_size, 'a', a);
  auto pending = append(j, 2 * width(), block_size, 'a', a, false);
  EXPECT_TRUE(j.dirty(a));
  EXPECT_TRUE(j.dirty(b));
  EXPECT_FALSE(j.dirty(object("c")));
  EXPECT_TRUE(j.object_ready(a));
  EXPECT_FALSE(j.begin_flush(false)); // no reclaim bound, no drain

  std::vector<Stripe> taken;
  while (auto stripe = j.begin_flush_object(a)) {
    taken.push_back(*stripe);
  }
  ASSERT_EQ(2u, taken.size());
  for (const auto& stripe : taken) {
    EXPECT_EQ(a, stripe.object);
    EXPECT_FALSE(stripe.full());
  }
  // One stripe still waits for its append to commit, the others flush.
  EXPECT_FALSE(j.object_ready(a));
  EXPECT_EQ(0, j.committed(pending, 0));
  EXPECT_TRUE(j.object_ready(a));
  auto last = j.begin_flush_object(a);
  ASSERT_TRUE(last);
  EXPECT_EQ(2 * width(), last->offset);
  taken.push_back(*last);
  EXPECT_FALSE(j.begin_flush_object(a));
  for (const auto& stripe : taken) {
    EXPECT_TRUE(j.dirty(a)); // until the last flush completes
    EXPECT_EQ(0, j.finish_flush(stripe, 0));
  }
  EXPECT_FALSE(j.dirty(a));
  EXPECT_EQ(1u, j.live_blocks());

  bool deferred = false;
  EXPECT_FALSE(j.begin_flush_object(b, [](const auto&) { return true; },
                                    &deferred));
  EXPECT_TRUE(deferred);
  EXPECT_TRUE(j.begin_flush_object(b, {}, &deferred));
  EXPECT_FALSE(deferred);
}

TEST_P(JournalTest, ObjectFlushCanBeLimitedToAByteRange)
{
  // A read waits only for the journaled stripes it overlaps.
  auto j = journal();
  const auto a = object("a");
  append(j, 0, block_size, 'x', a);
  append(j, width(), block_size, 'x', a);
  append(j, 3 * width(), block_size, 'x', a);
  EXPECT_TRUE(j.dirty(a, width() + 100, 10));
  EXPECT_FALSE(j.dirty(a, 2 * width(), width()));
  EXPECT_TRUE(j.dirty(a, 2 * width() + 1, width())); // reaches stripe 3
  EXPECT_TRUE(j.object_ready(a, width(), 1));
  EXPECT_FALSE(j.object_ready(a, 2 * width(), width()));

  auto s = j.begin_flush_object(a, {}, nullptr, width(), 2 * width());
  ASSERT_TRUE(s);
  EXPECT_EQ(width(), s->offset);
  EXPECT_FALSE(j.begin_flush_object(a, {}, nullptr, width(), 2 * width()));
  EXPECT_TRUE(j.dirty(a, width(), 1)); // flushing still counts as dirty
  EXPECT_EQ(0, j.finish_flush(*s, 0));
  EXPECT_FALSE(j.dirty(a, width(), width()));
  EXPECT_TRUE(j.dirty(a)); // stripes 0 and 3 remain
  EXPECT_TRUE(j.dirty(a, 3 * width(), Journal::whole_object)); // no overflow
  EXPECT_FALSE(j.dirty(object("b"), 0, width()));
}

TEST_P(JournalTest, ReadStateFollowsTheStripesARangeOverlaps)
{
  // A client read is served from the journal unless a stripe it overlaps
  // has a record in flight or is being flushed; other stripes do not matter.
  auto j = journal();
  const auto a = object("a");
  EXPECT_EQ(ReadState::clean, j.read_state(a, 0, 16 * width()));
  append(j, block_size, block_size, 'x', a);
  EXPECT_EQ(ReadState::ready, j.read_state(a, 0, 1));
  EXPECT_EQ(ReadState::ready, j.read_state(a, width() - 1, 1));
  EXPECT_EQ(ReadState::clean, j.read_state(a, width(), width()));
  EXPECT_EQ(ReadState::clean, j.read_state(a, 100, 0)); // reads nothing
  EXPECT_EQ(ReadState::clean, j.read_state(object("b"), 0, width()));

  auto pending = append(j, width() + block_size, 2 * block_size, 'y', a, false);
  EXPECT_EQ(ReadState::in_flight, j.read_state(a, width(), 1));
  EXPECT_EQ(ReadState::in_flight, j.read_state(a, width() - 1, 2));
  EXPECT_EQ(ReadState::ready, j.read_state(a, 0, width()));
  // A durable record on top of one in flight: the block has its newest
  // version, and the rest of the older record still has to commit.
  append(j, width() + block_size, block_size, 'z', a);
  EXPECT_EQ(ReadState::in_flight, j.read_state(a, width(), width()));
  EXPECT_EQ(0, j.committed(pending, 0));
  EXPECT_EQ(ReadState::ready, j.read_state(a, 0, 2 * width()));

  auto flushing = j.begin_flush_object(a, {}, nullptr, 0, width());
  ASSERT_TRUE(flushing);
  EXPECT_EQ(ReadState::flushing, j.read_state(a, 0, 1));
  EXPECT_EQ(ReadState::flushing, j.read_state(a, 0, 2 * width()));
  EXPECT_EQ(ReadState::ready, j.read_state(a, width(), width()));
  // A write of the stripe meanwhile is journaled and outlives the flush.
  append(j, 0, block_size, 'w', a);
  EXPECT_EQ(ReadState::flushing, j.read_state(a, 0, width()));
  EXPECT_EQ(0, j.finish_flush(*flushing, 0));
  EXPECT_EQ(ReadState::ready, j.read_state(a, 0, width()));
  Overlay overlay;
  j.read_overlay(a, 0, width(), &overlay);
  ASSERT_EQ(1u, overlay.blocks.size());
  EXPECT_EQ(std::string(block_size, 'w'), overlay.blocks.at(0).to_str());
}

TEST_P(JournalTest, ReadOverlayHoldsTheNewestBlocksOfTheRange)
{
  auto j = journal();
  const auto a = object("a");
  // A three-block record up to the end of stripe 0 on 4+3, and a newer
  // one-block write over its middle block.
  append(j, block_size, 3 * block_size, 'a', a);
  append(j, 2 * block_size, block_size, 'b', a); // supersedes one block of it
  append(j, width() + block_size, block_size, 'c', a);
  append(j, block_size, block_size, 'o', object("other"));

  Overlay overlay;
  j.read_overlay(a, block_size, 3 * block_size, &overlay);
  ASSERT_EQ(3u, overlay.blocks.size());
  EXPECT_EQ(std::string(block_size, 'a'), overlay.blocks.at(block_size).to_str());
  EXPECT_EQ(std::string(block_size, 'b'),
            overlay.blocks.at(2 * block_size).to_str());
  EXPECT_EQ(std::string(block_size, 'a'),
            overlay.blocks.at(3 * block_size).to_str());
  EXPECT_TRUE(overlay.covers(block_size, 3 * block_size));

  // Only the blocks a range touches, whatever its alignment.
  Overlay part;
  j.read_overlay(a, 2 * block_size - 100, 200, &part);
  ASSERT_EQ(2u, part.blocks.size());
  EXPECT_TRUE(part.blocks.contains(block_size));
  EXPECT_TRUE(part.blocks.contains(2 * block_size));
  Overlay none;
  j.read_overlay(a, 0, block_size, &none);
  j.read_overlay(a, 4 * block_size, width() - 4 * block_size + block_size, &none);
  j.read_overlay(a, 100, 0, &none);
  EXPECT_TRUE(none.empty());
  // Several stripes, and several ranges into one overlay.
  Overlay all;
  j.read_overlay(a, 0, width(), &all);
  j.read_overlay(a, width(), width(), &all);
  j.read_overlay(a, block_size, block_size, &all);
  EXPECT_EQ(4u, all.blocks.size());
  EXPECT_EQ(std::string(block_size, 'c'),
            all.blocks.at(width() + block_size).to_str());

  // The overlay is a snapshot: a later write and the flush that releases
  // the blocks leave it as it was.
  append(j, block_size, block_size, 'd', a);
  auto stripe = j.begin_flush_object(a, {}, nullptr, 0, width());
  ASSERT_TRUE(stripe);
  // The flush's blocks stay in the index until it commits.
  Overlay during;
  j.read_overlay(a, block_size, 3 * block_size, &during);
  EXPECT_EQ(3u, during.blocks.size());
  EXPECT_EQ(std::string(block_size, 'd'), during.blocks.at(block_size).to_str());
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  Overlay after;
  j.read_overlay(a, block_size, 3 * block_size, &after);
  EXPECT_TRUE(after.empty());
  EXPECT_EQ(std::string(block_size, 'a'), overlay.blocks.at(block_size).to_str());
  ceph::bufferlist read = bytes(3 * block_size, 'z');
  overlay.apply(block_size, 3 * block_size, read);
  EXPECT_EQ(std::string(block_size, 'a') + std::string(block_size, 'b') +
            std::string(block_size, 'a'), read.to_str());
}

TEST_P(JournalTest, MustFlushSeparatesTheFlushesThatHaveToRun)
{
  // A stripe that is merely complete can wait for the object's readers; one
  // that reclaim, a drain or replay asks for cannot.
  auto j = journal();
  const auto a = object("a");
  const auto first = append(j, 0, width(), 'x', a); // complete
  const auto second = append(j, width(), block_size, 'y', a);
  const Journal::StripeKey complete{a, 0};
  const Journal::StripeKey partial{a, width()};
  EXPECT_TRUE(j.flush_ready());
  EXPECT_FALSE(j.must_flush(complete, false, std::nullopt));
  EXPECT_FALSE(j.must_flush(partial, false, std::nullopt));
  EXPECT_TRUE(j.must_flush(complete, true, std::nullopt));
  EXPECT_TRUE(j.must_flush(partial, true, std::nullopt));
  EXPECT_TRUE(j.must_flush(complete, false, first.sequence));
  EXPECT_FALSE(j.must_flush(partial, false, first.sequence));
  EXPECT_TRUE(j.must_flush(partial, false, second.sequence));
  EXPECT_FALSE(j.must_flush({a, 2 * width()}, true, std::nullopt)); // no records
  EXPECT_FALSE(j.must_flush({object("b"), 0}, true, std::nullopt));
}

TEST_P(JournalTest, RandomReadsThroughTheOverlayMatchTheObject)
{
  // The reference for serving reads from the journal: at any moment, for any
  // byte range whose stripes have no record in flight and no flush in
  // flight, the overlay taken then, laid over the shards' bytes as they are
  // at any time until the read ends, is the object as its acknowledged
  // writes had left it when the read was issued. Random writes of one to
  // three blocks with out-of-order commits, flushes in flight, reads in
  // flight (which no flush of their stripes may overtake), an object tail
  // that ends inside a block, and reads at any offset and length.
  //
  // And for direct writes beside the journal: a write of any offset and
  // length that the journal does not take runs when no stripe of its range
  // has a record in flight or a flush in flight, takes the journaled
  // stripes it overlaps and writes, with its own bytes, the blocks of those
  // stripes it does not cover. Like the OSD's, it excludes the reads of its
  // object while it is in flight (the object lock), journaled writes of the
  // object go on meanwhile, and the writes and flushes of an object reach
  // the shards in the order they were issued (the RMW pipeline). At the end
  // everything is flushed and the shards alone must be the object.
  auto j = journal();
  std::mt19937 rng(20261003);
  // The last stripe holds two whole blocks and one that EOF cuts short.
  const uint64_t size = 3 * width() + 2 * block_size + 2560;
  const std::vector<hobject_t> objects{object("one"), object("two")};
  std::map<hobject_t, std::string> expected, physical;
  for (const auto& oid : objects) {
    std::string base(size, '\0');
    for (auto& c : base) {
      c = 'A' + rng() % 26;
    }
    expected.emplace(oid, base);
    physical.emplace(oid, base);
  }
  // A journaled write in flight: one record, or one per stripe for a write
  // across a stripe boundary; they become durable together.
  std::vector<std::vector<Ticket>> uncommitted;
  unsigned crossed = 0;
  struct Flush {
    Stripe stripe;
    uint64_t issued;
  };
  std::vector<Flush> flushing;
  struct Read {
    hobject_t oid;
    uint64_t off, len;
    Overlay overlay;  // taken when the read was issued
    std::string want; // the object's bytes then
  };
  std::vector<Read> reading;
  struct Direct {
    hobject_t oid;
    uint64_t off, len;
    std::string data;
    std::vector<Stripe> taken;
    std::map<uint64_t, ceph::bufferlist> runs;
    uint64_t issued;
  };
  std::deque<Direct> writing; // in the order issued
  uint64_t issued = 0;
  const auto commit_flush = [&](std::vector<Flush>::iterator i) {
    for (const auto& [off, block] : i->stripe.blocks) {
      physical.at(i->stripe.object).replace(i->stripe.offset + off,
                                            block.length(), block.to_str());
    }
    EXPECT_EQ(0, j.finish_flush(i->stripe, 0));
    flushing.erase(i);
  };
  const auto commit_direct = [&] {
    auto& w = writing.front();
    physical.at(w.oid).replace(w.off, w.len, w.data);
    for (const auto& [at, run] : w.runs) {
      physical.at(w.oid).replace(at, run.length(), run.to_str());
    }
    for (const auto& stripe : w.taken) {
      EXPECT_EQ(0, j.finish_flush(stripe, 0));
    }
    writing.pop_front();
  };
  // A flush queues behind the direct writes of its object issued before it.
  const auto flush_may_commit = [&](const Flush& f) {
    return std::none_of(writing.begin(), writing.end(), [&f](const Direct& w) {
      return w.oid == f.stripe.object && w.issued < f.issued;
    });
  };
  const auto write_locked = [&](const hobject_t& o) {
    return std::any_of(writing.begin(), writing.end(),
                       [&o](const Direct& w) { return w.oid == o; });
  };
  const auto read_locked = [&](const hobject_t& o) {
    return std::any_of(reading.begin(), reading.end(),
                       [&o](const Read& r) { return r.oid == o; });
  };
  uint64_t version = 1;
  unsigned checked = 0, overlaid = 0, from_journal = 0, waited = 0;
  unsigned direct = 0, direct_waited = 0, direct_took = 0;
  uint64_t taken_stripes = 0, dropped_stripes = 0;
  for (unsigned n = 0; n < 8000; ++n) {
    const auto& oid = objects[rng() % objects.size()];
    switch (rng() % 10) {
    case 0:
    case 1:
    case 2: { // a write; some stay in flight for a while
      const uint64_t len = rng() % 2 ? block_size :
        rng() % 4 ? 2 * block_size : 3 * block_size;
      const uint64_t off = (rng() % ((size - len) / block_size + 1)) * block_size;
      const char value = 'a' + rng() % 26;
      // One record per stripe: a write across a boundary is two.
      const auto pieces = stripe_fragments(off, len, width());
      crossed += pieces.size() > 1;
      std::vector<uint64_t> lengths;
      for (const auto& piece : pieces) {
        lengths.push_back(piece.second);
      }
      ASSERT_EQ(0, j.check_append(oid, lengths));
      Transaction t;
      std::vector<Ticket> tickets;
      for (const auto& [at, part] : pieces) {
        ASSERT_EQ(0, append_copy(j, oid, eversion_t(1, version++), size, at,
                                 bytes(part, value), t, &tickets.emplace_back()));
      }
      expected.at(oid).replace(off, len, std::string(len, value));
      if (rng() % 3 || uncommitted.size() > 2) {
        for (const auto& ticket : tickets) {
          ASSERT_EQ(0, j.committed(ticket, 0));
        }
      } else {
        uncommitted.push_back(std::move(tickets));
      }
      break;
    }
    case 3: // a commit, in any order
      if (!uncommitted.empty()) {
        const auto i = uncommitted.begin() + rng() % uncommitted.size();
        for (const auto& ticket : *i) {
          ASSERT_EQ(0, j.committed(ticket, 0));
        }
        uncommitted.erase(i);
      }
      break;
    case 4: // a flush starts, complete stripes or the oldest one ...
      if (flushing.size() < 2 && rng() % 4 == 0) {
        if (auto stripe = j.begin_flush(rng() % 2)) {
          flushing.push_back(Flush{std::move(*stripe), issued++});
        }
      }
      break;
    case 5: // ... and one commits: its blocks are on the shards
      if (!flushing.empty()) {
        const auto i = flushing.begin() + rng() % flushing.size();
        if (flush_may_commit(*i)) {
          commit_flush(i);
        }
      }
      break;
    case 8: { // a direct write is issued: it takes the stripes it overlaps
      if (writing.size() > 1 || read_locked(oid)) {
        break;
      }
      uint64_t off = rng() % size;
      uint64_t len = 1 + rng() % std::min<uint64_t>(size - off,
        rng() % 3 ? 3 * 4096 : 2 * width() + 4096);
      if (rng() % 2) {
        // Half of them whole blocks, as clients' 4 KiB writes are: those
        // cover journaled blocks whole and leave their stripes nothing.
        const uint64_t end = std::min(p2roundup(off + len, block_size), size);
        off = p2align(off, block_size);
        len = end - off;
      }
      const auto state = j.write_state(oid, off, len);
      if (state == ReadState::in_flight || state == ReadState::flushing) {
        ++direct_waited;
        std::vector<Stripe> none;
        ASSERT_EQ(-EBUSY, j.take_range(oid, off, len, &none));
        ASSERT_TRUE(none.empty());
        break;
      }
      const char digit = '0' + rng() % 10;
      Direct w{oid, off, len, std::string(len, digit), {}, {}, issued++};
      ASSERT_EQ(0, j.take_range(oid, off, len, &w.taken));
      if (state == ReadState::clean) {
        ASSERT_TRUE(w.taken.empty());
      }
      interval_set<uint64_t> written;
      written.insert(off, len);
      uint64_t dropped = 0;
      w.runs = absorb_runs(w.taken, written, &dropped);
      // What it carries is what the object held there, outside its bytes.
      for (const auto& [at, run] : w.runs) {
        ASSERT_FALSE(written.intersects(at, run.length()));
        ASSERT_EQ(expected.at(oid).substr(at, run.length()), run.to_str());
      }
      ++direct;
      direct_took += !w.taken.empty();
      taken_stripes += w.taken.size();
      dropped_stripes += dropped;
      expected.at(oid).replace(off, len, w.data);
      writing.push_back(std::move(w));
      break;
    }
    case 9: // ... and the oldest one commits
      if (!writing.empty()) {
        commit_direct();
      }
      break;
    case 6: { // a read is issued: its overlay now, its shard reads later
      if (reading.size() > 3 || write_locked(oid)) {
        break;
      }
      const uint64_t off = rng() % size;
      const uint64_t len = 1 + rng() % std::min<uint64_t>(size - off,
        rng() % 4 ? 6000 : 2 * width());
      const auto state = j.read_state(oid, off, len);
      if (state != ReadState::clean && state != ReadState::ready) {
        ++waited;
        break;
      }
      Read read{oid, off, len, {}, expected.at(oid).substr(off, len)};
      j.read_overlay(oid, off, len, &read.overlay);
      if (state == ReadState::clean) {
        EXPECT_TRUE(read.overlay.empty());
      }
      j.read_begin(oid, off, len);
      reading.push_back(std::move(read));
      break;
    }
    default: { // a read completes, whenever its shard reads ran meanwhile
      if (reading.empty()) {
        break;
      }
      const auto i = reading.begin() + rng() % reading.size();
      ceph::bufferlist data;
      data.append(physical.at(i->oid).substr(i->off, i->len));
      i->overlay.apply(i->off, i->len, data);
      ASSERT_EQ(i->want, data.to_str())
        << "read " << i->off << "~" << i->len << " ended at step " << n;
      ++checked;
      overlaid += !i->overlay.empty();
      if (i->overlay.covers(i->off, i->len)) {
        ceph::bufferlist alone;
        i->overlay.apply(i->off, i->len, alone);
        ASSERT_EQ(i->want, alone.to_str());
        ++from_journal;
      }
      j.read_end(i->oid, i->off, i->len);
      reading.erase(i);
      break;
    }
    }
  }
  // The run must have exercised every case.
  EXPECT_GT(checked, 150u);
  EXPECT_GT(overlaid, 50u);
  EXPECT_GT(from_journal, 10u);
  EXPECT_GT(waited, 50u);
  EXPECT_GT(crossed, 20u);
  EXPECT_GT(direct, 150u);
  EXPECT_GT(direct_took, 50u);
  EXPECT_GT(direct_waited, 20u);
  EXPECT_GT(taken_stripes, direct_took);  // some took several stripes
  EXPECT_GT(dropped_stripes, 10u);
  EXPECT_LT(dropped_stripes, taken_stripes);
  // Everything in flight completes and the journal is flushed: the shards
  // alone are the object.
  for (const auto& tickets : uncommitted) {
    for (const auto& ticket : tickets) {
      ASSERT_EQ(0, j.committed(ticket, 0));
    }
  }
  for (const auto& read : reading) {
    j.read_end(read.oid, read.off, read.len);
  }
  while (!writing.empty()) {
    commit_direct();
  }
  while (!flushing.empty()) {
    commit_flush(flushing.begin());
  }
  j.seal();
  while (auto stripe = j.begin_flush(any())) {
    flushing.push_back(Flush{std::move(*stripe), issued++});
    commit_flush(flushing.begin());
  }
  EXPECT_FALSE(j.dirty());
  for (const auto& oid : objects) {
    EXPECT_EQ(expected.at(oid), physical.at(oid));
  }
}

TEST_P(JournalTest, AWriteAcrossAStripeBoundaryIsOneRecordPerStripe)
{
  // The journal's index, flushes and replay are per stripe, so a write that
  // lies in two stripes is appended as two records with consecutive
  // versions. The caller commits them together; each stripe then flushes on
  // its own and is covered by its own marker.
  auto j = journal();
  const auto a = object("a");
  // Three blocks: the last of stripe 0 and the first two of stripe 1.
  const uint64_t off = width() - block_size;
  const uint64_t len = 3 * block_size;
  const auto pieces = stripe_fragments(off, len, width());
  ASSERT_EQ(2u, pieces.size());
  EXPECT_EQ(std::make_pair(off, block_size), pieces[0]);
  EXPECT_EQ(std::make_pair(width(), 2 * block_size), pieces[1]);
  EXPECT_EQ(0, j.check_append(a, std::vector<uint64_t>{block_size, 2 * block_size}));
  const auto first = append(j, pieces[0].first, pieces[0].second, 'c', a, false);
  const auto second = append(j, pieces[1].first, pieces[1].second, 'c', a, false);
  EXPECT_EQ(first.sequence + 1, second.sequence);
  // Nothing of it is read before both are durable.
  EXPECT_EQ(ReadState::in_flight, j.read_state(a, 0, 1));
  EXPECT_EQ(ReadState::in_flight, j.read_state(a, 2 * width() - 1, 1));
  EXPECT_EQ(0, j.committed(first, 0));
  EXPECT_EQ(0, j.committed(second, 0));
  EXPECT_EQ(ReadState::ready, j.read_state(a, 0, 2 * width()));
  Overlay overlay;
  j.read_overlay(a, off, len, &overlay);
  EXPECT_EQ(3u, overlay.blocks.size());
  EXPECT_TRUE(overlay.covers(off, len));
  // The first stripe flushes; the second keeps its part, whose version is
  // the oldest the object still holds.
  auto stripe = j.begin_flush_object(a, {}, nullptr, 0, width());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(eversion_t(1, first.sequence), stripe->newest_version);
  EXPECT_EQ(eversion_t(1, second.sequence), j.oldest_version(a, 0));
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.dirty(a, 0, width()));
  EXPECT_EQ(ReadState::ready, j.read_state(a, width(), 1));
  // Pieces of other shapes: inside one stripe, a stripe's worth across a
  // boundary, several stripes.
  EXPECT_EQ(1u, stripe_fragments(block_size, block_size, width()).size());
  EXPECT_EQ(1u, stripe_fragments(width(), width(), width()).size());
  const auto wide = stripe_fragments(width() - block_size, width(), width());
  ASSERT_EQ(2u, wide.size());
  EXPECT_EQ(width() - block_size, wide[1].second);
  const auto many = stripe_fragments(block_size, 3 * width(), width());
  ASSERT_EQ(4u, many.size());
  EXPECT_EQ(block_size, many[3].second);
  EXPECT_TRUE(stripe_fragments(block_size, 0, width()).empty());
}

TEST_P(JournalTest, DirectWriteTakesTheStripesItOverlaps)
{
  // A write the journal does not take runs as an ordinary EC write and
  // takes the journaled stripes its range overlaps, as a flush takes one.
  // It supersedes the blocks it covers and carries the rest.
  auto j = journal();
  const auto a = object("a");
  append(j, 0, block_size, 'a', a);                        // stripe 0
  append(j, width(), width(), 'b', a);                     // stripe 1, complete
  append(j, 2 * width() + block_size, block_size, 'c', a); // stripe 2
  append(j, 5 * width(), block_size, 'e', a); // stripe 5, outside the write
  append(j, 0, block_size, 'o', object("other"));
  // The write, not block aligned: from 2 KiB before the end of stripe 0 to
  // the middle of stripe 2's block, 6 KiB into the stripe.
  const uint64_t off = width() - 2048;
  const uint64_t len = width() + 2048 + block_size + 2048;
  EXPECT_EQ(ReadState::ready, j.write_state(a, off, len));
  EXPECT_EQ(ReadState::clean, j.write_state(a, 3 * width(), 2 * width()));
  EXPECT_EQ(ReadState::clean, j.write_state(a, off, 0));
  std::vector<Stripe> taken;
  ASSERT_EQ(0, j.take_range(a, off, len, &taken));
  ASSERT_EQ(3u, taken.size());
  EXPECT_EQ(0u, taken[0].offset);
  EXPECT_EQ(width(), taken[1].offset);
  EXPECT_EQ(2 * width(), taken[2].offset);
  // Taken stripes are neither flushed nor read nor taken again meanwhile.
  EXPECT_EQ(ReadState::flushing, j.write_state(a, 0, 1));
  EXPECT_EQ(ReadState::flushing, j.read_state(a, width(), 1));
  EXPECT_FALSE(j.begin_flush_object(a, {}, nullptr, 0, 3 * width()));
  std::vector<Stripe> again;
  EXPECT_EQ(-EBUSY, j.take_range(a, 100, 10, &again));
  EXPECT_TRUE(again.empty());
  EXPECT_TRUE(j.dirty(a, width(), width()));
  // Stripe 1 is covered whole: nothing to write for it. Stripe 0 keeps its
  // block, which lies outside the write, and stripe 2 the 2 KiB of its
  // record past the write's end.
  interval_set<uint64_t> written;
  written.insert(off, len);
  uint64_t dropped = 0;
  auto runs = absorb_runs(taken, written, &dropped);
  EXPECT_EQ(1u, dropped);
  ASSERT_EQ(2u, runs.size());
  EXPECT_EQ(std::string(block_size, 'a'), runs.at(0).to_str());
  EXPECT_EQ(std::string(2048, 'c'),
            runs.at(2 * width() + block_size + 2048).to_str());
  // What the write leaves live: stripe 5's record, the object's fourth.
  // The stripes it took do not count, whatever range is asked for: their
  // records belong to a write that is in the log by now.
  EXPECT_EQ(eversion_t(1, 4), j.oldest_version_outside(a, off, len));
  EXPECT_EQ(eversion_t(1, 4), j.oldest_version_outside(a, 3 * width(), width()));
  EXPECT_EQ(eversion_t(1, 4), j.oldest_version_outside(a, 0, 0));
  EXPECT_EQ(eversion_t(1, 4), j.oldest_version(a));
  EXPECT_FALSE(j.oldest_version_outside(a, 0, Journal::whole_object));
  EXPECT_FALSE(j.oldest_version(a, 5 * width()));
  // A record journaled on a taken stripe meanwhile is newer than the write
  // and outlives it; the write's commit kills what it took.
  append(j, block_size, block_size, 'n', a);
  for (const auto& stripe : taken) {
    EXPECT_EQ(0, j.finish_flush(stripe, 0));
  }
  EXPECT_EQ(ReadState::ready, j.write_state(a, 0, 3 * width()));
  Overlay overlay;
  j.read_overlay(a, 0, 3 * width(), &overlay);
  ASSERT_EQ(1u, overlay.blocks.size());
  EXPECT_EQ(std::string(block_size, 'n'), overlay.blocks.at(block_size).to_str());
  EXPECT_TRUE(j.dirty(a, 5 * width(), 1));
  EXPECT_TRUE(j.dirty(object("other")));
}

TEST_P(JournalTest, DirectWriteWaitsForRecordsInFlightAndFlushes)
{
  // Taken before every holder has it, a record could be acknowledged after
  // the write that superseded it; and a stripe has one flush at a time.
  auto j = journal();
  const auto a = object("a");
  append(j, 0, block_size, 'a', a);
  const auto pending = append(j, width() + block_size, block_size, 'b', a, false);
  EXPECT_EQ(ReadState::ready, j.write_state(a, 0, width()));
  EXPECT_EQ(ReadState::in_flight, j.write_state(a, 0, 2 * width()));
  std::vector<Stripe> taken;
  EXPECT_EQ(-EBUSY, j.take_range(a, 0, 2 * width(), &taken));
  EXPECT_TRUE(taken.empty());
  // Nothing was taken: stripe 0 is still there for a read and the flusher.
  EXPECT_EQ(ReadState::ready, j.read_state(a, 0, width()));
  // Clean stripes and other objects do not hold a write up.
  EXPECT_EQ(0, j.take_range(a, 2 * width(), width(), &taken));
  EXPECT_EQ(0, j.take_range(object("b"), 0, 2 * width(), &taken));
  EXPECT_TRUE(taken.empty());
  EXPECT_EQ(0, j.committed(pending, 0));
  EXPECT_EQ(ReadState::ready, j.write_state(a, 0, 2 * width()));

  auto flushing = j.begin_flush_object(a, {}, nullptr, 0, width());
  ASSERT_TRUE(flushing);
  EXPECT_EQ(ReadState::flushing, j.write_state(a, width() - 1, 2));
  EXPECT_EQ(-EBUSY, j.take_range(a, width() - 1, 2, &taken));
  EXPECT_TRUE(taken.empty());
  EXPECT_EQ(ReadState::ready, j.write_state(a, width(), width()));
  EXPECT_EQ(0, j.finish_flush(*flushing, 0));
  EXPECT_EQ(0, j.take_range(a, width() - 1, 2, &taken));
  ASSERT_EQ(1u, taken.size()); // stripe 0 is clean now
  EXPECT_EQ(width(), taken[0].offset);
  EXPECT_EQ(0, j.finish_flush(taken[0], 0));
  EXPECT_FALSE(j.dirty(a));

  // A stripe replay adopted is taken like any other: a read waits for its
  // flush, and a direct write is its flush.
  auto r = journal();
  ASSERT_EQ(0, r.adopt(Record{a, eversion_t(3, 40), 16 * width(), 0,
                              bytes(block_size, 'x'), {}, 0}));
  EXPECT_EQ(ReadState::adopted, r.read_state(a, 0, 1));
  EXPECT_EQ(ReadState::ready, r.write_state(a, 0, 1));
  std::vector<Stripe> adopted;
  ASSERT_EQ(0, r.take_range(a, 0, width(), &adopted));
  ASSERT_EQ(1u, adopted.size());
  EXPECT_EQ(eversion_t(3, 40), adopted[0].newest_version);
  EXPECT_EQ(0, r.finish_flush(adopted[0], 0));
  EXPECT_FALSE(r.adopting());
  EXPECT_EQ(-EINVAL, r.take_range(a, 0, width(), nullptr));
}

TEST_P(JournalTest, TakenStripesKeepTheirSegmentUntilTheWriteCommits)
{
  // Until the direct write has committed, peering can still roll it back,
  // and replay then needs the records it took: their segment must not be
  // released, or its slot would be reused, before finish_flush.
  auto l = limits();
  l.segment_bytes = 2 * record_bytes(); // two one-block records per segment
  l.max_bytes = 3 * l.segment_bytes;
  l.max_segments = 3;
  Journal j(prefix(), l);
  const auto first = append(j, 0, block_size);           // segment A
  append(j, width(), block_size);                        // segment A, now full
  const auto third = append(j, 2 * width(), block_size); // segment B: A is closed
  ASSERT_NE(first.segment, third.segment);
  std::vector<Stripe> taken;
  ASSERT_EQ(0, j.take_range(object(), 0, 2 * width(), &taken));
  ASSERT_EQ(2u, taken.size());
  EXPECT_TRUE(j.resident(first.segment));
  EXPECT_EQ(0, j.finish_flush(taken[0], 0));
  EXPECT_TRUE(j.resident(first.segment)); // stripe 1's record still pins it
  EXPECT_EQ(0, j.finish_flush(taken[1], 0));
  EXPECT_EQ(first.segment, trim(j));
  EXPECT_TRUE(j.resident(third.segment));
  // A write that failed gives its stripes back to the journal.
  taken.clear();
  ASSERT_EQ(0, j.take_range(object(), 2 * width(), 1, &taken));
  ASSERT_EQ(1u, taken.size());
  EXPECT_EQ(-EIO, j.finish_flush(taken[0], -EIO));
  EXPECT_EQ(ReadState::ready, j.write_state(object(), 2 * width(), width()));
  auto stripe = j.begin_flush_object(object());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(2 * width(), stripe->offset);
}

TEST_P(JournalTest, AStripeBeingReadIsNotHandedOutForFlushing)
{
  // A flush must not write a stripe while a client read's shard reads of
  // it are in flight; other stripes and objects are not held up.
  auto j = journal();
  const auto a = object("a");
  append(j, 0, width(), 'x', a);          // complete
  append(j, width(), block_size, 'y', a); // incomplete
  j.read_begin(a, 100, 10);
  EXPECT_TRUE(j.reading({a, 0}));
  EXPECT_FALSE(j.reading({a, width()}));
  bool deferred = false;
  EXPECT_FALSE(j.begin_flush(false, {}, &deferred));
  EXPECT_TRUE(deferred);
  EXPECT_FALSE(j.begin_flush_object(a, {}, &deferred, 0, width()));
  EXPECT_TRUE(deferred);
  // A drain takes the other stripe and passes over the one being read.
  auto other = j.begin_flush(any(), {}, &deferred);
  ASSERT_TRUE(other);
  EXPECT_EQ(width(), other->offset);
  EXPECT_TRUE(deferred);
  EXPECT_FALSE(j.begin_flush(any()));
  // Every reader of the stripe has to end first.
  j.read_begin(a, 0, 1);
  j.read_end(a, 100, 10);
  EXPECT_TRUE(j.reading({a, 0}));
  EXPECT_FALSE(j.begin_flush(false));
  j.read_end(a, 0, 1);
  EXPECT_FALSE(j.reading({a, 0}));
  EXPECT_EQ(0u, j.stripes_being_read());
  auto stripe = j.begin_flush(false, {}, &deferred);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0u, stripe->offset);
  EXPECT_FALSE(deferred);
  // And a read does not start on a stripe that is being flushed.
  EXPECT_EQ(ReadState::flushing, j.read_state(a, 0, 1));
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_EQ(0, j.finish_flush(*other, 0));

  // A read counts on every stripe its range overlaps, records or not.
  j.read_begin(a, width() - 1, width() + 2);
  EXPECT_EQ(3u, j.stripes_being_read());
  EXPECT_TRUE(j.reading({a, 2 * width()}));
  EXPECT_FALSE(j.reading({a, 3 * width()}));
  EXPECT_FALSE(j.reading({object("b"), 0}));
  j.read_begin(a, 5, 0); // reads nothing
  EXPECT_EQ(3u, j.stripes_being_read());
  j.read_end(a, 5, 0);
  j.read_end(a, width() - 1, width() + 2);
  EXPECT_EQ(0u, j.stripes_being_read());
  // An interval change drops the reads in flight: they never end.
  j.read_begin(a, 0, 1);
  j.on_reset();
  EXPECT_EQ(0u, j.stripes_being_read());
}

INSTANTIATE_TEST_SUITE_P(EC4plus3And8plus3And12plus3, JournalTest,
                        testing::Values(4, 8, 12));

TEST(ECWriteJournal, AbsorbRunsKeepWhatADirectWriteLeavesOfItsStripes)
{
  constexpr uint64_t width = 32768;
  const auto stripe = [](uint64_t offset, uint64_t valid) {
    Stripe s;
    s.object = object();
    s.offset = offset;
    s.width = width;
    s.valid_bytes = valid;
    s.object_size = offset + valid;
    return s;
  };
  // Blocks a, b, c, the second to fourth of stripe 0, and d, its last one.
  auto s0 = stripe(0, width);
  s0.blocks.emplace(block_size, bytes(block_size, 'a'));
  s0.blocks.emplace(2 * block_size, bytes(block_size, 'b'));
  s0.blocks.emplace(3 * block_size, bytes(block_size, 'c'));
  s0.blocks.emplace(width - block_size, bytes(block_size, 'd'));
  // A write that is not block aligned: the second half of a with the first
  // of b, and 100 bytes inside c.
  constexpr uint64_t half = block_size / 2;
  interval_set<uint64_t> written;
  written.insert(block_size + half, block_size);
  written.insert(3 * block_size + 512, 100);
  uint64_t dropped = 0;
  auto runs = absorb_runs({s0}, written, &dropped);
  EXPECT_EQ(0u, dropped);
  ASSERT_EQ(4u, runs.size());
  EXPECT_EQ(std::string(half, 'a'), runs.at(block_size).to_str());
  EXPECT_EQ(std::string(half, 'b') + std::string(512, 'c'),
            runs.at(2 * block_size + half).to_str());
  EXPECT_EQ(std::string(block_size - 612, 'c'),
            runs.at(3 * block_size + 612).to_str());
  EXPECT_EQ(std::string(block_size, 'd'), runs.at(width - block_size).to_str());
  // Blocks covered whole are left out, and a run goes on into the next
  // stripe.
  auto s1 = stripe(width, width);
  s1.blocks.emplace(0, bytes(block_size, 'e'));
  s1.blocks.emplace(2 * block_size, bytes(block_size, 'f'));
  interval_set<uint64_t> one_block;
  one_block.insert(width + 2 * block_size, block_size);
  runs = absorb_runs({s0, s1}, one_block, &dropped);
  EXPECT_EQ(0u, dropped);
  ASSERT_EQ(2u, runs.size());
  EXPECT_EQ(std::string(block_size, 'a') + std::string(block_size, 'b') +
            std::string(block_size, 'c'), runs.at(block_size).to_str());
  EXPECT_EQ(std::string(block_size, 'd') + std::string(block_size, 'e'),
            runs.at(width - block_size).to_str());
  // A stripe adds nothing when the write covers every live block of it,
  // whether it covers the stripe or just those blocks.
  interval_set<uint64_t> blocks_only;
  blocks_only.insert(width, block_size);
  blocks_only.insert(width + 2 * block_size, block_size);
  EXPECT_TRUE(absorb_runs({s1}, blocks_only, &dropped).empty());
  EXPECT_EQ(1u, dropped);
  interval_set<uint64_t> whole;
  whole.insert(width - block_size, width + 2 * block_size);
  EXPECT_TRUE(absorb_runs({s1}, whole, &dropped).empty());
  EXPECT_EQ(2u, dropped);
  // An object's last stripe ends at EOF, short of the stripe width.
  auto tail = stripe(2 * width, 2 * block_size);
  tail.blocks.emplace(block_size, bytes(block_size, 't'));
  interval_set<uint64_t> to_eof;
  to_eof.insert(2 * width, 2 * block_size);
  EXPECT_TRUE(absorb_runs({tail}, to_eof, &dropped).empty());
  EXPECT_EQ(3u, dropped);
  interval_set<uint64_t> short_of_eof;
  short_of_eof.insert(2 * width, block_size + half);
  runs = absorb_runs({tail}, short_of_eof);
  ASSERT_EQ(1u, runs.size());
  EXPECT_EQ(std::string(half, 't'),
            runs.at(2 * width + block_size + half).to_str());
  EXPECT_TRUE(absorb_runs({}, written).empty());
}

TEST(ECWriteJournal, OverlayLaysBlocksOverShardDataAtAnyAlignment)
{
  // Blocks a and b, the second and third of the object, and c, its sixth
  // (b: one block's bytes).
  constexpr uint64_t b = block_size;
  Overlay overlay;
  overlay.blocks.emplace(b, bytes(b, 'a'));
  overlay.blocks.emplace(2 * b, bytes(b, 'b'));
  overlay.blocks.emplace(5 * b, bytes(b, 'c'));
  auto read = [&overlay](uint64_t offset, uint64_t length) {
    auto data = bytes(length, 'z');
    overlay.apply(offset, length, data);
    EXPECT_EQ(length, data.length());
    return data.to_str();
  };
  EXPECT_EQ(std::string(b, 'z') + std::string(b, 'a') +
            std::string(b, 'b') + std::string(2 * b, 'z') +
            std::string(b, 'c') + std::string(2 * b, 'z'), read(0, 8 * b));
  // Partial blocks at both ends, and a range inside one block.
  EXPECT_EQ(std::string(548, 'a') + std::string(452, 'b'), read(2 * b - 548, 1000));
  EXPECT_EQ(std::string(100, 'a'), read(b + 100, 100));
  EXPECT_EQ(std::string(24, 'z') + std::string(1, 'a'), read(b - 24, 25));
  EXPECT_EQ(std::string(1, 'b') + std::string(9, 'z'), read(3 * b - 1, 10));
  EXPECT_EQ(std::string(120, 'z') + std::string(b, 'c') +
            std::string(1, 'z'), read(5 * b - 120, b + 121));
  // No block in the range: the data is left alone.
  EXPECT_EQ(std::string(b, 'z'), read(0, b));
  EXPECT_EQ(std::string(2 * b, 'z'), read(3 * b, 2 * b));
  EXPECT_EQ(std::string(500, 'z'), read(6 * b, 500));

  EXPECT_TRUE(overlay.covers(b, b));
  EXPECT_TRUE(overlay.covers(b, 2 * b));
  EXPECT_TRUE(overlay.covers(2 * b - 548, 1000));
  EXPECT_TRUE(overlay.covers(5 * b + 100, 200));
  EXPECT_FALSE(overlay.covers(b, 2 * b + 1));
  EXPECT_FALSE(overlay.covers(b - 24, 100));
  EXPECT_FALSE(overlay.covers(2 * b, 4 * b)); // a hole between two blocks
  EXPECT_FALSE(overlay.covers(0, 1));
  EXPECT_FALSE(overlay.covers(b, 0));
  EXPECT_FALSE(Overlay().covers(0, b));
  // A covered range needs nothing from the shards.
  ceph::bufferlist alone;
  overlay.apply(2 * b - 548, 1000, alone);
  EXPECT_EQ(std::string(548, 'a') + std::string(452, 'b'), alone.to_str());
}

TEST(ECWriteJournal, OverlayJoinsTheBlocksOfOneRecord)
{
  // A 16 KiB record (four merged 4 KiB writes) is four blocks of one
  // buffer: a read of it gets that buffer back in one piece, not four.
  Limits limits;
  Journal j(prefix(), limits);
  Transaction t;
  Ticket ticket;
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 1), limits.stripe_width,
                           block_size, bytes(4 * block_size, 'r'), t, &ticket));
  ASSERT_EQ(0, j.committed(ticket, 0));
  Overlay overlay;
  j.read_overlay(object(), block_size, 4 * block_size, &overlay);
  ASSERT_EQ(4u, overlay.blocks.size());
  ceph::bufferlist data;
  overlay.apply(block_size, 4 * block_size, data);
  EXPECT_EQ(std::string(4 * block_size, 'r'), data.to_str());
  EXPECT_EQ(1u, data.get_num_buffers());
}

TEST(ECWriteJournal, AReadWaitsForTheFlushOfAdoptedStripesOnly)
{
  // A record replay adopted may be on fewer holders than one acknowledged
  // in this interval, so a read does not see it before its stripe has
  // flushed; begin_flush_object takes those stripes alone for the read.
  Limits limits;
  const uint64_t width = limits.stripe_width;
  Journal j(prefix(), limits);
  const Record replayed{object(), eversion_t(4, 9), 4 * width, block_size,
                        bytes(block_size, 'r'), utime_t(), 0};
  EXPECT_FALSE(j.begin_flush_object(object(), {}, nullptr, 0, 4 * width, true));
  ASSERT_EQ(0, j.adopt(replayed));
  Transaction t;
  Ticket ticket;
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(5, j.next_append_sequence()),
                           4 * width, width, bytes(block_size, 'n'), t, &ticket));
  ASSERT_EQ(0, j.committed(ticket, 0));
  EXPECT_EQ(ReadState::adopted, j.read_state(object(), 0, width));
  EXPECT_EQ(ReadState::adopted, j.read_state(object(), 0, 2 * width));
  EXPECT_EQ(ReadState::ready, j.read_state(object(), width, width));
  EXPECT_TRUE(j.must_flush({object(), 0}, false, std::nullopt));
  EXPECT_FALSE(j.must_flush({object(), width}, false, std::nullopt));

  auto stripe = j.begin_flush_object(object(), {}, nullptr, 0, 4 * width, true);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0u, stripe->offset);
  // The fresh stripe is left to gather records.
  EXPECT_FALSE(j.begin_flush_object(object(), {}, nullptr, 0, 4 * width, true));
  EXPECT_EQ(ReadState::adopted, j.read_state(object(), 0, width));
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_EQ(ReadState::clean, j.read_state(object(), 0, width));
  EXPECT_EQ(ReadState::ready, j.read_state(object(), 0, 2 * width));
  EXPECT_FALSE(j.adopting());
}

TEST(ECWriteJournal, RejectInvalidGeometryAndLimits)
{
  for (uint64_t width : {0u, 1024u, 4097u}) {
    Limits l;
    l.stripe_width = width;
    EXPECT_THROW(Journal(prefix(), l), std::invalid_argument);
  }
  Limits l;
  l.max_bytes = l.segment_bytes - 1;
  EXPECT_THROW(Journal(prefix(), l), std::invalid_argument);
  l = Limits();
  l.segment_bytes = record_alignment - 1; // cannot hold one aligned record
  EXPECT_THROW(Journal(prefix(), l), std::invalid_argument);
}

TEST(ECWriteJournal, TruncatedRecordCannotDecode)
{
  Record record{object(), eversion_t(1, 1), 32768, block_size, bytes(block_size, 'x')};
  ceph::bufferlist encoded;
  record.encode_header(encoded, 0);
  ceph::bufferlist truncated;
  truncated.substr_of(encoded, 0, encoded.length() - 1);
  Record decoded;
  EXPECT_NO_THROW(decoded.decode_header(encoded, bytes(record_alignment, 'x')));
  EXPECT_THROW(decoded.decode_header(truncated, bytes(record_alignment, 'x')),
               ceph::buffer::error);
}
} // anonymous namespace
TEST(ECWriteJournal, RecordDetectsCorruptionAndTornTails)
{
  Record record{object(), eversion_t(5, 9), 32768, 4096, bytes(4096, 'r')};
  ceph::bufferlist encoded;
  record.encode_header(encoded, 0);
  {
    Record decoded;
    decoded.decode_header(encoded, record.data);
    EXPECT_EQ(eversion_t(5, 9), decoded.version);
    EXPECT_EQ(record.data, decoded.data);
  }
  // A flipped payload byte fails the crc.
  auto payload_byte = record.data.to_str();
  payload_byte[payload_byte.size() - 1] ^= 1;
  ceph::bufferlist corrupt;
  corrupt.append(payload_byte);
  Record decoded;
  EXPECT_THROW(decoded.decode_header(encoded, corrupt), ceph::buffer::malformed_input);
  EXPECT_THROW(decoded.decode_header(encoded, bytes(1024, 'r')), ceph::buffer::error);
  auto old_format = encoded.to_str();
  old_format[3] = '4'; // the previous format's magic
  ceph::bufferlist old;
  old.append(old_format);
  EXPECT_THROW(decoded.decode_header(old, record.data), ceph::buffer::malformed_input);
  ceph::bufferlist zeros;
  zeros.append_zero(record_alignment);
  EXPECT_THROW(decoded.decode_header(zeros, record.data), ceph::buffer::malformed_input);
}

TEST_P(JournalTest, RequestsAreAnsweredUntilTheStripeIsMaterialized)
{
  // RADOS executes a request at most once: a resend is answered from the PG
  // log. A journaled write has no log entry, so the journal answers for it
  // until the write that materializes its stripe logs the request as one of
  // its extra_reqids (Stripe::reqids); then the journal lets go of it.
  auto j = journal();
  EXPECT_FALSE(j.find_request(reqid(1)));
  const auto first = append_request(j, 0, block_size, 1, 70, false);
  auto found = j.find_request(reqid(1));
  ASSERT_TRUE(found);
  EXPECT_EQ(70u, found->user_version);
  EXPECT_FALSE(found->durable); // a resend waits for the holders
  EXPECT_EQ(1u, j.request_count());
  EXPECT_EQ(0, j.committed(first, 0));
  found = j.find_request(reqid(1));
  ASSERT_TRUE(found);
  EXPECT_TRUE(found->durable);
  // A second write of the same block supersedes the first record. Its
  // request stays known: a resend of it must not run over the newer data.
  append_request(j, 0, block_size, 2, 71);
  append_request(j, block_size, block_size, 3, 72);
  EXPECT_TRUE(j.find_request(reqid(1)));
  EXPECT_TRUE(j.find_request(reqid(2)));
  EXPECT_EQ(3u, j.request_count());
  // The snapshot hands every request of the stripe, superseded ones too,
  // with the user version each reply reported, to the materializing write.
  auto stripe = j.begin_flush_object(object());
  ASSERT_TRUE(stripe);
  ASSERT_EQ(3u, stripe->reqids.size());
  EXPECT_EQ(std::make_pair(reqid(1), version_t(70)), stripe->reqids[0]);
  EXPECT_EQ(std::make_pair(reqid(2), version_t(71)), stripe->reqids[1]);
  EXPECT_EQ(std::make_pair(reqid(3), version_t(72)), stripe->reqids[2]);
  // Until the flush commits the journal still answers (the log entry is
  // not durable yet); a record appended meanwhile is not in the snapshot.
  append_request(j, 2 * block_size, block_size, 4, 73);
  EXPECT_TRUE(j.find_request(reqid(1)));
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.find_request(reqid(1)));
  EXPECT_FALSE(j.find_request(reqid(2)));
  EXPECT_FALSE(j.find_request(reqid(3)));
  EXPECT_TRUE(j.find_request(reqid(4)));
  EXPECT_EQ(1u, j.request_count());
  // The later record flushes with its own request only.
  stripe = j.begin_flush_object(object());
  ASSERT_TRUE(stripe);
  ASSERT_EQ(1u, stripe->reqids.size());
  EXPECT_EQ(reqid(4), stripe->reqids[0].first);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_EQ(0u, j.request_count());
}

TEST_P(JournalTest, AFailedFlushKeepsTheRequests)
{
  auto j = journal();
  append_request(j, 0, block_size, 1);
  auto stripe = j.begin_flush_object(object());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(-EIO, j.finish_flush(*stripe, -EIO));
  EXPECT_TRUE(j.find_request(reqid(1)));
  stripe = j.begin_flush_object(object());
  ASSERT_TRUE(stripe);
  ASSERT_EQ(1u, stripe->reqids.size());
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.find_request(reqid(1)));
}

TEST_P(JournalTest, ADirectWriteTakesTheRequestsOfItsStripes)
{
  // A write that takes journaled stripes (take_range) materializes them
  // itself, so it logs their requests: the snapshots carry them, and the
  // journal lets go of them when the write commits (finish_flush).
  auto j = journal();
  append_request(j, 0, block_size, 1, 70);
  append_request(j, width(), block_size, 2, 71);
  std::vector<Stripe> taken;
  ASSERT_EQ(0, j.take_range(object(), 0, 2 * width(), &taken));
  ASSERT_EQ(2u, taken.size());
  ASSERT_EQ(1u, taken[0].reqids.size());
  ASSERT_EQ(1u, taken[1].reqids.size());
  EXPECT_EQ(reqid(1), taken[0].reqids[0].first);
  EXPECT_EQ(reqid(2), taken[1].reqids[0].first);
  EXPECT_TRUE(j.find_request(reqid(1)));
  for (const auto& stripe : taken) {
    EXPECT_EQ(0, j.finish_flush(stripe, 0));
  }
  EXPECT_FALSE(j.find_request(reqid(1)));
  EXPECT_FALSE(j.find_request(reqid(2)));
}

TEST_P(JournalTest, AWriteAcrossAStripeBoundaryIsOneRequest)
{
  // Two records, one request: either stripe's write logs it, and the
  // journal answers until both have (the newest record stands for it).
  auto j = journal();
  const uint64_t off = width() - block_size;
  const auto pieces = stripe_fragments(off, 2 * block_size, width());
  ASSERT_EQ(2u, pieces.size());
  Transaction t;
  Placement placement;
  Ticket first, second;
  ASSERT_EQ(0, j.append(object(), eversion_t(1, j.next_append_sequence()),
                        16 * width(), pieces[0].first, bytes(block_size, 'c'),
                        &placement, &first, {}, reqid(9), 70));
  ASSERT_EQ(0, j.append(object(), eversion_t(1, j.next_append_sequence()),
                        16 * width(), pieces[1].first, bytes(block_size, 'c'),
                        &placement, &second, {}, reqid(9), 70));
  EXPECT_EQ(1u, j.request_count());
  EXPECT_EQ(0, j.committed(first, 0));
  EXPECT_EQ(0, j.committed(second, 0));
  ASSERT_TRUE(j.find_request(reqid(9)));
  EXPECT_TRUE(j.find_request(reqid(9))->durable);
  auto stripe = j.begin_flush_object(object(), {}, nullptr, 0, width());
  ASSERT_TRUE(stripe);
  ASSERT_EQ(1u, stripe->reqids.size());
  EXPECT_EQ(reqid(9), stripe->reqids[0].first);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  // The request's other record is still unmaterialized: the journal keeps
  // answering for it (the PG log could too).
  EXPECT_TRUE(j.find_request(reqid(9)));
  stripe = j.begin_flush_object(object(), {}, nullptr, width(), width());
  ASSERT_TRUE(stripe);
  ASSERT_EQ(1u, stripe->reqids.size()); // logged a second time, harmless
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.find_request(reqid(9)));
}

TEST_P(JournalTest, TheWindowForgetsTheOldestRequests)
{
  // The journal remembers the request_window newest requests, as the PG log
  // keeps its dup window: an older resend runs again, as it does on a logged
  // pool. The materializing write logs only the requests still remembered.
  Limits l = limits();
  l.request_window = 4;
  Journal j(prefix(), l);
  for (ceph_tid_t tid = 1; tid <= 5; ++tid) {
    append_request(j, 0, block_size, tid, 70 + tid);
  }
  EXPECT_FALSE(j.find_request(reqid(1))); // the oldest left the window
  for (ceph_tid_t tid = 2; tid <= 5; ++tid) {
    EXPECT_TRUE(j.find_request(reqid(tid)));
  }
  EXPECT_EQ(4u, j.request_count());
  EXPECT_EQ(4u, j.window_size());
  auto stripe = j.begin_flush_object(object());
  ASSERT_TRUE(stripe);
  ASSERT_EQ(4u, stripe->reqids.size());
  EXPECT_EQ(reqid(2), stripe->reqids.front().first);
  EXPECT_EQ(reqid(5), stripe->reqids.back().first);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_EQ(0u, j.request_count());
  // A request that fell out and is resent runs again as a new record, which
  // the journal then remembers as that request's record.
  append_request(j, 0, block_size, 1, 90);
  auto found = j.find_request(reqid(1));
  ASSERT_TRUE(found);
  EXPECT_EQ(90u, found->user_version);
}

TEST_P(JournalTest, ADeadSegmentKeepsItsSlotWhileItsRequestsAreInTheWindow)
{
  // A replaying primary rebuilds the window from the records in the slots,
  // so a segment whose records are all dead keeps its slot until none of
  // their requests is in the window any more: then a resend after a
  // failover meets exactly what the old primary knew. The pinned segment
  // counts as free room (pressure, room_for).
  Limits l = limits();
  l.segment_bytes = 2 * record_bytes();
  l.max_segments = 8;
  l.max_bytes = l.max_segments * l.segment_bytes;
  l.request_window = 6;
  Journal j(prefix(), l);
  // Two records fill a segment; the next write of the same block kills both.
  const auto first = append_request(j, 0, block_size, 1);
  append_request(j, 0, block_size, 2);
  append_request(j, 0, block_size, 3); // segment 2 opens; segment 1 is dead
  EXPECT_EQ(2u, j.segment_count());
  EXPECT_TRUE(j.resident(first.segment));
  EXPECT_EQ(1u, j.pinned_segments());
  EXPECT_FALSE(j.pressure());
  EXPECT_TRUE(j.find_request(reqid(1)));
  // Requests 1 and 2 leave the window after six newer ones.
  for (ceph_tid_t tid = 4; tid <= 8; ++tid) {
    append_request(j, 0, block_size, tid);
  }
  EXPECT_FALSE(j.find_request(reqid(1)));
  EXPECT_FALSE(j.find_request(reqid(2)));
  EXPECT_TRUE(j.find_request(reqid(3)));
  EXPECT_FALSE(j.resident(first.segment)); // released with its last request
  EXPECT_EQ(6u, j.window_size());
}

TEST_P(JournalTest, LoggedRequestsUnpinTheirSegmentAtOnce)
{
  // Once the materializing write has logged the requests, the PG log
  // answers for them: their segment is free, whatever the window holds.
  Limits l = limits();
  l.segment_bytes = 2 * record_bytes();
  l.max_segments = 8;
  l.max_bytes = l.max_segments * l.segment_bytes;
  Journal j(prefix(), l);
  const auto first = append_request(j, 0, block_size, 1);
  append_request(j, 0, block_size, 2);
  append_request(j, 0, block_size, 3);
  EXPECT_TRUE(j.resident(first.segment));
  EXPECT_EQ(1u, j.pinned_segments());
  auto stripe = j.begin_flush_object(object());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(3u, stripe->reqids.size());
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.resident(first.segment));
  EXPECT_EQ(0u, j.pinned_segments());
  EXPECT_EQ(0u, j.request_count());
  EXPECT_EQ(3u, j.window_size()); // the window itself keeps the order
}

TEST_P(JournalTest, AnAppendTakesAPinnedSlotWhenNoneIsFree)
{
  // Large records and a long window can pin more dead segments than there
  // are slots: the log stays live by forgetting the oldest pinned segment's
  // requests, the window's own semantics applied early.
  Limits l = limits();
  l.segment_bytes = 2 * record_bytes();
  l.max_segments = 2;
  l.max_bytes = l.max_segments * l.segment_bytes;
  l.request_window = 100;
  Journal j(prefix(), l);
  const auto first = append_request(j, 0, block_size, 1);
  append_request(j, 0, block_size, 2);
  append_request(j, 0, block_size, 3); // the second slot; segment 1 pinned dead
  EXPECT_EQ(1u, j.pinned_segments());
  EXPECT_EQ(0, j.check_append(object(), block_size));
  append_request(j, 0, block_size, 4); // fills segment 2
  // Segment 3 needs a slot: only a pinned dead one is there; it is taken
  // and requests 1 and 2 are forgotten.
  EXPECT_EQ(0, j.check_append(object(), block_size));
  append_request(j, 0, block_size, 5);
  EXPECT_FALSE(j.resident(first.segment));
  EXPECT_FALSE(j.find_request(reqid(1)));
  EXPECT_FALSE(j.find_request(reqid(2)));
  EXPECT_TRUE(j.find_request(reqid(3)));
  EXPECT_TRUE(j.find_request(reqid(5)));
  EXPECT_EQ(2u, j.segment_count());
}

TEST_P(JournalTest, ObjectMetaFollowsTheNewestRecordUntilTheLastFlush)
{
  // A journaled write takes a user version and carries the client's mtime
  // without a log entry; the primary lays both over the object info it
  // reads from disk while the object has records. The write that
  // materializes the last of them persists them, and the newest user
  // version handed out is remembered for the PG's logged writes to allocate
  // above.
  auto j = journal();
  EXPECT_FALSE(j.object_meta(object()));
  EXPECT_EQ(0u, j.newest_user_version());
  Transaction t;
  Placement placement;
  Ticket ticket;
  ASSERT_EQ(0, j.append(object(), eversion_t(1, j.next_append_sequence()),
                        16 * width(), 0, bytes(block_size, 'a'), &placement,
                        &ticket, utime_t(100, 0), reqid(1), 70));
  EXPECT_EQ(0, j.committed(ticket, 0));
  auto meta = j.object_meta(object());
  ASSERT_TRUE(meta);
  EXPECT_EQ(70u, meta->user_version);
  EXPECT_EQ(utime_t(100, 0), meta->mtime);
  ASSERT_EQ(0, j.append(object(), eversion_t(1, j.next_append_sequence()),
                        16 * width(), width(), bytes(block_size, 'b'),
                        &placement, &ticket, utime_t(200, 0), reqid(2), 71));
  EXPECT_EQ(0, j.committed(ticket, 0));
  // The last write's timestamp stands even when it is older (a logged pool
  // does the same); the version always grows.
  ASSERT_EQ(0, j.append(object(), eversion_t(1, j.next_append_sequence()),
                        16 * width(), 2 * width(), bytes(block_size, 'c'),
                        &placement, &ticket, utime_t(150, 0), reqid(3), 72));
  EXPECT_EQ(0, j.committed(ticket, 0));
  meta = j.object_meta(object());
  ASSERT_TRUE(meta);
  EXPECT_EQ(72u, meta->user_version);
  EXPECT_EQ(utime_t(150, 0), meta->mtime);
  EXPECT_EQ(72u, j.newest_user_version());
  EXPECT_FALSE(j.object_meta(object("b")));
  // The overlay stays while any stripe of the object has records.
  auto stripe = j.begin_flush_object(object(), {}, nullptr, 0, width());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_TRUE(j.object_meta(object()));
  while ((stripe = j.begin_flush_object(object()))) {
    EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  }
  EXPECT_FALSE(j.dirty(object()));
  EXPECT_FALSE(j.object_meta(object()));
  EXPECT_EQ(72u, j.newest_user_version()); // the sequence goes on from here
  // Replay rebuilds it from the records; an interval change drops it.
  Record replayed{object(), eversion_t(4, 9), 16 * width(), 0,
                  bytes(block_size, 'r'), utime_t(300, 0), 0, reqid(9), 90};
  ASSERT_EQ(0, j.adopt(replayed));
  meta = j.object_meta(object());
  ASSERT_TRUE(meta);
  EXPECT_EQ(90u, meta->user_version);
  EXPECT_EQ(utime_t(300, 0), meta->mtime);
  EXPECT_EQ(90u, j.newest_user_version());
  j.on_reset();
  EXPECT_FALSE(j.object_meta(object()));
  EXPECT_EQ(0u, j.newest_user_version());
}

TEST(ECWriteJournal, HeaderCarriesTheClientRequest)
{
  // The request id and the user version the reply reported ride in the
  // header under the crc; a header from before them (version 2) decodes
  // with neither.
  const osd_reqid_t reqid(entity_name_t::CLIENT(4711), 2, 99);
  Record record{object(), eversion_t(5, 9), 32768, 4096, bytes(4096, 'r'),
                utime_t(), no_slot, reqid, 1234};
  ceph::bufferlist encoded;
  record.encode_header(encoded, 8192);
  Record decoded;
  EXPECT_EQ(8192u, decoded.decode_header(encoded, record.data, 8192));
  EXPECT_EQ(reqid, decoded.reqid);
  EXPECT_EQ(1234u, decoded.user_version);
  Record fields;
  fields.decode_fields(encoded);
  EXPECT_EQ(reqid, fields.reqid);
  EXPECT_EQ(1234u, fields.user_version);
  // The header's size depends on the object's name only, as the budget
  // arithmetic assumes (Journal::record_bytes).
  Record other{object(), eversion_t(1, 1), 32768, 0, bytes(4096, 'r')};
  ceph::bufferlist encoded_other;
  other.encode_header(encoded_other, 0);
  EXPECT_EQ(encoded.length(), encoded_other.length());
}

TEST(ECWriteJournal, AdoptRebuildsTheRequests)
{
  // Replay reads the records back from the slots and adopts the live ones:
  // their requests are known again, durable, until their stripe flushes.
  Limits limits;
  const uint64_t width = limits.stripe_width;
  Journal j(prefix(), limits);
  const osd_reqid_t reqid(entity_name_t::CLIENT(5), 0, 42);
  Record replayed{object(), eversion_t(4, 9), 4 * width, block_size,
                  bytes(block_size, 'r'), utime_t(), 0, reqid, 77};
  ASSERT_EQ(0, j.adopt(replayed));
  auto found = j.find_request(reqid);
  ASSERT_TRUE(found);
  EXPECT_TRUE(found->durable);
  EXPECT_EQ(77u, found->user_version);
  auto stripe = j.begin_flush(false);
  ASSERT_TRUE(stripe);
  ASSERT_EQ(1u, stripe->reqids.size());
  EXPECT_EQ(std::make_pair(reqid, version_t(77)), stripe->reqids[0]);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.find_request(reqid));
  // An interval change drops them with everything else.
  Record again = replayed;
  ASSERT_EQ(0, j.adopt(again));
  EXPECT_TRUE(j.find_request(reqid));
  j.on_reset();
  EXPECT_FALSE(j.find_request(reqid));
  EXPECT_EQ(0u, j.request_count());
}

TEST(ECWriteJournal, HeaderCarriesTheClientMtime)
{
  // Journaled writes leave the object's own mtime alone until the flush,
  // which applies the newest client mtime among the stripe's records: the
  // header carries it, under the crc like every other field.
  const utime_t mtime(1700000000, 123456);
  Record record{object(), eversion_t(5, 9), 32768, 4096, bytes(4096, 'r'), mtime};
  ceph::bufferlist encoded;
  record.encode_header(encoded, 8192);
  Record decoded;
  EXPECT_EQ(8192u, decoded.decode_header(encoded, record.data, 8192));
  EXPECT_EQ(mtime, decoded.mtime);
  EXPECT_EQ(eversion_t(5, 9), decoded.version);
  Record fields;
  const auto [offset, length] = fields.decode_fields(encoded);
  EXPECT_EQ(8192u, offset);
  EXPECT_EQ(4096u, length);
  EXPECT_EQ(mtime, fields.mtime);
  EXPECT_EQ(0u, fields.data.length());
}

TEST(ECWriteJournal, ListingWithoutSizeStillRequiresTiling)
{
  // Another holder's slot listing comes without that slot's size. The
  // headers must still place their payloads back to back from offset 0;
  // only the check against the slot's end is skipped.
  const Record first{object(), eversion_t(1, 1), 32768, 0, bytes(4096, 'a')};
  const Record second{object(), eversion_t(1, 2), 32768, 4096, bytes(4096, 'b')};
  SlotHeaders headers;
  first.encode_header(headers[record_header_key(0)], 0);
  second.encode_header(headers[record_header_key(4096)], 4096);
  const pg_t pgid(0, 1);
  const auto entries = decode_slot_headers(3, std::nullopt, headers, pgid, 0);
  ASSERT_EQ(2u, entries.size());
  EXPECT_EQ(eversion_t(1, 2), entries[1].header.version);
  EXPECT_EQ(3u, entries[1].locator.slot);
  EXPECT_EQ(4096u, entries[1].locator.slot_offset);
  SlotHeaders gap;
  first.encode_header(gap[record_header_key(0)], 0);
  second.encode_header(gap[record_header_key(8192)], 8192);
  EXPECT_THROW(decode_slot_headers(3, std::nullopt, gap, pgid, 0),
               ceph::buffer::malformed_input);
  // With the size, a listing that ends short of it is rejected as before.
  EXPECT_THROW(decode_slot_headers(3, 3 * 4096, headers, pgid, 0),
               ceph::buffer::malformed_input);
}

TEST(ECWriteJournal, DetachedHeaderChecksPayloadAndLocation)
{
  Record record{object(), eversion_t(5, 11), 32768, 4096,
                bytes(4096, 'd')};
  ceph::bufferlist header;
  record.encode_header(header, 8192);
  EXPECT_LT(header.length(), record_alignment);
  RecordLocator locator{2, 8192, 4096, 4096, header};
  auto decoded = locator.decode_record(record.data);
  EXPECT_EQ(record.object, decoded.object);
  EXPECT_EQ(record.version, decoded.version);
  EXPECT_TRUE(record.data.contents_equal(decoded.data));
  EXPECT_THROW(locator.decode_record(bytes(4096, 'x')), ceph::buffer::error);
  EXPECT_THROW(locator.decode_record(bytes(4095, 'd')), ceph::buffer::error);
  locator.slot_offset = 4096;
  EXPECT_THROW(locator.decode_record(record.data), ceph::buffer::error);
  locator.slot_offset = 8192;
  auto corrupt = header.to_str();
  corrupt.back() ^= 1;
  locator.header.clear();
  locator.header.append(corrupt);
  EXPECT_THROW(locator.decode_record(record.data), ceph::buffer::error);
}

TEST(ECWriteJournal, DetachedSlotHeadersAreRequiredAndFilterOwnership)
{
  const pg_t child(2, 1);
  auto child_object = object("child");
  child_object.set_hash(child.ps());
  Record first{object(), eversion_t(1, 1), 32768, 0, bytes(block_size, 'a')};
  Record second{child_object, eversion_t(1, 2), 32768, 4096,
                bytes(4096, 'b')};
  ceph::bufferlist payload = first.data;
  payload.append_zero(record_alignment - payload.length());
  payload.append(second.data);
  SlotHeaders headers;
  first.encode_header(headers[record_header_key(0)], 0);
  second.encode_header(headers[record_header_key(4096)], 4096);
  const auto records = decode_slot_records(payload, headers, child, 2);
  ASSERT_EQ(1u, records.size());
  EXPECT_EQ(child_object, records.front().object);
  EXPECT_TRUE(second.data.contents_equal(records.front().data));
  EXPECT_THROW(decode_slot_records(payload, {}, child, 2), ceph::buffer::error);
  EXPECT_TRUE(decode_slot_records({}, {}, child, 2).empty());
  auto missing = headers;
  missing.erase(record_header_key(0));
  EXPECT_THROW(decode_slot_records(payload, missing, child, 2), ceph::buffer::error);
  missing = headers;
  missing.erase(record_header_key(4096));
  EXPECT_THROW(decode_slot_records(payload, missing, child, 2), ceph::buffer::error);
  payload.append_zero(record_alignment);
  EXPECT_THROW(decode_slot_records(payload, headers, child, 2), ceph::buffer::error);
}

TEST(ECWriteJournal, EveryHolderGetsTheSameBytesAtTheSameOffset)
{
  Limits l;
  Journal j(prefix(), l);
  Placement placement;
  Ticket ticket;
  ASSERT_EQ(0, j.append(object(), eversion_t(1, 1), 16 * l.stripe_width, 4096,
                        bytes(4096, 'h'), &placement, &ticket));
  std::vector<ceph::bufferlist> written;
  for (unsigned shard : {0u, 8u, 9u, 10u}) {
    Transaction t;
    const coll_t coll(spg_t(pg_t(0, 1), shard_id_t(shard)));
    j.emit(placement, coll, shard_id_t(shard), t);
    auto it = t.begin();
    auto remove = it.decode_op();
    EXPECT_EQ(Transaction::OP_REMOVE, remove->op); // first record of the slot
    auto write = it.decode_op();
    ASSERT_EQ(Transaction::OP_WRITE, write->op);
    EXPECT_EQ(coll, it.get_cid(write->cid));
    const auto oid = it.get_oid(write->oid);
    EXPECT_EQ(shard_id_t(shard), oid.shard_id);
    EXPECT_EQ(j.slot_object(placement.slot, shard_id_t(shard)), oid);
    EXPECT_EQ(placement.offset, write->off);
    ceph::bufferlist data;
    it.decode_bl(data);
    written.push_back(data);
    EXPECT_EQ(4096u, data.length());
    ASSERT_TRUE(it.have_op());
    const auto metadata = it.decode_op();
    EXPECT_EQ(Transaction::OP_OMAP_SETKEYS, metadata->op);
    EXPECT_EQ(oid, it.get_oid(metadata->oid));
    SlotHeaders headers;
    it.decode_attrset(headers);
    EXPECT_TRUE(headers.at(record_header_key(placement.offset)).contents_equal(
      placement.header));
    EXPECT_EQ(1u, headers.size()); // every header names its record's magic
    EXPECT_FALSE(it.have_op());
  }
  for (const auto& data : written) {
    EXPECT_EQ(written.front(), data);
  }
}

TEST(ECWriteJournal, ReleasedSlotIsReusedAndReset)
{
  Limits l;
  l.segment_bytes = 3 * record_alignment; // two one-block records and headers
  l.max_bytes = 2 * l.segment_bytes;
  l.max_segments = 2;
  Journal j(prefix(), l);
  auto place = [&](uint64_t off, uint64_t version) {
    Placement placement;
    Ticket ticket;
    EXPECT_EQ(0, j.append(object(), eversion_t(1, version), 16 * l.stripe_width,
                          off, bytes(block_size, 'x'), &placement, &ticket));
    EXPECT_EQ(0, j.committed(ticket, 0));
    return placement;
  };
  const auto a = place(0, 1);
  const auto b = place(l.stripe_width, 2);
  const auto c = place(2 * l.stripe_width, 3); // second segment, other slot
  EXPECT_TRUE(a.reset);
  EXPECT_FALSE(b.reset);
  EXPECT_EQ(a.slot, b.slot);
  EXPECT_EQ(record_alignment, b.offset);
  EXPECT_TRUE(c.reset);
  EXPECT_NE(a.slot, c.slot);
  // Flush both stripes of the first segment: it dies and its slot is free.
  for (int i = 0; i < 2; ++i) {
    auto stripe = j.begin_flush(true);
    ASSERT_TRUE(stripe);
    ASSERT_EQ(0, j.finish_flush(*stripe, 0));
  }
  j.seal();
  EXPECT_EQ(1u, j.segment_count()); // only the one holding c
  const auto d = place(3 * l.stripe_width, 4);
  EXPECT_EQ(a.slot, d.slot); // the released slot, cleared by this append
  EXPECT_EQ(0u, d.offset);
  EXPECT_TRUE(d.reset);
}

TEST(ECWriteJournal, CheckAppendPredictsAppendWithoutChangingState)
{
  Limits l;
  l.segment_bytes = 3 * record_alignment; // two one-block records and headers
  l.max_bytes = 2 * l.segment_bytes;
  l.max_segments = 2;
  Journal j(prefix(), l);
  EXPECT_EQ(-EINVAL, j.check_append(object(), 0));
  EXPECT_EQ(-EINVAL, j.check_append(object(), block_size + block_size / 2));
  EXPECT_EQ(0, j.check_append(object(), block_size));
  EXPECT_EQ(0, j.check_append(object(), 2 * block_size));
  EXPECT_EQ(-E2BIG, j.check_append(object(), 4 * block_size));
  std::vector<Ticket> tickets;
  for (uint64_t n = 0; n < 4; ++n) {
    ASSERT_EQ(0, j.check_append(object(), block_size));
    EXPECT_EQ(n, j.records()); // no state change
    Placement placement;
    Ticket ticket;
    ASSERT_EQ(0, j.append(object(), eversion_t(1, n + 1), 16 * l.stripe_width,
                          n * l.stripe_width, bytes(block_size, 'x'), &placement,
                          &ticket));
    tickets.push_back(ticket);
  }
  EXPECT_EQ(-EAGAIN, j.check_append(object(), block_size));
  Placement placement;
  Ticket ticket;
  EXPECT_EQ(-EAGAIN, j.append(object(), eversion_t(1, 9), 16 * l.stripe_width,
                              0, bytes(block_size, 'x'), &placement, &ticket));
}

TEST(ECWriteJournal, CheckAppendAnswersForEveryPieceOfAWrite)
{
  // The pieces of a write across a stripe boundary are appended one after
  // the other: the answer covers them all, including a segment that a
  // later one has to open, and what it predicts is what append does.
  auto l = Limits();
  Journal probe(prefix(), l);
  Transaction t;
  Ticket ticket;
  ASSERT_EQ(0, append_copy(probe, object(), eversion_t(1, 1), 16 * l.stripe_width,
                           0, bytes(block_size, 'x'), t, &ticket));
  const uint64_t one = probe.bytes(); // one one-block record
  l.segment_bytes = 3 * one;
  l.max_bytes = 2 * l.segment_bytes;
  l.max_segments = 2;
  Journal j(prefix(), l);
  using lengths = std::vector<uint64_t>;
  constexpr uint64_t b = block_size;
  EXPECT_EQ(0, j.check_append(object(), lengths{}));
  EXPECT_EQ(0, j.check_append(object(), lengths{b, 2 * b}));
  EXPECT_EQ(-EINVAL, j.check_append(object(), lengths{b, 0}));
  EXPECT_EQ(-EINVAL, j.check_append(object(), lengths{b + b / 2, b}));
  EXPECT_EQ(-EINVAL, j.check_append(object(), lengths{b, 2 * l.stripe_width}));
  EXPECT_EQ(-E2BIG, j.check_append(object(), lengths{b, 4 * b}));
  EXPECT_EQ(0u, j.records()); // no state change
  uint64_t version = 1;
  const auto add = [&](uint64_t stripe) {
    Placement placement;
    Ticket ticket;
    const int r = j.append(object(), eversion_t(1, version), 16 * l.stripe_width,
                           stripe * l.stripe_width, bytes(b, 'x'),
                           &placement, &ticket);
    version += r == 0;
    return r;
  };
  // Two records in the first segment: one more fits it, the next opens the
  // second segment, which has room for three.
  ASSERT_EQ(0, add(0));
  ASSERT_EQ(0, add(1));
  EXPECT_EQ(0, j.check_append(object(), lengths{b, b}));
  EXPECT_EQ(0, j.check_append(object(), lengths{b, b, b, b}));
  EXPECT_EQ(-EAGAIN, j.check_append(object(), lengths{b, b, b, b, b}));
  ASSERT_EQ(0, add(2));
  ASSERT_EQ(0, add(3)); // opens the last segment
  ASSERT_EQ(0, add(4));
  // One record's room is left: a single record goes, a pair does not, and
  // append agrees.
  EXPECT_EQ(0, j.check_append(object(), b));
  EXPECT_EQ(0, j.check_append(object(), lengths{b}));
  EXPECT_EQ(-EAGAIN, j.check_append(object(), lengths{b, b}));
  ASSERT_EQ(0, add(5));
  EXPECT_EQ(-EAGAIN, j.check_append(object(), lengths{b}));
  EXPECT_EQ(-EAGAIN, add(6));
}

TEST(ECWriteJournal, FlushReportsTheNewestRecordVersion)
{
  Limits l;
  Journal j(prefix(), l);
  Transaction t;
  Ticket a, b, c;
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(4, 7), 16 * l.stripe_width,
                           0, bytes(block_size, 'a'), t, &a));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(4, 9), 16 * l.stripe_width,
                           block_size, bytes(block_size, 'b'), t, &b));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(4, 8), 16 * l.stripe_width,
                           l.stripe_width, bytes(block_size, 'c'), t, &c));
  for (auto ticket : {a, b, c}) {
    ASSERT_EQ(0, j.committed(ticket, 0));
  }
  auto stripe = j.begin_flush_object(object(), {}, nullptr, 0, 1);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0u, stripe->offset);
  EXPECT_EQ(b.sequence, stripe->newest_sequence);
  EXPECT_EQ(eversion_t(4, 9), stripe->newest_version);
}

TEST(ECWriteJournal, ResetDropsLiveRecordsForReplay)
{
  Limits l;
  Journal j(prefix(), l);
  Transaction t;
  Ticket ticket;
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 1), 16 * l.stripe_width,
                           0, bytes(block_size, 'a'), t, &ticket));
  EXPECT_TRUE(j.dirty());
  // The records stay on the holders' slots; memory is simply dropped.
  j.on_reset();
  EXPECT_FALSE(j.dirty());
  EXPECT_TRUE(j.empty());
  EXPECT_EQ(0u, j.live_blocks());
  EXPECT_EQ(0u, j.records());
}

TEST(ECWriteJournal, AdoptKeepsRecordsInTheirSlotsAndFreesThemAsTheyFlush)
{
  Limits l;
  Journal j(prefix(), l);
  const uint64_t size = 16 * l.stripe_width;
  // Two records of stripe 0 overlap at its second block (an older two-block
  // record and a newer one-block one); the newer version wins even when
  // adopted first, as when slots are scanned out of order. They were read
  // from slots 1 and 0; a record of stripe 1 from slot 1 as well.
  Record newer{object(), eversion_t(2, 20), size, block_size,
               bytes(block_size, 'N'), {}, 1};
  Record older{object(), eversion_t(2, 10), size, 0,
               bytes(2 * block_size, 'o'), {}, 0};
  Record other{object(), eversion_t(2, 15), size, l.stripe_width,
               bytes(2 * block_size, 's'), {}, 1};
  EXPECT_EQ(-EINVAL, j.adopt(Record{object(), eversion_t(2, 1), size, 0,
                                    bytes(block_size, 'x')})); // no slot
  EXPECT_EQ(-EINVAL, j.adopt(Record{object(), eversion_t(2, 1), size, 0,
                                    bytes(block_size, 'x'), {}, l.max_segments}));
  ASSERT_EQ(0, j.adopt(newer));
  ASSERT_EQ(0, j.adopt(older));
  ASSERT_EQ(0, j.adopt(other));
  EXPECT_TRUE(j.adopting());
  EXPECT_EQ(2u, j.adopted_segments());
  // Block 0 from older, block 1 from newer, two of other.
  EXPECT_EQ(2u + 2u, j.live_blocks());
  EXPECT_EQ(3u, j.records());
  EXPECT_LT(0u, j.bytes()); // charged like appends
  // The other slots take new records at once, the lowest free one first.
  EXPECT_EQ(0, j.check_append(object(), block_size));
  Placement placement;
  Ticket ticket;
  ASSERT_EQ(0, j.append(object(), eversion_t(2, 30), size, 2 * l.stripe_width,
                        bytes(block_size, 'x'), &placement, &ticket));
  EXPECT_EQ(2u, placement.slot);
  EXPECT_TRUE(placement.reset);
  ASSERT_EQ(0, j.committed(ticket, 0));
  // Adopted stripes flush without a drain, pressure or age, oldest first.
  EXPECT_TRUE(j.adopted_ready());
  auto first = j.begin_flush(false);
  ASSERT_TRUE(first);
  EXPECT_EQ(0u, first->offset);
  EXPECT_EQ(std::string(block_size, 'o'), first->blocks.at(0).to_str());
  EXPECT_EQ(std::string(block_size, 'N'), first->blocks.at(block_size).to_str());
  EXPECT_EQ(eversion_t(2, 20), first->newest_version);
  ASSERT_EQ(0, j.finish_flush(*first, 0));
  // Slot 0 held nothing else: free again. Slot 1 still holds other.
  EXPECT_TRUE(j.adopting());
  EXPECT_EQ(1u, j.adopted_segments());
  auto second = j.begin_flush(false);
  ASSERT_TRUE(second);
  EXPECT_EQ(l.stripe_width, second->offset);
  ASSERT_EQ(0, j.finish_flush(*second, 0));
  EXPECT_FALSE(j.adopting());
  EXPECT_FALSE(j.adopted_ready());
  // The fresh stripe is not affected: it waits for age or pressure.
  EXPECT_FALSE(j.begin_flush(false));
  EXPECT_TRUE(j.dirty(object(), 2 * l.stripe_width, l.stripe_width));
  // The next segment takes the lowest slot freed, and resets it.
  j.seal();
  ASSERT_EQ(0, j.append(object(), eversion_t(2, 31), size, 3 * l.stripe_width,
                        bytes(block_size, 'y'), &placement, &ticket));
  EXPECT_EQ(0u, placement.slot);
  EXPECT_TRUE(placement.reset);
}

TEST(ECWriteJournal, HeldSlotsStayOutOfUseUntilReleasedOrAdopted)
{
  Limits l;
  Journal j(prefix(), l);
  const uint64_t size = 16 * l.stripe_width;
  // Replay found two records in slot 0 and one in slot 1 that it cannot
  // place yet (a fetch, a recovery): neither slot may take a new segment.
  j.hold_slot(0);
  j.hold_slot(0);
  j.hold_slot(1);
  EXPECT_EQ(2u, j.held_slots());
  Placement placement;
  Ticket ticket;
  ASSERT_EQ(0, j.append(object(), eversion_t(3, 1), size, 0,
                        bytes(block_size, 'a'), &placement, &ticket));
  EXPECT_EQ(2u, placement.slot);
  ASSERT_EQ(0, j.committed(ticket, 0));
  j.seal();
  ASSERT_EQ(0, j.append(object(), eversion_t(3, 2), size, l.stripe_width,
                        bytes(block_size, 'b'), &placement, &ticket));
  EXPECT_EQ(3u, placement.slot);
  ASSERT_EQ(0, j.committed(ticket, 0));
  j.seal();
  EXPECT_EQ(-EAGAIN, j.check_append(object(), block_size));
  // Slot 1's record was judged dead: the slot is free again.
  j.release_hold(1);
  EXPECT_EQ(1u, j.held_slots());
  EXPECT_EQ(0, j.check_append(object(), block_size));
  ASSERT_EQ(0, j.append(object(), eversion_t(3, 3), size, 2 * l.stripe_width,
                        bytes(block_size, 'c'), &placement, &ticket));
  EXPECT_EQ(1u, placement.slot);
  ASSERT_EQ(0, j.committed(ticket, 0));
  j.seal();
  // One of slot 0's records is adopted, late: its segment takes the slot
  // over, the other record's hold still keeps the slot from being freed,
  // and the stripe flushes although younger than everything appended.
  ASSERT_EQ(0, j.adopt(Record{object(), eversion_t(1, 5), size,
                              3 * l.stripe_width, bytes(block_size, 'd'), {}, 0}));
  EXPECT_EQ(1u, j.held_slots());
  EXPECT_TRUE(j.adopting());
  EXPECT_EQ(-EAGAIN, j.check_append(object(), block_size));
  EXPECT_TRUE(j.adopted_ready());
  auto stripe = j.begin_flush(false);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(3 * l.stripe_width, stripe->offset);
  ASSERT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.adopting());
  EXPECT_EQ(-EAGAIN, j.check_append(object(), block_size)); // still held
  j.release_hold(0);
  EXPECT_EQ(0u, j.held_slots());
  EXPECT_EQ(0, j.check_append(object(), block_size));
  ASSERT_EQ(0, j.append(object(), eversion_t(3, 4), size, 4 * l.stripe_width,
                        bytes(block_size, 'e'), &placement, &ticket));
  EXPECT_EQ(0u, placement.slot);
  EXPECT_TRUE(placement.reset);
}

TEST_P(JournalTest, AdoptedRecordsCountAgainstTheBudget)
{
  Limits l = limits();
  l.segment_bytes = record_bytes() * 5 / 2; // two one-block records per segment
  l.max_bytes = 4 * l.segment_bytes;
  l.max_segments = 4;
  Journal j(prefix(), l);
  // Two live records in each of the four slots: the log is as full as the
  // slots were, and a new segment has to wait for the first flush.
  for (uint64_t slot = 0; slot < 4; ++slot) {
    for (uint64_t n = 0; n < 2; ++n) {
      ASSERT_EQ(0, j.adopt(Record{object(), eversion_t(1, 1 + slot * 2 + n),
        16 * width(), slot * width() + n * block_size, bytes(block_size, 'r'),
        {}, slot}));
    }
  }
  EXPECT_EQ(4u, j.adopted_segments());
  EXPECT_EQ(8u, j.records());
  EXPECT_EQ(8 * record_bytes(), j.bytes());
  EXPECT_EQ(-EAGAIN, j.check_append(object(), block_size));
  EXPECT_TRUE(j.pressure());
  auto stripe = j.begin_flush(false);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0u, stripe->offset);
  ASSERT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_EQ(6u, j.records());
  EXPECT_EQ(3u, j.adopted_segments());
  EXPECT_EQ(0, j.check_append(object(), block_size));
}

TEST(ECWriteJournal, MarkerEncodingIsCompactAndReadsTheFirstForm)
{
  // An entry for every stripe of a 4 MiB object on 8+3, as a mixed load
  // leaves it: versions a few thousand appends above base.
  Materialized m;
  m.base = eversion_t(41, 1000000);
  for (uint64_t stripe = 0; stripe < 128; ++stripe) {
    m.stripes[stripe * 32768] = eversion_t(41, 1000001 + stripe * 37);
  }
  ceph::bufferlist bl;
  encode(m, bl);
  EXPECT_LT(bl.length(), 128u * 5 + 32); // twenty bytes an entry before
  Materialized decoded;
  auto p = bl.cbegin();
  decode(decoded, p);
  EXPECT_TRUE(p.end());
  EXPECT_EQ(m.base, decoded.base);
  EXPECT_EQ(m.stripes, decoded.stripes);
  // Entries below base (a delta before its merge), another epoch, offsets
  // with no common unit, a first stripe that is not stripe 0, one entry.
  for (const Materialized& odd : {
         Materialized{eversion_t(7, 500), {{32768, eversion_t(7, 400)},
                                           {98304, eversion_t(6, 90000)},
                                           {131072, eversion_t(9, 3)}}},
         Materialized{eversion_t(), {{4096, eversion_t(1, 1)},
                                     {4097, eversion_t(1, 2)},
                                     {1ull << 40, eversion_t(1, 1ull << 50)}}},
         Materialized{eversion_t(3, 9), {{16384 * 5, eversion_t(3, 10)}}},
         Materialized{eversion_t(3, 9), {{0, eversion_t(3, 10)}}}}) {
    ceph::bufferlist one;
    encode(odd, one);
    Materialized back;
    auto q = one.cbegin();
    decode(back, q);
    EXPECT_TRUE(q.end());
    EXPECT_EQ(odd.base, back.base);
    EXPECT_EQ(odd.stripes, back.stripes);
  }
  // An empty map is written in the first form (version 1), which is what a
  // drained pool holds, and the first form is still read with entries.
  const auto first_form = [](const Materialized& v) {
    ceph::bufferlist out;
    ENCODE_START(1, 1, out);
    encode(v.base, out);
    encode(v.stripes, out);
    ENCODE_FINISH(out);
    return out;
  };
  const Materialized clean{eversion_t(41, 2000000), {}};
  ceph::bufferlist empty;
  encode(clean, empty);
  EXPECT_TRUE(empty.contents_equal(first_form(clean)));
  auto old = first_form(m);
  EXPECT_EQ(128u * 20 + 22, old.length());
  Materialized from_old;
  auto r = old.cbegin();
  decode(from_old, r);
  EXPECT_EQ(m.base, from_old.base);
  EXPECT_EQ(m.stripes, from_old.stripes);
  // A cut-off encoding does not decode.
  ceph::bufferlist cut;
  cut.substr_of(bl, 0, bl.length() - 3);
  Materialized bad;
  auto c = cut.cbegin();
  EXPECT_THROW(decode(bad, c), ceph::buffer::error);
}

TEST_P(JournalTest, MarkersNeverCoverALiveRecordAndEmptyWithTheObject)
{
  // The object's marker as flushes and direct writes build it (PrimaryLogPG:
  // ec_journal_flush, finish_ctx), merged in the order they are built, which
  // is the order of the PG log. Whatever has been built: a record that is
  // live and that no built write holds must not be covered (replay would
  // drop it), and once the object has no journaled data and no write in
  // flight, the map is empty.
  auto j = journal();
  const auto a = object("a");
  const uint64_t size = 6 * width();
  const epoch_t epoch = 3;
  std::mt19937 rng(20261004);
  Materialized marker; // the attr as the generator's cache holds it
  // Per live block: its record's version, and whether a built write holds it.
  std::map<uint64_t, std::pair<eversion_t, bool>> live;
  std::deque<std::vector<Stripe>> in_flight; // built, not committed, in order
  const auto hold = [&](const Stripe& stripe) {
    for (const auto& [off, data] : stripe.blocks) {
      live.at(stripe.offset + off).second = true;
    }
  };
  unsigned flushes = 0, directs = 0, took = 0, cleaned = 0, entries = 0;
  for (unsigned n = 0; n < 6000; ++n) {
    switch (rng() % 7) {
    case 0:
    case 1:
    case 2: { // a journaled write
      // One or two blocks at any block of a stripe, inside it.
      const uint64_t len = rng() % 2 ? block_size : 2 * block_size;
      const uint64_t stripe = (rng() % (size / width())) * width();
      const uint64_t off = stripe +
        (rng() % ((width() - len) / block_size + 1)) * block_size;
      const eversion_t version(epoch, j.next_append_sequence());
      Transaction t;
      Ticket ticket;
      ASSERT_EQ(0, append_copy(j, a, version, size, off, bytes(len, 'x'), t,
                               &ticket));
      ASSERT_EQ(0, j.committed(ticket, 0));
      for (uint64_t b = off; b < off + len; b += block_size) {
        live[b] = {version, false};
      }
      break;
    }
    case 3: { // a flush is built
      if (in_flight.size() > 3) {
        break;
      }
      auto stripe = j.begin_flush(any());
      if (!stripe) {
        break;
      }
      Materialized delta;
      delta.stripes[stripe->offset] = stripe->newest_version;
      const auto oldest = j.oldest_version(a, stripe->offset);
      delta.base = oldest ? just_below(*oldest) : j.version_now(epoch);
      delta.merge(marker);
      marker = delta;
      hold(*stripe);
      in_flight.push_back({std::move(*stripe)});
      ++flushes;
      break;
    }
    case 4: { // a direct write is built
      if (in_flight.size() > 3) {
        break;
      }
      const uint64_t off = rng() % size;
      const uint64_t len = 1 + rng() % std::min<uint64_t>(size - off,
        rng() % 3 ? 3 * 4096 : 2 * width() + 4096);
      const auto state = j.write_state(a, off, len);
      if (state == ReadState::in_flight || state == ReadState::flushing) {
        break;
      }
      const bool dirty = j.dirty(a);
      std::vector<Stripe> taken;
      ASSERT_EQ(0, j.take_range(a, off, len, &taken));
      const eversion_t now = j.version_now(epoch);
      Materialized delta{now, {}};
      if (dirty) {
        const auto oldest = j.oldest_version_outside(a, off, len);
        delta.base = oldest ? just_below(*oldest) : now;
        for (uint64_t stripe = off / width() * width(); stripe < off + len;
             stripe += width()) {
          delta.stripes[stripe] = now;
        }
        delta.merge(marker);
      }
      marker = delta;
      for (const auto& stripe : taken) {
        hold(stripe);
      }
      took += !taken.empty();
      in_flight.push_back(std::move(taken));
      ++directs;
      break;
    }
    default: // the oldest write in flight commits
      if (!in_flight.empty()) {
        for (const auto& stripe : in_flight.front()) {
          ASSERT_EQ(0, j.finish_flush(stripe, 0));
          for (const auto& [off, data] : stripe.blocks) {
            const auto i = live.find(stripe.offset + off);
            if (i != live.end() && i->second.second) {
              live.erase(i); // not written again meanwhile
            }
          }
        }
        in_flight.pop_front();
      }
      break;
    }
    for (const auto& [off, state] : live) {
      if (!state.second) {
        ASSERT_FALSE(marker.covers(off / width() * width(), state.first))
          << "step " << n << ": live block " << off << " " << state.first
          << " is covered, base " << marker.base;
      }
    }
    ceph::bufferlist bl;
    encode(marker, bl);
    Materialized decoded;
    auto p = bl.cbegin();
    decode(decoded, p);
    ASSERT_EQ(marker.base, decoded.base);
    ASSERT_EQ(marker.stripes, decoded.stripes);
    entries = std::max<unsigned>(entries, marker.stripes.size());
    if (!j.dirty(a)) {
      ASSERT_TRUE(live.empty());
      ASSERT_TRUE(marker.stripes.empty()) << "step " << n;
      ++cleaned;
    }
  }
  // The run must have exercised every case.
  EXPECT_GT(flushes, 100u);
  EXPECT_GT(directs, 100u);
  EXPECT_GT(took, 50u);
  EXPECT_GT(cleaned, 5u);
  EXPECT_GT(entries, 3u);
}

TEST(ECWriteJournal, MarkerMergeDropsEntriesBaseCovers)
{
  // The attr before: base (1,5), four stripes. A flush of stripe 1 whose
  // object's oldest other live record is (1,10) raises base to (1,9).
  const Materialized previous{eversion_t(1, 5), {{0, eversion_t(1, 6)},
    {32768, eversion_t(1, 8)}, {65536, eversion_t(1, 9)},
    {98304, eversion_t(1, 12)}}};
  Materialized update{just_below(eversion_t(1, 10)), {{32768, eversion_t(1, 11)}}};
  update.merge(previous);
  EXPECT_EQ(eversion_t(1, 9), update.base);
  const std::map<uint64_t, eversion_t> kept{{32768, eversion_t(1, 11)},
                                            {98304, eversion_t(1, 12)}};
  EXPECT_EQ(kept, update.stripes);
  // Every record the dropped entries covered is still covered, by base.
  for (const auto& [offset, version] : previous.stripes) {
    EXPECT_TRUE(update.covers(offset, version)) << offset;
  }
  EXPECT_FALSE(update.covers(0, eversion_t(1, 10))); // the oldest live record
  // A delta keeps the higher base it finds, and an older stripe entry loses.
  Materialized delta{eversion_t(), {{0, eversion_t(1, 7)}}};
  delta.merge(update);
  EXPECT_EQ(eversion_t(1, 9), delta.base);
  EXPECT_EQ(kept, delta.stripes);
  // Nothing else live: base is the flush's own version and the map empties.
  Materialized last{eversion_t(1, 21), {{98304, eversion_t(1, 20)}}};
  last.merge(delta);
  EXPECT_EQ(eversion_t(1, 21), last.base);
  EXPECT_TRUE(last.stripes.empty());
  // just_below covers exactly the versions before v, across epochs.
  const auto below = just_below(eversion_t(7, 100));
  EXPECT_TRUE(eversion_t(6, 99) <= below);
  EXPECT_TRUE(eversion_t(7, 99) <= below);
  EXPECT_FALSE(eversion_t(7, 100) <= below);
  EXPECT_FALSE(eversion_t(8, 101) <= below);
}

TEST_P(JournalTest, OldestVersionSpansTheObjectsLiveStripes)
{
  auto j = journal();
  const auto other = object("other");
  EXPECT_FALSE(j.oldest_version(object()));
  Transaction t;
  Ticket ticket;
  // Stripe 0 at (1,20), stripe 1 at (1,10) then (1,30) over the same block,
  // stripe 2 at (1,25); another object at (1,5).
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 20), 16 * width(), 0,
                           bytes(block_size, 'a'), t, &ticket));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 10), 16 * width(),
                           width(), bytes(block_size, 'b'), t, &ticket));
  ASSERT_EQ(0, append_copy(j, other, eversion_t(1, 5), 16 * width(), 0,
                           bytes(block_size, 'c'), t, &ticket));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 25), 16 * width(),
                           2 * width(), bytes(block_size, 'd'), t, &ticket));
  EXPECT_EQ(eversion_t(1, 10), j.oldest_version(object()));
  EXPECT_EQ(eversion_t(1, 20), j.oldest_version(object(), width()));
  EXPECT_EQ(eversion_t(1, 5), j.oldest_version(other));
  EXPECT_FALSE(j.oldest_version(other, 0));
  // Superseded: stripe 1's only block now holds (1,30).
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 30), 16 * width(),
                           width(), bytes(block_size, 'e'), t, &ticket));
  EXPECT_EQ(eversion_t(1, 20), j.oldest_version(object()));
  EXPECT_EQ(eversion_t(1, 25), j.oldest_version(object(), 0));
  // A flushed stripe leaves. The snapshot a flush in flight holds does not
  // count (its write is ahead of anything built now), but a block written
  // to the stripe meanwhile does.
  Journal k = journal();
  Transaction u;
  Ticket a, b, c;
  ASSERT_EQ(0, append_copy(k, object(), eversion_t(2, 10), 16 * width(), 0,
                           bytes(block_size, 'a'), u, &a));
  ASSERT_EQ(0, append_copy(k, object(), eversion_t(2, 11), 16 * width(),
                           width(), bytes(block_size, 'b'), u, &b));
  ASSERT_EQ(0, k.committed(a, 0));
  ASSERT_EQ(0, k.committed(b, 0));
  auto stripe = k.begin_flush_object(object(), {}, nullptr, 0, width());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0u, stripe->offset);
  EXPECT_EQ(eversion_t(2, 11), k.oldest_version(object()));
  EXPECT_FALSE(k.oldest_version(object(), width())); // all of stripe 0 is held
  ASSERT_EQ(0, append_copy(k, object(), eversion_t(2, 12), 16 * width(),
                           block_size, bytes(block_size, 'c'), u, &c));
  EXPECT_EQ(eversion_t(2, 12), k.oldest_version(object(), width()));
  ASSERT_EQ(0, k.finish_flush(*stripe, 0));
  EXPECT_EQ(eversion_t(2, 11), k.oldest_version(object()));
  EXPECT_EQ(eversion_t(2, 12), k.oldest_version(object(), width()));
  // A flush that fails gives its snapshot back: it counts again.
  auto failing = k.begin_flush_object(object(), {}, nullptr, width(), width());
  ASSERT_TRUE(failing);
  EXPECT_EQ(eversion_t(2, 12), k.oldest_version(object()));
  EXPECT_EQ(-EIO, k.finish_flush(*failing, -EIO));
  EXPECT_EQ(eversion_t(2, 11), k.oldest_version(object()));
  // Adopted records may come out of version order: the lowest still counts.
  Journal r = journal();
  ASSERT_EQ(0, r.adopt(Record{object(), eversion_t(3, 40), 16 * width(), 0,
                              bytes(block_size, 'x'), {}, 0}));
  ASSERT_EQ(0, r.adopt(Record{object(), eversion_t(3, 30), 16 * width(),
                              block_size, bytes(block_size, 'y'), {}, 1}));
  EXPECT_EQ(eversion_t(3, 30), r.oldest_version(object()));
}

namespace {
// One holder's copy of each slot, built from placements the way emit()
// writes them: a reset clears the slot, payloads go at their offsets and
// headers into the slot's OMAP.
struct SlotImage {
  std::map<uint64_t, std::pair<ceph::bufferlist, SlotHeaders>> slots;
  void apply(const Placement& placement) {
    auto& [data, headers] = slots[placement.slot];
    if (placement.reset) {
      data.clear();
      headers.clear();
    }
    EXPECT_EQ(data.length(), placement.offset);
    data.append(placement.bytes);
    SlotHeaders updates;
    auto cursor = placement.header_updates.cbegin();
    ceph::decode(updates, cursor);
    for (auto& [key, value] : updates) {
      EXPECT_TRUE(headers.emplace(key, value).second); // headers are immutable
    }
  }
};
} // anonymous namespace

TEST(ECWriteJournal, LaterSegmentsStartTheirSlotsAtOffsetZero)
{
  Limits l;
  l.segment_bytes = 3 * record_alignment; // two 4 KiB records and headers
  l.max_bytes = 3 * l.segment_bytes;
  l.max_segments = 3;
  Journal j(prefix(), l);
  SlotImage image;
  std::vector<Placement> placements;
  for (uint64_t n = 0; n < 6; ++n) {
    Placement placement;
    Ticket ticket;
    ASSERT_EQ(0, j.append(object(), eversion_t(1, n + 1), 16 * l.stripe_width,
                          n * l.stripe_width, bytes(4096, char('a' + n)),
                          &placement, &ticket));
    image.apply(placement);
    placements.push_back(std::move(placement));
  }
  // The third and fifth records open segments after records went into the
  // previous one: their headers name offset 0 of the new slot.
  for (const size_t n : {2u, 4u}) {
    EXPECT_TRUE(placements[n].reset);
    EXPECT_EQ(0u, placements[n].offset);
    EXPECT_NE(placements[n - 1].slot, placements[n].slot);
    Record header;
    EXPECT_EQ(0u, header.decode_fields(placements[n].header).first);
    EXPECT_FALSE(placements[n + 1].reset);
    EXPECT_EQ(record_alignment, placements[n + 1].offset);
  }
  ASSERT_EQ(3u, image.slots.size());
  size_t decoded = 0;
  for (const auto& [slot, contents] : image.slots) {
    const auto& [data, headers] = contents;
    const auto records = decode_slot_records(data, headers, pg_t(0, 1), 0);
    ASSERT_EQ(2u, records.size());
    for (const auto& record : records) {
      const auto n = record.version.version - 1;
      EXPECT_EQ(placements[n].slot, slot);
      EXPECT_EQ(n * l.stripe_width, record.offset);
      EXPECT_EQ(std::string(4096, char('a' + n)), record.data.to_str());
    }
    decoded += records.size();
  }
  EXPECT_EQ(placements.size(), decoded);
}

TEST(ECWriteJournal, MultiBlockPayloadsDecodeAtTheirPaddedOffsets)
{
  // librbd merges adjacent writes, so records of any number of blocks up to
  // a stripe are journaled.
  Limits l;
  Journal j(prefix(), l);
  SlotImage image;
  const std::vector<uint64_t> lengths{2 * block_size, block_size, 3 * block_size,
                                      5 * block_size, l.stripe_width, block_size,
                                      4 * block_size};
  std::vector<Placement> placements;
  std::vector<ceph::bufferlist> payloads;
  for (size_t n = 0; n < lengths.size(); ++n) {
    ceph::bufferlist payload;
    for (uint64_t off = 0; off < lengths[n]; off += block_size) {
      payload.append(std::string(block_size, char('A' + (n * 7 + off / block_size) % 26)));
    }
    Placement placement;
    Ticket ticket;
    ASSERT_EQ(0, j.append(object(), eversion_t(1, n + 1), 16 * l.stripe_width,
                          n * l.stripe_width, payload, &placement, &ticket));
    EXPECT_EQ(p2roundup<uint64_t>(lengths[n], record_alignment),
              placement.bytes.length());
    image.apply(placement);
    placements.push_back(std::move(placement));
    payloads.push_back(std::move(payload));
  }
  ASSERT_EQ(1u, image.slots.size());
  const auto& [slot, contents] = *image.slots.begin();
  const auto& [data, headers] = contents;
  const auto records = decode_slot_records(data, headers, pg_t(0, 1), 0);
  const auto entries = decode_slot_headers(slot, data.length(), headers, pg_t(0, 1), 0);
  ASSERT_EQ(lengths.size(), records.size());
  ASSERT_EQ(lengths.size(), entries.size());
  uint64_t slot_offset = 0;
  for (size_t n = 0; n < lengths.size(); ++n) {
    EXPECT_EQ(eversion_t(1, n + 1), records[n].version);
    EXPECT_EQ(n * l.stripe_width, records[n].offset);
    EXPECT_TRUE(payloads[n].contents_equal(records[n].data));
    // The header alone locates the payload, for a local or remote read.
    const auto& locator = entries[n].locator;
    EXPECT_EQ(slot, locator.slot);
    EXPECT_EQ(slot_offset, locator.slot_offset);
    EXPECT_EQ(placements[n].offset, locator.slot_offset);
    EXPECT_EQ(p2roundup<uint64_t>(lengths[n], record_alignment), locator.length);
    EXPECT_EQ(n * l.stripe_width, locator.offset);
    EXPECT_EQ(eversion_t(1, n + 1), entries[n].header.version);
    EXPECT_EQ(0u, entries[n].header.data.length());
    ceph::bufferlist payload;
    payload.substr_of(data, locator.slot_offset, locator.length);
    EXPECT_TRUE(payloads[n].contents_equal(locator.decode_record(payload).data));
    slot_offset += locator.length;
  }
  EXPECT_EQ(data.length(), slot_offset);
}

TEST(ECWriteJournal, SlotHeadersMustTileTheSlotData)
{
  const Record first{object(), eversion_t(1, 1), 65536, 0, bytes(8192, 'a')};
  const Record second{object(), eversion_t(1, 2), 65536, 8192, bytes(block_size, 'b')};
  SlotHeaders headers;
  first.encode_header(headers[record_header_key(0)], 0);
  second.encode_header(headers[record_header_key(8192)], 8192);
  const pg_t pgid(0, 1);
  const uint64_t size = 8192 + record_alignment;
  const auto entries = decode_slot_headers(5, size, headers, pgid, 0);
  ASSERT_EQ(2u, entries.size());
  EXPECT_EQ(5u, entries[1].locator.slot);
  EXPECT_EQ(8192u, entries[1].locator.slot_offset);
  EXPECT_EQ(record_alignment, entries[1].locator.length);
  EXPECT_EQ(second.version, entries[1].header.version);
  // The data is shorter than the headers say, or runs past the last one.
  EXPECT_THROW(decode_slot_headers(5, size - record_alignment, headers, pgid, 0),
               ceph::buffer::error);
  EXPECT_THROW(decode_slot_headers(5, size + record_alignment, headers, pgid, 0),
               ceph::buffer::error);
  // A header placed inside the previous payload, or keyed elsewhere.
  SlotHeaders overlap;
  first.encode_header(overlap[record_header_key(0)], 0);
  second.encode_header(overlap[record_header_key(4096)], 4096);
  EXPECT_THROW(decode_slot_headers(5, size, overlap, pgid, 0), ceph::buffer::error);
  auto rekeyed = headers;
  rekeyed[record_header_key(12288)] = rekeyed.at(record_header_key(8192));
  rekeyed.erase(record_header_key(8192));
  EXPECT_THROW(decode_slot_headers(5, size, rekeyed, pgid, 0), ceph::buffer::error);
  EXPECT_TRUE(decode_slot_headers(5, 0, {}, pgid, 0).empty());
  EXPECT_THROW(decode_slot_headers(5, record_alignment, {}, pgid, 0),
               ceph::buffer::error);
}

TEST(ECWriteJournal, SplitRemovesChildSlotsTheParentLacks)
{
  // Slots a merge left behind come back with the split: removed even where
  // the parent has nothing to copy.
  Journal j(prefix(), Limits());
  const spg_t child(pg_t(2, 1), shard_id_t(0));
  Transaction t;
  ASSERT_EQ(0, j.copy_slots_for_split(child,
    [](const ghobject_t&, ceph::bufferlist&, SlotHeaders&) { return -ENOENT; }, t));
  auto cursor = t.begin();
  for (uint64_t slot = 0; slot < Limits().max_segments; ++slot) {
    ASSERT_TRUE(cursor.have_op());
    const auto op = cursor.decode_op();
    ASSERT_EQ(Transaction::OP_REMOVE, op->op);
    EXPECT_EQ(coll_t(child), cursor.get_cid(op->cid));
    auto expected = j.slot_object(slot, child.shard);
    expected.hobj.set_hash(child.pgid.ps());
    EXPECT_EQ(expected, cursor.get_oid(op->oid));
  }
  EXPECT_FALSE(cursor.have_op());
}
