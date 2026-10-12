// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// SPDX-License-Identifier: LGPL-2.1-or-later
#include <algorithm>
#include <cerrno>
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
  // Encoded size of one 1 KiB record, to size segments in records.
  uint64_t record_bytes() const {
    auto j = journal();
    Transaction t;
    Ticket ticket;
    EXPECT_EQ(0, append_copy(j, object(), eversion_t(1, 1), width(), 0,
                             bytes(1024, 'x'), t, &ticket));
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
    ASSERT_EQ(0, append_copy(j, object(), version, 16 * width(), (n - 1) * 4096,
                             bytes(4096, 'v'), t, &ticket,
                             utime_t(100 + n, 0)));
    EXPECT_EQ(version.version, ticket.sequence);
    EXPECT_EQ(version, j.version_now(7));
    EXPECT_EQ(0, j.committed(ticket, 0));
  }
  j.seal();
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(eversion_t(7, 3), stripe->newest_version);
  EXPECT_EQ(utime_t(103, 0), stripe->newest_mtime);
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  // Records adopted from an earlier interval keep their versions, below
  // anything this interval appends or marks.
  auto replayed = journal();
  const Record old{object(), eversion_t(5, 40), 16 * width(), 0,
                   bytes(4096, 'a'), utime_t(50, 0), 0};
  ASSERT_EQ(0, replayed.adopt(old));
  EXPECT_LT(old.version, replayed.version_now(8));
  auto adopted = replayed.begin_flush(any());
  ASSERT_TRUE(adopted);
  EXPECT_EQ(eversion_t(5, 40), adopted->newest_version);
  EXPECT_EQ(utime_t(50, 0), adopted->newest_mtime);
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
  fetches.add(first, eversion_t(1, 2), RecordLocator{0, 4096, 4096, 1024});
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
    16 * width(), 1024, bytes(1024, 'b'), {}, 0}));
  ASSERT_EQ(0, journal.adopt(Record{other_object, eversion_t(1, 3),
    16 * width(), 0, bytes(1024, 'c'), {}, 1}));
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
                0, bytes(1024, 'a'), {}, 0};
  ASSERT_EQ(0, journal.adopt(record));
  ASSERT_TRUE(fetches.complete(record.object, record.version));
  stripe = journal.begin_flush(any(), blocked);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(pending_object, stripe->object);
  EXPECT_EQ("a", stripe->blocks.at(0).to_str().substr(0, 1));
  EXPECT_EQ("b", stripe->blocks.at(1024).to_str().substr(0, 1));
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
    bytes(1024, 'a'), &first, &ticket));
  ASSERT_EQ(0, journal.append(child_object, eversion_t(1, 2), width(), 4096,
    bytes(4096, 'b'), &second, &ticket));
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
  Record parent_record{object(), version, width(), 0, bytes(1024, 'a')};
  Record child_record{child_object, version, width(), 0, bytes(1024, 'b')};
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
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 1), width(), 1024,
                           bytes(1024, 'a'), t, &first));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 2), width(), 4096,
                           bytes(4096, 'b'), t, &second));
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
         std::tuple{first, 1024u, 1024u, 'a'},
         std::tuple{second, 4096u, 4096u, 'b'}}) {
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
    EXPECT_EQ(record_alignment, op->len);
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
    ceph::bufferlist padding;
    padding.substr_of(payload, len, op->len - len);
    EXPECT_TRUE(padding.is_zero());
    EXPECT_EQ(object(), record.object);
    EXPECT_EQ(eversion_t(1, ticket.sequence), record.version);
    EXPECT_EQ(width(), record.object_size);
    EXPECT_EQ(off, record.offset);
    EXPECT_EQ(std::string(len, value), record.data.to_str());
  }
  EXPECT_FALSE(it.have_op());
  EXPECT_EQ(end + metadata_bytes, j.bytes());
  EXPECT_EQ(2 * record_alignment, j.payload_bytes());
  EXPECT_EQ(5u, j.live_blocks());
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
  for (uint64_t off = 0; off < width(); off += 4096) {
    tickets.push_back(append(j, off, 4096, 'a' + off / 4096, object(), false));
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
  EXPECT_EQ(width() / 4096, j.records()); // the open segment keeps them
  EXPECT_FALSE(trim(j)); // still open
  j.seal();
  EXPECT_TRUE(trim(j)); // sealed and dead: released at once
  EXPECT_TRUE(j.empty());
  EXPECT_EQ(0u, j.records());
}

TEST_P(JournalTest, IncompleteStripeWaitsForReclaimOrDrain)
{
  auto j = journal();
  append(j, 0, 1024);
  EXPECT_TRUE(j.dirty());
  EXPECT_TRUE(j.partial_ready());
  EXPECT_FALSE(j.flush_ready());
  EXPECT_FALSE(j.pressure());
  EXPECT_FALSE(j.begin_flush(false)); // no reclaim bound, no drain
  auto stripe = j.begin_flush(true); // a drain takes it
  ASSERT_TRUE(stripe);
  EXPECT_FALSE(stripe->full());
  EXPECT_EQ(1024u, stripe->dirty_bytes());
  EXPECT_FALSE(j.partial_ready()); // nothing resident while it flushes
  EXPECT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.dirty());
}

TEST_P(JournalTest, WakeOnlyWhenTheOldestResidentStripeBecomesDurable)
{
  auto j = journal();
  auto first = append(j, 0, 1024, 'a', object(), false);
  auto second = append(j, width(), 1024, 'b', object(), false);
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
  for (unsigned size : {1024u, 4096u}) {
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

TEST_P(JournalTest, FourOneKWritesFillPageNotStripe)
{
  auto j = journal();
  for (unsigned slot = 0; slot < 4; ++slot) {
    append(j, slot * 1024, 1024);
  }
  EXPECT_FALSE(j.flush_ready());
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_FALSE(stripe->full());
  EXPECT_EQ(4096u, stripe->dirty_bytes());
  ASSERT_EQ(1u, holes(*stripe).size());
  EXPECT_EQ(std::make_pair(uint64_t(4096), width() - 4096), holes(*stripe)[0]);
  EXPECT_THROW(assemble(*stripe), std::invalid_argument);
}

TEST_P(JournalTest, DuplicateWritesSupersedeWithoutCreatingCoverage)
{
  auto j = journal();
  const unsigned n = width() / 1024;
  for (unsigned i = 0; i < n; ++i) {
    append(j, 1024, 1024, 'a' + i % 26);
  }
  EXPECT_EQ(n, j.records());       // the log holds every append
  EXPECT_EQ(1u, j.live_blocks());  // memory holds the latest only
  EXPECT_EQ(n - 1, j.superseded_blocks());
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(1024u, stripe->dirty_bytes());
  EXPECT_FALSE(stripe->full());
  EXPECT_EQ(std::string(1024, 'a' + (n - 1) % 26), stripe->blocks.at(1024).to_str());
}

TEST_P(JournalTest, OverlapsUseAppendNotCommitOrderAndPreserveHoles)
{
  auto j = journal();
  auto first = append(j, width(), 4096, 'a', object(), false);
  auto second = append(j, width() + 1024, 1024, 'b', object(), false);
  auto third = append(j, width() + 4096, 1024, 'c', object(), false);
  EXPECT_EQ(0, j.committed(third, 0));
  EXPECT_EQ(0, j.committed(second, 0));
  EXPECT_FALSE(j.begin_flush(any()));
  EXPECT_EQ(0, j.committed(first, 0));
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(width(), stripe->offset);
  EXPECT_EQ(5120u, stripe->dirty_bytes());
  auto base = bytes(width(), 'z');
  std::string expected(width(), 'z');
  expected.replace(0, 4096, std::string(4096, 'a'));
  expected.replace(1024, 1024, std::string(1024, 'b'));
  expected.replace(4096, 1024, std::string(1024, 'c'));
  EXPECT_EQ(expected, assemble(*stripe, &base).to_str());
  auto short_base = bytes(1024, 'z');
  EXPECT_THROW(assemble(*stripe, &short_base), std::invalid_argument);
}

TEST_P(JournalTest, ObjectsAndStripesAreIndependent)
{
  auto j = journal();
  append(j, 0, 1024, 'a', object("one"));
  append(j, width(), 1024, 'b', object("one"));
  append(j, 0, 1024, 'c', object("two"));
  unsigned flushed = 0;
  while (auto stripe = flush(j)) {
    EXPECT_EQ(1024u, stripe->dirty_bytes());
    EXPECT_FALSE(stripe->full());
    ++flushed;
  }
  EXPECT_EQ(3u, flushed);
}

TEST_P(JournalTest, NewerWriteToFlushingStripeStaysJournaled)
{
  auto j = journal();
  append(j, 0, 1024, 'a');
  auto old = j.begin_flush(any());
  ASSERT_TRUE(old);
  auto newer = append(j, 0, 1024, 'b'); // supersedes the copy being written
  EXPECT_EQ(1u, j.live_blocks());
  EXPECT_FALSE(j.begin_flush(any())); // the stripe is busy
  auto base = bytes(width(), 'z');
  auto old_data = assemble(*old, &base);
  EXPECT_EQ(std::string(1024, 'a'), old_data.to_str().substr(0, 1024));
  EXPECT_EQ(0, j.finish_flush(*old, 0));
  EXPECT_TRUE(j.dirty()); // 'b' has a newer sequence than the snapshot
  EXPECT_EQ(1u, j.live_blocks());
  auto next = j.begin_flush(any());
  ASSERT_TRUE(next);
  EXPECT_EQ(newer.sequence, next->newest_sequence);
  EXPECT_EQ(std::string(1024, 'b'),
            assemble(*next, &old_data).to_str().substr(0, 1024));
  EXPECT_EQ(0, j.finish_flush(*next, 0));
  EXPECT_FALSE(j.dirty());
}

TEST_P(JournalTest, CoalescesAcrossSegmentsAndKillsOldSegmentBySupersede)
{
  // Records of one stripe may live in several segments; the flush covers
  // them all. A segment dies when its records are superseded or flushed,
  // whichever comes first, and is removed in either order.
  auto l = limits();
  l.segment_bytes = record_bytes() * 5 / 2; // two 1 KiB records per segment
  l.max_bytes = 16 * l.segment_bytes;
  l.max_segments = 16;
  Journal j(prefix(), l);
  auto a = append(j, 0, 1024, 'a');
  auto b = append(j, 1024, 1024, 'b');
  auto c = append(j, 2048, 1024, 'c');
  EXPECT_NE(a.segment, c.segment);
  auto a2 = append(j, 0, 1024, 'A'); // kills a, in the first segment
  EXPECT_EQ(3u, j.live_blocks());
  EXPECT_EQ(1u, j.superseded_blocks());
  EXPECT_FALSE(trim(j)); // b is still live in the first segment
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(3072u, stripe->dirty_bytes());
  EXPECT_EQ(std::string(1024, 'A'), stripe->blocks.at(0).to_str());
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
  l.segment_bytes = record_bytes() * 5 / 2; // two 1 KiB records per segment
  l.max_bytes = 2 * l.segment_bytes;
  l.max_segments = 2;
  Journal j(prefix(), l);
  append(j, 0, 1024, 'a');
  append(j, width(), 1024, 'b');
  EXPECT_EQ(1u, j.segment_count());
  EXPECT_FALSE(j.pressure());
  append(j, 2 * width(), 1024, 'c'); // rotates into the last allowed segment
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
  l.segment_bytes = l.max_bytes = 8192;
  l.max_segments = 1;
  Journal j(prefix(), l);
  append(j, 0, 1024);
  EXPECT_TRUE(j.pressure());
  auto used = j.bytes();
  Transaction t;
  Ticket rejected;
  EXPECT_EQ(-EAGAIN, append_copy(j, object(), eversion_t(1, 2), width(), 1024,
                                 bytes(1024, 'b'), t, &rejected));
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
  append(j, 1024, 1024);
}

TEST_P(JournalTest, FullyMaterializedOnlyWhileNothingIsLive)
{
  // A PG reset may happen at any point. Only a journal whose records are
  // all durable on the shards is safe to reset over; anything with live
  // blocks is not.
  auto j = journal();
  EXPECT_FALSE(j.fully_materialized()); // empty is not "materialized"
  append(j, 0, 1024);
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
  append(j, 4096, 4096);
  EXPECT_FALSE(j.fully_materialized());
  EXPECT_TRUE(j.dirty());
}

TEST_P(JournalTest, ResetDropsSegmentsAndTheNextOneResetsItsSlot)
{
  auto j = journal();
  append(j, 0, 1024);
  ASSERT_TRUE(flush(j));
  j.seal();
  EXPECT_TRUE(j.empty());
  auto open = append(j, 4096, 4096, 'x', object(), false);
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
                        bytes(1024, 'y'), &placement, &fresh));
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
  append(j, 0, 1024);
  EXPECT_EQ(l.max_bytes, j.bytes());
  EXPECT_EQ(record_alignment, j.payload_bytes());
  Transaction t;
  Ticket ignored;
  EXPECT_EQ(-EAGAIN, append_copy(j, object(), eversion_t(1, 2), width(), 1024,
                                 bytes(1024, 'x'), t, &ignored));
  EXPECT_TRUE(t.empty());
  Journal big(prefix(), l);
  EXPECT_EQ(-E2BIG, append_copy(big, object(), eversion_t(1, 1), width(), 0,
                                bytes(8192, 'x'), t, &ignored));
  EXPECT_TRUE(t.empty());
  EXPECT_TRUE(big.empty());
  EXPECT_EQ(0, append_copy(big, object(), eversion_t(1, 1), width(), 0,
                           bytes(4096, 'x'), t, &ignored));
  EXPECT_EQ(record_alignment, big.payload_bytes());
  EXPECT_EQ(l.max_bytes, big.bytes());
}

TEST_P(JournalTest, RecordsOfBothSizesStayBlockAligned)
{
  auto j = journal();
  std::mt19937 rng(7);
  Transaction t;
  uint64_t expected = 0;
  for (unsigned n = 0; n < 40; ++n) {
    const uint64_t len = rng() % 2 ? 1024 : 4096;
    const uint64_t off = (rng() % (width() / len)) * len;
    Ticket ticket;
    ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, n + 1), width(), off,
                             bytes(len, 'x'), t, &ticket));
    expected += record_alignment;
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
      append(j, off, 1024, 'a', oid); // incomplete stripes: one 1 KiB block
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
  append(j, 0, 1024, 'a', object("0001"));
  append(j, 0, 1024, 'b', object("0002"));
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
  l.segment_bytes = record_bytes(); // one 1 KiB record per segment
  l.max_segments = 2;
  Journal j(prefix(), l);
  auto first = append(j, 0, 1024, 'a', object(), false);
  auto second = append(j, width(), 1024, 'b');
  EXPECT_NE(first.segment, second.segment);
  EXPECT_TRUE(j.pressure());
  // Strictly oldest first: the durable newer stripe waits for the older
  // one's commit rather than being materialized early.
  EXPECT_FALSE(j.begin_flush(any()));
  Transaction t;
  Ticket ignored;
  EXPECT_EQ(-EAGAIN, append_copy(j, object(), eversion_t(1, 3), width(), 2048,
                                 bytes(1024, 'c'), t, &ignored));
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
  l.segment_bytes = 2 * record_bytes(); // two 1 KiB records per segment
  l.max_bytes = 3 * l.segment_bytes;
  l.max_segments = 3;
  Journal j(prefix(), l);
  // One incomplete stripe per record: segments A = {0, 1}, B = {2, 3}, C = {4}.
  std::vector<Ticket> t;
  for (uint64_t i = 0; i < 5; ++i) {
    t.push_back(append(j, i * width(), 1024));
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
  append(j, 5 * width(), 1024); // fills C
  append(j, 6 * width(), 1024); // opens D: B, C, D
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
  append(j, 0, 1024, 'a');           // A: stripe 0
  append(j, width(), 1024, 'b');     // A: stripe 1
  append(j, 2 * width(), 1024, 'c'); // B: stripe 2
  append(j, width(), 1024, 'B');     // B: supersedes stripe 1's record in A
  append(j, 1024, 1024, 'd');        // C: a second block for stripe 0
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
  append(j, 0, 1024);
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
  auto ticket = append(j, 0, 1024, 'x', object(), false);
  EXPECT_EQ(-EIO, j.committed(ticket, -EIO));
  EXPECT_EQ(-EIO, j.error());
  EXPECT_FALSE(j.flush_ready());
  EXPECT_FALSE(j.partial_ready());
  EXPECT_FALSE(j.begin_flush(any()));
  Transaction t;
  Ticket ignored;
  EXPECT_EQ(-EIO, append_copy(j, object(), eversion_t(1, 2), width(), 1024,
                              bytes(1024, 'y'), t, &ignored));
  EXPECT_TRUE(t.empty());
  EXPECT_EQ(1u, j.records());
}

TEST_P(JournalTest, InvalidTransitionsDoNotReclaimRecords)
{
  auto j = journal();
  auto ticket = append(j, 0, 1024);
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
  for (uint64_t off = 0; off < width(); off += 4096) {
    append(j, off, 4096, 'a', object("busy"));
  }
  append(j, 0, 1024, 'b', object("free"));
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
         std::pair{uint64_t(0), uint64_t(1536)},
         std::pair{uint64_t(1), uint64_t(1024)},
         std::pair{width() - 1024, uint64_t(4096)}, // crosses stripe/EOF
         std::pair{width(), uint64_t(1024)},
         std::pair{std::numeric_limits<uint64_t>::max() - 1023, uint64_t(1024)}}) {
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
    const uint64_t size = rng() % 2 ? 1024 : 4096;
    const uint64_t off = (rng() % (width() * 3 / size)) * size;
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
  // a non-1K-aligned EOF: no full-stripe read may be requested beyond it.
  const uint64_t size = width() + 4609;
  Transaction t;
  Ticket ticket;
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 1), size, width() + 1024,
                           bytes(1024, 'x'), t, &ticket));
  ASSERT_EQ(0, j.committed(ticket, 0));
  auto stripe = j.begin_flush(any());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(4609u, stripe->valid_bytes);
  EXPECT_FALSE(stripe->full());
  for (const auto& [off, len] : holes(*stripe)) {
    EXPECT_LE(off + len, size);
  }
  auto base = bytes(4609, 'z');
  std::string expected(4609, 'z');
  expected.replace(1024, 1024, std::string(1024, 'x'));
  expected.resize(width(), '\0');
  EXPECT_EQ(expected, assemble(*stripe, &base).to_str());
}

TEST_P(JournalTest, FullyCoveredTailNeedsNoBaseRead)
{
  auto j = journal();
  Transaction t;
  Ticket ticket;
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 1), width() + 1024,
                           width(), bytes(1024, 'x'), t, &ticket));
  ASSERT_EQ(0, j.committed(ticket, 0));
  EXPECT_TRUE(j.flush_ready()); // the whole tail is covered
  auto stripe = j.begin_flush(false);
  ASSERT_TRUE(stripe);
  EXPECT_TRUE(stripe->full());
  EXPECT_TRUE(holes(*stripe).empty());
  std::string expected(1024, 'x');
  expected.resize(width(), '\0');
  EXPECT_EQ(expected, assemble(*stripe).to_str());
}

TEST_P(JournalTest, MergedWriteFillsEveryBlockItCovers)
{
  // librbd merges adjacent 4 KiB writes to one object into one request: one
  // record, one block per KiB, latest append wins per block as usual.
  auto j = journal();
  append(j, 4096, 8192, 'm');
  EXPECT_EQ(8u, j.live_blocks());
  EXPECT_EQ(8192u, j.payload_bytes());
  EXPECT_LT(8192u, j.bytes());
  append(j, 8192, 4096, 'n');
  EXPECT_EQ(4u, j.superseded_blocks());
  EXPECT_EQ(8u, j.live_blocks());
  EXPECT_FALSE(j.flush_ready());
  append(j, 0, 4096, 'a');
  append(j, 12288, width() - 12288, 'z'); // the rest, merged again
  EXPECT_TRUE(j.flush_ready());
  auto stripe = j.begin_flush(false);
  ASSERT_TRUE(stripe);
  EXPECT_TRUE(stripe->full());
  EXPECT_EQ(std::string(4096, 'a') + std::string(4096, 'm') +
            std::string(4096, 'n') + std::string(width() - 12288, 'z'),
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
  append(j, 0, 1024, 'b', b);
  append(j, 0, 1024, 'a', a);
  append(j, width(), 1024, 'a', a);
  auto pending = append(j, 2 * width(), 1024, 'a', a, false);
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
  append(j, 0, 1024, 'x', a);
  append(j, width(), 1024, 'x', a);
  append(j, 3 * width(), 1024, 'x', a);
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

INSTANTIATE_TEST_SUITE_P(EC4plus3And8plus3And12plus3, JournalTest,
                        testing::Values(4, 8, 12));

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
  Record record{object(), eversion_t(1, 1), 32768, 1024, bytes(1024, 'x')};
  ceph::bufferlist encoded;
  record.encode_header(encoded, 0);
  ceph::bufferlist truncated;
  truncated.substr_of(encoded, 0, encoded.length() - 1);
  Record decoded;
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
  Record first{object(), eversion_t(1, 1), 32768, 0, bytes(1024, 'a')};
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
  l.segment_bytes = 3 * record_alignment; // two 1 KiB records and headers
  l.max_bytes = 2 * l.segment_bytes;
  l.max_segments = 2;
  Journal j(prefix(), l);
  auto place = [&](uint64_t off, uint64_t version) {
    Placement placement;
    Ticket ticket;
    EXPECT_EQ(0, j.append(object(), eversion_t(1, version), 16 * l.stripe_width,
                          off, bytes(1024, 'x'), &placement, &ticket));
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
  l.segment_bytes = 3 * record_alignment; // two 1 KiB records and headers
  l.max_bytes = 2 * l.segment_bytes;
  l.max_segments = 2;
  Journal j(prefix(), l);
  EXPECT_EQ(-EINVAL, j.check_append(object(), 0));
  EXPECT_EQ(-EINVAL, j.check_append(object(), 1536));
  EXPECT_EQ(0, j.check_append(object(), 4096));
  EXPECT_EQ(0, j.check_append(object(), 8192));
  EXPECT_EQ(-E2BIG, j.check_append(object(), 16384));
  std::vector<Ticket> tickets;
  for (uint64_t n = 0; n < 4; ++n) {
    ASSERT_EQ(0, j.check_append(object(), 1024));
    EXPECT_EQ(n, j.records()); // no state change
    Placement placement;
    Ticket ticket;
    ASSERT_EQ(0, j.append(object(), eversion_t(1, n + 1), 16 * l.stripe_width,
                          n * l.stripe_width, bytes(1024, 'x'), &placement,
                          &ticket));
    tickets.push_back(ticket);
  }
  EXPECT_EQ(-EAGAIN, j.check_append(object(), 1024));
  Placement placement;
  Ticket ticket;
  EXPECT_EQ(-EAGAIN, j.append(object(), eversion_t(1, 9), 16 * l.stripe_width,
                              0, bytes(1024, 'x'), &placement, &ticket));
}

TEST(ECWriteJournal, FlushReportsTheNewestRecordVersion)
{
  Limits l;
  Journal j(prefix(), l);
  Transaction t;
  Ticket a, b, c;
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(4, 7), 16 * l.stripe_width,
                           0, bytes(1024, 'a'), t, &a));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(4, 9), 16 * l.stripe_width,
                           1024, bytes(1024, 'b'), t, &b));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(4, 8), 16 * l.stripe_width,
                           l.stripe_width, bytes(1024, 'c'), t, &c));
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
                           0, bytes(1024, 'a'), t, &ticket));
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
  // Two records of stripe 0 overlap at 1 KiB; the newer version wins even
  // when adopted first, as when slots are scanned out of order. They were
  // read from slots 1 and 0; a record of stripe 1 from slot 1 as well.
  Record newer{object(), eversion_t(2, 20), size, 1024, bytes(1024, 'N'), {}, 1};
  Record older{object(), eversion_t(2, 10), size, 0, bytes(2048, 'o'), {}, 0};
  Record other{object(), eversion_t(2, 15), size, l.stripe_width,
               bytes(4096, 's'), {}, 1};
  EXPECT_EQ(-EINVAL, j.adopt(Record{object(), eversion_t(2, 1), size, 0,
                                    bytes(1024, 'x')})); // no slot
  EXPECT_EQ(-EINVAL, j.adopt(Record{object(), eversion_t(2, 1), size, 0,
                                    bytes(1024, 'x'), {}, l.max_segments}));
  ASSERT_EQ(0, j.adopt(newer));
  ASSERT_EQ(0, j.adopt(older));
  ASSERT_EQ(0, j.adopt(other));
  EXPECT_TRUE(j.adopting());
  EXPECT_EQ(2u, j.adopted_segments());
  EXPECT_EQ(2u + 4u, j.live_blocks()); // 0 from older, 1 KiB from newer, 4 of other
  EXPECT_EQ(3u, j.records());
  EXPECT_LT(0u, j.bytes()); // charged like appends
  // The other slots take new records at once, the lowest free one first.
  EXPECT_EQ(0, j.check_append(object(), 1024));
  Placement placement;
  Ticket ticket;
  ASSERT_EQ(0, j.append(object(), eversion_t(2, 30), size, 2 * l.stripe_width,
                        bytes(1024, 'x'), &placement, &ticket));
  EXPECT_EQ(2u, placement.slot);
  EXPECT_TRUE(placement.reset);
  ASSERT_EQ(0, j.committed(ticket, 0));
  // Adopted stripes flush without a drain, pressure or age, oldest first.
  EXPECT_TRUE(j.adopted_ready());
  auto first = j.begin_flush(false);
  ASSERT_TRUE(first);
  EXPECT_EQ(0u, first->offset);
  EXPECT_EQ(std::string(1024, 'o'), first->blocks.at(0).to_str());
  EXPECT_EQ(std::string(1024, 'N'), first->blocks.at(1024).to_str());
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
                        bytes(1024, 'y'), &placement, &ticket));
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
  ASSERT_EQ(0, j.append(object(), eversion_t(3, 1), size, 0, bytes(1024, 'a'),
                        &placement, &ticket));
  EXPECT_EQ(2u, placement.slot);
  ASSERT_EQ(0, j.committed(ticket, 0));
  j.seal();
  ASSERT_EQ(0, j.append(object(), eversion_t(3, 2), size, l.stripe_width,
                        bytes(1024, 'b'), &placement, &ticket));
  EXPECT_EQ(3u, placement.slot);
  ASSERT_EQ(0, j.committed(ticket, 0));
  j.seal();
  EXPECT_EQ(-EAGAIN, j.check_append(object(), 1024));
  // Slot 1's record was judged dead: the slot is free again.
  j.release_hold(1);
  EXPECT_EQ(1u, j.held_slots());
  EXPECT_EQ(0, j.check_append(object(), 1024));
  ASSERT_EQ(0, j.append(object(), eversion_t(3, 3), size, 2 * l.stripe_width,
                        bytes(1024, 'c'), &placement, &ticket));
  EXPECT_EQ(1u, placement.slot);
  ASSERT_EQ(0, j.committed(ticket, 0));
  j.seal();
  // One of slot 0's records is adopted, late: its segment takes the slot
  // over, the other record's hold still keeps the slot from being freed,
  // and the stripe flushes although younger than everything appended.
  ASSERT_EQ(0, j.adopt(Record{object(), eversion_t(1, 5), size,
                              3 * l.stripe_width, bytes(1024, 'd'), {}, 0}));
  EXPECT_EQ(1u, j.held_slots());
  EXPECT_TRUE(j.adopting());
  EXPECT_EQ(-EAGAIN, j.check_append(object(), 1024));
  EXPECT_TRUE(j.adopted_ready());
  auto stripe = j.begin_flush(false);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(3 * l.stripe_width, stripe->offset);
  ASSERT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_FALSE(j.adopting());
  EXPECT_EQ(-EAGAIN, j.check_append(object(), 1024)); // still held
  j.release_hold(0);
  EXPECT_EQ(0u, j.held_slots());
  EXPECT_EQ(0, j.check_append(object(), 1024));
  ASSERT_EQ(0, j.append(object(), eversion_t(3, 4), size, 4 * l.stripe_width,
                        bytes(1024, 'e'), &placement, &ticket));
  EXPECT_EQ(0u, placement.slot);
  EXPECT_TRUE(placement.reset);
}

TEST_P(JournalTest, AdoptedRecordsCountAgainstTheBudget)
{
  Limits l = limits();
  l.segment_bytes = record_bytes() * 5 / 2; // two 1 KiB records per segment
  l.max_bytes = 4 * l.segment_bytes;
  l.max_segments = 4;
  Journal j(prefix(), l);
  // Two live records in each of the four slots: the log is as full as the
  // slots were, and a new segment has to wait for the first flush.
  for (uint64_t slot = 0; slot < 4; ++slot) {
    for (uint64_t n = 0; n < 2; ++n) {
      ASSERT_EQ(0, j.adopt(Record{object(), eversion_t(1, 1 + slot * 2 + n),
        16 * width(), slot * width() + n * 1024, bytes(1024, 'r'), {}, slot}));
    }
  }
  EXPECT_EQ(4u, j.adopted_segments());
  EXPECT_EQ(8u, j.records());
  EXPECT_EQ(8 * record_bytes(), j.bytes());
  EXPECT_EQ(-EAGAIN, j.check_append(object(), 1024));
  EXPECT_TRUE(j.pressure());
  auto stripe = j.begin_flush(false);
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0u, stripe->offset);
  ASSERT_EQ(0, j.finish_flush(*stripe, 0));
  EXPECT_EQ(6u, j.records());
  EXPECT_EQ(3u, j.adopted_segments());
  EXPECT_EQ(0, j.check_append(object(), 1024));
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
                           bytes(1024, 'a'), t, &ticket));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 10), 16 * width(),
                           width(), bytes(1024, 'b'), t, &ticket));
  ASSERT_EQ(0, append_copy(j, other, eversion_t(1, 5), 16 * width(), 0,
                           bytes(1024, 'c'), t, &ticket));
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 25), 16 * width(),
                           2 * width(), bytes(1024, 'd'), t, &ticket));
  EXPECT_EQ(eversion_t(1, 10), j.oldest_version(object()));
  EXPECT_EQ(eversion_t(1, 20), j.oldest_version(object(), width()));
  EXPECT_EQ(eversion_t(1, 5), j.oldest_version(other));
  EXPECT_FALSE(j.oldest_version(other, 0));
  // Superseded: stripe 1's only block now holds (1,30).
  ASSERT_EQ(0, append_copy(j, object(), eversion_t(1, 30), 16 * width(),
                           width(), bytes(1024, 'e'), t, &ticket));
  EXPECT_EQ(eversion_t(1, 20), j.oldest_version(object()));
  EXPECT_EQ(eversion_t(1, 25), j.oldest_version(object(), 0));
  // A flushed stripe leaves; one still flushing counts, as does a block
  // written to it meanwhile.
  Journal k = journal();
  Transaction u;
  Ticket a, b, c;
  ASSERT_EQ(0, append_copy(k, object(), eversion_t(2, 10), 16 * width(), 0,
                           bytes(1024, 'a'), u, &a));
  ASSERT_EQ(0, append_copy(k, object(), eversion_t(2, 11), 16 * width(),
                           width(), bytes(1024, 'b'), u, &b));
  ASSERT_EQ(0, k.committed(a, 0));
  ASSERT_EQ(0, k.committed(b, 0));
  auto stripe = k.begin_flush_object(object(), {}, nullptr, 0, width());
  ASSERT_TRUE(stripe);
  EXPECT_EQ(0u, stripe->offset);
  EXPECT_EQ(eversion_t(2, 10), k.oldest_version(object()));
  ASSERT_EQ(0, append_copy(k, object(), eversion_t(2, 12), 16 * width(), 1024,
                           bytes(1024, 'c'), u, &c));
  ASSERT_EQ(0, k.finish_flush(*stripe, 0));
  EXPECT_EQ(eversion_t(2, 11), k.oldest_version(object()));
  EXPECT_EQ(eversion_t(2, 12), k.oldest_version(object(), width()));
  // Adopted records may come out of version order: the lowest still counts.
  Journal r = journal();
  ASSERT_EQ(0, r.adopt(Record{object(), eversion_t(3, 40), 16 * width(), 0, bytes(1024, 'x'), {}, 0}));
  ASSERT_EQ(0, r.adopt(Record{object(), eversion_t(3, 30), 16 * width(), 1024, bytes(1024, 'y'), {}, 1}));
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
  // librbd merges adjacent writes, so records of up to a stripe are
  // journaled, 4 KiB multiples or not.
  Limits l;
  Journal j(prefix(), l);
  SlotImage image;
  const std::vector<uint64_t> lengths{8192, 2048, 12288, 6144, 32768, 1024, 16384};
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
  const Record second{object(), eversion_t(1, 2), 65536, 8192, bytes(1024, 'b')};
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
