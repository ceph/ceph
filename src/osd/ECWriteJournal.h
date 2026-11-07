// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// SPDX-License-Identifier: LGPL-2.1-or-later
#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/hobject.h"
#include "include/utime.h"
#include "os/Transaction.h"

namespace ECWriteJournal {

// Experimental journal building block. All methods must run under the owning
// PG lock. This class does NOT submit transactions, acknowledge client
// requests, or implement replay. See doc/dev/ec-write-journal-poc.rst.
//
// Two structures share the record data: append-only segments (the durable
// log) and an in-memory index of live 4 KiB blocks folded per stripe (latest
// append wins; a superseded block is dead immediately). Flushing is decided
// per stripe, not per segment: a complete stripe can be materialized without
// reads at any time, an incomplete one when the log must reclaim its oldest
// segment, when a mutation that cannot run beside journaled data waits for
// its object, or on a drain. Client reads are served from the index
// (Overlay), not by a flush, and a plain overwrite the journal does not take
// takes the stripes it overlaps with it (take_range), not by a flush either.
//
// Each segment occupies one of max_segments fixed slot objects. A record is
// placed once (slot, offset) and the caller writes the same bytes into every
// holder shard's copy of that slot (emit), so all copies are identical and a
// replaying primary can read any of them. A segment's slot is reused as soon
// as none of its records is live and all its appends are durable: the first
// append of the next segment removes the old contents in the same
// transaction, so trimming needs no transaction of its own and happens on
// every holder at once.
// The index's unit and the journal's write granularity: a journaled write
// covers whole blocks, so a 4 KiB write is one block, with one version, one
// sequence and one buffer reference. The production writes sampled were all
// 4 KiB aligned, and smaller ones are 512-byte sector writes that 1 KiB
// blocks did not take either; they run as direct writes.
inline constexpr uint64_t block_size = 4096;
// Every payload starts at a multiple of this offset in its segment object and
// is zero padded to a multiple of it, so each append is a whole-block write
// into never-written space. BlueStore turns an append that shares a device
// block with the previous record's tail into a read of that block plus a
// deferred (WAL) rewrite of the merged blocks: one device read and a doubled
// write per client write. Headers live in immutable slot OMAP entries.
inline constexpr uint64_t record_alignment = 4096;
inline constexpr uint32_t detached_record_magic = 0x364a4345; // "ECJ6"
using SlotHeaders = std::map<std::string, ceph::bufferlist>;
std::string record_header_key(uint64_t slot_offset);
// Slot objects are "ecj.<n>" in the PG-local internal namespace.
inline constexpr std::string_view slot_name_prefix = "ecj.";
bool is_slot_object(const hobject_t& object);

// Header: magic, crc32c, header length, header (slot offset, object,
// version, object_size, offset, payload length, mtime, and since header
// version 3 the client's request id and the user version its reply
// reported). The crc covers header and payload; decode_header() throws
// buffer::malformed_input on a bad magic or crc. Its encoded length depends
// only on the object's name.
//
// A record's version is a journal version, not a PG version: the epoch is
// the interval's start epoch and the version number the primary's append
// sequence (Journal::next_append_sequence). Records are not PG-logged; the
// object's own version is unchanged until the flush writes the data. The
// mtime is the client's, applied by the flush.
//
// The request id is what lets a resent write be recognized (RADOS executes a
// request at most once): until the stripe is materialized the journal
// answers for it (Journal::find_request), from then on the PG log does, as
// the materializing write carries the ids of the records it wrote as its
// log entry's extra_reqids. A record from before header version 3 has no
// request id and gets no such protection.
// A record's slot when nothing placed it (Record::slot).
inline constexpr uint64_t no_slot = std::numeric_limits<uint64_t>::max();

struct Record {
  hobject_t object;
  eversion_t version;
  uint64_t object_size = 0;
  uint64_t offset = 0;
  ceph::bufferlist data;
  utime_t mtime;
  // The slot replay read it from, the same on every holder
  // (RecordLocator::decode_record); adopt() keeps it in that slot. Not encoded.
  uint64_t slot = no_slot;
  // The client request this record executed, and the user version the reply
  // to it reported (the object's, unchanged by a journaled write). Encoded.
  osd_reqid_t reqid;
  version_t user_version = 0;

  void encode_header(ceph::bufferlist& out, uint64_t slot_offset) const;
  // Fields only, no payload (data stays empty): returns the slot offset and
  // the payload length. decode_header() also checks the crc.
  std::pair<uint64_t, uint32_t> decode_fields(const ceph::bufferlist& header);
  uint64_t decode_header(const ceph::bufferlist& header,
                         const ceph::bufferlist& payload,
                         uint64_t payload_offset = 0);
};

// Object xattr kept by the flusher and by every other data-modifying write
// of an enrolled pool (see doc/dev/ec-write-journal-poc.rst). Its versions
// are journal versions (Record). A record of the object with version V for
// stripe s is materialized when V <= max(base, stripes[s]). base is the
// journal version current at the last write that did not go through the
// journal while the object had no records (such a write waits until they
// are all flushed); stripes[s] the newest record version the last flush of
// s wrote, or the journal version current at a plain overwrite of s that
// went past the journal while the object had records (those take the
// records of the stripes they write with them, keep base below the other
// stripes' records and name only the stripes they write).
// It lives with the object's other attributes, so recovery and backfill carry
// it and a replaying primary can judge records against it.
inline constexpr std::string_view materialized_attr = "ec_journal";

// Where a record lives: the same slot and offset on every holder. Replay
// builds one from each listed header (SlotEntry) and fetches the payload
// through it from another holder when its own copy is missing or unreadable.
struct RecordLocator {
  uint64_t slot = 0;
  uint64_t slot_offset = 0;
  uint64_t length = 0; // padded payload bytes
  uint64_t offset = 0; // object offset of the write
  ceph::bufferlist header{};
  // The record, from its header and this payload. Throws buffer::error if
  // the payload is short, fails the checksum or is not this record's.
  Record decode_record(const ceph::bufferlist& payload) const;
};

// One record of a slot as its OMAP header describes it. Replay judges a
// record by its header alone and reads only the payloads it still needs,
// through its locator: from this shard's copy of the slot, or from another
// holder's identical copy if this one does not read back intact or lacks
// the record altogether (the other holders' listings are read too).
struct SlotEntry {
  Record header; // no data
  RecordLocator locator;
};
// The records of one slot that belong to pgid, oldest first. Throws
// buffer::error unless the headers place their padded payloads back to
// back from offset 0 up to slot_bytes, the size of the slot's data (unset:
// another holder's listing, whose slot size is not known here; the headers
// must still tile from offset 0).
std::vector<SlotEntry> decode_slot_headers(uint64_t slot,
                                           std::optional<uint64_t> slot_bytes,
                                           const SlotHeaders& headers,
                                           pg_t pgid, unsigned split_bits);
// Headers and payloads of a whole slot read at once (decode_slot_headers,
// then every locator's decode_record).
std::vector<Record> decode_slot_records(const ceph::bufferlist& data,
                                        const SlotHeaders& headers,
                                        pg_t pgid, unsigned split_bits);

struct Materialized {
  eversion_t base;
  std::map<uint64_t, eversion_t> stripes; // stripe offset -> covered version

  bool covers(uint64_t stripe_offset, eversion_t version) const;
  // Fold the attr's previous value into this update (a flush's or a direct
  // write's own stripes, merged in write order), then drop the entries base
  // now covers: they decide nothing, and the attr is rewritten with the
  // object's onode on every later write of it.
  void merge(const Materialized& previous);
  // The attr is set on shard 0 and the parity shards, and its previous value
  // goes into the log entry's rollback data on every shard, with each flush
  // and each direct write of an object that has live records. Under a mixed
  // load such an object keeps an entry for nearly every stripe (base rises
  // only to its oldest live record, and direct writes keep marking stripes),
  // so the map is delta coded (version 2): stripe offsets as differences in
  // units of their common divisor, versions as differences from base, four
  // or five bytes an entry instead of twenty. An empty map is still written
  // in the first form: that is what every object has once its journaled
  // data is gone, so a drained pool can be read by a build that knows only
  // that form.
  void encode(ceph::bufferlist& out) const;
  void decode(ceph::bufferlist::const_iterator& in);
};
WRITE_CLASS_ENCODER(Materialized)

// The newest version below v: a PG's versions grow across epochs, so
// "V <= just_below(v)" means "V < v". A marker's base set just below the
// oldest record of the object still live covers every older record.
eversion_t just_below(eversion_t v);

class ReplayFetches {
 public:
  struct Pending {
    hobject_t object;
    eversion_t version;
    RecordLocator locator;
  };

  void add(const hobject_t& object, eversion_t version, RecordLocator locator);
  bool complete(const hobject_t& object, eversion_t version);
  void fail(int result, bool all_objects = false);
  void clear();
  bool blocked(const hobject_t& object) const {
    return unreadable || objects.contains(object);
  }
  bool empty() const { return entries.empty(); }
  int error() const { return failed; }
  const std::vector<Pending>& pending() const { return entries; }

 private:
  std::vector<Pending> entries;
  std::multiset<hobject_t> objects;
  int failed = 0;
  bool unreadable = false;
};

// Snapshot of a stripe's live blocks handed to the flusher.
struct Stripe {
  hobject_t object;
  uint64_t object_size = 0;
  uint64_t offset = 0;
  uint64_t width = 0;
  uint64_t valid_bytes = 0; // logical bytes before EOF, at most width
  // Relative, 4 KiB aligned offsets. Bufferlists share immutable record data.
  std::map<uint64_t, ceph::bufferlist> blocks;
  // Newest append sequence in the snapshot. A record admitted to the stripe
  // while it flushes has a higher sequence and stays journaled.
  uint64_t newest_sequence = 0;
  // Newest journal version among the snapshot's blocks: every record of
  // this stripe at or below it is materialized once the flush commits.
  eversion_t newest_version;
  // The requests of every record the snapshot materializes, live or
  // superseded, with the user version each reply reported: the write that
  // materializes the stripe logs them as its entry's extra_reqids, so a
  // resend is answered from the PG log once the journal has let go of them
  // (Journal::find_request until then).
  std::vector<std::pair<osd_reqid_t, version_t>> reqids;

  uint64_t dirty_bytes() const { return blocks.size() * block_size; }
  bool full() const { return dirty_bytes() == valid_bytes; }
  // The OSD materializes a Stripe through ECJournalFlush's shard planner and
  // assembly. The logical reference model lives with the unit tests.
};

// The pieces of [offset, offset + length), one per stripe it overlaps, as
// (offset, length). A write that crosses a stripe boundary is journaled as
// one record per stripe, placed and committed together: the index, the
// flushes and replay are per stripe, so each piece is judged and
// materialized on its own.
std::vector<std::pair<uint64_t, uint64_t>> stripe_fragments(
  uint64_t offset, uint64_t length, uint64_t stripe_width);

// What a direct write that took these stripes (Journal::take_range, in
// offset order) has to write for them besides its own bytes: their live
// blocks outside `written`, the bytes the write itself writes, joined into
// runs at object offsets. The write supersedes the blocks it covers, at any
// alignment. *dropped counts the stripes that add nothing: the write covers
// every live block of theirs, and their records die unwritten.
std::map<uint64_t, ceph::bufferlist> absorb_runs(
  const std::vector<Stripe>& stripes, const interval_set<uint64_t>& written,
  uint64_t* dropped = nullptr);

// What a client read of an object range meets in the journal
// (Journal::read_state). The shards hold a stripe's bytes from before its
// records, so a read of journaled data reads the shards and lays the
// stripe's live blocks over the result (Overlay), without a flush. It has
// to wait first when the range is
//   in_flight: a record there is not yet durable on every holder. Shown
//     now, it could be gone again after a failover;
//   flushing: the stripe is being written to the shards, which a read that
//     reconstructs a chunk from parity must not meet half done;
//   adopted: replay adopted a record there. It may be on fewer holders
//     than a record acknowledged in this interval, so the read waits for
//     the stripe's flush, which replay starts at once.
// clean: no stripe of the range holds a record. ready: every record there
// is durable and none of the above applies. Of several stripes, the last
// of these that applies to any.
enum class ReadState { clean, ready, in_flight, flushing, adopted };

// The live blocks over an object range, taken when a client read is issued
// (Journal::read_overlay). The bufferlists share the records' immutable
// data, so the snapshot stays what it was while later writes supersede its
// blocks and flushes release them.
struct Overlay {
  std::map<uint64_t, ceph::bufferlist> blocks; // object offset -> one block

  bool empty() const { return blocks.empty(); }
  // Every byte of [offset, offset + length) is in a block: the read needs
  // nothing from the shards.
  bool covers(uint64_t offset, uint64_t length) const;
  // Lay the blocks over data, the object's bytes [offset, offset + length)
  // as the shards returned them. A range the blocks cover needs no data
  // (pass it empty).
  void apply(uint64_t offset, uint64_t length, ceph::bufferlist& data) const;
};

struct Limits {
  // The defaults are the unit tests' small geometry; the OSD splits
  // osd_ec_journal_max_bytes over 16 slots (read_limits): 4 MiB
  // segments in a 64 MiB budget per PG by default.
  uint64_t stripe_width = 8 * 4096;
  uint64_t segment_bytes = 4 * 1024 * 1024;
  uint64_t max_bytes = 16 * 1024 * 1024;
  uint64_t max_segments = 4;
  // How many of the newest requests the journal remembers for duplicate
  // detection (find_request): the PG log's own window, osd_pg_log_dups_tracked
  // plus osd_min_pg_log_entries (read_limits). An older resend runs again, as
  // it does on a logged pool. A dead segment keeps its slot while a request
  // of its records is still in the window, so a replaying primary rebuilds
  // the same window from the slots. 0: none remembered.
  uint64_t request_window = 3250;

  bool valid() const;
};

struct Ticket {
  uint64_t segment = 0;
  uint64_t sequence = 0;
};

// Where one appended record goes. Every holder writes these bytes at this
// offset of its own copy of the slot (Journal::emit).
struct Placement {
  uint64_t slot = 0;
  uint64_t offset = 0;
  // First record of a segment: the slot may still hold an older segment's
  // records (or, after a restart, an older run's), removed before the write.
  bool reset = false;
  uint64_t sequence = 0;
  ceph::bufferlist bytes; // payload, zero padded to record_alignment
  ceph::bufferlist header;
  ceph::bufferlist header_updates;
};

class Journal {
 public:
  static constexpr uint64_t whole_object = std::numeric_limits<uint64_t>::max();
  using StripeKey = std::pair<hobject_t, uint64_t>; // object, stripe offset
  using Skip = std::function<bool(const StripeKey&)>;

  // prefix names the slot objects: a PG-local internal object that no client
  // can address, the same on every shard (only its shard id differs).
  Journal(ghobject_t prefix, Limits limits);

  // Place an append. Writes of whole 4 KiB blocks, aligned to 4 KiB, within
  // a prefilled object and one stripe are supported (librbd merges adjacent
  // 4 KiB writes to one object); the caller appends a write that crosses a
  // stripe boundary piece by piece (stripe_fragments). On success the caller MUST emit the
  // placement into the transaction of every holder and submit them.
  // Append does NOT mean durable/ACKable. -EAGAIN means the log is full;
  // no state change on error. The version is the record's journal version;
  // its version number must be the sequence this append takes
  // (next_append_sequence), so that versions and sequences order alike.
  int append(const hobject_t& object, eversion_t version,
             uint64_t object_size, uint64_t offset,
             const ceph::bufferlist& data, Placement* placement,
             Ticket* ticket, utime_t mtime = {},
             const osd_reqid_t& reqid = {}, version_t user_version = 0);
  // A request a record not yet materialized executed (append, or adopt),
  // while it is among the request_window newest: the user version its reply
  // reported and whether every holder has the record. A resend of it must
  // not run again: answer it like the original once durable, or hold it
  // until then. Requests of records a flush or a direct write materialized
  // are gone from here (finish_flush) and in that write's log entry
  // (Stripe::reqids); older ones than the window are forgotten. Dropped with
  // everything else at on_reset(); replay adopts them again.
  struct Request {
    version_t user_version = 0;
    bool durable = false;
    uint64_t segment = 0;  // the record's segment, pinned while this exists
    uint64_t sequence = 0; // the record's append sequence
  };
  std::optional<Request> find_request(const osd_reqid_t& reqid) const;
  uint64_t request_count() const { return requests.size(); }
  uint64_t window_size() const { return window.size(); }
  // Dead segments that keep their slot for requests still in the window.
  uint64_t pinned_segments() const;
  // What the object's journaled writes did to its metadata: the user version
  // handed out to the last of them and that write's mtime, as long as the
  // object has records (the write that materializes the last of them
  // persists both in the object info). The primary lays them over the object
  // info it reads from disk, so stat, assert-version and every reply see the
  // journaled writes as if they were logged.
  struct ObjectMeta {
    version_t user_version = 0;
    utime_t mtime;
  };
  std::optional<ObjectMeta> object_meta(const hobject_t& object) const;
  uint64_t object_meta_count() const { return metas.size(); }
  // The newest user version any record ever carried (this interval's
  // appends and the records replay adopted): a logged write of the PG
  // allocates above it, so user versions stay a pool-wide sequence.
  version_t newest_user_version() const { return newest_user_version_; }
  // The sequence the next append takes: the version number of its record.
  uint64_t next_append_sequence() const { return next_sequence; }
  // The journal version current now: every record appended so far, in this
  // interval or adopted from earlier ones, sorts at or below (epoch, this).
  eversion_t version_now(epoch_t epoch) const {
    return eversion_t(epoch, next_sequence - 1);
  }
  // What append would return for a record of this object and length, with
  // no state change: 0, -EAGAIN (no room until something is released),
  // -E2BIG (never fits a segment), -EINVAL or a latched error.
  int check_append(const hobject_t& object, uint64_t length) const;
  // The same for several records appended one after the other (the pieces
  // of a write that crosses a stripe boundary): 0 only if all of them fit,
  // the segment a later one has to open included.
  int check_append(const hobject_t& object,
                   const std::vector<uint64_t>& lengths) const;
  // Write one holder's copy of a placed record into t.
  void emit(const Placement& placement, const coll_t& collection,
            shard_id_t shard, ceph::os::Transaction& t) const;

  using ReadSlot = std::function<int(const ghobject_t&, ceph::bufferlist&,
                                     SlotHeaders&)>;
  int copy_slots_for_split(spg_t child, const ReadSlot& read,
                           ceph::os::Transaction& transaction) const;

  // Call ONLY after the append transactions of every holder commit, with the
  // PG lock held. RMW on_all_commit already holds it; direct store callbacks
  // need blessing. Append errors latch the journal closed; never ACK them or
  // silently bypass.
  int committed(Ticket ticket, int result);

  // Close the open segment for appends (drain, or a full log that must be
  // reclaimed). Its live records still flush per stripe; its slot is reused
  // once none is live. Empty is a no-op.
  void seal();

  // Choose the next stripe to materialize and mark it flushing. A complete,
  // durable stripe is always eligible and needs no base reads. Otherwise a
  // durable incomplete stripe is chosen, oldest first (which is also the order
  // that frees the oldest segment), if every_partial is set (a drain) or its
  // oldest live record has a sequence at or below partial_upto.
  // Candidates for which skip() returns true (scrub, degraded object) are
  // passed over, and so are stripes a client read is reading (read_begin);
  // *deferred reports that at least one was. Only records whose
  // append committed are ever exposed; a later append to a flushing stripe
  // stays journaled.
  std::optional<Stripe> begin_flush(
    bool every_partial,
    const Skip& skip = {}, bool* deferred = nullptr,
    std::optional<uint64_t> partial_upto = std::nullopt);
  // Every durable stripe of one object, complete or not:
  // an unsupported mutation of the object waits for its buffered data only,
  // and other objects' stripes keep gathering records meanwhile. With a byte
  // range, only the stripes overlapping it; with adopted_only, only those
  // replay adopted a record of (a read waits for those, see ReadState).
  std::optional<Stripe> begin_flush_object(const hobject_t& object,
    const Skip& skip = {}, bool* deferred = nullptr,
    uint64_t offset = 0, uint64_t length = whole_object,
    bool adopted_only = false);
  // begin_flush(every_partial, ..., partial_upto) has to take this stripe:
  // a drain, the reclaim bound or replay asks for it. Otherwise it is taken
  // only once it is complete, which can wait.
  bool must_flush(const StripeKey& key, bool every_partial,
                  std::optional<uint64_t> partial_upto) const;
  // A direct write of [offset, offset + length) of this object takes every
  // journaled stripe the range overlaps with it, as a flush takes one: the
  // stripes leave the resident set and their snapshots are appended to
  // *taken, in offset order. The write materializes them itself: it
  // overwrites their blocks or carries them in its own transaction
  // (ECJournalFlush's absorb_runs), and finish_flush() of each snapshot at
  // its commit kills them. Until then they keep their segments resident, so
  // no slot is reused under a record whose replacement can still roll back.
  // A record appended to a taken stripe meanwhile is newer than the write
  // and stays journaled. -EBUSY, with nothing taken, if write_state() says
  // the write has to wait.
  int take_range(const hobject_t& object, uint64_t offset, uint64_t length,
                 std::vector<Stripe>* taken);
  // Call only after ALL EC writes covering this snapshot have durably
  // completed. Blocks the snapshot covered become dead; newer ones stay. On
  // error the stripe returns to the resident set for a retry.
  int finish_flush(const Stripe& flushed, int result);

  // After a PG interval change: commits and flushes registered before it
  // never run, so everything in memory is dropped and every slot is free.
  // The records stay in the slots on disk, where a replaying primary reads
  // them back (adopt); the next segment in a slot removes the old contents.
  void on_reset();
  // Replay: take a record read back from the slots (record.slot) and judged
  // live into the index, durable. It stays in its slot: a closed segment per
  // slot holds the adopted records, charged to the budget like appends and
  // placed before this interval's segments, and the slot is reused once
  // they have all flushed (collect), like any other segment's. The other
  // slots take new records meanwhile. Adopt in version order; per block the
  // newest version wins. -EINVAL without a slot.
  int adopt(const Record& record);
  // A segment of adopted records is still resident.
  bool adopting() const;
  uint64_t adopted_segments() const;
  // Replay: keep a slot out of use while a record it holds is still being
  // fetched from another holder or waits for its object's recovery: a new
  // segment in that slot would remove the record on every holder before it
  // has been flushed. adopt() takes the hold over; release it for a record
  // judged dead instead. One hold per record.
  void hold_slot(uint64_t slot);
  void release_hold(uint64_t slot);
  uint64_t held_slots() const { return holds.size(); }
  // A durable stripe with an adopted record is waiting: adopted stripes
  // flush without a drain or pressure (begin_flush).
  bool adopted_ready() const;
  uint64_t slot_count() const { return limits.max_segments; }

  ghobject_t slot_object(uint64_t slot, shard_id_t shard) const;
  // prefix's shard copy of a resident segment's slot.
  ghobject_t segment_object(uint64_t segment) const;
  // Bytes of resident segments (padded payloads plus headers and keys), the
  // budget max_bytes bounds, and their record count, live or dead. Capacity
  // returns when a segment is dead and its appends are durable.
  uint64_t bytes() const { return used_bytes; }
  uint64_t payload_bytes() const { return used_payload_bytes; }
  uint64_t records() const { return used_records; }
  // Live, unflushed blocks held in memory (one version per block).
  uint64_t live_blocks() const { return live; }
  uint64_t live_bytes() const { return live * block_size; }
  uint64_t superseded_blocks() const { return superseded; }
  uint64_t segment_count() const { return segments.size(); }
  // The segment still holds budget and its slot.
  bool resident(uint64_t segment) const;
  bool empty() const { return segments.empty(); }
  bool dirty() const { return !index.empty(); }
  // A stripe of this object (overlapping the range) is resident or flushing.
  bool dirty(const hobject_t& object, uint64_t offset = 0,
             uint64_t length = whole_object) const;
  // The oldest version among this object's live blocks that no flush or
  // direct write in flight holds (the snapshot of a taken stripe), other
  // than those of the stripe at skip_stripe. Every record of the object
  // below it is materialized, superseded by a live one, or held by a write
  // that is in the PG log ahead of whatever is built now: a marker built
  // now can only become durable with those writes, so its base may cover
  // their records.
  std::optional<eversion_t> oldest_version(const hobject_t& object,
    std::optional<uint64_t> skip_stripe = std::nullopt) const;
  // The same, other than the stripes [offset, offset + length) overlaps:
  // what a direct write that took that range (take_range) leaves live.
  std::optional<eversion_t> oldest_version_outside(const hobject_t& object,
    uint64_t offset, uint64_t length) const;
  // A durable stripe of this object (overlapping the range) is waiting for
  // begin_flush_object().
  bool object_ready(const hobject_t& object, uint64_t offset = 0,
                    uint64_t length = whole_object) const;
  // What a client read of [offset, offset + length) of this object meets
  // (ReadState).
  ReadState read_state(const hobject_t& object, uint64_t offset,
                       uint64_t length) const;
  // What a direct write of [offset, offset + length) of this object meets:
  // clean (no stripe of the range holds a record), ready (take_range() takes
  // them all), or what it has to wait for: in_flight (a record of the range
  // is not durable on every holder yet; taken now, the write could be
  // acknowledged ahead of it) or flushing (a flush or another direct write
  // holds a stripe of the range). Never adopted: such a stripe is taken
  // like any other.
  ReadState write_state(const hobject_t& object, uint64_t offset,
                        uint64_t length) const;
  // Add the live blocks that overlap the range to *overlay, newest version
  // per block. For a range that is ready; a clean one adds nothing.
  void read_overlay(const hobject_t& object, uint64_t offset, uint64_t length,
                    Overlay* overlay) const;
  // Client reads whose shard reads are in flight. A flush writes a stripe's
  // shards, and a read that rebuilds a chunk from parity must not combine
  // chunks from before and after it, so the two never overlap in time:
  // begin_flush() and begin_flush_object() pass over a stripe that is being
  // read, like a skipped candidate, and read_state() says flushing for one
  // that is being flushed. A read counts on every stripe its range overlaps,
  // with or without records, from read_begin() to read_end() of that range.
  void read_begin(const hobject_t& object, uint64_t offset, uint64_t length);
  void read_end(const hobject_t& object, uint64_t offset, uint64_t length);
  bool reading(const StripeKey& key) const { return readers.contains(key); }
  uint64_t stripes_being_read() const { return readers.size(); }
  // True once every record has been written to the EC shards and only append
  // commits keep a segment resident. Nothing is lost if the PG resets in
  // this state.
  bool fully_materialized() const { return !segments.empty() && index.empty(); }
  bool has_open_segment() const;
  // A complete, durable stripe is waiting (flushable without reads).
  bool flush_ready() const;
  // The oldest resident stripe is durable, so a partial flush can proceed.
  bool partial_ready() const;
  // The log cannot take another segment. Stays set until a segment is
  // released, which can be later than reclaim should stop: see
  // reclaim_bound().
  bool pressure() const;
  // What reclaim has to flush, as a partial_upto for begin_flush: the newest
  // sequence of the oldest segment that still holds live records, while the
  // log is short of room once every already-dead segment is removed. Every
  // incomplete stripe pinning that segment has its oldest live record at or
  // below the bound; nothing younger has to go yet. Unset when no reclaim is
  // needed, including while the only shortfall is an append commit.
  //
  // Flushing by pressure() alone takes stripes oldest first until a removal
  // commits, and with several flushes in flight that runs well past the
  // oldest segment into stripes still gathering records.
  std::optional<uint64_t> reclaim_bound() const;
  // A durable incomplete stripe within reclaim_bound() is waiting.
  bool reclaim_ready() const;
  int error() const { return failed; }

 private:
  enum class State { open, closed };
  struct Pending {
    hobject_t object;
    uint64_t offset = 0;
    uint32_t length = 0;
    bool durable = false;
    osd_reqid_t reqid;
  };
  // Budget charge of a record (padded payload, header value and key), and
  // whether a record of that size can be appended now and opens a new
  // segment (*rotate): the one formula and rule append and check_append use.
  uint64_t record_bytes(const hobject_t& object, uint64_t length) const;
  int room_for(uint64_t bytes, bool* rotate = nullptr) const;
  struct Segment {
    uint64_t id;
    uint64_t slot;
    State state = State::open;
    bool adopted = false; // replay's records of the slot; always closed
    uint64_t bytes = 0;
    uint64_t payload_bytes = 0;
    uint64_t durable_records = 0;
    uint64_t live_blocks = 0;
    // Records of this segment whose request is in the window and not yet in
    // a log entry (requests): the segment keeps its slot while any is.
    uint64_t window_ids = 0;
    std::map<uint64_t, Pending> entries;

    Segment(uint64_t id, uint64_t slot) : id(id), slot(slot) {}
  };
  struct Block {
    ceph::bufferlist data;
    eversion_t version;
    uint64_t sequence = 0;
    uint64_t segment = 0;
    bool durable = false;
    utime_t mtime;
  };
  // One record's request, in a stripe's list (Entry::reqids). The sequence
  // ties it to the snapshot that materializes it (Stripe::newest_sequence).
  struct Reqid {
    osd_reqid_t reqid;
    version_t user_version = 0;
    uint64_t sequence = 0;
    uint64_t segment = 0;
  };
  struct Entry {
    uint64_t object_size = 0;
    uint64_t valid_bytes = 0;
    std::map<uint64_t, Block> blocks; // relative 4 KiB offsets
    // Every record appended to the stripe since its last materialization
    // that is still in the request window, live or superseded, oldest
    // first; the next snapshot takes them (extra_reqids).
    std::deque<Reqid> reqids;
    uint32_t undurable = 0;
    bool flushing = false;
    // While flushing: the newest sequence of the snapshot the flush or the
    // direct write holds. Blocks above it were appended since.
    uint64_t taken = 0;
    uint64_t oldest = 0; // sequence of the oldest live block
    // Lowest version among the live blocks that no snapshot in flight holds
    // (adopted records need not arrive in version order), kept by reindex()
    // for oldest_version(). Unset: a flush or direct write holds them all.
    std::optional<eversion_t> oldest_version;

    bool full() const { return blocks.size() * block_size == valid_bytes; }
  };

  Segment* find_segment(uint64_t id);
  bool adopted_segment(uint64_t id) const;
  bool adopted_stripe(const Entry& entry) const;
  // adopting is adopting(), which the caller of a loop evaluates once.
  bool must_flush(const Entry& entry, bool every_partial,
                  std::optional<uint64_t> partial_upto, bool adopting) const;
  // Remember a record's request (append, adopt): the oldest leaves the
  // window first.
  void add_request(Entry& entry, const StripeKey& key, const osd_reqid_t& reqid,
                   version_t user_version, uint64_t sequence, uint64_t segment,
                   bool durable);
  void forget_oldest_request();
  // The request leaves requests (logged, forgotten or unpinned): its
  // segment is pinned by one request less.
  void drop_request(const osd_reqid_t& reqid, uint64_t sequence);
  // Space held only by dead segments that keep their slot for requests in
  // the window: room_for and pressure count it as free, append takes it
  // when it needs a slot (unpin_oldest).
  struct Reclaimable {
    uint64_t bytes = 0;
    uint64_t count = 0;
  };
  Reclaimable reclaimable() const;
  bool unpin_oldest();
  // Remember what a record did to its object's metadata (append, adopt).
  void note_meta(const hobject_t& object, version_t user_version, utime_t mtime);
  // The index entries of `object` whose stripe overlaps [offset, offset+length).
  std::pair<std::map<StripeKey, Entry>::const_iterator,
            std::map<StripeKey, Entry>::const_iterator>
  stripes_of(const hobject_t& object, uint64_t offset, uint64_t length) const;
  StripeKey key_of(const hobject_t& object, uint64_t offset) const;
  void release(Entry& entry, Block& block);
  void reindex(const StripeKey& key, Entry& entry);
  Stripe take(StripeKey key);
  // Release every closed segment none of whose records is live and whose
  // appends are all durable: its budget and its slot become free.
  void collect();

  ghobject_t prefix;
  Limits limits;
  std::deque<Segment> segments;
  std::set<uint64_t> free_slots;
  std::map<uint64_t, uint64_t> holds; // slot -> replay records not placed yet
  std::map<StripeKey, Entry> index;
  std::map<uint64_t, StripeKey> by_age; // oldest live sequence -> resident stripe
  std::set<StripeKey> ready;            // complete, durable, resident
  std::map<StripeKey, unsigned> readers; // client reads in flight per stripe
  // Requests of records not yet materialized and within the window, for
  // find_request (the PG log's mempool: dup-detection state).
  mempool::osd_pglog::unordered_map<osd_reqid_t, Request> requests;
  // The request_window newest appends, oldest first, logged or not.
  struct Remembered {
    osd_reqid_t reqid;
    uint64_t sequence = 0;
    StripeKey stripe;
  };
  std::deque<Remembered> window;
  std::map<hobject_t, ObjectMeta> metas; // objects with records (object_meta)
  version_t newest_user_version_ = 0;
  uint64_t next_segment = 1;
  uint64_t next_sequence = 1;
  uint64_t used_bytes = 0;
  uint64_t used_payload_bytes = 0;
  uint64_t used_records = 0;
  uint64_t live = 0;
  uint64_t superseded = 0;
  int failed = 0;
};

} // namespace ECWriteJournal
