// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

// Multipart upload — Phase 2 (Milestone 1, plans/multipart.md):
// InitiateMultipartUpload / UploadPart / ListParts / ListMultipartUploads.
// Phase 3 (Complete/Abort) follows below in this same file.
//
// UploadPart Phase 1/3, Complete, Abort, and the G:M/P:M cleanup-directive
// split below follow the corrected design in plans/fixing_agent_mess.md
// (supersedes plans/multipart_upload_part_refactor.md's PEND_ABORT/
// highest-generation-wins model).

#include "service_impl.hpp"

#include "bucket_policy.hpp"
#include "error_codes.hpp"
#include "etag.hpp"
#include "fdb_latency.hpp"
#include "keys.hpp"
#include "kv_store.hpp"
#include "multipart_value.hpp"
#include "object_value.hpp"
#include "tier_config_state.hpp"

#include <algorithm>
#include <array>
#include <arpa/inet.h>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <openssl/evp.h>

namespace kvrgw {
constexpr FDBStreamingMode streamingMode = FDB_STREAMING_MODE_EXACT;

// Builds "ref_tag_hex-part_number-generation" into caller-supplied out.
// Returns a string_view into out on success, empty on failure (truncation).
// Caller should pass a char buf[64] — max possible length is 41 bytes
// (24 hex + '-' + up to 5 digits + '-' + up to 10 digits, generation now 32-bit).
//--------------------------------------------------------------------------------
std::string_view KvRgwServiceImpl::make_part_filename(const RefTag& ref_tag,
                                                      uint16_t part_number,
                                                      uint32_t generation,
                                                      std::span<char> out)
{
  if (out.size() < kRefTagSize * 2 + 1) {
    return {};  // can't even fit the hex prefix
  }
  ref_tag.to_hex(out.data());
  const size_t hex_len = kRefTagSize * 2;
  const int n = snprintf(out.data() + hex_len, out.size() - hex_len,
                         "-%u-%u", part_number, generation);
  if (n < 0 || static_cast<size_t>(n) >= out.size() - hex_len) {
    return {};  // truncated
  }
  return {out.data(), hex_len + static_cast<size_t>(n)};
}

//--------------------------------------------------------------------------------
KvRgwServiceImpl::InitiateMultipartResult
KvRgwServiceImpl::initiate_multipart_upload(tenant_id_t tenant_id,
                                            std::string_view bucket_name,
                                            std::string_view object_name,
                                            std::string_view content_type,
                                            std::span<const TagPair> tags,
                                            std::span<const MetaPair> metadata,
                                            CksumAlg cksum_alg,
                                            CksumType cksum_type)
{
  InitiateMultipartResult result;
  if (cksum_alg == CksumAlg::NONE) {
    if (cksum_type != CksumType::NONE) {
      result.error_code = KVRGW_ERR_INVALID_REQUEST;
      return result;
    }
  }
  else {
    if (cksum_type == CksumType::NONE) {
      cksum_type = cksum_default_type(cksum_alg);
    }
    if (!cksum_type_valid_for_alg(cksum_alg, cksum_type)) {
      result.error_code = KVRGW_ERR_INVALID_REQUEST;
      return result;
    }
  }
  // TBD: create unified parameters-check for PUT() and initiate_multipart_upload()
  if (object_name.empty() || object_name.size() > AWS_MaxObjectNameLen) {
    result.error_code = KVRGW_ERR_INVALID_ARGUMENT;
    return result;
  }

  if (content_type.size() > kMaxContentTypeLen) {
    result.error_code = KVRGW_ERR_INVALID_ARGUMENT;
    return result;
  }

  auto cached = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached) {
    result.error_code = fdb_to_error(cached.error());
    return result;
  }
  if (!*cached) {
    result.error_code = KVRGW_ERR_NO_SUCH_BUCKET;
    return result;
  }
  // bucket_id is mutable — may be updated on BUCKET_ID_MISMATCH retries.
  bucket_id_t bucket_id = (**cached).bucket_id;

  const RefTag ref_tag = ref_tags_.next();
  const int64_t initiated_unix_ms = now_unix_ms();

  std::array<uint8_t, kMaxTagFrameBytes> tag_buf;
  size_t tag_len = 0;
  uint8_t tags_count = 0;
  if (!tags.empty()) {
    if (!encode_tags(tags, std::span<uint8_t>(tag_buf), tag_len)) {
      result.error_code = KVRGW_ERR_INVALID_TAG;
      return result;
    }
    tags_count = static_cast<uint8_t>(tags.size());
  }
  const ChildValueBuf ct_value(
      ChildValueHeader{},
      std::string_view(reinterpret_cast<const char *>(tag_buf.data()), tag_len));

  MHeadValueBuf head_buf;
  bool success = make_m_head_value(head_buf, initiated_unix_ms, content_type,
                                   tags_count, metadata, cksum_alg, cksum_type);
  if (!success) {
    result.error_code = KVRGW_ERR_VALUE_TOO_LARGE;
    return result;
  }

  // Blind writes — Multipart has a unique ref_tag, will resolve conflicts
  //   at complete_multipart_upload() commit phase.
  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    KvrgwErrorCode loop_ec = KVRGW_ERR_OK;

    // Rebuilt each iteration so bucket_id updates on mismatch are reflected.
    KeyBuf head_key;
    make_m_head_key(bucket_id, object_name, ref_tag.view(), head_key);
    KeyBuf ct_key;
    if (tag_len > 0) {
      make_ct_key(bucket_id, ref_tag.view(), ct_key);
    }

    auto tr_result = store_.begin_transaction();
    if (tr_result) {
      auto &tr = *tr_result;
      auto f_bkt = issue_bucket_get(*tr, tenant_id, bucket_name);
      tr->kv_put(head_key.view(), head_buf.view());
      if (tag_len > 0) {
        tr->kv_put(ct_key.view(), ct_value.value());
      }

      // Authoritative bucket verification before commit — catches a deleted or
      // re-created bucket before the head row lands.
      auto vb = resolve_bucket_verify(tenant_id, bucket_name, f_bkt, bucket_id, kDenyWrite);
      if (!vb) [[unlikely]] {
        loop_ec = vb.error();
        if (loop_ec == KVRGW_ERR_BUCKET_ID_MISMATCH) {
          // bucket was overwritten -> refresh bucket_id, keys rebuilt at top of loop
          auto cached = get_bucket_id_cached(tenant_id, bucket_name);
          if (!cached) {
            loop_ec = fdb_to_error(cached.error());
          }
          else if (!*cached) {
            loop_ec = KVRGW_ERR_NO_SUCH_BUCKET;
          }
          else {
            bucket_id = (**cached).bucket_id;
            continue;
          }
        }
      }
      else if (fdb_error_t err = tr->commit()) [[unlikely]] {
        loop_ec = fdb_to_error(err);
      }
      else {
        result.upload_id = ref_tag.to_hex();
        return result;
      }
    }
    else {
      loop_ec = fdb_to_error(tr_result.error());
    }

    // handle loop errors
    if (is_retriable(loop_ec)) {
      sleep_for_msec(10 * (attempt + 1));
      continue;
    }
    result.error_code = loop_ec;
    return result;
  }
  result.error_code = KVRGW_ERR_MAX_RETRIES_EXCEEDED;
  return result;
}

namespace {

// upload_id wire format: lowercase hex of the 12-byte ref_tag
// (RefTag::filename()). A malformed upload_id is treated uniformly as
// NoSuchUpload by every caller (see Phase 2 "LOCKED" note in
// plans/multipart.md) — RefTag::from_hex() simply returns false for that
// case; decoding is private to RefTag, friended to KvRgwServiceImpl only.

//--------------------------------------------------------------------------------
// One physical S:U entry under a (bucket_id, ref_tag, part_number) row.
// Trimmed to just what scan_upload_rows/process_manifest_row actually read
// out of the on-wire MPartValue -- state, etag, size. chunk/mtime are parsed
// (parse_m_part_value is shared with other callers) but never copied in.
struct PartRowEntry {
  uint64_t size{0};
  uint32_t generation{0};
  ETag etag{};
  uint8_t state{M_PART_PENDING};
  uint8_t cksum[kCksumMaxDigestBytes]{};
};

//--------------------------------------------------------------------------------
// Delete uploaded_part KV from S:U and PUT() a matching entry in GC domain
// GcWorker will free the storage-tier data from BG
void migrate_uploaded_part_kv_to_gc(KvTransaction &tr,
                                    bucket_id_t bucket_id,
                                    std::string_view ref_tag,
                                    uint16_t part_number,
                                    uint32_t generation,
                                    uint64_t object_size,
                                    const KeyBuf &old_key)
{
  KeyBuf gu_key;
  make_gu_key(bucket_id, ref_tag, part_number, generation, gu_key);
  const GuEntry entry{part_number, generation};
  GuValueBuf gu_buf;
  make_gu_value(gu_buf, &entry, 1, object_size);
  tr.kv_put(gu_key.view(), gu_buf.view());

  tr.kv_del(old_key.view());
}

//--------------------------------------------------------------------------------
// Scans the whole (bucket,ref_tag,part_number) S:U row and claims a
// generation slot for a new PENDING entry — evicting the oldest COMMITTED
// entry first if the row is already at capacity.
// See plans/fixing_agent_mess.md Section 3's N-bound eviction rule.
//
// Returns:
//   - KVRGW_ERR_OK — *out_generation holds the generation to use for the
//     new PENDING row (the caller still has to write it). A free slot
//     already existed, or the oldest COMMITTED entry was just evicted
//     (delete S:U + write a single-entry G:U, in `tr`) to make one.
//   - KVRGW_ERR_UPLOAD_PART_BUSY — row already at N entries and every one
//     of them is PENDING (nothing evictable). *out_generation untouched.
KvrgwErrorCode allocate_part_slot(KvTransaction &tr,
                                  const FdbRangeHolder &first_page,
                                  bucket_id_t bucket_id,
                                  std::string_view rt_view,
                                  uint16_t part_number,
                                  uint32_t *out_generation)
{
  assert(!first_page.more());

  uint32_t count = 0;
  bool any_found = false;
  uint32_t max_generation = 0;
  bool have_oldest_committed = false;
  uint32_t oldest_committed_generation = 0;
  uint64_t oldest_committed_size = 0;

  for (const auto &[key, value] : first_page) {
    auto parsed = parse_u_key(key);
    if (!parsed) {
      continue;  // defensive — shouldn't occur under a part-scoped prefix
    }
    auto pv = parse_m_part_value(value);
    if (!pv) {
      continue;  // defensive — corrupt row, skip
    }
    any_found = true;
    ++count;
    max_generation = std::max(max_generation, parsed->generation);
    // Ascending generation order (guaranteed by key order) — the first
    // COMMITTED entry seen is the oldest one. Never evict PENDING.
    if (!have_oldest_committed && pv->state == M_PART_COMMITTED) {
      have_oldest_committed = true;
      oldest_committed_generation = parsed->generation;
      oldest_committed_size = pv->size;
    }
  }

  if (count >= kMaxGenerationsPerPart) [[unlikely]] {
    // No empty slot — evict the oldest COMMITTED entry. Never evict a
    // PENDING entry.
    if (!have_oldest_committed) [[unlikely]] {
      // All N entries are PENDING — nothing evictable.
      return KVRGW_ERR_UPLOAD_PART_BUSY;
    }
    KeyBuf old_key;
    make_u_part_key(bucket_id, rt_view, part_number, oldest_committed_generation, old_key);
    migrate_uploaded_part_kv_to_gc(tr, bucket_id, rt_view, part_number,
                                   oldest_committed_generation, oldest_committed_size,
                                   old_key);
  }

  *out_generation = any_found ? max_generation + 1 : 0;
  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
void write_new_pending_part(KvTransaction &tr,
                            bucket_id_t bucket_id,
                            std::string_view ref_tag,
                            uint16_t part_number,
                            uint32_t generation)
{
  MPartValue pv{};
  pv.state = M_PART_PENDING;
  KeyBuf part_key;
  make_u_part_key(bucket_id, ref_tag, part_number, generation, part_key);
  uint8_t raw[sizeof(MPartValue)];
  make_m_part_value(pv, raw);
  tr.kv_put(part_key.view(), std::string_view(reinterpret_cast<const char *>(raw),
                                              sizeof(raw)));
}

//--------------------------------------------------------------------------------
// UploadPart Phase 1:
// head-get and the part's row range-scan are issued concurrently in the
// same transaction as the eviction (if any) and the new PENDING row
// insert — all one atomic unit, so two concurrent callers can never both
// see the same evictable row and overshoot N (Section 3).
//
// Returns:
//   - KVRGW_ERR_OK on success — *out_generation holds the claimed
//     generation. *out_generation is unspecified/untouched on every other
//     return path.
//   - KVRGW_ERR_NO_SUCH_UPLOAD if the head is missing or ABORTED (terminal,
//     no write).
//   - KVRGW_ERR_UPLOAD_PART_BUSY if the row is already at N entries and
//     every one of them is PENDING (nothing evictable).
//   - KVRGW_ERR_UPLOAD_IN_PROGRESS if the head is IN_TRANSIENT — not a
//     real error; an internal-only signal to the caller (upload_part_phase1
//     below) meaning "poll and retry". Reused deliberately: that same code
//     is also upload_part_phase1's own terminal, externally-visible
//     SlowDown result once its 1-second budget is exhausted — same
//     "busy, try later" family, different layer.
//   - any other KvrgwErrorCode (possibly retriable) straight through —
//     caller decides whether/how to retry.
KvrgwErrorCode upload_part_phase1_attempt(KvStore &store,
                                          std::string_view head_key_view,
                                          std::string_view part_prefix_view,
                                          std::string_view scan_end,
                                          bucket_id_t bucket_id,
                                          std::string_view rt_view,
                                          uint16_t part_number,
                                          uint32_t *out_generation,
                                          CksumAlg *out_alg)
{
  auto tr_result = store.begin_transaction();
  if (!tr_result) [[unlikely]] {
    return fdb_to_error(tr_result.error());
  }
  auto &tr = *tr_result;

  // Fired concurrently — independent reads under the same read-version
  // (Section 2/9).
  FdbGetHolder head_holder(tr->kv_async_get_holder(head_key_view));
  bool exclusive = false;
  FdbRangeHolder range_holder(tr->kv_async_get_range_holder(part_prefix_view, exclusive, scan_end,
                                                            kMaxGenerationsPerPart+1, streamingMode));
  if (const fdb_error_t werr = head_holder.wait()) [[unlikely]] {
    return fdb_to_error(werr);
  }

  // Head-Part doesn't exist or in ABORT state -> session was canceled
  if (!head_holder.present() ||
      m_head_state(head_holder.value()) == M_UPLOAD_ABORTED) [[unlikely]] {
    return KVRGW_ERR_NO_SUCH_UPLOAD;
  }

  // We are processing complete_multipart_upload() -> poll-wait and retry
  if (m_head_state(head_holder.value()) == M_UPLOAD_IN_TRANSIENT) [[unlikely]] {
    return KVRGW_ERR_UPLOAD_IN_PROGRESS;
  }

  if (const fdb_error_t werr = range_holder.wait()) [[unlikely]] {
    return fdb_to_error(werr);
  }

  // The upload's checksum algorithm lives in the S:M head.
  *out_alg = CksumAlg::NONE;
  if (auto hv = view_m_head_value(head_holder.value())) {
    *out_alg = hv->cksum_alg;
  }

  uint32_t my_generation = 0;
  const KvrgwErrorCode ec = allocate_part_slot(*tr, range_holder, bucket_id, rt_view,
                                               part_number, &my_generation);
  if (ec) [[unlikely]] {
    return ec;
  }
  write_new_pending_part(*tr, bucket_id, rt_view, part_number, my_generation);

  fdb_error_t err = tr->commit();
  if (!err) {
    *out_generation = my_generation;
    return KVRGW_ERR_OK;
  }
  return fdb_to_error(err);
}

//--------------------------------------------------------------------------------
// UploadPart Phase 1 — outer retry/poll loop (Section 5). Keys are built
// once, outside the loop — scan_end/part_prefix/head_key are invariant
// across every attempt.
//
// One overall 1-second wall-clock budget (kInTransientPollMaxMs) covers
// both reasons an attempt doesn't finish outright:
//   - a retriable FDB error: keeps the usual 10*(attempt+1) linear
//     backoff, attempt-indexed.
//   - IN_TRANSIENT: flat kInTransientPollIntervalMs poll interval, and
//     each clean (non-error) IN_TRANSIENT observation resets the
//     retriable-error backoff counter back to 0 -- it's evidence FDB
//     itself isn't struggling right now, so a subsequent error should
//     restart from the smallest delay.
//  A non-retriable error always returns immediately, bypassing the budget entirely.
struct Phase1Out {
  uint32_t generation{0};
  CksumAlg alg{CksumAlg::NONE};  // the upload's checksum algorithm
};

std::expected<Phase1Out, KvrgwErrorCode> upload_part_phase1(KvStore &store,
                                                           bucket_id_t bucket_id,
                                                           std::string_view object_name,
                                                           const RefTag &ref_tag,
                                                           uint16_t part_number)
{
  const std::string_view rt_view = ref_tag.view();
  KeyBuf head_key;
  make_m_head_key(bucket_id, object_name, rt_view, head_key);
  KeyBuf part_prefix;
  make_u_part_prefix(bucket_id, rt_view, part_number, part_prefix);
  const std::string scan_end = prefix_range_end(part_prefix.view());

  const int64_t poll_start_ms = steady_now_ms();
  int attempt = 0;
  do {
    Phase1Out out;
    const auto ec = upload_part_phase1_attempt(store, head_key.view(), part_prefix.view(),
                                               scan_end, bucket_id, rt_view, part_number,
                                               &out.generation, &out.alg);
    if (ec == KVRGW_ERR_OK) {
      return out;
    }

    if (ec == KVRGW_ERR_UPLOAD_IN_PROGRESS) {
      // Head was IN_TRANSIENT — a clean (non-error) execution, so reset
      // the retriable-error backoff counter.
      attempt = 0;
      sleep_for_msec(kInTransientPollIntervalMs);
      continue;
    }

    if (!is_retriable(ec)) {
      return std::unexpected(ec);
    }
    sleep_for_msec(10 * (attempt + 1));
    ++attempt;
    if (attempt > kMaxTxnRetries) {
      return std::unexpected(KVRGW_ERR_MAX_RETRIES_EXCEEDED);
    }
  } while (steady_now_ms() - poll_start_ms < kInTransientPollMaxMs);

  return std::unexpected(KVRGW_ERR_UPLOAD_IN_PROGRESS);
}

//--------------------------------------------------------------------------------
// UploadPart Phase 3 (txn) — finish. See plans/fixing_agent_mess.md
// Section 7. A point-get on the head's state and a point-get on this
// call's own S:U entry, fired concurrently, in one transaction:
//   - head missing or ABORTED -> self-migrate to GC (delete S:U + write a
//     single-entry G:U group) and return NoSuchUpload.
//   - head ACTIVE or IN_TRANSIENT -> commit unconditionally (put(S:U),
//     state=COMMITTED) -- no gen-id/highest-generation check. A row can
//     end up holding more than one COMMITTED entry; Complete picks the one
//     whose ETag the client sent (Section 8).
KvrgwErrorCode upload_part_phase3(KvStore &store,
                                  bucket_id_t bucket_id,
                                  std::string_view object_name,
                                  const RefTag &ref_tag,
                                  uint16_t part_number,
                                  uint32_t generation,
                                  uint64_t size,
                                  const ETag &etag,
                                  const uint8_t *cksum, size_t cksum_len)
{
  const std::string_view rt_view = ref_tag.view();
  KeyBuf head_key;
  make_m_head_key(bucket_id, object_name, rt_view, head_key);
  KeyBuf own_key;
  make_u_part_key(bucket_id, rt_view, part_number, generation, own_key);

  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    auto tr_result = store.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    FdbGetHolder head_holder(tr->kv_async_get_holder(head_key.view()));
    FdbGetHolder own_holder(tr->kv_async_get_holder(own_key.view()));

    if (const fdb_error_t werr = head_holder.wait()) {
      auto ec = fdb_to_error(werr);
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    if (const fdb_error_t werr = own_holder.wait()) {
      auto ec = fdb_to_error(werr);
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }

    if (!own_holder.present()) {
      // Already resolved by a concurrent force-clean/self-migration race
      // (Section 10) — nothing left to do.
      return KVRGW_ERR_NO_SUCH_UPLOAD;
    }

    const bool head_ok = head_holder.present() && m_head_state(head_holder.value()) != M_UPLOAD_ABORTED;

    if (!head_ok) [[unlikely]] {
      // migrate to GC in a single txn — delete S:U + write a single-entry G:U group
      migrate_uploaded_part_kv_to_gc(*tr, bucket_id, rt_view, part_number, generation,
                                     size, own_key);
      fdb_error_t err = tr->commit();
      if (!err) {
        return KVRGW_ERR_NO_SUCH_UPLOAD;
      }
      auto commit_ec = fdb_to_error(err);
      if (!is_retriable(commit_ec)) {
        return commit_ec;
      }
      sleep_for_msec(10 * (attempt + 1));
      continue;
    }

    // head is ACTIVE or IN_TRANSIENT — commit unconditionally.
    MPartValue updated{};
    updated.state = M_PART_COMMITTED;
    updated.chunk = ChunkDescriptor(CHUNK_STORAGE, rt_view.data(), bucket_id);
    updated.etag = etag;
    updated.size = size;
    updated.mtime = static_cast<uint32_t>(now_unix());
    if (cksum_len > 0) {
      memcpy(updated.cksum, cksum, std::min(cksum_len, sizeof(updated.cksum)));
    }
    uint8_t raw_buf[sizeof(MPartValue)];
    make_m_part_value(updated, raw_buf);
    tr->kv_put(own_key.view(),
              std::string_view(reinterpret_cast<const char *>(raw_buf), sizeof(raw_buf)));

    fdb_error_t err = tr->commit();
    if (!err) {
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(err);
    if (!is_retriable(commit_ec)) {
      return commit_ec;
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

}  // namespace

//--------------------------------------------------------------------------------
KvRgwServiceImpl::UploadPartResult
KvRgwServiceImpl::upload_part(tenant_id_t tenant_id,
                              std::string_view bucket_name,
                              std::string_view object_name,
                              std::string_view upload_id,
                              uint16_t part_number,
                              const uint8_t *data,
                              size_t data_len,
                              CksumAlg client_alg,
                              std::string_view client_cksum)
{
  UploadPartResult result;

  // Phase 0 — AWS validation, no KV touched. See "UploadPart — locked
  // design" in plans/multipart.md.
  if (part_number < kMinPartNumber || part_number > kMaxPartNumber) [[unlikely]] {
    result.error_code = KVRGW_ERR_INVALID_ARGUMENT;
    return result;
  }
  if (data_len == 0) [[unlikely]] {
    result.error_code = KVRGW_ERR_INVALID_ARGUMENT;
    return result;
  }
  if (data_len > kMaxPartSizeBytes) [[unlikely]] {
    result.error_code = KVRGW_ERR_ENTITY_TOO_LARGE;
    return result;
  }

  RefTag ref_tag;
  if (!RefTag::from_hex(upload_id, &ref_tag)) [[unlikely]] {
    result.error_code = KVRGW_ERR_NO_SUCH_UPLOAD;
    return result;
  }

  // Client-supplied checksum: verified before any KV is touched, so a
  // BadDigest leaves no PENDING row behind.
  uint8_t cksum[kCksumMaxDigestBytes]{};
  size_t cksum_len = 0;
  if (client_alg != CksumAlg::NONE) {
    CksumHasher hasher(client_alg);
    hasher.update(data, data_len);
    cksum_len = hasher.finalize(cksum);
    if (base64_encode(cksum, cksum_len) != client_cksum) [[unlikely]] {
      result.error_code = KVRGW_ERR_BAD_DIGEST;
      return result;
    }
  }

  auto cached = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached) [[unlikely]] {
    result.error_code = fdb_to_error(cached.error());
    return result;
  }
  if (!*cached) [[unlikely]] {
    result.error_code = KVRGW_ERR_NO_SUCH_BUCKET;
    return result;
  }
  const bucket_id_t bucket_id = (**cached).bucket_id;

  // Phase 1 (txn) — head-get + row-scan + eviction (if needed) + new
  // PENDING row insert, all atomic. See plans/fixing_agent_mess.md
  // Sections 2/3/5.
  auto gen_result = upload_part_phase1(store_, bucket_id, object_name, ref_tag, part_number);
  if (!gen_result) [[unlikely]] {
    result.error_code = gen_result.error();
    return result;
  }
  const uint32_t generation = gen_result->generation;
  const CksumAlg upload_alg = gen_result->alg;

  // Store a digest only for the upload's algorithm. A client checksum for a
  // different algorithm is rejected; for an upload without one it was only
  // verified above.
  if (upload_alg == CksumAlg::NONE) {
    cksum_len = 0;
  }
  else if (client_alg != upload_alg) {
    if (client_alg != CksumAlg::NONE) [[unlikely]] {
      result.error_code = KVRGW_ERR_INVALID_REQUEST;
      return result;
    }
    CksumHasher hasher(upload_alg);
    hasher.update(data, data_len);
    cksum_len = hasher.finalize(cksum);
  }

  // Phase 2 — data-tier write, no KV.
  unsigned char digest[16];
  unsigned int digest_len = 0;
  EVP_MD_CTX *md5_ctx = EVP_MD_CTX_new();
  if (md5_ctx == nullptr) {
    result.error_code = KVRGW_ERR_INTERNAL;
    return result;
  }
  EVP_DigestInit_ex(md5_ctx, EVP_md5(), nullptr);
  if (data_len > 0) {
    EVP_DigestUpdate(md5_ctx, data, data_len);
  }
  EVP_DigestFinal_ex(md5_ctx, digest, &digest_len);
  EVP_MD_CTX_free(md5_ctx);
  ETag etag;
  etag.load_raw(digest);

  char fname_buf[64];
  const auto fname = make_part_filename(ref_tag, part_number, generation, fname_buf);
  if (fname.empty()) [[unlikely]] {
    result.error_code = KVRGW_ERR_INTERNAL;
    return result;
  }
  auto write_ec = data_store_.write(fname, std::string_view(reinterpret_cast<const char *>(data), data_len));
  if (write_ec) [[unlikely]] {
    result.error_code = KVRGW_ERR_INTERNAL;
    return result;
  }

  // Phase 3 (txn).
  auto ec = upload_part_phase3(store_, bucket_id, object_name, ref_tag,
                               part_number, generation, data_len, etag, cksum, cksum_len);
  if (ec == KVRGW_ERR_OK) {
    result.etag = etag.to_hex();
    if (cksum_len > 0) {
      result.checksum_alg = upload_alg;
      result.checksum = base64_encode(cksum, cksum_len);
    }
    return result;
  }
  else {
    result.error_code = ec;
    return result;
  }
}

//--------------------------------------------------------------------------------
static void set_scan_begin(KeyBuf *p_prefix_buf, // IN-OUT PARAM
                           uint16_t part_number_marker)
{
  const uint16_t start_part = part_number_marker + 1;
  const uint16_t pn_be = htons(start_part);
  const uint32_t gen_be = htonl(0);
  p_prefix_buf->append(&pn_be, sizeof(pn_be));
  p_prefix_buf->append(&gen_be, sizeof(gen_be));
}

// Closes out curr_part's run: if its resolved -> highest-generation value is
//         committed, append it to out->parts.
// `exhausted` must be true only when the caller already knows the whole
//         scanned key range is spent (the final close-out after the scan
//         loop ends naturally) -- in that case, even if this push exactly
//         fills max_parts, there is nothing left to page to, so
//         is_truncated must NOT be set. When false (the mid-scan close,
//         triggered by seeing a different part_number's row), filling the
//         budget legitimately means more data follows.
// Returns true if that push filled the output budget (caller should stop scanning);
//         false otherwise (not committed, or committed but budget still has room).
//--------------------------------------------------------------------------------
static bool try_close_part(KvRgwServiceImpl::ListPartsResult* out,
                           uint32_t max_parts,
                           uint16_t curr_part,
                           const MPartValue& part_latest_gen,
                           bool exhausted)
{
  if (part_latest_gen.state != M_PART_COMMITTED) [[unlikely]] {
    return false;
  }
  out->parts.push_back(KvRgwServiceImpl::PartListEntry{curr_part,
                                                       part_latest_gen.etag.to_hex(),
                                                       part_latest_gen.size,
                                                       static_cast<int64_t>(part_latest_gen.mtime)});
  if (!exhausted && out->parts.size() >= max_parts) {
    out->is_truncated = true;
    out->next_part_number_marker = curr_part;
    return true;
  }
  return false;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::list_parts(tenant_id_t tenant_id,
                                            std::string_view bucket_name,
                                            std::string_view object_name,
                                            std::string_view upload_id,
                                            uint16_t part_number_marker,
                                            uint32_t max_parts,
                                            ListPartsResult *out)
{
  *out = {};
  if (max_parts == 0 || max_parts > AWS_MaxKeys) {
    max_parts = AWS_MaxKeys;
  }

  RefTag ref_tag;
  if (!RefTag::from_hex(upload_id, &ref_tag)) {
    return KVRGW_ERR_NO_SUCH_UPLOAD;
  }
  const std::string_view rt_view = ref_tag.view();

  auto cached = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached) {
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  bucket_id_t bucket_id = (**cached).bucket_id;

  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    KeyBuf head_key;
    make_m_head_key(bucket_id, object_name, rt_view, head_key);

    KeyBuf prefix_buf;
    make_u_prefix(bucket_id, rt_view, prefix_buf);
    // copies prefix_buf's bytes before we mutate it
    const std::string scan_end = prefix_range_end(prefix_buf.view());
    set_scan_begin(&prefix_buf, part_number_marker);
    const std::string_view scan_begin = prefix_buf.view();

    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    auto f_bkt = issue_bucket_get(*tr, tenant_id, bucket_name);
    FdbGetHolder head_holder(tr->kv_async_get_holder(head_key.view()));

    out->parts.clear();
    out->is_truncated = false;
    out->next_part_number_marker = 0;

    bool open_run = false;
    uint16_t curr_part = 0;
    uint16_t last_returned_part = part_number_marker;
    MPartValue part_latest_gen{};
    std::string cur_scan_pos(scan_begin);
    bool exclusive = false;
    KvrgwErrorCode iter_ec = KVRGW_ERR_OK;
    bool budget_hit = false;

    // while we could easily fit 1000 KV in a single call,
    // in some pathological cases we might have many old-gen from upload_part retries
    // Provide headroom for common cases (where a few retry might lurk), but use
    // looping to handle the pathological cases
    for (;;) {
      FdbRangeHolder holder(tr->kv_async_get_range_holder(cur_scan_pos,
                                                          exclusive,
                                                          scan_end,
                                                          max_parts + kListPartsGenHeadroom,
                                                          streamingMode));
      exclusive = true;
      const fdb_error_t werr = holder.wait();
      if (werr) {
        iter_ec = fdb_to_error(werr);
        break;
      }
      if (holder.count() == 0) {
        break;
      }

      for (const auto &[key, value] : holder) {
        auto parsed = parse_u_key(key);
        if (!parsed) {
          iter_ec = KVRGW_ERR_CORRUPT_VALUE;
          break;
        }
        if (open_run && parsed->part_number != curr_part) {
          if (try_close_part(out, max_parts, curr_part, part_latest_gen, /*exhausted=*/false)) {
            budget_hit = true;
            break;
          }
          open_run = false;
        }
        auto part_val = parse_m_part_value(value);
        if (!part_val) {
          iter_ec = KVRGW_ERR_CORRUPT_VALUE;
          break;
        }
        curr_part = parsed->part_number;
        part_latest_gen = *part_val;
        open_run = true;
      } // loop over scanned range

      if (budget_hit || iter_ec != KVRGW_ERR_OK) {
        break;
      }
      if (!holder.more()) {
        break;
      }
      cur_scan_pos.assign(holder.back().key);
    }

    if (iter_ec != KVRGW_ERR_OK) {
      *out = {};
      if (is_retriable(iter_ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return iter_ec;
    }

    if (!budget_hit && open_run) {
      // Scan fully exhausted without hitting the budget mid-run — close out
      // the final open run. The whole key range is spent at this point, so
      // even if this exactly fills max_parts, is_truncated must stay false.
      try_close_part(out, max_parts, curr_part, part_latest_gen, /*exhausted=*/true);
    }

    // Head existence check (LOCKED order: head before bucket).
    const fdb_error_t head_err = head_holder.wait();
    if (head_err) {
      *out = {};
      auto ec = fdb_to_error(head_err);
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    if (!head_holder.present()) {
      *out = {};
      return KVRGW_ERR_NO_SUCH_UPLOAD;
    }

    auto vb = resolve_bucket_verify(tenant_id, bucket_name, f_bkt, bucket_id, kDenyList);
    if (!vb) {
      *out = {};
      if (vb.error() == KVRGW_ERR_BUCKET_ID_MISMATCH) {
        // bucket overwritten underneath us -- re-resolve and restart clean
        auto recached = get_bucket_id_cached(tenant_id, bucket_name);
        if (!recached) {
          return fdb_to_error(recached.error());
        }
        if (!*recached) {
          return KVRGW_ERR_NO_SUCH_BUCKET;
        }
        bucket_id = (**recached).bucket_id;
        continue;
      }
      if (is_retriable(vb.error())) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return vb.error();
    }

    return KVRGW_ERR_OK;
  }
  *out = {};
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//--------------------------------------------------------------------------------
// @prefix - a filter limiting results to uploads whose object_name starts with this string
// @key_marker — the primary pagination cursor. Resumes listing after a given object_name
// @upload_id_marker — the secondary pagination cursor, only meaningful when key_marker is set
KvrgwErrorCode KvRgwServiceImpl::list_multipart_uploads(tenant_id_t tenant_id,
                                                        std::string_view bucket_name,
                                                        std::string_view prefix,
                                                        std::string_view key_marker,
                                                        std::string_view upload_id_marker,
                                                        uint32_t max_uploads,
                                                        ListMultipartUploadsResult *out)
{
  *out = {};
  if (max_uploads == 0 || max_uploads > AWS_MaxKeys) {
    max_uploads = AWS_MaxKeys;
  }

  if (!upload_id_marker.empty() && key_marker.empty()) {
    // upload_id_marker is only valid for a given key_marker
    return KVRGW_ERR_INVALID_ARGUMENT;
  }

  auto cached = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached) {
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  bucket_id_t bucket_id = (**cached).bucket_id;

  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    KeyBuf bprefix;
    make_m_bucket_prefix(bucket_id, bprefix);
    const std::string scan_end = prefix.empty() ? prefix_range_end(bprefix.view())
      : prefix_range_end(bprefix.view(), &prefix);

    KeyBuf scan_begin_key_buff;
    bool exclusive = false;
    if (!upload_id_marker.empty() && !key_marker.empty()) {
      RefTag marker_ref_tag;
      if (!RefTag::from_hex(upload_id_marker, &marker_ref_tag)) {
        return KVRGW_ERR_INVALID_ARGUMENT;
      }
      make_m_head_key(bucket_id, key_marker, marker_ref_tag.view(), scan_begin_key_buff);
      exclusive = true;
    }
    else if (!key_marker.empty()) {
      // No upload-id-marker: AWS semantics are "object_name strictly
      // greater than key_marker". Append a single 0x01 byte (object_name
      // never embeds a raw NUL byte -- the same assumption the S:M
      // NUL-delimiter scheme already relies on) so a row with
      // object_name==key_marker (whose key continues with the NUL
      // delimiter, 0x00) sorts before this bound, while any longer
      // object_name that happens to share key_marker as a textual prefix
      // (e.g. "foobar" vs. key_marker "foo") still sorts at/after it --
      // unlike prefix_range_end(header+key_marker), which would
      // incorrectly skip such names.
      KeyHeaderS hdr('S', kShardCount, kShardId, bucket_id, kCategoryMultipart);
      scan_begin_key_buff.set_header(hdr);
      scan_begin_key_buff.append(key_marker.data(), key_marker.size());
      scan_begin_key_buff.append_byte(0x01);
      exclusive = false;
    }
    else if (!prefix.empty()) {
      // prefix only needed for scan_begin when no key_marker was passed
      scan_begin_key_buff = bprefix;
      scan_begin_key_buff.append(prefix.data(), prefix.size());
      exclusive = false;
    }
    else {
      // no prefix/tokens -> scan bucket from the beginning
      scan_begin_key_buff = bprefix;
      exclusive = false;
    }

    // When a key_marker and a prefix are both present, the marker position may
    // fall before the prefix in key order. Clamp scan_begin forward to the
    // prefix start so no out-of-prefix rows are ever fetched.
    if (!prefix.empty() && !key_marker.empty()) {
      KeyBuf prefix_start(bprefix);
      prefix_start.append(prefix.data(), prefix.size());
      if (scan_begin_key_buff.view() < prefix_start.view()) {
        scan_begin_key_buff = prefix_start;
        exclusive = false;
      }
    }

    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;
    auto f_bkt = issue_bucket_get(*tr, tenant_id, bucket_name);

    out->uploads.clear();
    out->is_truncated = false;
    out->next_key_marker.clear();
    out->next_upload_id_marker.clear();

    std::string cur_scan_pos(scan_begin_key_buff.view());
    KvrgwErrorCode iter_ec = KVRGW_ERR_OK;
    bool budget_hit = false;

    for (;;) {
      FdbRangeHolder holder(tr->kv_async_get_range_holder(cur_scan_pos, exclusive, scan_end,
                                                          max_uploads, streamingMode));
      exclusive = true;
      const fdb_error_t werr = holder.wait();
      if (werr) {
        iter_ec = fdb_to_error(werr);
        break;
      }
      if (holder.count() == 0) {
        break;
      }

      for (const auto &[key, value] : holder) {
        auto head = parse_m_head_key_view(key);
        if (!head) {
          iter_ec = KVRGW_ERR_CORRUPT_VALUE;
          break;
        }

        auto hv = parse_m_head_value(value);
        if (!hv) {
          iter_ec = KVRGW_ERR_CORRUPT_VALUE;
          break;
        }
        out->uploads.push_back(MultipartUploadListEntry{std::string(head->object_name),
                                                        head->ref_tag.to_hex(),
                                                        hv->hdr.initiated_unix_ms / 1000});
        if (out->uploads.size() >= max_uploads) {
          out->is_truncated = true;
          out->next_key_marker = out->uploads.back().object_name;
          out->next_upload_id_marker = out->uploads.back().upload_id;
          budget_hit = true;
          break;
        }
      }
      if (budget_hit || iter_ec != KVRGW_ERR_OK) {
        break;
      }
      if (!holder.more()) {
        break;
      }
      cur_scan_pos.assign(holder.back().key);
    }

    if (iter_ec != KVRGW_ERR_OK) {
      *out = {};
      if (is_retriable(iter_ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return iter_ec;
    }

    auto vb = resolve_bucket_verify(tenant_id, bucket_name, f_bkt, bucket_id, kDenyList);
    if (!vb) {
      *out = {};
      if (vb.error() == KVRGW_ERR_BUCKET_ID_MISMATCH) {
        auto recached = get_bucket_id_cached(tenant_id, bucket_name);
        if (!recached) {
          return fdb_to_error(recached.error());
        }
        if (!*recached) {
          return KVRGW_ERR_NO_SUCH_BUCKET;
        }
        bucket_id = (**recached).bucket_id;
        continue;
      }
      if (is_retriable(vb.error())) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return vb.error();
    }

    return KVRGW_ERR_OK;
  }
  *out = {};
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

namespace {

// Per-part_number result of process_manifest_row. Kept free of
// ManifestScanResult/KvRgwServiceImpl (a private nested type a free function
// can't name) so this stays a small, independently testable pure function;
// scan_upload_rows folds it into the running ManifestScanResult itself via
// its local `apply` lambda.
struct RowOutcome {
  bool has_pending{false};
  bool abort{false};
  KvrgwErrorCode abort_error{KVRGW_ERR_OK};
  bool matched{false};
  ETag etag{};
  uint64_t size{0};
  uint16_t generation{0};
  uint8_t cksum[kCksumMaxDigestBytes]{};
};

// Processes one part_number's full run of S:U entries (every generation
// present) once scan_upload_rows's range-scan has moved past it. Shared by
// both Complete (client_parts non-empty) and Abort (client_parts empty --
// is_abort selects the two modes) via scan_upload_rows.
//
// `cursor` is an in/out forward-only index into client_parts, seeded to 0
// before the scan starts and threaded across every call for one upload's
// scan. Both sequences are ascending in part_number (client_parts is
// validated strictly increasing before scan_upload_rows is ever called; the
// S:U keys sort numerically since part_number is packed big-endian -- see
// make_u_part_prefix), so cursor only ever advances, turning what used to be
// a fresh std::lower_bound per row (O(rows * log N), plus binary search's
// cache-unfriendly/mispredicted jumps) into a single merge-style forward walk
// over client_parts (O(rows + N), sequential/branch-predictable access).
RowOutcome process_manifest_row(uint16_t part_number,
                                 std::span<const PartRowEntry> row,
                                 std::span<const KvRgwServiceImpl::CompletedPart> client_parts,
                                 bool is_abort,
                                 size_t &cursor,
                                 uint64_t min_part_size,
                                 CksumAlg up_alg)
{
  RowOutcome outcome;
  for (const auto &e : row) {
    if (e.state == M_PART_PENDING) {
      outcome.has_pending = true;
      break;
    }
  }

  if (is_abort) {
    return outcome;  // has_pending is all Abort needs.
  }

  // Complete-only from here on: last_client_part is only meaningful (and
  // only ever read) in this branch.
  const uint16_t last_client_part = client_parts.back().part_number;

  while (cursor < client_parts.size() && client_parts[cursor].part_number < part_number) {
    ++cursor;
  }

  if (cursor >= client_parts.size() || client_parts[cursor].part_number != part_number)  [[unlikely]] {
    return outcome;  // excluded — GC's keep-list-exclusion handles it, no entry needed.
  }

  ETag client_etag;
  if (!ETag::from_hex(client_parts[cursor].etag, &client_etag)) [[unlikely]] {
    outcome.abort = true;
    outcome.abort_error = KVRGW_ERR_INVALID_PART;
    return outcome;
  }
  // Among this row's COMMITTED entries, find every one whose ETag
  // matches the client's; prefer the highest generation on a tie
  // (Section 8) — row is in ascending generation order, so the last
  // match wins.
  const PartRowEntry *chosen = nullptr;
  for (const auto &e : row) {
    if (e.state == M_PART_COMMITTED && e.etag == client_etag) {
      chosen = &e;
    }
  }
  // no entry in this row matched client ETag -> fail complete request
  if (!chosen) [[unlikely]] {
    outcome.abort = true;
    outcome.abort_error = KVRGW_ERR_INVALID_PART;
    return outcome;
  }

  // AWS requires all part except the last one to be >= kMinPartSizeBytes
  // (min_part_size: runtime value, kMinPartSizeBytes unless overridden)
  if (part_number != last_client_part && chosen->size < min_part_size) {
    outcome.abort = true;
    outcome.abort_error = KVRGW_ERR_ENTITY_TOO_SMALL;
    return outcome;
  }

  // A per-part checksum the client lists must equal the stored one (BadDigest
  // otherwise); a part listed without one is accepted, as RGW does. Decoded
  // into a stack buffer, compared in place.
  if (up_alg != CksumAlg::NONE) {
    const auto &cp = client_parts[cursor];
    const size_t dlen = cksum_digest_len(up_alg);
    if (cp.cksum_alg != CksumAlg::NONE || !cp.cksum.empty()) {
      uint8_t dec[kCksumMaxDigestBytes + 2];
      size_t n = 0;
      if (cp.cksum_alg != up_alg || !base64_decode_to(cp.cksum, dec, sizeof(dec), &n) ||
          n != dlen || std::memcmp(dec, chosen->cksum, dlen) != 0) [[unlikely]] {
        outcome.abort = true;
        outcome.abort_error = KVRGW_ERR_BAD_DIGEST;
        return outcome;
      }
    }
    std::memcpy(outcome.cksum, chosen->cksum, dlen);
  }

  outcome.matched = true;
  outcome.etag = chosen->etag;
  outcome.size = chosen->size;
  outcome.generation = chosen->generation;
  return outcome;
}

}  // namespace

//--------------------------------------------------------------------------------
// Read-only whole-upload manifest scan shared by Complete (client_parts non-empty)
//     and Abort (client_parts empty, only has_pending is meaningful).
// See plans/fixing_agent_mess.md Sections 6/8/10/11.
std::expected<KvRgwServiceImpl::ManifestScanResult, KvrgwErrorCode>
KvRgwServiceImpl::scan_upload_rows(KvTransaction &tr,
                                   bucket_id_t bucket_id,
                                   std::string_view ref_tag,
                                   std::span<const CompletedPart> client_parts,
                                   CksumAlg up_alg)
{
  ManifestScanResult result;
  result.cksum_alg = up_alg;
  const size_t dlen = cksum_digest_len(up_alg);
  if (dlen > 0) {
    result.cksum_blob.reserve(client_parts.size() * dlen);
  }
  result.included.reserve(client_parts.size());
  result.keep_list.reserve(client_parts.size());
  result.sizes.reserve(client_parts.size());

  KeyBuf prefix_buf;
  make_u_prefix(bucket_id, ref_tag, prefix_buf);
  const std::string scan_end = prefix_range_end(prefix_buf.view());

  const uint64_t min_part_size = min_part_size_.load(std::memory_order_relaxed);
  size_t matched_count = 0;
  // Abort always passes an empty client_parts (see abort_multipart_upload)
  // only Complete needs part-matching/selection, Abort only needs has_pending.
  const bool is_abort = client_parts.empty();

  // Forward-only index into client_parts
  size_t client_cursor = 0;

  // Folds one part_number's RowOutcome (process_manifest_row above) into the
  // running result/matched_count. "First abort wins" is preserved: once
  // result.abort is set by an earlier row, later rows' abort_error is
  // discarded (matches the original inline-lambda behavior).
  auto apply = [&](uint16_t part_number, const RowOutcome &outcome) {
    if (outcome.has_pending) {
      result.has_pending = true;
    }
    if (outcome.abort && !result.abort) {
      result.abort = true;
      result.abort_error = outcome.abort_error;
    }
    if (outcome.matched) {
      if (matched_count == 0) {
        // Lowest-numbered included part (rows arrive in ascending order).
        result.first_part.part_number = part_number;
        result.first_part.generation = static_cast<uint16_t>(outcome.generation);
        result.first_part.size = outcome.size;
      }
      ++matched_count;
      result.included.emplace_back(outcome.etag);
      result.total_size += outcome.size;
      result.keep_list.emplace_back(outcome.generation);
      result.sizes.emplace_back(outcome.size);
      if (dlen > 0) {
        result.cksum_blob.append(reinterpret_cast<const char *>(outcome.cksum), dlen);
      }
    }
  };

  // A row never holds more than kMaxGenerationsPerPart entries -- UploadPart
  // evicts the oldest COMMITTED one atomically before ever inserting past
  // that bound (Section 3). On-stack, no per-row heap churn.
  std::array<PartRowEntry, kMaxGenerationsPerPart> row_buf;
  size_t row_count = 0;
  bool open_run = false;
  uint16_t curr_part = 0;

  // KV are small, can fit 5K KV in a single scan
  constexpr unsigned ENTRIES_PER_SCAN = 5*1024;
  std::string scan_pos(prefix_buf.view());
  bool exclusive = false;
  for (;;) {
    FdbRangeHolder holder(tr.kv_async_get_range_holder(scan_pos, exclusive, scan_end,
                                                       ENTRIES_PER_SCAN));
    exclusive = true;
    const fdb_error_t werr = holder.wait();
    if (werr) [[unlikely]] {
      return std::unexpected(fdb_to_error(werr));
    }
    if (holder.count() == 0) [[unlikely]] {
      break;
    }
    for (const auto &[key, value] : holder) {
      auto parsed = parse_u_key(key);
      if (!parsed) [[unlikely]] {
        continue;  // defensive — corrupt row
      }
      if (open_run && parsed->part_number != curr_part) {
        const auto &outcome = process_manifest_row(curr_part, std::span(row_buf.data(), row_count),
                                                   client_parts, is_abort, client_cursor,
                                                   min_part_size, up_alg);
        apply(curr_part, outcome);
        row_count = 0;
        open_run = false;
      }
      auto pv = parse_m_part_value(value);
      if (!pv) [[unlikely]] {
        return std::unexpected(KVRGW_ERR_CORRUPT_VALUE);
      }
      curr_part = parsed->part_number;
      if (row_count < row_buf.size()) [[likely]] {
        PartRowEntry &re = row_buf[row_count++];
        re.size = pv->size;
        re.generation = parsed->generation;
        re.etag = pv->etag;
        re.state = pv->state;
        std::memcpy(re.cksum, pv->cksum, sizeof(re.cksum));
      }
      open_run = true;
    }
    if (!holder.more()) {
      break;
    }
    scan_pos.assign(holder.back().key);
  }
  if (open_run) {
    const auto &outcome = process_manifest_row(curr_part, std::span(row_buf.data(), row_count),
                                               client_parts, is_abort, client_cursor,
                                               min_part_size, up_alg);
    apply(curr_part, outcome);
  }

  // Coverage check: a requested part_number that never appeared in the
  // scan at all. For Abort, is_abort implies matched_count==0==
  // client_parts.size() — this never fires.
  if (!is_abort && !result.abort && matched_count < client_parts.size()) {
    result.abort = true;
    result.abort_error = KVRGW_ERR_INVALID_PART;
  }

  return result;
}

namespace {

// ---- TXN 1 — verify head is ACTIVE, flip to IN_TRANSIENT. ----
//--------------------------------------------------------------------------------
KvrgwErrorCode complete_multipart_upload_step1(KvStore &store, std::string_view head_key,
                                               CksumAlg *up_alg, CksumType *up_type)
{
  auto tr_result = store.begin_transaction();
  if (!tr_result) [[unlikely]] {
    return fdb_to_error(tr_result.error());
  }
  auto &tr = *tr_result;

  FdbGetHolder head_holder(tr->kv_async_get_holder(head_key));
  if (const fdb_error_t werr = head_holder.wait()) [[unlikely]] {
    return fdb_to_error(werr);
  }

  if (!head_holder.present()) [[unlikely]] {
    return KVRGW_ERR_NO_SUCH_UPLOAD;
  }

  const uint8_t state = m_head_state(head_holder.value());
  if (state == M_UPLOAD_ABORTED) [[unlikely]] {
    return KVRGW_ERR_NO_SUCH_UPLOAD;
  }
  if (state == M_UPLOAD_IN_TRANSIENT) [[unlikely]] {
    // A second Complete/Abort arriving while Complete is already
    // consolidating — never a straight transition to ABORTED.
    return KVRGW_ERR_INTERNAL;
  }

  if (auto hv = view_m_head_value(head_holder.value())) {
    *up_alg = hv->cksum_alg;
    *up_type = hv->cksum_type;
  }

  // block the session — zero-allocation: copy the raw value into a stack
  // buffer and patch just the state byte (Section 9/no struct-level parse).
  MHeadValueBuf buf;
  write_m_head_state(head_holder.value(), M_UPLOAD_IN_TRANSIENT, buf);
  tr->kv_put(head_key, buf.view());
  fdb_error_t err = tr->commit();
  if (!err) {
    return KVRGW_ERR_OK;
  }
  return fdb_to_error(err);
}
}  // namespace

// TXN 2 — RangeScan S:U, build the manifest / keep-list.
// On failure, revert the head back to ACTIVE in this same txn so the
// client can retry (not a terminal abort).
//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::complete_multipart_upload_step2(std::string_view head_key,
                                                                 bucket_id_t bucket_id,
                                                                 std::string_view rt_view,
                                                                 std::span<const CompletedPart> client_parts,
                                                                 CksumAlg up_alg, CksumType up_type,
                                                                 const CompleteCksumReq& cksum_req,
                                                                 ManifestScanResult *p_scan_result /*OUT*/)
{
  auto tr_result = store_.begin_transaction();
  if (!tr_result) [[unlikely]] {
    return fdb_to_error(tr_result.error());
  }
  auto &tr = *tr_result;

  // Fired up front — only consumed on the (rare) revert-to-ACTIVE path
  // below, but issuing it now lets it overlap with the S:U scan's own
  // I/O instead of paying its RTT afterward (Section 9).
  FdbGetHolder head_holder(tr->kv_async_get_holder(head_key));

  auto scan = scan_upload_rows(*tr, bucket_id, rt_view, client_parts, up_alg);
  if (!scan) {
    return scan.error();
  }
  *p_scan_result = std::move(*scan);

  // Combined checksum + the client's whole-object checksum. All verification
  // happens here, before anything is written; a mismatch takes the same
  // IN_TRANSIENT -> ACTIVE revert as a bad part.
  if (!p_scan_result->abort && up_alg != CksumAlg::NONE) {
    p_scan_result->cksum_type = up_type;
    const size_t dlen = cksum_digest_len(up_alg);
    CombinedCksumBuilder builder(up_alg, up_type);
    const auto *blob = reinterpret_cast<const uint8_t *>(p_scan_result->cksum_blob.data());
    for (size_t i = 0; i < p_scan_result->sizes.size(); ++i) {
      builder.fold(blob + i * dlen, p_scan_result->sizes[i]);
    }
    builder.finalize_raw(p_scan_result->cksum_combined);

    const bool want_check = cksum_req.alg != CksumAlg::NONE;
    if (want_check && cksum_req.type != CksumType::NONE && cksum_req.type != up_type) {
      p_scan_result->abort = true;
      p_scan_result->abort_error = KVRGW_ERR_INVALID_REQUEST;
    }
    else if (want_check) {
      char rendered[96];
      const size_t n = cksum_render(up_type, p_scan_result->cksum_combined, dlen,
                                    static_cast<uint32_t>(p_scan_result->sizes.size()), rendered);
      if (cksum_req.alg != up_alg ||
          std::string_view(rendered, n) != cksum_req.value) {
        p_scan_result->abort = true;
        p_scan_result->abort_error = KVRGW_ERR_BAD_DIGEST;
      }
    }
  }

  if (!p_scan_result->abort) {
    return KVRGW_ERR_OK;
  }

  // Revert IN_TRANSIENT -> ACTIVE, same txn — client can retry.
  if (const fdb_error_t werr = head_holder.wait()) [[unlikely]] {
    return fdb_to_error(werr);
  }

  if (!head_holder.present()) [[unlikely]] {
    // Should not happen (only Abort can remove it, and it can't run
    // concurrently with IN_TRANSIENT — Section 5) — internal/corrupt.
    return KVRGW_ERR_INTERNAL;
  }

  MHeadValueBuf buf;
  write_m_head_state(head_holder.value(), M_UPLOAD_ACTIVE, buf);
  tr->kv_put(head_key, buf.view());
  fdb_error_t err = tr->commit();
  if (!err) {
    return p_scan_result->abort_error;
  }

  return fdb_to_error(err);
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::complete_multipart_upload_step3(
    tenant_id_t tenant_id, std::string_view bucket_name,
    std::string_view object_name, std::string_view head_key,
    std::string_view rt_view, const RefTag& ref_tag,
    const ETag& composite_etag, std::span<const CompletedPart> client_parts,
    const ManifestScanResult& scan_result, const PutCondition *cond,
    Step3Result *out)
{
  KeyBuf object_key, gm_key, pm_key;
  make_object_key(out->bucket_id, object_name, object_key);
  make_gm_key(out->bucket_id, rt_view, gm_key);
  make_pm_key(out->bucket_id, rt_view, pm_key);

  auto tr_result = store_.begin_transaction();
  if (!tr_result) {
    return fdb_to_error(tr_result.error());
  }
  auto &tr = *tr_result;

  FdbGetHolder head_holder(tr->kv_async_get_holder(head_key));
  FdbGetHolder existing_holder(tr->kv_async_get_holder(object_key.view()));
  FdbGetHolder bkt_holder = issue_bucket_get(*tr, tenant_id, bucket_name);

  if (const fdb_error_t werr = head_holder.wait()) [[unlikely]] {
    return fdb_to_error(werr);
  }
  if (!head_holder.present()) {
    // Should not happen (only Abort can remove it, and it can't run
    // concurrently with IN_TRANSIENT — Section 5) — internal/corrupt.
    return KVRGW_ERR_INTERNAL;
  }
  auto head_view = view_m_head_value(head_holder.value());
  if (!head_view) [[unlikely]] {
    return KVRGW_ERR_CORRUPT_VALUE;
  }
  // Only an IN_TRANSIENT upload may be installed: after an unknown commit
  // outcome of the condition-failure revert the head may already be ACTIVE.
  if (m_head_state(head_holder.value()) != M_UPLOAD_IN_TRANSIENT) [[unlikely]] {
    return KVRGW_ERR_INTERNAL;
  }

  // Hard bucket check.
  auto vb = resolve_bucket_verify(tenant_id, bucket_name, bkt_holder,
                                  out->bucket_id, kDenyWrite);
  if (!vb) [[unlikely]] {
    auto ec = vb.error();
    if (ec == KVRGW_ERR_BUCKET_ID_MISMATCH) {
      auto recached = get_bucket_id_cached(tenant_id, bucket_name);
      if (!recached) {
        return fdb_to_error(recached.error());
      }
      if (!*recached) {
        return KVRGW_ERR_NO_SUCH_BUCKET;
      }
      out->bucket_id = (**recached).bucket_id;
      return KVRGW_ERR_BUCKET_ID_MISMATCH;  // caller re-enters with updated bucket_id
    }
    return ec;
  }

  // Read existing :O:, displace + version.
  if (const fdb_error_t werr = existing_holder.wait()) [[unlikely]] {
    return fdb_to_error(werr);
  }

  // TBD:
  // PRECONDITION should be checked before modiying version state!

  // Check condition -> displace -> version. Nothing is written on a failure.
  ObjectValue new_value;
  auto ids = install_object_value(*tr, vb->versioning_state, object_key.view(),
                                  existing_holder.value_or_empty(), cond, new_value);
  if (!ids) {
    const auto ids_ec = ids.error();
    if (ids_ec == KVRGW_ERR_PRECONDITION_FAILED || ids_ec == KVRGW_ERR_NO_SUCH_KEY) {
      // Same as missing parts / wrong ETag: revert IN_TRANSIENT -> ACTIVE
      MHeadValueBuf buf;
      write_m_head_state(head_holder.value(), M_UPLOAD_ACTIVE, buf);
      tr->kv_put(head_key, buf.view());
      const fdb_error_t cerr = tr->commit();
      if (cerr) {
        return fdb_to_error(cerr);
      }
    }
    // else we are dealing with corrupted object, nothing to do
    return ids_ec;
  }

  // TBD:
  // manifest encoding should be done before starting txn

  // Build the manifest once -- its keep-list's part-numbers come from
  // client_parts (scan_upload_rows only reaches this point once every
  // entry has matched, so it's exactly the included set); generations/
  // sizes come from the scan result, same ascending order. The buffer is
  // [8 B G:M header][manifest]: first a FREE_PARTS blob (stored as is on
  // the :O: value / S:C:M child), then the header is patched to MIXED and
  // the Knob 1+2 prefix of the same bytes becomes the G:M / P:M value.
  std::array<uint16_t, kMaxPartNumber> included_part_numbers;
  for (size_t i = 0; i < client_parts.size(); ++i) {
    included_part_numbers[i] = client_parts[i].part_number;
  }
  const ManifestInput manifest_in{
      std::span<const uint16_t>(included_part_numbers.data(), client_parts.size()),
      scan_result.keep_list, scan_result.sizes};
  std::array<uint8_t, kGmHeaderSize + manifest_body_bound(kMaxPartNumber)> manifest_buf;
  size_t keep_len = 0;
  const size_t manifest_len = encode_manifest_body(manifest_buf.data() + kGmHeaderSize,
                                                   manifest_buf.size() - kGmHeaderSize,
                                                   manifest_in, &keep_len);
  if (manifest_len == 0) [[unlikely]] {
    return KVRGW_ERR_INTERNAL;
  }
  write_gm_header(manifest_buf.data(), GM_MODE_FREE_PARTS, 0);
  const std::string_view manifest_blob(reinterpret_cast<const char*>(manifest_buf.data()),
                                       kGmHeaderSize + manifest_len);

  // Assemble + write the final :O: value.
  new_value.hdr.ref_tag = ref_tag;
  new_value.hdr.etag = composite_etag;
  new_value.hdr.size = scan_result.total_size;
  stamp_mtime(new_value.hdr);
  new_value.hdr.chunk_type = CHUNK_MULTIPART;
  new_value.hdr.flags = 0;
  new_value.hdr.tags_count = head_view->tags_count;
  if (new_value.hdr.tags_count > 0) {
    new_value.hdr.flags |= kFlagExternalTags;
  }
  new_value.hdr.version_id = ids->version_id;
  new_value.hdr.next_vid = ids->next_vid;
  new_value.content_type = head_view->content_type;
  new_value.hdr.metadata_count = head_view->metadata_count;
  new_value.set_self_chunk(out->bucket_id);

  // Checksum fields, then the two-phase placement (the payloads in `placed`
  // are views of manifest_buf / scan_result.cksum_blob / the head value).
  const size_t cdlen = cksum_digest_len(scan_result.cksum_alg);
  if (scan_result.cksum_alg != CksumAlg::NONE) {
    new_value.hdr.set_cksum_alg(scan_result.cksum_alg);
    new_value.set_cksum_combined(
        std::span<const uint8_t>(scan_result.cksum_combined, cdlen));
  }
  MultipartPlacementIn place_in;
  place_in.metadata_frame = head_view->metadata_frame;
  place_in.manifest = manifest_blob;
  place_in.cksum_blob = scan_result.cksum_blob;
  place_in.first_part = scan_result.first_part;
  place_in.composite = scan_result.cksum_type == CksumType::COMPOSITE;
  MultipartPlacementOut placed;
  if (!place_multipart(new_value, place_in, placed)) [[unlikely]] {
    return KVRGW_ERR_VALUE_TOO_LARGE;
  }
  if (placed.has_child_e) {
    const ChildValueBuf e_buf(placed.child_e);
    KeyBuf child_key;
    make_child_key(out->bucket_id, rt_view, kChildTypeExtended, child_key);
    tr->kv_put(child_key.view(), e_buf.value());
  }
  if (new_value.manifest_loc == kManifestChildM) {
    KeyBuf child_key;
    make_child_key(out->bucket_id, rt_view, kChildTypeManifest, child_key);
    tr->kv_put(child_key.view(), placed.manifest_child);
  }
  if (new_value.cksum_loc == kCksumChildS) {
    KeyBuf child_key;
    make_child_key(out->bucket_id, rt_view, kChildTypeChecksum, child_key);
    tr->kv_put(child_key.view(), placed.checksum_child);
  }

  size_t written = 0;
  if (!write_o_value(*tr, object_key.view(), new_value, &written)) [[unlikely]] {
    return KVRGW_ERR_VALUE_TOO_LARGE;
  }
  store_.fdb_put_stats().record_value(written);

  tr->kv_del(head_key);
  // The :O: / S:C:M writes above already copied the FREE_PARTS blob into the
  // transaction, so the header can now be patched to the cleanup directive.
  write_gm_header(manifest_buf.data(), GM_MODE_MIXED, static_cast<uint32_t>(now_unix()));
  tr->kv_put(scan_result.has_pending ? pm_key.view() : gm_key.view(),
             std::string_view(reinterpret_cast<const char*>(manifest_buf.data()),
                              kGmHeaderSize + keep_len));

  // Tier 3: the only write outside the transaction. Verification has passed;
  // a retried Complete overwrites the same file.
  char cks_name[kRefTagSize * 2 + 16];
  std::string_view cks_file;
  if (new_value.cksum_loc == kCksumFile) {
    cks_file = checksum_filename_buf(ref_tag, cks_name);
    if (cks_file.empty() || data_store_.write(cks_file, placed.checksum_file)) [[unlikely]] {
      return KVRGW_ERR_INTERNAL;
    }
  }

  fdb_error_t err = tr->commit();
  if (!err) {
    out->etag = composite_etag.to_hex();
    out->version_id = ids->version_id;
    if (cdlen > 0) {
      char rendered[96];
      const size_t n = cksum_render(scan_result.cksum_type, scan_result.cksum_combined, cdlen,
                                    static_cast<uint32_t>(scan_result.sizes.size()), rendered);
      out->checksum.assign(rendered, n);
    }
    return KVRGW_ERR_OK;
  }
  // Remove the file on a definite failure; after an unknown commit outcome it
  // may belong to the committed object, so it is left in place.
  if (!cks_file.empty() && fdb_to_error(err) != KVRGW_ERR_FDB_COMMIT_UNKNOWN) {
    (void)data_store_.remove(cks_file);
  }
  return fdb_to_error(err);
}

//--------------------------------------------------------------------------------
KvRgwServiceImpl::CompleteMultipartResult KvRgwServiceImpl::complete_multipart_upload(
    tenant_id_t tenant_id, std::string_view bucket_name,
    std::string_view object_name, std::string_view upload_id,
    std::span<const CompletedPart> client_parts,
    std::string_view if_match, std::string_view if_none_match,
    const CompleteCksumReq& cksum_req)
{
  CompleteMultipartResult result;

  // Phase 0 — pure request-shape validation, no KV touched.
  if (client_parts.empty() || client_parts.size() > kMaxPartNumber) [[unlikely]] {
    result.error_code = KVRGW_ERR_INVALID_ARGUMENT;
    return result;
  }

  for (size_t i = 1; i < client_parts.size(); ++i) {
    if (!(client_parts[i].part_number > client_parts[i - 1].part_number)) [[unlikely]] {
      result.error_code = KVRGW_ERR_INVALID_PART_ORDER;
      return result;
    }
  }

  RefTag ref_tag;
  if (!RefTag::from_hex(upload_id, &ref_tag)) [[unlikely]] {
    result.error_code = KVRGW_ERR_NO_SUCH_UPLOAD;
    return result;
  }
  const std::string_view rt_view = ref_tag.view();

  // Encode the condition once, before step1 flips the head to IN_TRANSIENT,
  // so a malformed header fails early and leaves the upload untouched.
  PutCondition bin_cond{};
  const PutCondition *cond_ptr = nullptr;
  if (!if_match.empty() || !if_none_match.empty()) {
    const auto cec = bin_cond.encode(if_match, if_none_match);
    if (cec != KVRGW_ERR_OK) [[unlikely]] {
      result.error_code = cec;
      return result;
    }
    cond_ptr = &bin_cond;
  }

  auto cached = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached) [[unlikely]] {
    result.error_code = fdb_to_error(cached.error());
    return result;
  }
  if (!*cached) [[unlikely]] {
    result.error_code = KVRGW_ERR_NO_SUCH_BUCKET;
    return result;
  }
  bucket_id_t bucket_id = (**cached).bucket_id;


  KeyBuf head_key;
  make_m_head_key(bucket_id, object_name, rt_view, head_key);

  KvrgwErrorCode ec = KVRGW_ERR_OK;
  CksumAlg up_alg = CksumAlg::NONE;
  CksumType up_type = CksumType::NONE;
  // ---- TXN 1 — verify head is ACTIVE, flip to IN_TRANSIENT. ----
  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    ec = complete_multipart_upload_step1(store_, head_key.view(), &up_alg, &up_type);
    if (ec == KVRGW_ERR_OK) {
      break;  // proceed to TXN 2
    }
    if (is_retriable(ec)) {
      sleep_for_msec(10 * (attempt + 1));
      continue;
    }
    result.error_code = ec;
    return result;
  }

  if (ec != KVRGW_ERR_OK) [[unlikely]] {
    // Exhausted retries trying to flip to IN_TRANSIENT
    result.error_code = KVRGW_ERR_MAX_RETRIES_EXCEEDED;
    return result;
  }

  // ---- TXN 2 — RangeScan S:U, build the manifest / keep-list.
  ManifestScanResult scan_result;
  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    ec = complete_multipart_upload_step2(head_key.view(), bucket_id, rt_view,
                                         client_parts, up_alg, up_type, cksum_req,
                                         &scan_result);
    if (ec == KVRGW_ERR_OK) {
      break;  // scan succeeded (read-only so far) — proceed to TXN 3
    }
    if (is_retriable(ec)) {
      sleep_for_msec(10 * (attempt + 1));
      continue;
    }
    result.error_code = ec;
    return result;
  }

  if (ec != KVRGW_ERR_OK) [[unlikely]] {
    // Exhausted retries while still trying to commit the revert.
    result.error_code = KVRGW_ERR_MAX_RETRIES_EXCEEDED;
    return result;
  }

  // Composite ETag, folded over each included part's own etag in part-
  // number order (step3 below builds the real manifest from scan_result).
  CompositeEtagBuilder etag_builder;
  for (const auto &part_etag : scan_result.included) {
    etag_builder.fold(part_etag);
  }
  const ETag composite_etag = etag_builder.finalize();

  // ---- TXN 3 — write :O:, delete head, write the cleanup directive. ----
  Step3Result s3;
  s3.bucket_id = bucket_id;
  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    // Rebuild head_key if bucket_id was updated on a BUCKET_ID_MISMATCH retry.
    if (s3.bucket_id != bucket_id) {
      make_m_head_key(s3.bucket_id, object_name, rt_view, head_key);
    }
    ec = complete_multipart_upload_step3(tenant_id, bucket_name, object_name,
                                         head_key.view(), rt_view, ref_tag,
                                         composite_etag, client_parts, scan_result,
                                         cond_ptr, &s3);
    if (ec == KVRGW_ERR_OK) {
      result.etag = std::move(s3.etag);
      result.version_id = s3.version_id;
      result.checksum_alg = scan_result.cksum_alg;
      result.checksum_type = scan_result.cksum_type;
      result.checksum = std::move(s3.checksum);
      return result;
    }
    // BUCKET_ID_MISMATCH: step3 has already refreshed s3.bucket_id — retry.
    if (ec == KVRGW_ERR_BUCKET_ID_MISMATCH || is_retriable(ec)) {
      sleep_for_msec(10 * (attempt + 1));
      continue;
    }
    result.error_code = ec;
    return result;
  }
  result.error_code = KVRGW_ERR_MAX_RETRIES_EXCEEDED;
  return result;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::abort_multipart_upload(tenant_id_t tenant_id,
                                                        std::string_view bucket_name,
                                                        std::string_view object_name,
                                                        std::string_view upload_id)
{
  RefTag ref_tag;
  if (!RefTag::from_hex(upload_id, &ref_tag)) {
    return KVRGW_ERR_NO_SUCH_UPLOAD;
  }
  const std::string_view rt_view = ref_tag.view();

  auto cached = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached) {
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  const bucket_id_t bucket_id = (**cached).bucket_id;

  KeyBuf head_key;
  make_m_head_key(bucket_id, object_name, rt_view, head_key);

  // ---- TXN 1 — verify head is ACTIVE, flip to ABORTED. ----
  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    FdbGetHolder head_holder(tr->kv_async_get_holder(head_key.view()));
    if (const fdb_error_t werr = head_holder.wait()) {
      auto ec = fdb_to_error(werr);
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    if (!head_holder.present()) {
      return KVRGW_ERR_OK;  // idempotent — already gone
    }
    const uint8_t state = m_head_state(head_holder.value());
    if (state == M_UPLOAD_ABORTED) {
      return KVRGW_ERR_OK;  // idempotent
    }
    if (state == M_UPLOAD_IN_TRANSIENT) {
      // Abort arriving while Complete is consolidating.
      return KVRGW_ERR_INTERNAL;
    }

    // Zero-allocation: copy the raw value into a stack buffer and patch
    // just the state byte (Section 9/no struct-level parse).
    MHeadValueBuf buf;
    write_m_head_state(head_holder.value(), M_UPLOAD_ABORTED, buf);
    tr->kv_put(head_key.view(), buf.view());
    fdb_error_t err = tr->commit();
    if (!err) {
      break;  // proceed to TXN 2
    }
    auto commit_ec = fdb_to_error(err);
    if (!is_retriable(commit_ec)) {
      return commit_ec;
    }
    sleep_for_msec(10 * (attempt + 1));
    if (attempt == kMaxTxnRetries - 1) {
      return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
    }
  }

  // ---- TXN 2 (read-only) — RangeScan S:U, classify clean vs messy. ----
  ManifestScanResult scan_result;
  bool scanned = false;
  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    auto scan = scan_upload_rows(*tr, bucket_id, rt_view, std::span<const CompletedPart>{});
    if (!scan) {
      auto ec = scan.error();
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    scan_result = std::move(*scan);
    scanned = true;
    break;
  }
  if (!scanned) {
    return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
  }

  KeyBuf gm_key;
  make_gm_key(bucket_id, rt_view, gm_key);
  KeyBuf pm_key;
  make_pm_key(bucket_id, rt_view, pm_key);

  // ---- TXN 3 — delete head, write the cleanup directive. ----
  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    tr->kv_del(head_key.view());
    uint8_t deep[kGmHeaderSize];
    write_gm_header(deep, GM_MODE_DEEP, static_cast<uint32_t>(now_unix()));
    tr->kv_put(scan_result.has_pending ? pm_key.view() : gm_key.view(),
               std::string_view(reinterpret_cast<const char*>(deep), sizeof(deep)));

    fdb_error_t err = tr->commit();
    if (!err) {
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(err);
    if (!is_retriable(commit_ec)) {
      return commit_ec;
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

}  // namespace kvrgw
