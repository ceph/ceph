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

#include "kv_store.hpp"

#include "constants.hpp"
#include "error_codes.hpp"
#include "keys.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>

namespace kvrgw {

namespace {

struct FdbKeyValue {
  std::string key;
  std::string value;
};

struct FdbRangePage {
  std::vector<FdbKeyValue> rows;
  bool more = false;
};

FdbFuture fdb_issue_get_range(FDBTransaction *tr, std::string_view begin_key,
                              bool begin_or_equal, int begin_offset,
                              std::string_view end_key, int limit,
                              FDBStreamingMode mode)
{
  return FdbFuture(fdb_transaction_get_range(
      tr, reinterpret_cast<const uint8_t *>(begin_key.data()),
      static_cast<int>(begin_key.size()), begin_or_equal ? 1 : 0, begin_offset,
      reinterpret_cast<const uint8_t *>(end_key.data()),
      static_cast<int>(end_key.size()), 0, 1, limit, 0, mode, 0, 0, 0));
}

std::expected<FdbRangePage, fdb_error_t> fdb_wait_get_range(FdbFuture &future)
{
  if (fdb_error_t err = fdb_future_block_until_ready(future.raw())) {
    return std::unexpected(err);
  }

  const ::FDBKeyValue *kv = nullptr;
  int count = 0;
  fdb_bool_t more = false;
  if (fdb_error_t err =
          fdb_future_get_keyvalue_array(future.raw(), &kv, &count, &more)) {
    return std::unexpected(err);
  }

  FdbRangePage page;
  page.more = more != 0;
  page.rows.reserve(static_cast<size_t>(count));
  for (int i = 0; i < count; ++i) {
    FdbKeyValue row;
    row.key.assign(reinterpret_cast<const char *>(kv[i].key), kv[i].key_length);
    row.value.assign(reinterpret_cast<const char *>(kv[i].value),
                     kv[i].value_length);
    page.rows.push_back(std::move(row));
  }
  return page;
}

std::expected<FdbRangePage, fdb_error_t>
fdb_get_range(FDBTransaction *tr, std::string_view begin_key,
              bool begin_or_equal, int begin_offset, std::string_view end_key,
              int limit, FDBStreamingMode mode)
{
  FdbFuture future = fdb_issue_get_range(tr, begin_key, begin_or_equal,
                                         begin_offset, end_key, limit, mode);
  return fdb_wait_get_range(future);
}

//--------------------------------------------------------------------------------
std::expected<std::vector<RangeScanResult>, fdb_error_t>
range_scan_in_transaction(FDBTransaction *tr, std::string_view start,
                          std::string_view end, int limit, bool exclusive_scan,
                          FDBStreamingMode mode = FDB_STREAMING_MODE_EXACT)
{
  std::vector<RangeScanResult> results;
  std::string range_begin(start);
  bool begin_or_equal = exclusive_scan;
  // assert(limit > 0);
  if (!limit || limit > kFdbMaxKeys) {
    limit = kFdbMaxKeys; // force 5000 keys limit
  }
  while (true) {
    const int remaining = limit - static_cast<int>(results.size());
    auto page =
        fdb_get_range(tr, range_begin, begin_or_equal, 1, end, remaining, mode);
    if (!page) {
      return std::unexpected(page.error());
    }

    if (page->rows.empty()) {
      break;
    }

    for (const auto &row : page->rows) {
      results.push_back(RangeScanResult{row.key, row.value});
      if (limit > 0 && static_cast<int>(results.size()) >= limit) {
        return results;
      }
    }

    range_begin = page->rows.back().key;
    if (!page->more) {
      break;
    }
    begin_or_equal = true;
  }
  return results;
}

} // namespace

struct KvStore::Impl {
  FDBDatabase *db{nullptr};
};

struct KvTransaction::Impl {
  FDBTransaction *tr{nullptr};
};

std::expected<KvStore, fdb_error_t> KvStore::create()
{
  KvStore store;
  store.impl_ = std::make_unique<Impl>();
  if (fdb_error_t err = fdb_create_database(nullptr, &store.impl_->db)) {
    return std::unexpected(err);
  }
  return store;
}

KvStore::~KvStore()
{
  if (impl_ && impl_->db) {
    fdb_database_destroy(impl_->db);
  }
}

KvStore::KvStore(KvStore &&other) noexcept : impl_(std::move(other.impl_)) {}
KvStore &KvStore::operator=(KvStore &&other) noexcept
{
  impl_ = std::move(other.impl_);
  return *this;
}

FDBDatabase *KvStore::database() const { return impl_->db; }

std::expected<std::optional<std::string>, fdb_error_t>
KvStore::get(std::string_view key)
{
  auto tr = begin_transaction();
  if (!tr) {
    return std::unexpected(tr.error());
  }
  return (*tr)->kv_get(key);
}

std::expected<void, fdb_error_t> KvStore::set(std::string_view key,
                                              std::string_view value)
{
  auto tr = begin_transaction();
  if (!tr) {
    return std::unexpected(tr.error());
  }
  (*tr)->kv_put(key, value);
  return (*tr)->commit();
}

std::expected<std::vector<RangeScanResult>, fdb_error_t>
KvStore::range_scan(std::string_view start, std::string_view end, int limit,
                    bool exclusive_scan, bool disable_ryw,
                    FDBStreamingMode mode)
{
  FDBTransaction *tr = nullptr;
  if (fdb_error_t err = fdb_database_create_transaction(impl_->db, &tr)) {
    return std::unexpected(err);
  }
  if (disable_ryw) {
    if (fdb_error_t err = fdb_transaction_set_option(
            tr, FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, nullptr, 0)) {
      fdb_transaction_destroy(tr);
      return std::unexpected(err);
    }
  }
  auto results =
      range_scan_in_transaction(tr, start, end, limit, exclusive_scan, mode);
  fdb_transaction_destroy(tr);
  return results;
}

std::expected<std::unique_ptr<KvTransaction>, fdb_error_t>
KvStore::begin_transaction()
{
  FDBTransaction *raw_tr = nullptr;
  if (fdb_error_t err = fdb_database_create_transaction(impl_->db, &raw_tr)) {
    return std::unexpected(err);
  }
  return std::unique_ptr<KvTransaction>(new KvTransaction(*this, raw_tr));
}

std::expected<uint32_t, KvrgwErrorCode> KvStore::allocate_rgw_id()
{
  constexpr int kMaxRetries = 10;
  const auto key = make_l_key(kLocalTypeNumeric, kLocalCounterRgwId);
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr = begin_transaction();
    if (!tr) {
      if (fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED,
                              tr.error())) {
        continue;
      }
      return std::unexpected(fdb_to_error(tr.error()));
    }
    auto val = (*tr)->kv_get(key.view());
    if (!val) {
      if (fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED,
                              val.error())) {
        continue;
      }
      return std::unexpected(fdb_to_error(val.error()));
    }

    uint64_t counter = 0;
    if (*val) {
      if ((*val)->size() != sizeof(uint64_t)) {
        return std::unexpected(KVRGW_ERR_CORRUPT_VALUE);
      }
      std::memcpy(&counter, (*val)->data(), sizeof(counter));
    }

    const uint64_t next = counter + 1;
    std::string buf(sizeof(uint64_t), '\0');
    std::memcpy(buf.data(), &next, sizeof(next));
    (*tr)->kv_put(key.view(), buf);
    auto rc = (*tr)->commit();
    if (rc) {
      return static_cast<uint32_t>(next);
    }
    if (!fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED,
                             rc.error())) {
      return std::unexpected(fdb_to_error(rc.error()));
    }
  }
  return std::unexpected(KVRGW_ERR_MAX_RETRIES_EXCEEDED);
}

//---------------------------------------------------------------------------------
KvTransaction::KvTransaction(KvStore &store, FDBTransaction *tr)
    : store_(store), impl_(std::make_unique<Impl>())
{
  impl_->tr = tr;
}

//---------------------------------------------------------------------------------
KvTransaction::~KvTransaction()
{
  if (impl_->tr) {
    fdb_transaction_destroy(impl_->tr);
  }
}

//---------------------------------------------------------------------------------
FdbFuture KvTransaction::kv_async_get(std::string_view key)
{
  return FdbFuture(fdb_transaction_get(
      impl_->tr, reinterpret_cast<const uint8_t *>(key.data()),
      static_cast<int>(key.size()), 0));
}

//---------------------------------------------------------------------------------
std::expected<std::optional<std::string>, fdb_error_t>
KvTransaction::kv_wait_get(FdbFuture &f)
{
  if (fdb_error_t err = fdb_future_block_until_ready(f.raw())) {
    return std::unexpected(err);
  }

  fdb_bool_t present = 0;
  const uint8_t *value = nullptr;
  int value_length = 0;

  fdb_error_t err =
      fdb_future_get_value(f.raw(), &present, &value, &value_length);
  if (err) {
    return std::unexpected(err);
  }

  if (!present || value == nullptr) {
    return std::nullopt;
  }

  return std::string(reinterpret_cast<const char *>(value), value_length);
}

//---------------------------------------------------------------------------------
std::expected<std::optional<std::string>, fdb_error_t>
KvTransaction::kv_get(std::string_view key)
{
  auto f = kv_async_get(key);
  return kv_wait_get(f);
}

//---------------------------------------------------------------------------------
std::expected<void, fdb_error_t> KvTransaction::disable_ryw()
{
  if (fdb_error_t err = fdb_transaction_set_option(
          impl_->tr, FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, nullptr, 0)) {
    return std::unexpected(err);
  }
  return {};
}

//---------------------------------------------------------------------------------
FdbFuture KvTransaction::kv_async_get_range(std::string_view start,
                                            bool exclusive_begin,
                                            std::string_view end, int limit,
                                            FDBStreamingMode mode)
{
  return fdb_issue_get_range(impl_->tr, start, exclusive_begin, 1, end, limit,
                             mode);
}

//---------------------------------------------------------------------------------
fdb_error_t FdbGetHolder::wait() noexcept
{
  if (fdb_error_t err = fdb_future_block_until_ready(f_.raw())) {
    return err;
  }
  fdb_bool_t present = 0;
  if (fdb_error_t err = fdb_future_get_value(f_.raw(), &present, &data_, &len_)) {
    return err;
  }
  present_ = present != 0 && data_ != nullptr;
  ready_ = true;
  return 0;
}

//---------------------------------------------------------------------------------
FdbGetHolder KvTransaction::kv_async_get_holder(std::string_view key)
{
  return FdbGetHolder(kv_async_get(key));
}

//---------------------------------------------------------------------------------
fdb_error_t FdbRangeHolder::wait() noexcept
{
  if (fdb_error_t err = fdb_future_block_until_ready(f_.raw())) {
    return err;
  }
  fdb_bool_t more = 0;
  if (fdb_error_t err = fdb_future_get_keyvalue_array(
          f_.raw(), &kv_, &count_, &more)) {
    return err;
  }
  more_ = more != 0;
  ready_ = true;
  return 0;
}

//---------------------------------------------------------------------------------
FdbRangeHolder KvTransaction::kv_async_get_range_holder(
    std::string_view start, bool exclusive_begin, std::string_view end,
    int limit, FDBStreamingMode mode)
{
  return FdbRangeHolder(fdb_issue_get_range(impl_->tr, start, exclusive_begin, 1,
                                            end, limit, mode));
}

//---------------------------------------------------------------------------------
std::expected<std::vector<RangeScanResult>, fdb_error_t>
KvTransaction::kv_wait_range(FdbFuture &f, bool *more)
{
  auto page = fdb_wait_get_range(f);
  if (!page) {
    return std::unexpected(page.error());
  }
  if (more != nullptr) {
    *more = page->more;
  }
  std::vector<RangeScanResult> out;
  out.reserve(page->rows.size());
  for (auto &row : page->rows) {
    out.push_back(RangeScanResult{std::move(row.key), std::move(row.value)});
  }
  return out;
}

//---------------------------------------------------------------------------------
void KvTransaction::kv_put(std::string_view key, std::string_view value)
{
  store_.fdb_put_stats().record(key.size(), value.size());
  fdb_transaction_set(impl_->tr, reinterpret_cast<const uint8_t *>(key.data()),
                      static_cast<int>(key.size()),
                      reinterpret_cast<const uint8_t *>(value.data()),
                      static_cast<int>(value.size()));
}

//---------------------------------------------------------------------------------
void KvTransaction::kv_del(std::string_view key)
{
  fdb_transaction_clear(impl_->tr,
                        reinterpret_cast<const uint8_t *>(key.data()),
                        static_cast<int>(key.size()));
}

//---------------------------------------------------------------------------------
void KvTransaction::kv_range_clear(std::string_view begin, std::string_view end)
{
  fdb_transaction_clear_range(impl_->tr,
                              reinterpret_cast<const uint8_t *>(begin.data()),
                              static_cast<int>(begin.size()),
                              reinterpret_cast<const uint8_t *>(end.data()),
                              static_cast<int>(end.size()));
}

std::expected<std::vector<RangeScanResult>, fdb_error_t>
KvTransaction::kv_range_scan(std::string_view start, std::string_view end,
                             int limit)
{
  bool exclusive_scan = false;
  return range_scan_in_transaction(impl_->tr, start, end, limit,
                                   exclusive_scan);
}

std::expected<void, fdb_error_t> KvTransaction::commit()
{
  FdbFuture future(fdb_transaction_commit(impl_->tr));
  if (fdb_error_t err = fdb_future_block_until_ready(future.raw())) {
    return std::unexpected(err);
  }
  if (fdb_error_t err = fdb_future_get_error(future.raw())) {
    return std::unexpected(err);
  }
  return {};
}

FdbFuture KvTransaction::commit_async()
{
  return FdbFuture(fdb_transaction_commit(impl_->tr));
}

std::expected<void, fdb_error_t> KvTransaction::resolve_commit(FdbFuture &f)
{
  if (fdb_error_t err = fdb_future_get_error(f.raw())) {
    return std::unexpected(err);
  }
  return {};
}

} // namespace kvrgw
