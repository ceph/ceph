// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RGW_FDB_EXPERIMENTAL_OPERATIONS_H
 #define CEPH_RGW_FDB_EXPERIMENTAL_OPERATIONS_H

// Included by the selected backend after defining its native factories.
namespace ceph::libfdb::experimental {

[[nodiscard]] inline auto read_version(transaction_handle txn)
{
 return detail::make_future_operation(
   detail::int64_request<fdb_transaction_get_read_version> {{}, std::move(txn)});
}

[[nodiscard]] inline auto approximate_commit_bytes(transaction_handle txn)
{
 return detail::make_future_operation(
   detail::int64_request<fdb_transaction_get_approximate_size> {{}, std::move(txn)});
}

template <query::expression SelectionT>
[[nodiscard]] auto approximate_range_size(transaction_handle txn, SelectionT selection)
{
 return detail::make_future_operation(detail::range_size_request {
  std::move(txn), ::ceph::libfdb::detail::intervals(std::move(selection))
 });
}

template <query::expression SelectionT>
[[nodiscard]] auto partitions(transaction_handle txn, SelectionT selection,
                              const std::int64_t target_bytes)
{
 return detail::make_future_operation(detail::partition_request {
  {}, std::move(txn), ::ceph::libfdb::detail::intervals(std::move(selection)), target_bytes
 });
}

// Lazily reads an owning point result on an existing transaction:
[[nodiscard]] inline auto get(raw_t, transaction_handle txn, std::string key,
                              const read_mode mode = read_mode::serializable)
{
 return detail::make_future_operation(detail::point_request {
  {}, std::move(txn), std::move(key), mode
 });
}

[[nodiscard]] inline auto get(raw_t, transaction_handle txn, const concepts::libfdb_key auto& key,
                              const read_mode mode = read_mode::serializable)
{
 return get(raw, std::move(txn), std::string {::ceph::libfdb::detail::as_libfdb_key_view(key)}, mode);
}

// Consuming the window yields the cursor for the next read:
[[nodiscard]] inline auto read_window(raw_t, transaction_handle txn, scan_cursor cursor,
                                      const read_mode mode = read_mode::serializable)
{
 return detail::make_future_operation(detail::scan_request {
  {}, std::move(txn), std::move(cursor), mode
 });
}

// Prepare once; the caller decides whether the whole transaction body may replay.
[[nodiscard]] inline auto reset_for_replay(transaction_handle txn, const std::error_code error)
{
 return detail::make_future_operation(detail::replay_request {{}, std::move(txn), error});
}

// Adopts an already-created watch; its underlying future may be active:
[[nodiscard]] inline auto when_changed(watch_handle watch)
{
 return detail::make_future_operation(detail::watch_request {
  {}, ::ceph::libfdb::detail::take_watch_future(std::move(watch))
 });
}

// A submitted commit retains its actual disposition despite cancellation:
[[nodiscard]] inline auto commit(transaction_handle txn)
{
 return detail::make_future_operation(detail::commit_request<false> {std::move(txn)});
}

[[nodiscard]] inline auto commit(with_result_t, transaction_handle txn)
{
 return detail::make_future_operation(detail::commit_request<true> {std::move(txn)});
}

// Replays one native asynchronous body and publishes only after confirmed commit:
template <typename FnT, typename ...ArgTs>
requires detail::transaction_invocation<std::decay_t<FnT>, std::decay_t<ArgTs>...>
[[nodiscard]] auto transact(database_handle dbh, FnT&& fn, ArgTs&& ...args)
{
 return detail::make_transaction_operation(std::move(dbh), std::nullopt,
   ::ceph::libfdb::detail::bind_transaction_invocation(
     std::forward<FnT>(fn), std::forward<ArgTs>(args)...));
}

template <typename FnT, typename ...ArgTs>
requires detail::transaction_invocation<std::decay_t<FnT>, std::decay_t<ArgTs>...>
[[nodiscard]] auto transact(database_handle dbh, transaction_options options,
                           FnT&& fn, ArgTs&& ...args)
{
 return detail::make_transaction_operation(std::move(dbh), std::move(options),
   ::ceph::libfdb::detail::bind_transaction_invocation(
     std::forward<FnT>(fn), std::forward<ArgTs>(args)...));
}

template <typename PolicyT, typename FnT, typename ...ArgTs>
requires detail::transaction_invocation<std::decay_t<FnT>, std::decay_t<ArgTs>...>
[[nodiscard]] auto transact(retry_if<PolicyT> policy, database_handle dbh, FnT&& fn, ArgTs&& ...args)
{
 return detail::make_transaction_operation(std::move(dbh), std::nullopt,
  ::ceph::libfdb::detail::bind_transaction_invocation(
   std::forward<FnT>(fn), std::forward<ArgTs>(args)...), std::move(policy));
}

template <typename PolicyT, typename FnT, typename ...ArgTs>
requires detail::transaction_invocation<std::decay_t<FnT>, std::decay_t<ArgTs>...>
[[nodiscard]] auto transact(retry_if<PolicyT> policy, database_handle dbh,
                           transaction_options options, FnT&& fn, ArgTs&& ...args)
{
 return detail::make_transaction_operation(std::move(dbh), std::move(options),
  ::ceph::libfdb::detail::bind_transaction_invocation(
   std::forward<FnT>(fn), std::forward<ArgTs>(args)...), std::move(policy));
}

} // ceph::libfdb::experimental

#endif
