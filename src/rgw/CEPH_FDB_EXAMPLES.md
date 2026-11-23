<!--
Ceph - scalable distributed file system

Copyright (C) 2026 International Business Machines Corp. (IBM)

This is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License version 2.1, as published by the Free Software
Foundation. See file COPYING.
-->

# Ceph metadata maintenance with libfdb

Include [`rgw/ceph_fdb.h`](ceph_fdb.h) when storing Ceph values such as
`ceph::buffer::list`. It supplies the serialization adaptation; the transaction
and query interfaces are the same as in the [libfdb cookbook](fdb/EXAMPLES.md).
The example below uses an application-owned metadata namespace in a Ceph process.

Suppose a maintenance operation must archive records from two bucket subspaces,
exclude their internal records, and report exactly the keys from its confirmed
batch. Query algebra describes the eligible keys. A bounded collection controls
the batch size. A borrowed helper returns typed application failures while the
outer Transactor owns recovery, the atomic commit, and staged publication.

## Select and archive one batch

The source records contain Ceph buffer lists. Each successful record operation
copies that payload to a separate archive subspace and clears its source key in
the same transaction. An existing destination or an oversized record rejects the
whole attempt before commit. The example limits a batch to 16 records and each
payload to 64 KiB; production callers must also choose appropriate native
transaction timeout and size budgets.

```cpp
#include "rgw/ceph_fdb.h"

#include <cstddef>
#include <expected>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace metadata_example {

namespace lfdb = ceph::libfdb;
namespace q = lfdb::query;

enum struct archive_error { destination_exists, payload_too_large };

struct archive_progress final
{
  std::size_t records = 0;
  std::size_t bytes = 0;

  bool has_more = false;
};

q::interval_set archive_selection(const std::string_view marker = {})
{
  q::interval_set selected {
    q::prefix("example/ceph/live/bucket-a/") | q::prefix("example/ceph/live/bucket-b/")
  };

  selected -= q::prefix("example/ceph/live/bucket-a/.internal/");
  selected -= q::prefix("example/ceph/live/bucket-b/.internal/");

  if (not marker.empty()) {
    selected &= q::after(marker);
  }

  return selected;
}

auto archive_record(lfdb::transaction_handle& txn, const std::string& source,
                    const ceph::buffer::list& payload)
  -> std::expected<std::size_t, archive_error>
{
  if (64 * 1024 < payload.length()) {
    return std::unexpected(archive_error::payload_too_large);
  }

  const auto destination = "example/ceph/archive/" + source;

  if (lfdb::key_exists(txn, destination)) {
    return std::unexpected(archive_error::destination_exists);
  }

  lfdb::set(txn, destination, payload);
  lfdb::erase(txn, source);

  return payload.length();
}

auto archive_batch(const lfdb::database_handle& dbh, const q::interval_set& selected,
                   std::vector<std::string>& archived)
{
  const auto txr = lfdb::make_transactor(dbh);

  return txr([&txr, &selected](auto& txn, auto& output)
    -> std::expected<archive_progress, lfdb::transaction_failure<archive_error>> {
      const auto batch = lfdb::collect(
        lfdb::scan<ceph::buffer::list>(txn, selected), lfdb::page{16});

      std::size_t bytes = 0;

      for (const auto& [key, payload] : batch.rows) {
        auto moved = txr(txn, archive_record, key, payload);

        if (not moved) {
          return std::unexpected(std::move(moved.error()));
        }

        bytes += *moved;
        output.push_back(key);
      }

      return archive_progress {batch.rows.size(), bytes, batch.has_more};
    }, lfdb::staged(std::in_place, archived));
}

} // namespace metadata_example
```

The scan, destination check, copy, and erase all use the supplied transaction.
The borrowed call commits nothing: returning its complete classified failure
lets the owner distinguish native recovery from application rejection. The scan
already supplied each payload, so the helper does not read the record again.
The archive prefix lies outside the selected source prefixes.

`page{16}` bounds returned records across the entire composed selection.
`query_options::result_limit` instead applies to each emitted native interval;
it is not an overall batch limit. The rows retain owning keys and payloads until
the body finishes. Per-attempt output starts empty, and only a confirmed commit
replaces the caller's `archived` vector.

## Use completion knowledge and resume explicitly

```cpp
namespace metadata_example {

void show_one_batch(const lfdb::database_handle& dbh)
{
  std::vector<std::string> archived;

  const auto completed = archive_batch(dbh, archive_selection(), archived);

  if (not completed) {
    const auto& failure = completed.error();

    std::cerr << "Archive failed in phase " << std::to_underlying(failure.phase)
              << "; uncertain: " << failure.status.has_unknown_commit()
              << "; may finish later: " << failure.status.may_commit_later() << '\n';

    if (const auto *rejection =
          std::get_if<lfdb::application_error<archive_error>>(&failure.cause)) {
      std::cerr << "Application rejection: " << static_cast<int>(rejection->error) << '\n';
    }

    return;
  }

  std::cout << "Archived " << completed->value.records << " records / "
            << completed->value.bytes << " payload bytes\n";

  if (completed->value.has_more) {
    std::cout << "Resume after " << archived.back() << '\n';
  }
}

} // namespace metadata_example
```

For the next batch, pass `archive_selection(archived.back())` after successful
completion with `has_more`. Each batch is its own transaction and can observe a
newer read version; coordinate writers or repeat the pass if inserts behind the
marker must also be included.

Keep logging, notifications, and other external effects after result delivery.
Native replay can repeat the body, but it does not publish intermediate vectors.
The default policy stops after an uncertain commit. Reconcile that status with
durable database state before deciding whether to submit another batch; an empty
or unchanged output vector alone does not prove that the database was unchanged.
Applications needing recoverable notifications can write an outbox record in
the transaction and deliver it separately.

This recipe composes the existing geometry, traversal, serialization, and
completion contracts. See [interval mathematics](fdb/EXAMPLES.md#interval-mathematics-for-reconciliation)
for deriving additions/removals and scoped exclusions, and
[transaction composition](fdb/EXAMPLES.md#reusing-a-transaction-and-composing-results)
for the full ownership and error contract.
