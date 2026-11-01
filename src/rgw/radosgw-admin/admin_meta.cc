// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/admin_meta.h"

#include <list>
#include <string>

#include "common/ceph_json.h"
#ifdef WITH_RADOSGW_RADOS
#include "cls/rgw/cls_rgw_types.h"
#endif
#include "include/scope_guard.h"
#include "rgw_formats.h"
#include "rgw_sal.h"
#include "radosgw-admin/util.h"

using ceph::Formatter;

namespace {

constexpr int DEFAULT_MAX_KEYS = 1000;

} // anonymous namespace

int rgw_admin_meta_list_keys(const DoutPrefixProvider* dpp,
                             rgw::sal::Driver* driver,
                             RGWFormatterFlusher& stream_flusher,
                             const rgw_admin_meta_list_options& opts)
{
  if (opts.max_entries && *opts.max_entries < 0) {
    return rgw_admin::report_error("invalid max entries", -EINVAL);
  }

  void* handle = nullptr;
  int ret = driver->meta_list_keys_init(dpp, opts.metadata_key, opts.marker,
                                        &handle);
  if (ret < 0) {
    return rgw_admin::report_error("can't get key", ret);
  }

  auto handle_guard = make_scope_guard([&] {
    driver->meta_list_keys_complete(handle);
  });

  bool truncated = false;
  uint64_t count = 0;
  Formatter* formatter = stream_flusher.get_formatter();
  const bool limit_specified = opts.max_entries.has_value();

  if (limit_specified) {
    formatter->open_object_section("result");
  }
  formatter->open_array_section("keys");

  do {
    std::list<std::string> keys;
    const uint64_t left = limit_specified
        ? static_cast<uint64_t>(*opts.max_entries) - count
        : static_cast<uint64_t>(DEFAULT_MAX_KEYS);

    // NOTE: intentional behavior change vs. the original inline code in
    // radosgw-admin.cc, which looped on `while (truncated && left > 0)`
    // using the pre-fetch `left`. That could issue one extra
    // meta_list_keys_next() call with left == 0 once count reached
    // max_entries exactly. We break here instead, and the loop condition
    // below re-checks `count` (post-fetch) rather than the stale `left`.
    if (left == 0) {
      break;
    }

    ret = driver->meta_list_keys_next(dpp, handle, left, keys, &truncated);
    if (ret < 0 && ret != -ENOENT) {
      return rgw_admin::report_error("failed to list metadata keys", ret);
    }
    if (ret != -ENOENT) {
      for (const auto& key : keys) {
        formatter->dump_string("key", key);
        ++count;
      }
      formatter->flush(std::cout);
    }
  } while (truncated &&
           (!limit_specified ||
            count < static_cast<uint64_t>(*opts.max_entries)));

  formatter->close_section(); // keys

  if (limit_specified) {
    encode_json("truncated", truncated, formatter);
    encode_json("count", count, formatter);
    if (truncated) {
      encode_json("marker", driver->meta_get_marker(handle), formatter);
    }
    formatter->close_section();
  }
  formatter->flush(std::cout);

  return 0;
}
