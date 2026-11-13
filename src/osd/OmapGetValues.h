// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#ifndef CEPH_OSD_OMAP_GET_VALUES_H
 #define CEPH_OSD_OMAP_GET_VALUES_H

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <utility>

#include "include/buffer.h"

namespace ceph::osd {

// A synchronous, internal CLS read. Arguments are borrowed until execution completes;
// values own their bytes independently of the store, this request, and later reads.
class OmapGetValues final
{
  bufferlist arena;

public:
  const std::string& start_after;
  const std::string& filter_prefix;
  const uint64_t max_return;

  std::map<std::string, bufferlist> values;
  bool more = false;

public:
  OmapGetValues(const std::string& start_after, const std::string& filter_prefix,
                const uint64_t max_return)
    : start_after(start_after), filter_prefix(filter_prefix), max_return(max_return)
  {}

public:
  void append(std::string_view key, std::string_view value)
  {
    bufferlist data;
    if (!value.empty()) {
      // Share page allocations between rows, as the encoded-page decoder does.
      const auto length = static_cast<unsigned>(value.size());
      arena.append_hole(length).copy_in(length, value.data());
      const auto& tail = arena.back();
      data.append(tail, tail.length() - length, length);
    }
    values.emplace(key, std::move(data));
  }

  // Store retries discard partially collected rows and the prior truncation state.
  void reset()
  {
    values.clear();
    arena.clear();
    more = false;
  }
};

} // namespace ceph::osd

#endif
