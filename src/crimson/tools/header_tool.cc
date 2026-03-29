// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/// \file: tool to read and print the Crimson device super-block

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "include/buffer.h"
#include "include/denc.h"
#include "crimson/os/seastore/device.h"

static void
usage(const char* prog)
{
  fmt::print(
      stderr,
      "Usage: {} [--json] <device-superblock-file>\n"
      "Reads the first block from a Crimson device and prints the superblock "
      "fields.\n",
      prog);
}

static std::string json_escape(std::string_view s)
{
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    switch (c) {
    case '"': out += "\\\""; break;
    case '\\': out += "\\\\"; break;
    case '\b': out += "\\b"; break;
    case '\f': out += "\\f"; break;
    case '\n': out += "\\n"; break;
    case '\r': out += "\\r"; break;
    case '\t': out += "\\t"; break;
    default:
      if (static_cast<unsigned char>(c) < 0x20) {
        char buf[8];
        fmt::format_to(buf, "\\u{:04x}", (unsigned)c);
        out += buf;
      } else {
        out += c;
      }
    }
  }
  return out;
}

int main(int argc, char **argv)
{
  bool json{false};
  std::string path;
  if (argc == 2) {
    path = argv[1];
  } else if (argc == 3 && std::string(argv[1]) == "--json") {
    json = true;
    path = argv[2];
  } else {
    usage(argv[0]);
    return 1;
  }
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    fmt::print(stderr, "failed to open '{}'\n", path);
    return 1;
  }

  // Read a single block (typical superblock size) -- 64KiB should be plenty.
  constexpr size_t kReadSize = 64 * 1024;
  std::vector<char> buf(kReadSize);
  in.read(buf.data(), buf.size());
  const auto nread = in.gcount();
  if (nread == 0) {
    fmt::print(stderr, "failed to read from '{}'\n", path);
    return 1;
  }

  ceph::bufferlist bl;
  bl.append(buf.data(), nread);
  auto it = bl.cbegin();

  crimson::os::seastore::device_superblock_t sb;
  try {
    decode(sb, it);
  } catch (const std::exception &e) {
    fmt::print(stderr, "failed to decode superblock: {}\n", e.what());
    return 1;
  }

  // ---- formatting helpers ----
  bool first_section{true};

  auto open_doc = [&]() {
    if (json) fmt::print("{{\n");
  };
  auto close_doc = [&]() {
    if (json) fmt::print("}}\n");
  };
  // Begin a named section.  JSON: emit comma separator + opening brace.
  // Table: emit a blank line separator + "[name]" header.
  auto open_section = [&](std::string_view name) {
    if (json) {
      if (!first_section) fmt::print(",\n");
      first_section = false;
      fmt::print("  \"{}\": {{\n", name);
    } else {
      if (!first_section) fmt::print("\n");
      first_section = false;
      fmt::print("[{}]\n{:<24} {}\n", name, "field", "value");
    }
  };
  auto close_section = [&]() {
    if (json) fmt::print("  }}");
  };
  // Row for string-valued fields: JSON emits a quoted+escaped value,
  // table emits a plain left-aligned value.
  auto srow = [&](std::string_view key, auto val, bool last = false) {
    if (json) {
      auto s = fmt::format("{}", val);
      fmt::print("    \"{}\": \"{}\"{}\n", key, json_escape(s), last ? "" : ",");
    } else {
      fmt::print("{:<24} {}\n", key, val);
    }
  };
  // Row for numeric/opaque fields: JSON emits an unquoted value.
  auto nrow = [&](std::string_view key, auto val, bool last = false) {
    if (json)
      fmt::print("    \"{}\": {}{}\n", key, val, last ? "" : ",");
    else
      fmt::print("{:<24} {}\n", key, val);
  };

  open_doc();

  open_section("superblock");
  srow("magic", sb.magic);
  nrow("version", sb.version);
  nrow("shard_num", sb.shard_num);
  nrow("segment_size", sb.segment_size);
  nrow("block_size", sb.block_size, true);
  close_section();

  open_section("rbm");
  nrow("total_size", sb.total_size);
  nrow("journal_size", sb.journal_size);
  nrow("crc", sb.crc);
  nrow("feature", sb.feature);
  nrow("nvme_block_size", sb.nvme_block_size, true);
  close_section();

  open_section("zbd");
  nrow("segment_capacity", sb.segment_capacity);
  nrow("zones_per_segment", sb.zones_per_segment);
  nrow("zone_size", sb.zone_size);
  nrow("zone_capacity", sb.zone_capacity, true);
  close_section();

  open_section("config");
  nrow("major_dev", sb.config.major_dev);
  nrow("spec.magic", sb.config.spec.magic);
  srow("spec.dtype", sb.config.spec.dtype);
  nrow("spec.id", sb.config.spec.id);
  {
    std::ostringstream oss;
    oss << sb.config.meta;
    srow("meta", oss.str(), true);
  }
  close_section();

  // secondary_devices: JSON array (inline after config) vs separate table section
  if (json) {
    fmt::print(",\n  \"secondary_devices\": [\n");
    for (auto it = sb.config.secondary_devices.begin();
         it != sb.config.secondary_devices.end(); ++it) {
      fmt::print(
        "    {{ \"device_id\": {}, \"magic\": 0x{:x}, \"dtype\": \"{}\" }}{}\n",
        it->first, it->second.magic, it->second.dtype,
        (std::next(it) == sb.config.secondary_devices.end()) ? "" : ",");
    }
    fmt::print("  ]");
  } else if (!sb.config.secondary_devices.empty()) {
    fmt::print("\n[secondary_devices]\n");
    fmt::print("{:<24} {}\n", "device_id", "spec (magic,dtype)");
    for (auto const &p : sb.config.secondary_devices) {
      fmt::print("{:<24} (0x{:x}, {})\n", p.first, p.second.magic, p.second.dtype);
    }
  }

  // shards: JSON array vs table section
  if (json) {
    fmt::print(",\n  \"shards\": [\n");
    for (auto i = 0u; i < sb.shard_infos.size(); ++i) {
      auto &si = sb.shard_infos[i];
      fmt::print(
        "    {{ \"shard\": {}, \"size\": {}, \"segments\": {}, "
        "\"first_segment_offset\": {}, \"tracker_offset\": {}, \"start_offset\": {} }}{}\n",
        i, si.size, si.segments, si.first_segment_offset,
        si.tracker_offset, si.start_offset,
        (i + 1 == sb.shard_infos.size()) ? "" : ",");
    }
    fmt::print("  ]\n");
  } else {
    fmt::print("\n[shards]\n");
    fmt::print(
      "{:<6} {:<18} {:<12} {:<22} {:<20} {:<16}\n",
      "shard", "size", "segments", "first_segment_offset",
      "tracker_offset", "start_offset");
    for (auto i = 0u; i < sb.shard_infos.size(); ++i) {
      auto &si = sb.shard_infos[i];
      fmt::print(
        "{:<6} {:<#18x} {:<#12x} {:<#22x} {:<#20x} {:<#16x}\n",
        i,
        si.size,
        si.segments,
        si.first_segment_offset,
        si.tracker_offset,
        si.start_offset);
    }
  }

  close_doc();
  return 0;
}
