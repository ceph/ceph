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

#include "data_store.hpp"
#include "fdb.hpp"
#include "gc_value.hpp"
#include "keys.hpp"
#include "kv_store.hpp"
#include "ref_tag.hpp"

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

void run_network()
{
  if (fdb_error_t err = fdb_run_network()) {
    std::cerr << "fdb_run_network failed: " << fdb_get_error(err) << std::endl;
    std::abort();
  }
}

std::string prefix_range_end(std::string_view prefix)
{
  std::string end(prefix);
  while (!end.empty()) {
    const unsigned char last = static_cast<unsigned char>(end.back());
    if (last < 0xFF) {
      end.back() = static_cast<char>(last + 1);
      return end;
    }
    end.pop_back();
  }
  return std::string("\xFF", 1);
}

std::optional<uint64_t> blob_file_size(const kvrgw::DataStore &data_store,
                                       std::string_view ref_tag)
{
  const auto path = data_store.path_for(ref_tag);
  std::error_code ec;
  if (!std::filesystem::exists(path, ec) || ec) {
    return std::nullopt;
  }
  const auto size = std::filesystem::file_size(path, ec);
  if (ec) {
    return std::nullopt;
  }
  return size;
}

uint64_t parse_size_arg(const char *text)
{
  if (text == nullptr || *text == '\0') {
    throw std::runtime_error("empty size argument");
  }

  char *end = nullptr;
  const double value = std::strtod(text, &end);
  if (end == text) {
    throw std::runtime_error(std::string("invalid size: ") + text);
  }

  uint64_t multiplier = 1;
  if (*end != '\0') {
    std::string suffix(end);
    for (char &ch : suffix) {
      ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    if (suffix == "B" || suffix.empty()) {
      multiplier = 1;
    }
    else if (suffix == "K" || suffix == "KB") {
      multiplier = 1024ULL;
    }
    else if (suffix == "M" || suffix == "MB") {
      multiplier = 1024ULL * 1024ULL;
    }
    else if (suffix == "G" || suffix == "GB") {
      multiplier = 1024ULL * 1024ULL * 1024ULL;
    }
    else {
      throw std::runtime_error(std::string("unknown size suffix: ") + suffix);
    }
  }

  if (value < 0) {
    throw std::runtime_error("size must be non-negative");
  }
  return static_cast<uint64_t>(value * static_cast<double>(multiplier));
}

struct GcEntry {
  std::string ref_tag_hex;
  uint8_t size_tier{};
  std::optional<uint64_t> blob_bytes;
  std::string bucket_id_hex;
  uint16_t shard_count{};
  uint16_t shard_id{};
  bool size_mismatch{};
};

bool tier_overlaps_range(uint8_t tier, uint64_t min_bytes, uint64_t max_bytes)
{
  const uint64_t tier_min = kvrgw::size_tier_min_bytes(tier);
  const uint64_t tier_max = kvrgw::size_tier_max_bytes(tier);
  return tier_min <= max_bytes && tier_max >= min_bytes;
}

bool entry_in_size_range(const GcEntry &entry, uint64_t min_bytes,
                         uint64_t max_bytes)
{
  if (entry.blob_bytes.has_value()) {
    const uint64_t size = *entry.blob_bytes;
    return size >= min_bytes && size <= max_bytes;
  }
  return tier_overlaps_range(entry.size_tier, min_bytes, max_bytes);
}

std::vector<GcEntry> scan_gc_entries(kvrgw::KvStore &store,
                                     kvrgw::DataStore &data_store,
                                     int scan_limit)
{
  const auto prefix = kvrgw::make_g_prefix();
  const auto end = prefix_range_end(prefix.view());
  auto rows_result = store.range_scan(prefix.view(), end, scan_limit);
  if (!rows_result) {
    std::cerr << "range_scan failed: " << fdb_get_error(rows_result.error())
              << '\n';
    return {};
  }
  const auto &rows = *rows_result;

  std::vector<GcEntry> entries;
  entries.reserve(rows.size());
  for (const auto &row : rows) {
    const auto parts = kvrgw::parse_go_key(row.key);
    if (!parts) {
      continue;
    }
    const auto rt_view = kvrgw::ref_tag_view(parts->ref_tag);
    GcEntry entry;
    entry.size_tier = parts->size_tier;
    entry.ref_tag_hex = kvrgw::RefTagGenerator::to_hex(rt_view);
    entry.bucket_id_hex = parts->bucket_id.to_hex();
    entry.shard_count = parts->shard_count;
    entry.shard_id = parts->shard_id;

    const auto gc_val = kvrgw::parse_gc_value(row.value);
    if (gc_val) {
      entry.blob_bytes = gc_val->hdr.object_size;
      if (gc_val->hdr.chunk.type == kvrgw::CHUNK_STORAGE) {
        const auto actual = blob_file_size(data_store, rt_view);
        if (actual.has_value() && *actual != gc_val->hdr.object_size) {
          entry.size_mismatch = true;
        }
      }
      else if (gc_val->hdr.chunk.type == kvrgw::CHUNK_CHILD_D) {
        const uint8_t st =
            kvrgw::d_size_tier_from_size(gc_val->hdr.object_size);
        const auto d_key =
            kvrgw::make_d_key(parts->bucket_id, st, rt_view, gc_val->hdr.mtime);
        auto d_val_result = store.get(d_key.view());
        if (d_val_result && *d_val_result &&
            (*d_val_result)->size() !=
                sizeof(kvrgw::ChildValueHeader) + gc_val->hdr.object_size) {
          entry.size_mismatch = true;
        }
      }
    }
    else {
      entry.blob_bytes = blob_file_size(data_store, rt_view);
    }
    entries.push_back(std::move(entry));
  }
  return entries;
}

std::filesystem::path data_root_from_env()
{
  if (const char *env = std::getenv("KVRGW_DATA")) {
    return std::filesystem::path(env);
  }
  return std::filesystem::current_path() / "data";
}

void usage(const char *prog)
{
  std::cerr << "Usage:\n"
            << "  " << prog << " count\n"
            << "  " << prog << " count-by-tier\n"
            << "  " << prog << " list [--limit N]\n"
            << "  " << prog << " list-by-size <min> <max> [--limit N]\n"
            << "  " << prog << " set-gc-config key=value ...\n"
            << "  " << prog << " query-active-age\n"
            << "  " << prog << " get-gc-config\n"
            << "  " << prog
            << " wait-applied --handle N [--max-gap N] [--poll-sec N]\n"
            << "  " << prog << " raw-get <key-hex>\n"
            << "  " << prog << " raw-set <key-hex>  (reads value from stdin)\n"
            << "\n"
            << "Inspect commands scan pending GC keys (G:O). Admin commands "
               "use KVRGW_ADMIN_SOCKET\n"
            << "(default /tmp/kvrgw-admin.sock). Size args accept suffixes B, "
               "K/KB, M/MB, G/GB.\n";
}

std::string admin_socket_path()
{
  if (const char *env = std::getenv("KVRGW_ADMIN_SOCKET")) {
    return env;
  }
  return "/tmp/kvrgw-admin.sock";
}

std::string admin_request(const std::string &line)
{
  const int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("admin socket create failed");
  }

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  const std::string path = admin_socket_path();
  if (path.size() >= sizeof(addr.sun_path)) {
    close(fd);
    throw std::runtime_error("admin socket path too long");
  }
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

  if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(fd);
    throw std::runtime_error("admin connect failed: " + path);
  }

  std::string out = line;
  out.push_back('\n');
  if (write(fd, out.data(), out.size()) < 0) {
    close(fd);
    throw std::runtime_error("admin write failed");
  }

  std::string response;
  char buf[512];
  while (true) {
    const ssize_t n = read(fd, buf, sizeof(buf));
    if (n <= 0) {
      break;
    }
    response.append(buf, static_cast<size_t>(n));
    if (response.find('\n') != std::string::npos) {
      break;
    }
  }
  close(fd);
  const auto nl = response.find('\n');
  if (nl != std::string::npos) {
    response.resize(nl);
  }
  return response;
}

uint32_t parse_active_age(const std::string &response)
{
  const std::string prefix = "ACTIVE_AGE=";
  if (response.rfind(prefix, 0) != 0) {
    throw std::runtime_error("unexpected query response: " + response);
  }
  return static_cast<uint32_t>(std::stoul(response.substr(prefix.size())));
}

uint32_t parse_handle(const std::string &response)
{
  const std::string prefix = "OK HANDLE=";
  if (response.rfind(prefix, 0) != 0) {
    if (response == "BUSY") {
      throw std::runtime_error("BUSY");
    }
    throw std::runtime_error("set-gc-config failed: " + response);
  }
  return static_cast<uint32_t>(std::stoul(response.substr(prefix.size())));
}

struct ParsedGcConfig {
  uint32_t active_age = 0;
  uint32_t pending_age = 0;
  bool suspended = false;
  int interval_sec = 0;
  int max_objects_per_sec = 0;
  int max_mb_per_sec = 0;
};

ParsedGcConfig parse_gc_config(const std::string &response)
{
  if (response.rfind("CONFIG", 0) != 0) {
    throw std::runtime_error("unexpected get response: " + response);
  }
  ParsedGcConfig cfg;
  std::istringstream in(response.substr(6));
  std::string token;
  while (in >> token) {
    const auto eq = token.find('=');
    if (eq == std::string::npos) {
      continue;
    }
    const std::string key = token.substr(0, eq);
    const std::string value = token.substr(eq + 1);
    if (key == "active_age") {
      cfg.active_age = static_cast<uint32_t>(std::stoul(value));
    }
    else if (key == "pending_age") {
      cfg.pending_age = static_cast<uint32_t>(std::stoul(value));
    }
    else if (key == "suspended") {
      cfg.suspended = (value != "0");
    }
    else if (key == "interval_sec") {
      cfg.interval_sec = std::stoi(value);
    }
    else if (key == "max_objects_per_sec") {
      cfg.max_objects_per_sec = std::stoi(value);
    }
    else if (key == "max_mb_per_sec") {
      cfg.max_mb_per_sec = std::stoi(value);
    }
  }
  return cfg;
}

bool is_admin_command(const std::string &command)
{
  return command == "set-gc-config" || command == "query-active-age" ||
         command == "get-gc-config" || command == "wait-applied";
}

int cmd_set_gc_config(int argc, char **argv)
{
  if (argc < 3) {
    throw std::runtime_error("set-gc-config requires key=value fields");
  }
  std::ostringstream req;
  req << "SET";
  for (int i = 2; i < argc; ++i) {
    req << ' ' << argv[i];
  }
  const std::string response = admin_request(req.str());
  const uint32_t handle = parse_handle(response);
  std::cout << "HANDLE=" << handle << '\n';
  return 0;
}

int cmd_query_active_age()
{
  const uint32_t age = parse_active_age(admin_request("QUERY"));
  std::cout << "ACTIVE_AGE=" << age << '\n';
  return 0;
}

int cmd_get_gc_config()
{
  const ParsedGcConfig cfg = parse_gc_config(admin_request("GET"));
  std::cout << "active_age=" << cfg.active_age << '\n'
            << "pending_age=" << cfg.pending_age << '\n'
            << "suspended=" << (cfg.suspended ? 1 : 0) << '\n'
            << "interval_sec=" << cfg.interval_sec << '\n'
            << "max_objects_per_sec=" << cfg.max_objects_per_sec << '\n'
            << "max_mb_per_sec=" << cfg.max_mb_per_sec << '\n';
  return 0;
}

int cmd_wait_applied(int argc, char **argv)
{
  uint32_t handle = 0;
  uint32_t max_gap = 1;
  int poll_sec = 1;
  for (int i = 2; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--handle") {
      handle = static_cast<uint32_t>(std::stoul(argv[++i]));
    }
    else if (arg == "--max-gap") {
      max_gap = static_cast<uint32_t>(std::stoul(argv[++i]));
    }
    else if (arg == "--poll-sec") {
      poll_sec = std::stoi(argv[++i]);
    }
    else {
      throw std::runtime_error("unknown wait-applied option: " + arg);
    }
  }
  if (handle == 0) {
    throw std::runtime_error("wait-applied requires --handle");
  }
  if (poll_sec <= 0) {
    throw std::runtime_error("poll-sec must be positive");
  }

  while (true) {
    const uint32_t active_age = parse_active_age(admin_request("QUERY"));
    if (active_age == handle - 1) {
      sleep(static_cast<unsigned>(poll_sec));
      continue;
    }
    if (active_age == handle) {
      std::cout << "state=done active_age=" << active_age << '\n';
      return 0;
    }
    if (active_age < handle - 1) {
      std::cerr
          << "state=error reason=active_age_lt_handle_minus_one active_age="
          << active_age << " handle=" << handle << '\n';
      return 2;
    }
    if (active_age > handle + max_gap) {
      std::cerr
          << "state=error reason=active_age_gt_handle_plus_max_gap active_age="
          << active_age << " handle=" << handle << " max_gap=" << max_gap
          << '\n';
      return 2;
    }
    std::cout << "state=expired active_age=" << active_age
              << " handle=" << handle << '\n';
    return 3;
  }
}

int run_admin_command(int argc, char **argv)
{
  const std::string command = argv[1];
  if (command == "set-gc-config") {
    return cmd_set_gc_config(argc, argv);
  }
  if (command == "query-active-age") {
    return cmd_query_active_age();
  }
  if (command == "get-gc-config") {
    return cmd_get_gc_config();
  }
  if (command == "wait-applied") {
    return cmd_wait_applied(argc, argv);
  }
  usage(argv[0]);
  return 1;
}

int parse_list_limit(int argc, char **argv, int start_idx, int default_limit)
{
  int limit = default_limit;
  for (int i = start_idx; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--limit") {
      if (i + 1 >= argc) {
        throw std::runtime_error("--limit requires a value");
      }
      limit = std::stoi(argv[++i]);
      if (limit <= 0) {
        throw std::runtime_error("--limit must be positive");
      }
      continue;
    }
    throw std::runtime_error("unknown argument: " + arg);
  }
  return limit;
}

int cmd_count(kvrgw::KvStore &store, kvrgw::DataStore &data_store)
{
  const auto entries = scan_gc_entries(store, data_store, 0);
  uint64_t total_bytes = 0;
  size_t missing_blobs = 0;
  for (const auto &entry : entries) {
    if (entry.blob_bytes.has_value()) {
      total_bytes += *entry.blob_bytes;
    }
    else {
      ++missing_blobs;
    }
  }
  std::cout << "pending_gc_entries=" << entries.size() << '\n'
            << "total_bytes=" << total_bytes << '\n'
            << "missing_blobs=" << missing_blobs << '\n';
  return 0;
}

int cmd_count_by_tier(kvrgw::KvStore &store, kvrgw::DataStore &data_store)
{
  const auto entries = scan_gc_entries(store, data_store, 0);
  struct TierStats {
    size_t count = 0;
    uint64_t bytes = 0;
    size_t missing = 0;
  };
  std::vector<TierStats> tiers(35);
  for (const auto &entry : entries) {
    auto &stats = tiers[entry.size_tier];
    ++stats.count;
    if (entry.blob_bytes.has_value()) {
      stats.bytes += *entry.blob_bytes;
    }
    else {
      ++stats.missing;
    }
  }

  std::cout << "tier\tcount\ttotal_bytes\tmissing_blobs\ttier_min_bytes\ttier_"
               "max_bytes\n";
  for (uint8_t tier = 0; tier < tiers.size(); ++tier) {
    if (tiers[tier].count == 0) {
      continue;
    }
    std::cout << static_cast<unsigned>(tier) << '\t' << tiers[tier].count
              << '\t' << tiers[tier].bytes << '\t' << tiers[tier].missing
              << '\t' << kvrgw::size_tier_min_bytes(tier) << '\t'
              << kvrgw::size_tier_max_bytes(tier) << '\n';
  }
  return 0;
}

int cmd_list(kvrgw::KvStore &store, kvrgw::DataStore &data_store, int limit)
{
  const auto entries = scan_gc_entries(store, data_store, limit);
  std::cout
      << "ref_tag\tsize_tier\tblob_bytes\tbucket_id\tshard_count\tshard_id\n";
  for (const auto &entry : entries) {
    std::cout << entry.ref_tag_hex << '\t'
              << static_cast<unsigned>(entry.size_tier) << '\t';
    if (entry.blob_bytes.has_value()) {
      std::cout << *entry.blob_bytes;
    }
    else {
      std::cout << '-';
    }
    std::cout << '\t' << entry.bucket_id_hex << '\t' << entry.shard_count
              << '\t' << entry.shard_id << '\n';
  }
  std::cout << "listed=" << entries.size() << '\n';
  return 0;
}

int cmd_list_by_size(kvrgw::KvStore &store, kvrgw::DataStore &data_store,
                     uint64_t min_bytes, uint64_t max_bytes, int limit)
{
  if (min_bytes > max_bytes) {
    throw std::runtime_error("min size must be <= max size");
  }

  const auto entries = scan_gc_entries(store, data_store, 0);
  std::cout
      << "ref_tag\tsize_tier\tblob_bytes\tbucket_id\tshard_count\tshard_id\n";
  int listed = 0;
  for (const auto &entry : entries) {
    if (!entry_in_size_range(entry, min_bytes, max_bytes)) {
      continue;
    }
    std::cout << entry.ref_tag_hex << '\t'
              << static_cast<unsigned>(entry.size_tier) << '\t';
    if (entry.blob_bytes.has_value()) {
      std::cout << *entry.blob_bytes;
    }
    else {
      std::cout << '-';
    }
    std::cout << '\t' << entry.bucket_id_hex << '\t' << entry.shard_count
              << '\t' << entry.shard_id << '\n';
    ++listed;
    if (limit > 0 && listed >= limit) {
      break;
    }
  }
  std::cout << "listed=" << listed << "\tmin_bytes=" << min_bytes
            << "\tmax_bytes=" << max_bytes << '\n';
  return 0;
}

} // namespace

int main(int argc, char **argv)
{
  if (argc < 2) {
    usage(argv[0]);
    return 1;
  }

  const std::string command = argv[1];
  if (is_admin_command(command)) {
    try {
      return run_admin_command(argc, argv);
    }
    catch (const std::exception &ex) {
      std::cerr << "gc_ctl failed: " << ex.what() << '\n';
      return 1;
    }
  }

  fdb_error_t code = fdb_select_api_version(FDB_API_VERSION);
  if (code) {
    std::cerr << "fdb_select_api_version failed: " << fdb_get_error(code)
              << "\n";
    return 1;
  }
  if (fdb_error_t net_err = fdb_setup_network()) {
    std::cerr << "fdb_setup_network failed: " << fdb_get_error(net_err) << "\n";
    return 1;
  }
  std::thread network_thread(run_network);

  int rc = 1;
  try {
    auto store_result = kvrgw::KvStore::create();
    if (!store_result) {
      std::cerr << "fdb_create_database failed: "
                << fdb_get_error(store_result.error()) << '\n';
      if (fdb_error_t e = fdb_stop_network()) {
        std::cerr << "fdb_stop_network failed: " << fdb_get_error(e) << "\n";
      }
      network_thread.join();
      return 1;
    }
    auto &store = *store_result;
    kvrgw::FileDataStore data_store(data_root_from_env());

    if (command == "count") {
      if (argc != 2) {
        usage(argv[0]);
        rc = 1;
      }
      else {
        rc = cmd_count(store, data_store);
      }
    }
    else if (command == "count-by-tier") {
      if (argc != 2) {
        usage(argv[0]);
        rc = 1;
      }
      else {
        rc = cmd_count_by_tier(store, data_store);
      }
    }
    else if (command == "list") {
      const int limit = parse_list_limit(argc, argv, 2, 100);
      rc = cmd_list(store, data_store, limit);
    }
    else if (command == "list-by-size") {
      if (argc < 4) {
        usage(argv[0]);
        rc = 1;
      }
      else {
        const uint64_t min_bytes = parse_size_arg(argv[2]);
        const uint64_t max_bytes = parse_size_arg(argv[3]);
        const int limit = parse_list_limit(argc, argv, 4, 100);
        rc = cmd_list_by_size(store, data_store, min_bytes, max_bytes, limit);
      }
    }
    else if (command == "raw-get") {
      if (argc != 3) {
        std::cerr << "Usage: gc_ctl raw-get <key-hex>\n";
        rc = 1;
      }
      else {
        const std::string key_hex = argv[2];
        std::string key;
        for (size_t i = 0; i + 1 < key_hex.size(); i += 2) {
          key.push_back(
              static_cast<char>(std::stoi(key_hex.substr(i, 2), nullptr, 16)));
        }
        auto val = store.get(key);
        if (!val) {
          std::cerr << "fdb error: " << fdb_get_error(val.error()) << '\n';
          rc = 1;
        }
        else if (!*val) {
          std::cerr << "not found\n";
          rc = 1;
        }
        else {
          std::cout.write((*val)->data(),
                          static_cast<std::streamsize>((*val)->size()));
          rc = 0;
        }
      }
    }
    else if (command == "raw-set") {
      if (argc != 3) {
        std::cerr << "Usage: gc_ctl raw-set <key-hex> (value from stdin)\n";
        rc = 1;
      }
      else {
        const std::string key_hex = argv[2];
        std::string key;
        for (size_t i = 0; i + 1 < key_hex.size(); i += 2) {
          key.push_back(
              static_cast<char>(std::stoi(key_hex.substr(i, 2), nullptr, 16)));
        }
        std::string val((std::istreambuf_iterator<char>(std::cin)),
                        std::istreambuf_iterator<char>());
        auto tr = store.begin_transaction();
        if (!tr) {
          std::cerr << "begin_transaction failed: " << fdb_get_error(tr.error())
                    << '\n';
          rc = 1;
        }
        else {
          (*tr)->kv_put(key, val);
          auto crc = (*tr)->commit();
          if (!crc) {
            std::cerr << "commit failed: " << fdb_get_error(crc.error())
                      << '\n';
            rc = 1;
          }
          else {
            rc = 0;
          }
        }
      }
    }
    else {
      usage(argv[0]);
      rc = 1;
    }
  }
  catch (const std::exception &ex) {
    std::cerr << "gc_ctl failed: " << ex.what() << '\n';
    rc = 1;
  }

  if (fdb_error_t e = fdb_stop_network()) {
    std::cerr << "fdb_stop_network failed: " << fdb_get_error(e) << "\n";
  }
  network_thread.join();
  return rc;
}
