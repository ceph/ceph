#include "debug.h"

#include "include/compat.h"

#include "MemoryModel.h"
#if defined(__linux__)
#include <malloc.h>
#endif

#include <charconv>

#include "common/fmt_common.h"


#define dout_subsys ceph_subsys_

using namespace std;
using mem_snap_t = MemoryModel::mem_snap_t;

inline bool MemoryModel::cmp_against(
    const std::string &ln,
    std::string_view param,
    long &v) const
{
  if (ln.size() < (param.size() + 10)) {
    return false;
  }
  if (ln.starts_with(param)) {
    auto p = ln.c_str();
    auto s = p + param.size();
    // charconv does not like leading spaces
    while (*s && isblank(*s)) {
      s++;
    }
    from_chars(s, p + ln.size(), v);
    return true;
  }
  return false;
}


tl::expected<int64_t, std::string> MemoryModel::get_mapped_heap()
{
  if (!proc_maps.is_open()) {
    return tl::unexpected("unable to open proc/maps");
  }
  // always rewind before reading
  proc_maps.clear();
  proc_maps.seekg(0);

  int64_t heap = 0;

  while (proc_maps.is_open() && !proc_maps.eof()) {
    string line;
    getline(proc_maps, line);

    if (line.length() < 48) {
      // a malformed line. We expect at least
      // '560c03f8d000-560c03fae000 rw-p 00000000 00:00 0'
      continue;
    }

    const char* start = line.c_str();
    const char* dash = start;
    while (*dash && *dash != '-')
      dash++;
    if (!*dash)
      continue;
    const char* end = dash + 1;
    while (*end && *end != ' ')
      end++;
    if (!*end)
      continue;

    auto addr_end = end;
    end++;
    const char* mode = end;

    /*
     * anything 'rw' and anon is assumed to be heap.
     * But we should count lines with inode '0' and '[heap]' as well
     */
    if (mode[0] != 'r' || mode[1] != 'w') {
      continue;
    }

    auto the_rest = line.substr(5 + end - start);
    if (!the_rest.starts_with("00000000 00:00 0")) {
      continue;
    }

    std::string_view final_token{the_rest.begin() + sizeof("00000000 00:00 0") - 1,
                                 the_rest.end()};
    if (final_token.size() < 3 ||
        final_token.ends_with("[heap]") || final_token.ends_with("[stack]")) {
      // calculate and sum the size of the heap segment
      uint64_t as{0ull};
      from_chars(start, dash, as, 16);
      uint64_t ae{0ull};
      from_chars(dash + 1, addr_end, ae, 16);
      //     fmt::print("\t\tas:{:x} ae:{:x} -> {}\n", as, ae, ((ae - as) >> 10));
      long size = ae - as;
      heap += size;
    }
  }

  return heap;
}


tl::expected<mem_snap_t, std::string> MemoryModel::full_sample()
{
  if (!proc_status.is_open()) {
    return tl::unexpected("unable to open proc/status");
  }
  // always rewind before reading
  proc_status.clear();
  proc_status.seekg(0);

  mem_snap_t s;
  // we will be looking for 6 entries
  int yet_to_find = 6;

  while (!proc_status.eof() && yet_to_find > 0) {
    string ln;
    getline(proc_status, ln);

    if (cmp_against(ln, "VmSize:", s.size) ||
	cmp_against(ln, "VmRSS:", s.rss) || cmp_against(ln, "VmHWM:", s.hwm) ||
	cmp_against(ln, "VmLib:", s.lib) ||
	cmp_against(ln, "VmPeak:", s.peak) ||
	cmp_against(ln, "VmData:", s.data)) {
      yet_to_find--;
    }
  }

  // get heap size
  s.heap = static_cast<long>(get_mapped_heap().value_or(0));
  return s;
}
