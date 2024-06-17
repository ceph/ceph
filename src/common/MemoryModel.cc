#include "MemoryModel.h"
#include "include/compat.h"
#include "debug.h"
#if defined(__linux__)
#include <malloc.h>
#endif

#include <charconv>

#include "common/fmt_common.h"


#define dout_subsys ceph_subsys_

using namespace std;
using mem_snap_t = MemoryModel::mem_snap_t;

MemoryModel::MemoryModel(CephContext *cct_)
  : cct(cct_)
{
}

std::optional<int64_t> MemoryModel::get_mapped_heap()
{
  if (!proc_maps.is_open()) {
    ldout(cct, 0) <<  fmt::format("MemoryModel::get_mapped_heap() unable to open {}", proc_maps_fn) << dendl;
    return std::nullopt;
  }
  // always rewind before reading
  proc_maps.clear();
  proc_maps.seekg(0);

  int64_t heap = 0;

  while (proc_maps.is_open() && !proc_maps.eof()) {
    string line;
    getline(proc_maps, line);

    const char *start = line.c_str();
    const char *dash = start;
    while (*dash && *dash != '-') dash++;
    if (!*dash)
      continue;
    const char *end = dash + 1;
    while (*end && *end != ' ') end++;
    if (!*end)
      continue;
    unsigned long long as = strtoll(start, 0, 16);
    unsigned long long ae = strtoll(dash+1, 0, 16);

    end++;
    const char *mode = end;

    int skip = 4;
    while (skip--) {
      end++;
      while (*end && *end != ' ') end++;
    }
    if (*end)
      end++;

    long size = ae - as;

    /*
     * anything 'rw' and anon is assumed to be heap.
     */
    if (mode[0] == 'r' && mode[1] == 'w' && !*end)
      heap += size;
  }

  return heap;
}



void MemoryModel::_sample(mem_snap_t *psnap)
{
  if (!proc_status.is_open()) {
    ldout(cct, 0) <<  fmt::format("MemoryModel::sample() unable to open {}", proc_stat_fn) << dendl;
    return;
  }
  // always rewind before reading
  proc_status.clear();
  proc_status.seekg(0);

  while (!proc_status.eof()) {
    string line;
    getline(proc_status, line);

    if (strncmp(line.c_str(), "VmSize:", 7) == 0)
      psnap->size = atol(line.c_str() + 7);
    else if (strncmp(line.c_str(), "VmRSS:", 6) == 0)
      psnap->rss = atol(line.c_str() + 7);
    else if (strncmp(line.c_str(), "VmHWM:", 6) == 0)
      psnap->hwm = atol(line.c_str() + 7);
    else if (strncmp(line.c_str(), "VmLib:", 6) == 0)
      psnap->lib = atol(line.c_str() + 7);
    else if (strncmp(line.c_str(), "VmPeak:", 7) == 0)
      psnap->peak = atol(line.c_str() + 7);
    else if (strncmp(line.c_str(), "VmData:", 7) == 0)
      psnap->data = atol(line.c_str() + 7);
  }

  // get heap size
  psnap->heap = static_cast<long>(get_mapped_heap().value_or(0));
}
