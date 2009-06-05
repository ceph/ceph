
#include "include/types.h"
#include "MemoryModel.h"
#include "config.h"
#include "debug.h"

#include <fstream>

void MemoryModel::_sample(snap *psnap)
{
  ifstream f;

  f.open("/proc/self/status");
  if (!f.is_open()) {
    dout(0) << "check_memory_usage unable to open /proc/self/status" << dendl;
    return;
  }

  while (!f.eof()) {
    string line;
    getline(f, line);
    
    if (strncmp(line.c_str(), "VmSize:", 7) == 0)
      psnap->size = atoi(line.c_str() + 7);
    else if (strncmp(line.c_str(), "VmRSS:", 6) == 0)
      psnap->rss = atoi(line.c_str() + 7);
    else if (strncmp(line.c_str(), "VmHWM:", 6) == 0)
      psnap->hwm = atoi(line.c_str() + 7);
    else if (strncmp(line.c_str(), "VmLib:", 6) == 0)
      psnap->lib = atoi(line.c_str() + 7);
    else if (strncmp(line.c_str(), "VmPeak:", 7) == 0)
      psnap->peak = atoi(line.c_str() + 7);
    else if (strncmp(line.c_str(), "VmData:", 7) == 0)
      psnap->data = atoi(line.c_str() + 7);
  }
  f.close();

  f.open("/proc/self/maps");
  if (!f.is_open()) {
    dout(0) << "check_memory_usage unable to open /proc/self/maps" << dendl;
    return;
  }

  int heap = 0;
  while (f.is_open() && !f.eof()) {
    string line;
    getline(f, line);
    //dout(0) << "line is " << line << dendl;

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

    //dout(0) << std::hex << as << " to " << ae << std::dec << dendl;

    end++;
    const char *mode = end;

    int skip = 4;
    while (skip--) {
      end++;
      while (*end && *end != ' ') end++;
    }
    if (*end)
      end++;

    int size = ae - as;
    //dout(0) << "size " << size << " mode is '" << mode << "' end is '" << end << "'" << dendl;

    /*
     * anything 'rw' and anon is assumed to be heap.
     */
    if (mode[0] == 'r' && mode[1] == 'w' && !*end)
      heap += size;
  }

  psnap->heap = heap >> 10;

  // ...
  struct mallinfo mi = mallinfo();
  
  psnap->malloc = mi.uordblks >> 10;
  psnap->mmap = mi.hblks >> 10;
  

  ofstream log("/tmp/memlog", ios::app);
  log << "heap " << heap
      << "\trss " << psnap->rss
      << "\tmi\t" << mi.arena
      << "\t" << mi.ordblks
      << "\t" << mi.uordblks / 1024
      << "\t" << mi.fordblks / 1024
      << "\t" << mi.hblks 
      << "\t" << mi.hblkhd / 1024
      << std::endl;
}
