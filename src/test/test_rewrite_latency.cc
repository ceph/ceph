
#include <unistd.h>
#include <map>
#include <errno.h>

#include "include/utime.h"
#include "common/Clock.h"
#include "common/errno.h"

using namespace std;

int main(int argc, const char **argv)
{
  const char *fn = argv[1];
  multimap<utime_t, utime_t> latency;
  unsigned max = 10;

  int fd = ::open(fn, O_CREAT|O_RDWR, 0644);
  if (fd < 1) {
    int err = errno;
    cerr << "failed to open " << fn << " with " << cpp_strerror(err) << std::endl;
    return -1;
  }

  while (true) {
    utime_t now = ceph_clock_now(NULL);
    int r = ::pwrite(fd, fn, strlen(fn), 0);
    assert(r >= 0);
    utime_t lat = ceph_clock_now(NULL);
    lat -= now;
    utime_t oldmin;
    if (!latency.empty())
      oldmin = latency.begin()->first;
    latency.insert(make_pair(lat, now));
    utime_t newmin = latency.begin()->first;
    while (latency.size() > max)
      latency.erase(latency.begin());
    if (oldmin == newmin) {
      cout << "latency\tat" << std::endl;
      for (multimap<utime_t,utime_t>::reverse_iterator p = latency.rbegin();
	   p != latency.rend();
	   ++p) {
	cout << p->first << "\t" << p->second << std::endl;
      }
    }
  }
}
