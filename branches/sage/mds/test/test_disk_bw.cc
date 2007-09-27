
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/uio.h>

#include "common/Clock.h"

#include <iostream>
using namespace std;

int main(int argc, char **argv)
{
  void   *buf;
  int     dev_id, count, loop = 0, ret;
  
  if (argc != 3) {
    fprintf(stderr, "Usage: %s device mb\n", argv[0]);
    exit (0);
  }
  
  count = atoi(argv[2]);
  int bsize = 1048576;
  
  posix_memalign(&buf, sysconf(_SC_PAGESIZE), bsize);
  
  if ((dev_id = open(argv[1], O_DIRECT|O_RDWR)) < 0) {
    fprintf(stderr, "Can't open device %s\n", argv[1]);
    exit (4);
  }
  
  fprintf(stderr, "device is %s, dev_id is %d\n", argv[1], dev_id);
  
  utime_t start = g_clock.now();
  while (loop++ < count) {
    ret = ::write(dev_id, buf, bsize);
    if ((loop % 100) == 0) 
      fprintf(stderr, ".");
  }
  utime_t end = g_clock.now();
  end -= start;

  int mb = count;

  cout << mb << " MB, " << end << " seconds, " << ((double)mb / (double)end) << " MB/sec" << std::endl;
}
