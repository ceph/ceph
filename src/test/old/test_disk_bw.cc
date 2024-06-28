
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
#include "common/safe_io.h"

#include <iostream>
using namespace std;

int main(int argc, char **argv)
{
  void   *buf;
  int     fd, count, loop = 0;
  
  if (argc != 4) {
    fprintf(stderr, "Usage: %s device bsize count\n", argv[0]);
    exit (0);
  }
  
  int bsize = atoi(argv[2]);
  count = atoi(argv[3]);
  
  posix_memalign(&buf, sysconf(_SC_PAGESIZE), bsize);
  
  //if ((fd = open(argv[1], O_SYNC|O_RDWR)) < 0) {  
  if ((fd = open(argv[1], O_DIRECT|O_RDWR)) < 0) {

    fprintf(stderr, "Can't open device %s\n", argv[1]);
    exit (4);
  }
  
 
  utime_t start = ceph_clock_now();
  while (loop++ < count) {
    int ret = safe_write(fd, buf, bsize);
    if (ret)
      ceph_abort();
    //if ((loop % 100) == 0) 
    //fprintf(stderr, ".");
  }
  ::fsync(fd);
  ::close(fd);
  utime_t end = ceph_clock_now();
  end -= start;


  char hostname[80];
  gethostname(hostname, 80);
  
  double mb = bsize*count/1024/1024;

  cout << hostname << "\t" << mb << " MB\t" << end << " seconds\t" << (mb / (double)end) << " MB/sec" << std::endl;
}
