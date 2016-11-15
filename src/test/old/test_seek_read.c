#include "include/types.h"
#include "common/Clock.h"

#include <linux/fs.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

int main(int argc, char **argv)
{
  char *fn = argv[1];

  int fd = ::open(fn, O_RDWR|O_DIRECT);//|O_SYNC|O_DIRECT);
  if (fd < 0) return 1;

  uint64_t bytes = 0;
  int r = ioctl(fd, BLKGETSIZE64, &bytes);
  uint64_t numblocks = bytes / 4096;

  //uint64_t numblocks = atoll(argv[2]) * 4;// / 4096;
  int count = 1000;
  
  cout << "fn " << fn << endl;
  cout << "numblocks " << numblocks << endl;
  
  int blocks = 1;
  while (blocks <= 1024) {
    //cout << "fd is " << fd << endl;

    void *buf;
    ::posix_memalign(&buf, 4096, 4096*blocks);
    
    int s = blocks*4096;

    utime_t start = ceph_clock_now();
    for (int i=0; i<count; i++) {
      off64_t o = (lrand48() % numblocks) * 4096;
      //cout << "s = " << s << " o = " << o << endl;
      //::lseek(fd, o, SEEK_SET);
      lseek64(fd, o, SEEK_SET);
      
      int r = ::read(fd, buf, blocks*4096);
      //int r = ::read(fd, buf, s);
      if (r < 0) cout << "r = " << r << " " << strerror(errno) << endl;
    }
    utime_t end = ceph_clock_now();
    
    double timeper = end - start;
    timeper /= count;
    cout << blocks << "\t" << s << "\t" << (double)timeper << endl;

    blocks *= 2;
    free(buf);
  }

  close(fd);  

}

