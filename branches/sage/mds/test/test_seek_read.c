#include "include/types.h"
#include "common/Clock.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

int main(int argc, char **argv)
{
  char *fn = argv[1];
  int numblocks = atoi(argv[2]) / 4096;
  int count = 400;
  
  cout << "fn " << fn << endl;
  cout << "numblocks " << numblocks << endl;
  
  int blocks = 1;
  while (blocks <= 1024) {
    int fd = ::open(fn, O_RDWR|O_DIRECT);//|O_SYNC|O_DIRECT);
    if (fd < 0) return 1;
    //cout << "fd is " << fd << endl;

    void *buf;
    ::posix_memalign(&buf, 4096, 4096*blocks);
    
    int s = blocks*4096;

    utime_t start = g_clock.now();
    for (int i=0; i<count; i++) {
      off_t o = (rand() % numblocks) * 4096;
      //cout << "s = " << s << " o = " << o << endl;
      //::lseek(fd, o, SEEK_SET);
      int r = ::pread(fd, buf, blocks*4096, o);
      //int r = ::read(fd, buf, s);
      if (r < 0) cout << "r = " << r << " " << strerror(errno) << endl;
    }
    utime_t end = g_clock.now();
    
    double timeper = end - start;
    timeper /= count;
    cout << blocks << "\t" << s << "\t" << (double)timeper << endl;

    blocks *= 2;
    free(buf);
    close(fd);  
  }


}

