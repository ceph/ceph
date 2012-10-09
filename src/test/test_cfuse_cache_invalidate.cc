
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#define REGION 1048576
int main(int argc, char *argv[]) {

  pid_t p = fork();
  char buf[REGION];
  memset(buf, 0, sizeof(buf));

  if (p != 0) {
    int done = 0;
    int fd = open(argv[1], O_RDWR|O_CREAT, 0644);
    if (fd < 0) {
      perror(argv[1]);
      return 1;
    }

    int i = 0;
    while(!done) {
      printf("writing %d\n", i++);
      assert(pwrite(fd, buf, REGION, 0) == REGION);
      int status;
      int ret = waitpid(p, &status, WNOHANG);
      assert(ret >= 0);
      if (ret > 0) {
	done = 1;
      }
    }
    close(fd);
  } else {
    sleep(1);
    int fd = open(argv[2], O_RDONLY, 0644);
    if (fd < 0) {
      perror(argv[2]);
      return 1;
    }

    printf("reading\n");
    assert(pread(fd, buf, REGION, 0) == REGION);
    close(fd);
  }

  return 0;
}
