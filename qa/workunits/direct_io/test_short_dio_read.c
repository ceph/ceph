#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

int main()
{
        char buf[409600];
        ssize_t r;
	int err;
	int fd = open("shortfile", O_WRONLY|O_CREAT, 0644);

	if (fd < 0) {
		err = errno;
		printf("error: open() failed with: %d (%s)\n", err, strerror(err));
		exit(err);
	}

	printf("writing first 3 bytes of 10k file\n");
        r = write(fd, "foo", 3);
	if (r == -1) {
		err = errno;
		printf("error: write() failed with: %d (%s)\n", err, strerror(err));
		close(fd);
		exit(err);
	}
        r = ftruncate(fd, 10000);
	if (r == -1) {
		err = errno;
		printf("error: ftruncate() failed with: %d (%s)\n", err, strerror(err));
		close(fd);
		exit(err);
	}
	
        fsync(fd);
        close(fd);

	printf("reading O_DIRECT\n");
        fd = open("shortfile", O_RDONLY|O_DIRECT);
	if (fd < 0) {
		err = errno;
		printf("error: open() failed with: %d (%s)\n", err, strerror(err));
		exit(err);
	}

        r = read(fd, buf, sizeof(buf));
        close(fd);

        printf("got %d\n", (int)r);
	if (r != 10000)
		return 1;
        return 0;
}
