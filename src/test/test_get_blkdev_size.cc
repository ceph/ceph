#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "include/uuid.h"
#include "common/blkdev.h"

int main(int argc, char **argv)
{
	int fd, ret;
	int64_t size;

	if (argc != 2) {
		fprintf(stderr, "usage: %s <blkdev>\n", argv[0]);
		return -1;
	}

	fd = open(argv[1], O_RDONLY);
	if (fd < 0) {
		perror("open");
		return -1;
	}

	ret = get_block_device_size(fd, &size);
	if (ret < 0) {
		fprintf(stderr, "get_block_device_size: %s\n", strerror(-ret));
		return -1;
	}

	fprintf(stdout, "%" PRId64, size);

	return 0;
}
