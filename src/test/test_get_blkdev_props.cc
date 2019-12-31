#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "include/uuid.h"
#include "common/blkdev.h"

#define BUFSIZE	80

int main(int argc, char **argv)
{
	int fd, ret;
	int64_t size;
	bool discard_support;
	bool nvme;
	bool rotational;
	char dev[BUFSIZE];
	char model[BUFSIZE];
	char serial[BUFSIZE];
	char wholedisk[BUFSIZE];
	char partition[BUFSIZE];

	if (argc != 2) {
		fprintf(stderr, "usage: %s <blkdev>\n", argv[0]);
		return -1;
	}

	fd = open(argv[1], O_RDONLY);
	if (fd < 0) {
		perror("open");
		return -1;
	}

	BlkDev blkdev(fd);

	if ((ret = blkdev.get_size(&size)) < 0) {
		fprintf(stderr, "get_size: %s\n", strerror(-ret));
		return -1;
	}

	discard_support = blkdev.support_discard();

	rotational = blkdev.is_rotational();

	if ((ret = blkdev.dev(dev, BUFSIZE)) < 0) {
		fprintf(stderr, "dev: %s\n", strerror(-ret));
		return -1;
	}

	if ((ret = blkdev.partition(partition, BUFSIZE)) < 0) {
		fprintf(stderr, "partition: %s\n", strerror(-ret));
		return -1;
	}

	if ((ret = blkdev.wholedisk(wholedisk, BUFSIZE)) < 0) {
		fprintf(stderr, "wholedisk: %s\n", strerror(-ret));
		return -1;
	}

	ret = blkdev.model(model, BUFSIZE);
	if (ret == -ENOENT) {
		snprintf(model, BUFSIZE, "unknown");
	} else if (ret < 0) {
		fprintf(stderr, "model: %s\n", strerror(-ret));
		return -1;
	}

	ret = blkdev.serial(serial, BUFSIZE);
	if (ret == -ENOENT) {
		snprintf(serial, BUFSIZE, "unknown");
	} else if (ret < 0) {
		fprintf(stderr, "serial: %s\n", strerror(-ret));
		return -1;
	}

	fprintf(stdout, "Size:\t\t%" PRId64 "\n", size);
	fprintf(stdout, "Discard:\t%s\n",
		discard_support ? "supported" : "not supported");
	fprintf(stdout, "Rotational:\t%s\n", rotational ? "yes" : "no");
	fprintf(stdout, "Dev:\t\t%s\n", dev);
	fprintf(stdout, "Whole disk:\t%s\n", wholedisk);
	fprintf(stdout, "Partition:\t%s\n", partition);
	fprintf(stdout, "Model:\t\t%s\n", model);
	fprintf(stdout, "Serial:\t\t%s\n", serial);

	return 0;
}
