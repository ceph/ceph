
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/ioctl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netdb.h>

#include "../kernel/ioctl.h"

int main(int argc, char **argv)
{
	char *fn;
	int fd, err;
	struct ceph_ioctl_layout l;
	struct ceph_ioctl_dataloc dl;

	if (argc < 3) {
		printf("usage: test_ioctls <filename> <offset>\n");
		return 1;
	}
	fn = argv[1];

	fd = open(fn, O_CREAT|O_RDWR);
	if (fd < 0) {
		perror("couldn't open file");
		return 1;
	}
	printf("file %s\n", fn);

	/* get layout */
	err = ioctl(fd, CEPH_IOC_GET_LAYOUT, (unsigned long)&l);
	if (err < 0) {
		perror("ioctl IOC_GET_LAYOUT error");
		return 1;
	}
	printf("layout:\n stripe_unit %lld\n stripe_count %lld\n object_size %lld\n data_pool %lld\n",
	       l.stripe_unit, l.stripe_count, l.object_size, l.data_pool);

	/* dataloc */
	dl.file_offset = atoll(argv[2]);
	err = ioctl(fd, CEPH_IOC_GET_DATALOC, (unsigned long)&dl);
	if (err < 0) {
		perror("ioctl IOC_GET_DATALOC error");
		return 1;
	}

	printf("dataloc:\n");
	printf(" file_offset %lld (of object start)\n", dl.file_offset);
	printf(" object '%s'\n object_offset %lld\n object_size %lld object_no %lld\n",
	       dl.object_name, dl.object_offset, dl.object_size, dl.object_no);
	printf(" block_offset %lld\n block_size %lld\n",
	       dl.block_offset, dl.block_size);

	char buf[80];
	getnameinfo((struct sockaddr *)&dl.osd_addr, sizeof(dl.osd_addr), buf, sizeof(buf), 0, 0, NI_NUMERICHOST);
	printf(" osd%lld %s\n", dl.osd, buf);

	return 0;	
}
