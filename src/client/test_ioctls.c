
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>

#include <sys/ioctl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netdb.h>

#include "ioctl.h"

char new_file_name[PATH_MAX];

int main(int argc, char **argv)
{
	char *fn;
	int fd, err;
	struct ceph_ioctl_layout l;
	struct ceph_ioctl_dataloc dl;

	if (argc < 3) {
		printf("usage: ceph_test_ioctls <filename> <offset>\n");
		return 1;
	}
	fn = argv[1];

	fd = open(fn, O_CREAT|O_RDWR, 0644);
	if (fd < 0) {
		perror("couldn't open file");
		return 1;
	}

	/* get layout */
        err = ioctl(fd, CEPH_IOC_GET_LAYOUT, (unsigned long)&l);
        if (err < 0) {
                perror("ioctl IOC_GET_LAYOUT error");
                return 1;
        }
        printf("layout:\n stripe_unit %lld\n stripe_count %lld\n object_size %lld\n data_pool %lld\n",
               (long long)l.stripe_unit, (long long)l.stripe_count, (long long)l.object_size, (long long)l.data_pool);


        /* set layout */
        l.stripe_unit = 1048576;
        l.stripe_count = 2;
        err = ioctl(fd, CEPH_IOC_SET_LAYOUT, (unsigned long)&l);
        if (err < 0) {
               perror("ioctl IOC_SET_LAYOUT error");
               return 1;
        }
        printf("set layout, writing to file\n");

	printf("file %s\n", fn);
	/* get layout again */
	err = ioctl(fd, CEPH_IOC_GET_LAYOUT, (unsigned long)&l);
	if (err < 0) {
		perror("ioctl IOC_GET_LAYOUT error");
		return 1;
	}
	printf("layout:\n stripe_unit %lld\n stripe_count %lld\n object_size %lld\n data_pool %lld\n",
	       (long long)l.stripe_unit, (long long)l.stripe_count, (long long)l.object_size, (long long)l.data_pool);

	/* dataloc */
	dl.file_offset = atoll(argv[2]);
	err = ioctl(fd, CEPH_IOC_GET_DATALOC, (unsigned long)&dl);
	if (err < 0) {
		perror("ioctl IOC_GET_DATALOC error");
		return 1;
	}

	printf("dataloc:\n");
	printf(" file_offset %lld (of object start)\n", (long long)dl.file_offset);
	printf(" object '%s'\n object_offset %lld\n object_size %lld object_no %lld\n",
	       dl.object_name, (long long)dl.object_offset, (long long)dl.object_size, (long long)dl.object_no);
	printf(" block_offset %lld\n block_size %lld\n",
	       (long long)dl.block_offset, (long long)dl.block_size);

	char buf[80];
	getnameinfo((struct sockaddr *)&dl.osd_addr, sizeof(dl.osd_addr), buf, sizeof(buf), 0, 0, NI_NUMERICHOST);
	printf(" osd%lld %s\n", (long long)dl.osd, buf);

	if (argc < 4)
	  return 0;

	/* set dir default layout */
	printf("testing dir policy setting\n");
	fd = open(argv[3], O_RDONLY);
        if (fd < 0) {
                perror("couldn't open dir");
                return 1;
        }

        l.object_size = 1048576;
        l.stripe_count = 1;
        err = ioctl(fd, CEPH_IOC_SET_LAYOUT_POLICY, (unsigned long)&l);
        if (err < 0) {
               perror("ioctl IOC_SET_LAYOUT_POLICY error");
               return 1;
        }
        printf("set layout, creating file\n");

        snprintf(new_file_name, sizeof(new_file_name),
		 "%s/testfile", argv[3]);
        fd = open(new_file_name, O_CREAT | O_RDWR, 0644);
        if (fd < 0) {
                perror("couldn't open file");
                return 1;
        }
        err = ioctl(fd, CEPH_IOC_GET_LAYOUT, (unsigned long)&l);
        if (err < 0) {
                perror("ioctl IOC_GET_LAYOUT error");
                return 1;
        }
        printf("layout:\n stripe_unit %lld\n stripe_count %lld\n object_size %lld\n data_pool %lld\n",
               (long long)l.stripe_unit, (long long)l.stripe_count, (long long)l.object_size, (long long)l.data_pool);
        return 0;
}
