#define _FILE_OFFSET_BITS 64
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/statvfs.h>
#include "../../src/include/cephfs/libcephfs.h"

#define MB64 (1<<26)

int main(int argc, const char **argv)
{
        struct ceph_mount_info *cmount;
        int ret, fd, len;
        char buf[1024];

        if (argc < 3) {
                fprintf(stderr, "usage: ./%s <conf> <file>\n", argv[0]);
                exit(1);
        }

        ret = ceph_create(&cmount, NULL);
        if (ret) {
                fprintf(stderr, "ceph_create=%d\n", ret);
                exit(1);
        }

        ret = ceph_conf_read_file(cmount, argv[1]);
        if (ret) {
                fprintf(stderr, "ceph_conf_read_file=%d\n", ret);
                exit(1);
        }

        ret = ceph_conf_parse_argv(cmount, argc, argv);
        if (ret) {
                fprintf(stderr, "ceph_conf_parse_argv=%d\n", ret);
                exit(1);
        }

        ret = ceph_mount(cmount, NULL);
        if (ret) {
                fprintf(stderr, "ceph_mount=%d\n", ret);
                exit(1);
        }

        ret = ceph_chdir(cmount, "/");
        if (ret) {
                fprintf(stderr, "ceph_chdir=%d\n", ret);
                exit(1);
        }

        fd = ceph_open(cmount, argv[2], O_CREAT|O_TRUNC|O_RDWR, 0777); 
        if (fd < 0) {
                fprintf(stderr, "ceph_open=%d\n", fd);
                exit(1);
        }

        memset(buf, 'a', sizeof(buf));

        len = ceph_write(cmount, fd, buf, sizeof(buf), 0);

        fprintf(stdout, "wrote %d bytes\n", len);

        ceph_shutdown(cmount);

        return 0;
}
