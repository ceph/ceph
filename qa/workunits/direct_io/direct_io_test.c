/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <inttypes.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/*
 * direct_io_test
 *
 * This test does some I/O using O_DIRECT.
 *
 * Semantics of O_DIRECT can be found at http://lwn.net/Articles/348739/
 *
 */

static int g_num_pages = 100;

static int g_duration = 10;

struct chunk {
        uint64_t offset;
        uint64_t pad0;
        uint64_t pad1;
        uint64_t pad2;
        uint64_t pad3;
        uint64_t pad4;
        uint64_t pad5;
        uint64_t not_offset;
} __attribute__((packed));

static int page_size;

static char temp_file[] = "direct_io_temp_file_XXXXXX";

static int safe_write(int fd, const void *buf, signed int len)
{
        const char *b = (const char*)buf;
        /* Handle EINTR and short writes */
        while (1) {
                int res = write(fd, b, len);
                if (res < 0) {
                        int err = errno;
                        if (err != EINTR) {
                                return err;
                        }
                }
                len -= res;
                b += res;
                if (len <= 0)
                        return 0;
        }
}

static int do_read(int fd, char *buf, int buf_sz)
{
        /* We assume no short reads or EINTR. It's not really clear how
         * those things interact with O_DIRECT. */
        int ret = read(fd, buf, buf_sz);
        if (ret < 0) {
                int err = errno;
                printf("do_read: error: %d (%s)\n", err, strerror(err));
                return err;
        }
        if (ret != buf_sz) {
                printf("do_read: short read\n");
                return -EIO;
        }
        return 0;
}

static int setup_temp_file(void)
{
        int fd;
        int64_t num_chunks, i;

        if (page_size % sizeof(struct chunk)) {
                printf("setup_big_file: page_size doesn't divide evenly "
                        "into data blocks.\n");
                return -EINVAL;
        }

        fd = mkstemp(temp_file);
        if (fd < 0) {
                int err = errno;
                printf("setup_big_file: mkostemps failed with error %d\n", err);
                return err;
        }

        num_chunks = g_num_pages * (page_size / sizeof(struct chunk));
        for (i = 0; i < num_chunks; ++i) {
                int ret;
                struct chunk c;
                memset(&c, 0, sizeof(c));
                c.offset = i * sizeof(struct chunk);
                c.pad0 = 0;
                c.pad1 = 1;
                c.pad2 = 2;
                c.pad3 = 3;
                c.pad4 = 4;
                c.pad5 = 5;
                c.not_offset = ~c.offset;
                ret = safe_write(fd, &c, sizeof(struct chunk));
                if (ret) {
                        printf("setup_big_file: safe_write failed with "
                               "error: %d\n", ret);
                        TEMP_FAILURE_RETRY(close(fd));
                        unlink(temp_file);
                        return ret;
                }
        }
        TEMP_FAILURE_RETRY(close(fd));
        return 0;
}

static int verify_chunk(const struct chunk *c, uint64_t offset)
{
        if (c->offset != offset) {
                printf("verify_chunk(%" PRId64 "): bad offset value (got: %"
                       PRId64 ", expected: %" PRId64 "\n", offset, c->offset, offset);
                return EIO;
        }
        if (c->pad0 != 0) {
                printf("verify_chunk(%" PRId64 "): bad pad0 value\n", offset);
                return EIO;
        }
        if (c->pad1 != 1) {
                printf("verify_chunk(%" PRId64 "): bad pad1 value\n", offset);
                return EIO;
        }
        if (c->pad2 != 2) {
                printf("verify_chunk(%" PRId64 "): bad pad2 value\n", offset);
                return EIO;
        }
        if (c->pad3 != 3) {
                printf("verify_chunk(%" PRId64 "): bad pad3 value\n", offset);
                return EIO;
        }
        if (c->pad4 != 4) {
                printf("verify_chunk(%" PRId64 "): bad pad4 value\n", offset);
                return EIO;
        }
        if (c->pad5 != 5) {
                printf("verify_chunk(%" PRId64 "): bad pad5 value\n", offset);
                return EIO;
        }
        if (c->not_offset != ~offset) {
                printf("verify_chunk(%" PRId64 "): bad not_offset value\n",
                       offset);
                return EIO;
        }
        return 0;
}

static int do_o_direct_reads(void)
{
        int fd, ret;
        unsigned int i;
        void *buf = 0;
        time_t cur_time, end_time;
        ret = posix_memalign(&buf, page_size, page_size);
        if (ret) {
                printf("do_o_direct_reads: posix_memalign returned %d\n", ret);
                goto done;
        }

        fd = open(temp_file, O_RDONLY | O_DIRECT);
        if (fd < 0) {
                ret = errno;
                printf("do_o_direct_reads: error opening fd: %d\n", ret);
                goto free_buf;
        }

        // read the first chunk and see if it looks OK
        ret = do_read(fd, buf, page_size);
        if (ret)
                goto close_fd;
        ret = verify_chunk((struct chunk*)buf, 0);
        if (ret)
                goto close_fd;

        // read some random chunks and see how they look
        cur_time = time(NULL);
        end_time = cur_time + g_duration;
        i = 0;
        do {
                time_t next_time;
                uint64_t offset;
                int page;
                unsigned int seed;

                seed = i++;
                page = rand_r(&seed) % g_num_pages;
                offset = page;
                offset *= page_size;
                if (lseek64(fd, offset, SEEK_SET) == -1) {
                        int err = errno;
                        printf("lseek64(%" PRId64 ") failed: error %d (%s)\n",
                               offset, err, strerror(err));
                        goto close_fd;
                }
                ret = do_read(fd, buf, page_size);
                if (ret)
                        goto close_fd;
                ret = verify_chunk((struct chunk*)buf, offset);
                if (ret)
                        goto close_fd;
                next_time = time(NULL);
                if (next_time > cur_time) {
                        printf(".");
                }
                cur_time = next_time;
        } while (time(NULL) < end_time);

        printf("\ndo_o_direct_reads: SUCCESS\n");
close_fd:
        TEMP_FAILURE_RETRY(close(fd));
free_buf:
        free(buf);
done:
        return ret;
}

static void usage(char *argv0)
{
        printf("%s: tests direct I/O\n", argv0);
        printf("-d <seconds>:          sets duration to <seconds>\n");
        printf("-h:                    this help\n");
        printf("-p <pages>:            sets number of pages to allocate\n");
}

static void parse_args(int argc, char *argv[])
{
        int c;
        while ((c = getopt (argc, argv, "d:hp:")) != -1) {
                switch (c) {
                case 'd':
                        g_duration = atoi(optarg);
                        if (g_duration <= 0) {
                                printf("tried to set invalid value of "
                                       "g_duration: %d\n", g_num_pages);
                                exit(1);
                        }
                        break;
                case 'h':
                        usage(argv[0]);
                        exit(0);
                        break;
                case 'p':
                        g_num_pages = atoi(optarg);
                        if (g_num_pages <= 0) {
                                printf("tried to set invalid value of "
                                       "g_num_pages: %d\n", g_num_pages);
                                exit(1);
                        }
                        break;
                case '?':
                        usage(argv[0]);
                        exit(1);
                        break;
                default:
                        usage(argv[0]);
                        exit(1);
                        break;
                }
        }
}

int main(int argc, char *argv[])
{
        int ret;

        parse_args(argc, argv);

        setvbuf(stdout, NULL, _IONBF, 0);

        page_size = getpagesize();

        ret = setup_temp_file();
        if (ret) {
                printf("setup_temp_file failed with error %d\n", ret);
                goto done;
        }

        ret = do_o_direct_reads();
        if (ret) {
                printf("do_o_direct_reads failed with error %d\n", ret);
                goto unlink_temp_file;
        }

unlink_temp_file:
        unlink(temp_file);
done:
        return ret;
}
