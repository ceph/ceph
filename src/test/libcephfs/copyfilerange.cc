// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "include/compat.h"
#include "include/cephfs/libcephfs.h"
#include "include/ceph_fs.h"
#include "include/stat.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef __linux__
#include <limits.h>
#endif

#include <fmt/format.h>
#include <string>
#include <cstring>
#include <algorithm>
#include <chrono>

using namespace std;
using namespace std::chrono;

static const size_t OBJECT_SIZE = 4 * 1024 * 1024;  // 4MB default

/*
 * Helper: create a file with known repeating content for verification.
 */
static int create_pattern_file(struct ceph_mount_info *cmount, const char *path,
                               size_t size, int flags = O_WRONLY | O_CREAT)
{
  int fd = ceph_open(cmount, path, flags, 0666);
  if (fd < 0)
    return fd;

  char buf[4096];
  for (size_t off = 0; off < size; off += sizeof(buf)) {
    size_t chunk = std::min(size - off, sizeof(buf));
    for (size_t i = 0; i < chunk; i++)
      buf[i] = (char)((off + i) & 0xff);
    int r = ceph_write(cmount, fd, buf, chunk, off);
    if (r != (int)chunk) {
      ceph_close(cmount, fd);
      return r < 0 ? r : -EIO;
    }
  }

  ceph_close(cmount, fd);
  return 0;
}

/*
 * Helper: verify file content against the pattern created by create_pattern_file.
 */
static int verify_pattern_file(struct ceph_mount_info *cmount, const char *path,
                               size_t offset, size_t size)
{
  int fd = ceph_open(cmount, path, O_RDONLY, 0);
  if (fd < 0)
    return fd;

  char buf[4096];
  for (size_t off = offset; off < offset + size; off += sizeof(buf)) {
    size_t chunk = std::min((offset + size) - off, sizeof(buf));
    int r = ceph_read(cmount, fd, buf, chunk, off);
    if (r != (int)chunk) {
      ceph_close(cmount, fd);
      return r < 0 ? r : -EIO;
    }
    for (size_t i = 0; i < chunk; i++) {
      char expected = (char)(((off - offset) + i) & 0xff);
      if (buf[i] != expected) {
        fmt::print(stderr, "mismatch at off {}: got 0x{:02x}, expected 0x{:02x}\n",
                   off + i, (unsigned char)buf[i], (unsigned char)expected);
        ceph_close(cmount, fd);
        return -EIO;
      }
    }
  }

  ceph_close(cmount, fd);
  return 0;
}

/*
 * Test basic copy_file_range with a small file (manual fallback path).
 * Small files below object_size exercise the manual read/write fallback.
 */
TEST(LibCephFS, CopyFileRange_SmallFile) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, nullptr));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  auto dir = fmt::format("test_cfr_small_{}", getpid());
  ASSERT_EQ(0, ceph_mkdirs(cmount, dir.c_str(), 0777));

  auto src_path = fmt::format("{}/src", dir);
  auto dst_path = fmt::format("{}/dst", dir);

  size_t file_size = 1024;
  ASSERT_EQ(0, create_pattern_file(cmount, src_path.c_str(), file_size));

  int src_fd = ceph_open(cmount, src_path.c_str(), O_RDONLY, 0);
  ASSERT_GT(src_fd, 0);
  int dst_fd = ceph_open(cmount, dst_path.c_str(), O_WRONLY | O_CREAT, 0666);
  ASSERT_GT(dst_fd, 0);

  int64_t src_off = 0, dst_off = 0;
  ssize_t ret = ceph_copy_file_range(cmount, src_fd, &src_off, dst_fd, &dst_off,
                                     file_size, 0);
  ASSERT_EQ(ret, (ssize_t)file_size);
  ASSERT_EQ(src_off, (int64_t)file_size);
  ASSERT_EQ(dst_off, (int64_t)file_size);

  ceph_close(cmount, src_fd);
  ceph_close(cmount, dst_fd);

  ASSERT_EQ(0, verify_pattern_file(cmount, dst_path.c_str(), 0, file_size));
  ASSERT_EQ(0, verify_pattern_file(cmount, src_path.c_str(), 0, file_size));

  ceph_unlink(cmount, src_path.c_str());
  ceph_unlink(cmount, dst_path.c_str());
  ceph_rmdir(cmount, dir.c_str());
  ceph_shutdown(cmount);
}

/*
 * Test copy_file_range with a file larger than object_size.
 * This exercises the CEPH_OSD_OP_COPY_FROM2 server-side copy path.
 */
TEST(LibCephFS, CopyFileRange_LargeFile) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, nullptr));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  auto dir = fmt::format("test_cfr_large_{}", getpid());
  ASSERT_EQ(0, ceph_mkdirs(cmount, dir.c_str(), 0777));

  auto src_path = fmt::format("{}/src", dir);
  auto dst_path = fmt::format("{}/dst", dir);

  size_t file_size = 100 * OBJECT_SIZE;
  ASSERT_EQ(0, create_pattern_file(cmount, src_path.c_str(), file_size));

  int src_fd = ceph_open(cmount, src_path.c_str(), O_RDONLY, 0);
  ASSERT_GT(src_fd, 0);
  int dst_fd = ceph_open(cmount, dst_path.c_str(), O_WRONLY | O_CREAT, 0666);
  ASSERT_GT(dst_fd, 0);

  int64_t src_off = 0, dst_off = 0;
  ssize_t ret = ceph_copy_file_range(cmount, src_fd, &src_off, dst_fd, &dst_off,
                                     file_size, 0);
  ASSERT_EQ(ret, (ssize_t)file_size);
  ASSERT_EQ(src_off, (int64_t)file_size);
  ASSERT_EQ(dst_off, (int64_t)file_size);

  ceph_close(cmount, src_fd);
  ceph_close(cmount, dst_fd);

  ASSERT_EQ(0, verify_pattern_file(cmount, dst_path.c_str(), 0, file_size));
  ASSERT_EQ(0, verify_pattern_file(cmount, src_path.c_str(), 0, file_size));

  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cmount, dst_path.c_str(), &stx, CEPH_STATX_SIZE, 0));
  ASSERT_EQ(stx.stx_size, (uint64_t)file_size);

  ceph_unlink(cmount, src_path.c_str());
  ceph_unlink(cmount, dst_path.c_str());
  ceph_rmdir(cmount, dir.c_str());
  ceph_shutdown(cmount);
}

/*
 * Test copying a sub-range of a file (not starting at offset 0).
 * Both object-aligned and non-aligned sub-ranges are tested.
 */
TEST(LibCephFS, CopyFileRange_SubRange) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, nullptr));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  auto dir = fmt::format("test_cfr_subrange_{}", getpid());
  ASSERT_EQ(0, ceph_mkdirs(cmount, dir.c_str(), 0777));

  auto src_path = fmt::format("{}/src", dir);
  auto dst_path = fmt::format("{}/dst", dir);

  size_t file_size = 3 * OBJECT_SIZE;
  ASSERT_EQ(0, create_pattern_file(cmount, src_path.c_str(), file_size));

  /*
   * Test 1: Object-aligned sub-range copy (uses server-side offload)
   */
  {
    int src_fd = ceph_open(cmount, src_path.c_str(), O_RDONLY, 0);
    ASSERT_GT(src_fd, 0);
    int dst_fd = ceph_open(cmount, dst_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    ASSERT_GT(dst_fd, 0);

    int64_t src_off = OBJECT_SIZE, dst_off = 0;
    size_t copy_len = OBJECT_SIZE;
    ssize_t ret = ceph_copy_file_range(cmount, src_fd, &src_off, dst_fd, &dst_off,
                                       copy_len, 0);
    ASSERT_EQ(ret, (ssize_t)copy_len);
    ASSERT_EQ(src_off, (int64_t)(OBJECT_SIZE + copy_len));
    ASSERT_EQ(dst_off, (int64_t)copy_len);

    ceph_close(cmount, src_fd);
    ceph_close(cmount, dst_fd);

    ASSERT_EQ(0, verify_pattern_file(cmount, dst_path.c_str(), 0, copy_len));
  }

  /*
   * Test 2: Non-object-aligned sub-range (uses manual fallback)
   */
  auto dst2_path = fmt::format("{}/dst2", dir);
  {
    int src_fd = ceph_open(cmount, src_path.c_str(), O_RDONLY, 0);
    ASSERT_GT(src_fd, 0);
    int dst_fd = ceph_open(cmount, dst2_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    ASSERT_GT(dst_fd, 0);

    int64_t src_off = 512, dst_off = 100;
    size_t copy_len = 5000;
    ssize_t ret = ceph_copy_file_range(cmount, src_fd, &src_off, dst_fd, &dst_off,
                                       copy_len, 0);
    ASSERT_EQ(ret, (ssize_t)copy_len);
    ASSERT_EQ(src_off, (int64_t)(512 + copy_len));
    ASSERT_EQ(dst_off, (int64_t)(100 + copy_len));

    ceph_close(cmount, src_fd);
    ceph_close(cmount, dst_fd);

    // Verify: read dst from offset 100, compare with src from offset 512
    int vfd = ceph_open(cmount, dst2_path.c_str(), O_RDONLY, 0);
    ASSERT_GT(vfd, 0);
    int sfd = ceph_open(cmount, src_path.c_str(), O_RDONLY, 0);
    ASSERT_GT(sfd, 0);

    char dbuf[1024], sbuf[1024];
    for (size_t i = 0; i < copy_len; i += sizeof(dbuf)) {
      size_t chunk = std::min(copy_len - i, sizeof(dbuf));
      ASSERT_EQ((int)chunk, ceph_read(cmount, sfd, sbuf, chunk, 512 + i));
      ASSERT_EQ((int)chunk, ceph_read(cmount, vfd, dbuf, chunk, 100 + i));
      ASSERT_EQ(0, memcmp(sbuf, dbuf, chunk));
    }
    ceph_close(cmount, sfd);
    ceph_close(cmount, vfd);
  }

  ceph_unlink(cmount, src_path.c_str());
  ceph_unlink(cmount, dst_path.c_str());
  ceph_unlink(cmount, dst2_path.c_str());
  ceph_rmdir(cmount, dir.c_str());
  ceph_shutdown(cmount);
}

/*
 * Test that copy_file_range correctly handles zero-length copy.
 */
TEST(LibCephFS, CopyFileRange_ZeroLength) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, nullptr));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  auto dir = fmt::format("test_cfr_zero_{}", getpid());
  ASSERT_EQ(0, ceph_mkdirs(cmount, dir.c_str(), 0777));

  auto src_path = fmt::format("{}/src", dir);
  auto dst_path = fmt::format("{}/dst", dir);

  ASSERT_EQ(0, create_pattern_file(cmount, src_path.c_str(), 1024));

  int src_fd = ceph_open(cmount, src_path.c_str(), O_RDONLY, 0);
  ASSERT_GT(src_fd, 0);
  int dst_fd = ceph_open(cmount, dst_path.c_str(), O_WRONLY | O_CREAT, 0666);
  ASSERT_GT(dst_fd, 0);

  int64_t src_off = 0, dst_off = 0;
  ssize_t ret = ceph_copy_file_range(cmount, src_fd, &src_off, dst_fd, &dst_off, 0, 0);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(src_off, 0);
  ASSERT_EQ(dst_off, 0);

  ceph_close(cmount, src_fd);
  ceph_close(cmount, dst_fd);

  ceph_unlink(cmount, src_path.c_str());
  ceph_unlink(cmount, dst_path.c_str());
  ceph_rmdir(cmount, dir.c_str());
  ceph_shutdown(cmount);
}

/*
 * Test copy_file_range to an existing file (overwrite a sub-range).
 */
TEST(LibCephFS, CopyFileRange_Overwrite) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, nullptr));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  auto dir = fmt::format("test_cfr_overwrite_{}", getpid());
  ASSERT_EQ(0, ceph_mkdirs(cmount, dir.c_str(), 0777));

  auto src_path = fmt::format("{}/src", dir);
  auto dst_path = fmt::format("{}/dst", dir);

  size_t file_size = 1024 * 1024;  // 1MB
  ASSERT_EQ(0, create_pattern_file(cmount, src_path.c_str(), file_size));

  // Create a dst file with different content
  int dst_fd = ceph_open(cmount, dst_path.c_str(), O_WRONLY | O_CREAT, 0666);
  ASSERT_GT(dst_fd, 0);
  char fill[4096];
  memset(fill, 0x42, sizeof(fill));
  for (size_t off = 0; off < file_size; off += sizeof(fill))
    ASSERT_EQ((int)sizeof(fill), ceph_write(cmount, dst_fd, fill, sizeof(fill), off));
  ceph_close(cmount, dst_fd);

  // Copy over a middle section of dst
  int src_fd = ceph_open(cmount, src_path.c_str(), O_RDONLY, 0);
  ASSERT_GT(src_fd, 0);
  dst_fd = ceph_open(cmount, dst_path.c_str(), O_RDWR, 0);
  ASSERT_GT(dst_fd, 0);

  int64_t src_off = 4096, dst_off = 8192;
  size_t copy_len = 16384;
  ssize_t ret = ceph_copy_file_range(cmount, src_fd, &src_off, dst_fd, &dst_off,
                                     copy_len, 0);
  ASSERT_EQ(ret, (ssize_t)copy_len);

  ceph_close(cmount, src_fd);
  ceph_close(cmount, dst_fd);

  // Verify: dst[8192..24576) should match src[4096..20480)
  src_fd = ceph_open(cmount, src_path.c_str(), O_RDONLY, 0);
  dst_fd = ceph_open(cmount, dst_path.c_str(), O_RDONLY, 0);
  char sbuf[4096], dbuf[4096];
  for (size_t i = 0; i < copy_len; i += sizeof(sbuf)) {
    size_t chunk = std::min(copy_len - i, sizeof(sbuf));
    ASSERT_EQ((int)chunk, ceph_read(cmount, src_fd, sbuf, chunk, 4096 + i));
    ASSERT_EQ((int)chunk, ceph_read(cmount, dst_fd, dbuf, chunk, 8192 + i));
    ASSERT_EQ(0, memcmp(sbuf, dbuf, chunk));
  }
  ceph_close(cmount, src_fd);
  ceph_close(cmount, dst_fd);

  ceph_unlink(cmount, src_path.c_str());
  ceph_unlink(cmount, dst_path.c_str());
  ceph_rmdir(cmount, dir.c_str());
  ceph_shutdown(cmount);
}

/*
 * Helper: drop OSD and system page caches for fair perf comparison.
 */
static void sync_drop_caches() {
  // sync filesystem first
  sync();
  // drop OSD caches via admin sockets
  for (int i = 0; i < 10; i++) {
    char cmd[128];
    snprintf(cmd, sizeof(cmd),
             "ceph daemon osd.%d cache drop 2>/dev/null", i);
    if (system(cmd) != 0) break;  // no more OSDs
  }
  // drop system page cache
  system("echo 3 > /proc/sys/vm/drop_caches 2>/dev/null");
}

/*
 * Performance comparison: COPY2 vs manual fallback for large files.
 * Each path uses its own source file and drops caches for fairness.
 */
TEST(LibCephFS, CopyFileRange_PerfCompare) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, nullptr));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, nullptr));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  auto dir = fmt::format("test_cfr_perf_{}", getpid());
  ASSERT_EQ(0, ceph_mkdirs(cmount, dir.c_str(), 0777));

  size_t file_size = 100 * OBJECT_SIZE;

  double t_copy2 = 0, t_fallback = 0;

  /* COPY2 path: object-aligned, separate source file */
  {
    auto sp = fmt::format("{}/src_c2", dir);
    auto dst = fmt::format("{}/dst_c2", dir);
    ASSERT_EQ(0, create_pattern_file(cmount, sp.c_str(), file_size));
    sync_drop_caches();
    int sf = ceph_open(cmount, sp.c_str(), O_RDONLY, 0);
    ASSERT_GT(sf, 0);
    int df = ceph_open(cmount, dst.c_str(), O_WRONLY | O_CREAT, 0666);
    ASSERT_GT(df, 0);
    int64_t so = 0, dout = 0;
    auto t0 = steady_clock::now();
    ssize_t r = ceph_copy_file_range(cmount, sf, &so, df, &dout, file_size, 0);
    t_copy2 = duration<double>(steady_clock::now() - t0).count();
    ASSERT_EQ(r, (ssize_t)file_size);
    ceph_close(cmount, sf);
    ceph_close(cmount, df);
    ceph_unlink(cmount, sp.c_str());
    ceph_unlink(cmount, dst.c_str());
  }

  /* Manual fallback: unaligned src_off forces -EOPNOTSUPP, own src */
  {
    auto sp = fmt::format("{}/src_fb", dir);
    auto dst = fmt::format("{}/dst_fb", dir);
    ASSERT_EQ(0, create_pattern_file(cmount, sp.c_str(), file_size));
    sync_drop_caches();
    int sf = ceph_open(cmount, sp.c_str(), O_RDONLY, 0);
    ASSERT_GT(sf, 0);
    int df = ceph_open(cmount, dst.c_str(), O_WRONLY | O_CREAT, 0666);
    ASSERT_GT(df, 0);
    int64_t so = 1, dout = 0;
    size_t cl = file_size - 1;
    auto t0 = steady_clock::now();
    ssize_t r = ceph_copy_file_range(cmount, sf, &so, df, &dout, cl, 0);
    t_fallback = duration<double>(steady_clock::now() - t0).count();
    ASSERT_EQ(r, (ssize_t)cl);
    ceph_close(cmount, sf);
    ceph_close(cmount, df);
    ceph_unlink(cmount, sp.c_str());
    ceph_unlink(cmount, dst.c_str());
  }

  ceph_rmdir(cmount, dir.c_str());
  ceph_shutdown(cmount);

  printf("\n=== PerfCompare (100x4MB) ===\n");
  printf("  COPY2:       %.2fs\n", t_copy2);
  printf("  fallback:    %.2fs\n", t_fallback);
  printf("  ratio:       %.1fx\n", t_fallback / t_copy2);
  printf("  NOTE: in single-node vstart, COPY2 overhead may exceed local IPC.\n");
  printf("        COPY2 is faster in real clusters with client<->OSD latency.\n");
  printf("==============================\n");
}
