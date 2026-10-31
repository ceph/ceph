// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/mman.h>
#include <unistd.h>

#include <cstring>
#include <memory>
#include <vector>

#include "gtest/gtest.h"

// FSCrypt.h is not self-contained: it needs bufferlist (unqualified),
// ceph_le64 and CephContext declared before it
#include "include/encoding.h"
#include "common/ceph_context.h"
#include "client/FSCrypt.h"
#include "global/global_context.h"
#include "include/buffer.h"

using namespace ceph;

namespace {

static constexpr uint64_t NUM_BLOCKS = 3;
static constexpr uint64_t FILE_SIZE = NUM_BLOCKS * FSCRYPT_BLOCK_SIZE;

FSCryptFDataDencRef make_fdata_denc()
{
  auto ctx = std::make_shared<FSCryptContext>(g_ceph_context);
  ctx->version = FSCRYPT_POLICY_V2;
  ctx->contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
  ctx->filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
  ctx->flags = FSCRYPT_POLICY_FLAGS_PAD_32;
  for (unsigned i = 0; i < sizeof(ctx->nonce); ++i) {
    ctx->nonce[i] = i + 1;
  }

  char raw_key[FSCRYPT_MAX_KEY_SIZE];
  for (unsigned i = 0; i < sizeof(raw_key); ++i) {
    raw_key[i] = 0x40 + i;
  }
  auto master_key = std::make_shared<FSCryptKey>();
  if (master_key->init(raw_key, sizeof(raw_key)) < 0) {
    return nullptr;
  }

  auto denc = std::make_shared<FSCryptFDataDenc>(g_ceph_context);
  if (!denc->setup(ctx, master_key)) {
    return nullptr;
  }
  return denc;
}

bufferlist make_plaintext()
{
  bufferlist bl;
  bl.append_hole(FILE_SIZE);
  auto p = reinterpret_cast<uint64_t *>(bl.c_str());
  for (uint64_t i = 0; i < FILE_SIZE / sizeof(uint64_t); ++i) {
    // never zero: decrypt_bl() treats a leading zero word as a hole
    p[i] = i * sizeof(uint64_t) + 1;
  }
  return bl;
}

// Holds a copy of the given bytes so that they end exactly where an
// inaccessible guard page begins, so any read past the end faults.
class GuardedBuffer {
  char *m_map = nullptr;
  size_t m_map_len = 0;
  char *m_data = nullptr;

public:
  GuardedBuffer(const char *src, size_t len) {
    size_t page = sysconf(_SC_PAGESIZE);
    size_t data_pages = (len + page - 1) / page;
    m_map_len = (data_pages + 1) * page;
    void *p = mmap(nullptr, m_map_len, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ceph_assert(p != MAP_FAILED);
    m_map = static_cast<char *>(p);
    char *guard = m_map + data_pages * page;
    ceph_assert(mprotect(guard, page, PROT_NONE) == 0);
    m_data = guard - len;
    memcpy(m_data, src, len);
  }
  ~GuardedBuffer() {
    munmap(m_map, m_map_len);
  }
  GuardedBuffer(const GuardedBuffer&) = delete;
  GuardedBuffer& operator=(const GuardedBuffer&) = delete;

  char *data() { return m_data; }
};

} // anonymous namespace

TEST(FSCryptFDataDenc, RoundTripAligned)
{
  auto denc = make_fdata_denc();
  ASSERT_TRUE(denc);

  bufferlist plain = make_plaintext();
  bufferlist enc;
  ASSERT_EQ(0, denc->encrypt_bl(0, FILE_SIZE, plain, &enc));
  ASSERT_EQ(FILE_SIZE, enc.length());

  std::vector<FSCryptFDataDenc::Segment> holes;
  ASSERT_EQ(0, denc->decrypt_bl(0, FILE_SIZE, 0, holes, &enc));
  ASSERT_EQ(FILE_SIZE, enc.length());
  ASSERT_TRUE(plain.contents_equal(enc));
}

/*
 * A read with an unaligned offset whose end lands inside a block. As in
 * FSCrypt::prepare_data_read(), the encrypted read starts at the block
 * containing off but only extends to off + len rounded up to
 * FSCRYPT_DATA_ALIGNMENT, so the buffer stops partway into the last block.
 * decrypt_bl() must not read past the end of that buffer.
 */
TEST(FSCryptFDataDenc, UnalignedReadPartialLastBlock)
{
  auto denc = make_fdata_denc();
  ASSERT_TRUE(denc);

  bufferlist plain = make_plaintext();
  bufferlist enc;
  ASSERT_EQ(0, denc->encrypt_bl(0, FILE_SIZE, plain, &enc));
  ASSERT_EQ(FILE_SIZE, enc.length());

  // (off, len) pairs mirroring the direct_io/test_sync_io workunit
  std::vector<std::pair<uint64_t, uint64_t>> reads = {
    {1536, 8192},
    {512, 1024},
    {3584, 4096},
    {4096 + 2048, 2048},
    {16, 4096 + 16},
  };

  for (auto& [off, len] : reads) {
    SCOPED_TRACE(testing::Message() << "off=" << off << " len=" << len);

    uint64_t read_start = fscrypt_block_start(off);
    uint64_t read_end = fscrypt_align_ofs(off + len);
    uint64_t read_len = read_end - read_start;
    ASSERT_LE(read_end, FILE_SIZE);

    GuardedBuffer guarded(enc.c_str() + read_start, read_len);
    bufferlist bl;
    bl.push_back(buffer::create_static(read_len, guarded.data()));

    std::vector<FSCryptFDataDenc::Segment> holes;
    ASSERT_EQ(0, denc->decrypt_bl(off, len, read_start, holes, &bl));
    ASSERT_EQ(len, bl.length());
    ASSERT_EQ(0, memcmp(plain.c_str() + off, bl.c_str(), len));
  }
}
