// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/client/FailingWriteback.h"

class TestFlushRange : public TestClient {
protected:
  static constexpr size_t file_size = 128 << 10;
  const std::string initial_data = std::string(file_size, 'a');
  Inode* file = nullptr;
  Fh* buffered = nullptr;
  std::vector<Fh*> handles;
  std::string filename;

  ClientScaffold* create_client() override {
    return new FailingWritebackClient(messenger, mc, objecter, &fse);
  }

  FailingWritebackClient* fault_client() {
    return static_cast<FailingWritebackClient*>(client);
  }

  void SetUp() override {
    TestClient::SetUp();
    if (!g_ceph_context->_conf->client_oc) {
      GTEST_SKIP() << "range flushes need client_oc";
    }
  }

  void create_file() {
    filename = "test_flush_range." + std::to_string(getpid());
    struct ceph_statx stx;
    ASSERT_EQ(0, client->ll_createx(client->get_root(), filename.c_str(),
                                   0600, O_CREAT | O_TRUNC | O_RDWR,
                                   &file, &buffered, &stx, 0, 0, myperm));
    handles.push_back(buffered);
    ASSERT_EQ(file_size, client->ll_write(buffered, 0, file_size,
                                          initial_data.data()));
    ASSERT_EQ(0, client->ll_fsync(buffered, false));
  }

  void dirty_file() {
    ASSERT_EQ(file_size, client->ll_write(buffered, 0, file_size,
                                          initial_data.data()));
    ASSERT_TRUE(fault_client()->has_dirty_data(file));
  }

  Fh* open_file(int flags) {
    Fh* fh = nullptr;
    EXPECT_EQ(0, client->ll_open(file, flags, &fh, myperm));
    if (fh) {
      handles.push_back(fh);
    }
    return fh;
  }

  void check_error_and_retry(int flags, bool write, bool asynchronous,
                             int error, bool do_fsync = false) {
    SCOPED_TRACE(::testing::Message() << "flags=" << flags
        << " write=" << write << " async=" << asynchronous
        << " error=" << error);
    Fh* fh = open_file(O_RDWR | flags);
    ASSERT_NE(nullptr, fh);
    ASSERT_NO_FATAL_FAILURE(dirty_file());
    const auto failures_before = fault_client()->failure_count();
    fault_client()->arm_write(file, error);

    std::string data(4096, 'b');
    struct iovec iov = {data.data(), data.size()};
    bufferlist bl;
    CountingCompletion failed, retried;
    const auto result = client->ll_preadv_pwritev(
        fh, &iov, 1, -1, write, asynchronous ? &failed : nullptr,
        &bl, do_fsync);
    if (asynchronous) {
      ASSERT_EQ(0, result);
      EXPECT_EQ(error, failed.wait());
      EXPECT_EQ(1u, failed.calls.load());
    } else {
      EXPECT_EQ(error, result);
    }
    ASSERT_EQ(failures_before + 1, fault_client()->failure_count());
    EXPECT_EQ(0, client->ll_lseek(fh, 0, SEEK_CUR));

    if (write && (flags & O_DIRECT)) {
      // A failed preflush must stop before modifying data.
      bufferlist unchanged;
      ASSERT_EQ(file_size, client->ll_read(buffered, 0, file_size, &unchanged));
      EXPECT_EQ(initial_data, unchanged.to_str());
    }

    // Retrying the same handle also catches leaked caps or a locked position.
    bl.clear();
    const auto retry = client->ll_preadv_pwritev(
        fh, &iov, 1, -1, write, asynchronous ? &retried : nullptr,
        &bl, do_fsync);
    if (asynchronous) {
      ASSERT_EQ(0, retry);
      ASSERT_EQ(data.size(), retried.wait());
      EXPECT_EQ(1u, retried.calls.load());
    } else {
      ASSERT_EQ(data.size(), retry);
    }
    EXPECT_EQ(data.size(), client->ll_lseek(fh, 0, SEEK_CUR));
    ASSERT_EQ(0, client->sync_fs());
    if (asynchronous) {
      EXPECT_EQ(1u, failed.calls.load());
      EXPECT_EQ(1u, retried.calls.load());
    }
    EXPECT_EQ(failures_before + 1, fault_client()->failure_count());

    bufferlist contents;
    // Direct writes need a direct read to verify the committed data.
    auto verify_fh = (flags & O_DIRECT) ? fh : buffered;
    ASSERT_EQ(file_size, client->ll_read(verify_fh, 0, file_size, &contents));
    std::string expected = initial_data;
    if (write) {
      expected.replace(0, data.size(), data);
    }
    EXPECT_EQ(expected, contents.to_str());
  }

  void TearDown() override {
    fault_client()->disarm();
    EXPECT_EQ(0, client->sync_fs());
    for (auto fh : handles) {
      EXPECT_EQ(0, client->ll_release(fh));
    }
    if (file) {
      EXPECT_EQ(0, client->ll_unlink(client->get_root(), filename.c_str(),
                                    myperm));
      client->ll_put(file);
    }
    TestClient::TearDown();
  }
};

TEST_F(TestFlushRange, BufferedSyncWrite) {
  ASSERT_NO_FATAL_FAILURE(create_file());
  for (int flags : {O_SYNC, O_DSYNC}) {
    for (int error : {-EIO, -ENOSPC}) {
      ASSERT_NO_FATAL_FAILURE(check_error_and_retry(flags, true, false, error));
    }
  }
}

TEST_F(TestFlushRange, NonblockingBufferedSyncWrite) {
  ASSERT_NO_FATAL_FAILURE(create_file());
  for (int flags : {O_SYNC, O_DSYNC}) {
    for (int error : {-EIO, -ENOSPC}) {
      // A requested fsync after the write must not hide its earlier failure.
      ASSERT_NO_FATAL_FAILURE(check_error_and_retry(flags, true, true,
                                                     error, true));
    }
  }
}

TEST_F(TestFlushRange, FailedSyncWriteExtendsFile) {
  ASSERT_NO_FATAL_FAILURE(create_file());
  Fh* fh = open_file(O_RDWR | O_SYNC);
  ASSERT_NE(nullptr, fh);
  std::string data(4096, 'b');
  for (bool asynchronous : {false, true}) {
    SCOPED_TRACE(::testing::Message() << "async=" << asynchronous);
    struct ceph_statx before, after;
    ASSERT_EQ(0, client->ll_getattrx(file, &before,
                                     CEPH_STATX_SIZE | CEPH_STATX_VERSION, 0,
                                     myperm));
    const int64_t offset = before.stx_size;
    ASSERT_EQ(offset, client->ll_lseek(fh, offset, SEEK_SET));
    fault_client()->arm_write(file, -EIO);

    struct iovec iov = {data.data(), data.size()};
    CountingCompletion failed;
    const auto result = client->ll_preadv_pwritev(
        fh, &iov, 1, -1, true, asynchronous ? &failed : nullptr);
    if (asynchronous) {
      ASSERT_EQ(0, result);
      EXPECT_EQ(-EIO, failed.wait());
    } else {
      EXPECT_EQ(-EIO, result);
    }

    // As on Linux, the error leaves the position alone. The data remains
    // cached and dirty, so the file must already include it.
    EXPECT_EQ(offset, client->ll_lseek(fh, 0, SEEK_CUR));
    ASSERT_EQ(0, client->ll_getattrx(file, &after,
                                     CEPH_STATX_SIZE | CEPH_STATX_VERSION, 0,
                                     myperm));
    EXPECT_EQ(offset + data.size(), after.stx_size);
    EXPECT_LT(before.stx_version, after.stx_version);

    // Later writeback succeeds, and the data must lie within the file.
    ASSERT_EQ(0, client->sync_fs());
    bufferlist bl;
    EXPECT_EQ(data.size(), client->ll_read(buffered, offset, data.size(), &bl));
    EXPECT_EQ(data, bl.to_str());
  }
}

TEST_F(TestFlushRange, DirectRead) {
  ASSERT_NO_FATAL_FAILURE(create_file());
  for (int error : {-EIO, -ENOSPC}) {
    ASSERT_NO_FATAL_FAILURE(check_error_and_retry(O_DIRECT, false, false,
                                                   error));
  }
}

TEST_F(TestFlushRange, DirectWrite) {
  ASSERT_NO_FATAL_FAILURE(create_file());
  for (bool asynchronous : {false, true}) {
    for (int error : {-EIO, -ENOSPC}) {
      ASSERT_NO_FATAL_FAILURE(check_error_and_retry(O_DIRECT, true,
                                                     asynchronous, error));
    }
  }
}
