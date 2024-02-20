// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/Operations.h"
#include "librbd/api/Io.h"
#include "librbd/api/Image.h"
#include "librbd/api/Snapshot.h"
#include "librbd/AsioEngine.h"
#include "librbd/DeepCopyRequest.h"
#include "librbd/deep_copy/Handler.h"
#include "librbd/ExclusiveLock.h"
#include "common/Cond.h"
#include "librbd/internal.h"
#include "librbd/io/ReadResult.h"
#include "test/librados/crimson_utils.h"

void register_test_deep_copy() {
}

namespace librbd {

struct TestDeepCopy : public TestFixture {
  void SetUp() override {
    TestFixture::SetUp();

    std::string image_name = get_temp_image_name();
    int order = 22;
    uint64_t size = (1 << order) * 20;
    uint64_t features = 0;
    bool old_format = !::get_features(&features);
    EXPECT_EQ(0, create_image_full_pp(m_rbd, m_ioctx, image_name, size,
                                      features, old_format, &order));
    ASSERT_EQ(0, open_image(image_name, &m_src_ictx));

    if (old_format) {
      // The destination should always be in the new format.
      uint64_t format = 2;
      ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_FORMAT, format));
    }
  }

  void TearDown() override {
    if (m_src_ictx != nullptr) {
      deep_copy();
      if (m_dst_ictx != nullptr) {
        compare();
        close_image(m_dst_ictx);
      }
      close_image(m_src_ictx);
    }

    TestFixture::TearDown();
  }

  void deep_copy() {
    if (m_src_snap_id_start == 0 &&
	m_src_snap_id_end == CEPH_NOSNAP &&
	m_dst_snap_id_start == 0) {
      std::string dst_name = get_temp_image_name();
      librbd::NoOpProgressContext no_op;
      EXPECT_EQ(0, api::Io<>::flush(*m_src_ictx));
      EXPECT_EQ(0, librbd::api::Image<>::deep_copy(m_src_ictx, m_src_ictx->md_ctx,
                                                 dst_name.c_str(), m_opts,
                                                 no_op));
      EXPECT_EQ(0, open_image(dst_name, &m_dst_ictx));
    } else {
      deep_copy_incremental();
    }
  }

  void deep_copy_incremental() {
    EXPECT_NE(m_dst_ictx, nullptr);
    EXPECT_EQ(0, api::Io<>::flush(*m_src_ictx));

    C_SaferCond lock_ctx;
    {
      std::unique_lock owner_locker{m_dst_ictx->owner_lock};
      if (m_dst_ictx->exclusive_lock == nullptr ||
	  m_dst_ictx->exclusive_lock->is_lock_owner()) {
	lock_ctx.complete(0);
      } else {
	m_dst_ictx->exclusive_lock->try_acquire_lock(&lock_ctx);
      }
    }
    ASSERT_EQ(0, lock_ctx.wait());

    C_SaferCond ctx;
    SnapSeqs snap_seqs;
    AsioEngine asio_engine(m_src_ictx->md_ctx);
    librbd::deep_copy::NoOpHandler no_op;

    auto request = DeepCopyRequest<>::create(
        m_src_ictx, m_dst_ictx, m_src_snap_id_start, m_src_snap_id_end,
	m_dst_snap_id_start, false, boost::none, asio_engine.get_work_queue(),
	&snap_seqs, &no_op, &ctx);
    request->send();
    ASSERT_EQ(0, ctx.wait());
  }


  void compare() {
    std::vector<librbd::snap_info_t> src_snaps, dst_snaps;

    EXPECT_EQ(m_src_ictx->size, m_dst_ictx->size);
    EXPECT_EQ(0, librbd::api::Snapshot<>::list(m_src_ictx, src_snaps));
    EXPECT_EQ(0, librbd::api::Snapshot<>::list(m_dst_ictx, dst_snaps));
    EXPECT_EQ(src_snaps.size(), dst_snaps.size());
    for (size_t i = 0; i <= src_snaps.size(); i++) {
      const char *src_snap_name = nullptr;
      const char *dst_snap_name = nullptr;
      if (i < src_snaps.size()) {
        EXPECT_EQ(src_snaps[i].name, dst_snaps[i].name);
        src_snap_name = src_snaps[i].name.c_str();
        dst_snap_name = dst_snaps[i].name.c_str();
      }
      EXPECT_EQ(0, librbd::api::Image<>::snap_set(
                     m_src_ictx, cls::rbd::UserSnapshotNamespace(),
                     src_snap_name));
      EXPECT_EQ(0, librbd::api::Image<>::snap_set(
                     m_dst_ictx, cls::rbd::UserSnapshotNamespace(),
                     dst_snap_name));
      uint64_t src_size, dst_size;
      {
        std::shared_lock src_locker{m_src_ictx->image_lock};
        std::shared_lock dst_locker{m_dst_ictx->image_lock};
        src_size = m_src_ictx->get_image_size(m_src_ictx->snap_id);
        dst_size = m_dst_ictx->get_image_size(m_dst_ictx->snap_id);
      }
      EXPECT_EQ(src_size, dst_size);

      if (m_dst_ictx->test_features(RBD_FEATURE_LAYERING)) {
        bool flags_set;
        std::shared_lock dst_locker{m_dst_ictx->image_lock};
        EXPECT_EQ(0, m_dst_ictx->test_flags(m_dst_ictx->snap_id,
                                            RBD_FLAG_OBJECT_MAP_INVALID,
                                            m_dst_ictx->image_lock, &flags_set));
        EXPECT_FALSE(flags_set);
      }

      ssize_t read_size = 1 << m_src_ictx->order;
      uint64_t offset = 0;
      while (offset < src_size) {
        read_size = std::min(read_size, static_cast<ssize_t>(src_size - offset));

        bufferptr src_ptr(read_size);
        bufferlist src_bl;
        src_bl.push_back(src_ptr);
        librbd::io::ReadResult src_result{&src_bl};
        EXPECT_EQ(read_size, api::Io<>::read(
                    *m_src_ictx, offset, read_size,
                    librbd::io::ReadResult{src_result}, 0));

        bufferptr dst_ptr(read_size);
        bufferlist dst_bl;
        dst_bl.push_back(dst_ptr);
        librbd::io::ReadResult dst_result{&dst_bl};
        EXPECT_EQ(read_size, api::Io<>::read(
                    *m_dst_ictx, offset, read_size,
                    librbd::io::ReadResult{dst_result}, 0));

        if (!src_bl.contents_equal(dst_bl)) {
          std::cout << "snap: " << (src_snap_name ? src_snap_name : "null")
                    << ", block " << offset << "~" << read_size << " differs"
                    << std::endl;
          std::cout << "src block: " << std::endl; src_bl.hexdump(std::cout);
          std::cout << "dst block: " << std::endl; dst_bl.hexdump(std::cout);
        }
        EXPECT_TRUE(src_bl.contents_equal(dst_bl));
        offset += read_size;
      }
    }
  }

  void test_no_snaps() {
    bufferlist bl;
    bl.append(std::string(((1 << m_src_ictx->order) * 2) + 1, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl.length(),
                               bufferlist{bl}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 2 * bl.length(), bl.length(),
                               bufferlist{bl}, 0));
  }

  void test_snaps() {
    bufferlist bl;
    bl.append(std::string(((1 << m_src_ictx->order) * 2) + 1, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl.length(),
                               bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap1"));

    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 1 * bl.length(), bl.length(),
                               bufferlist{bl}, 0));
    bufferlist bl1;
    bl1.append(std::string(1000, 'X'));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl1.length(),
                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::discard(*m_src_ictx, bl1.length() + 10,
                                 bl1.length(), false));

    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap2"));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::write(*m_src_ictx, 1 * bl.length(), bl1.length(),
                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::discard(*m_src_ictx, 2 * bl1.length() + 10,
                                 bl1.length(), false));
  }

  void test_snaps2() {
    bufferlist bl;
    bl.append(std::string(((1 << m_src_ictx->order) * 2) + 1, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl.length(), // Writes 8MB at offset 0
                               bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap1"));    // Creates snap

    deep_copy();

    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 1 * bl.length(), bl.length(),  // Writes 8 MB at offset 8MB + 1
                               bufferlist{bl}, 0));
    bufferlist bl1;
    bl1.append(std::string(1000, 'X'));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl1.length(), // Writes 1000 bytes at offset 0
                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::discard(*m_src_ictx, bl1.length() + 10,     // Discards 1000 bytes at offset 1010
                                 bl1.length(), false));

    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap2"));

    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap1", &m_src_snap_id_start));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap2", &m_src_snap_id_end));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_dst_ictx, "snap1", &m_dst_snap_id_start));

  }

  void test_snap_discard() {
    bufferlist bl;
    bl.append(std::string(100, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0, bl.length(), bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));

    size_t len = (1 << m_src_ictx->order) * 2;
    ASSERT_EQ(static_cast<ssize_t>(len),
              api::Io<>::discard(*m_src_ictx, 0, len, false));
  }


  void test_snap_discard2() {
    bufferlist bl;
    bl.append(std::string((1 << m_src_ictx->order) * 2, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0, bl.length(), bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap1"));
    deep_copy();

    size_t len = (1 << m_src_ictx->order);
    ASSERT_EQ(static_cast<ssize_t>(len),
              api::Io<>::discard(*m_src_ictx, 0, len, false));
    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap2"));

    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap1", &m_src_snap_id_start));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap2", &m_src_snap_id_end));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_dst_ictx, "snap1", &m_dst_snap_id_start));

  }


  void test_snap_discard3() {
    bufferlist bl;
    bl.append(std::string((1 << m_src_ictx->order) * 2, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0, bl.length(), bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap1"));
    deep_copy();

    size_t len = (1 << m_src_ictx->order);
    ASSERT_EQ(static_cast<ssize_t>(len),
              api::Io<>::discard(*m_src_ictx, 2097152, len, false)); //discard part of 2 blocks
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));
    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap2"));

    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap1", &m_src_snap_id_start));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap2", &m_src_snap_id_end));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_dst_ictx, "snap1", &m_dst_snap_id_start));

  }

  void test_clone_discard() {
    bufferlist bl;
    bl.append(std::string(100, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0, bl.length(), bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap"));

    std::string clone_name = get_temp_image_name();
    int order = m_src_ictx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));
    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    size_t len = (1 << m_src_ictx->order) * 2;
    ASSERT_EQ(static_cast<ssize_t>(len),
              api::Io<>::discard(*m_src_ictx, 0, len, false));
  }

  void test_clone_shrink() {
    bufferlist bl;
    bl.append(std::string(100, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0, bl.length(), bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap"));

    std::string clone_name = get_temp_image_name();
    int order = m_src_ictx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));
    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    librbd::NoOpProgressContext no_op;
    auto new_size = m_src_ictx->size >> 1;
    ASSERT_EQ(0, m_src_ictx->operations->resize(new_size, true, no_op));
  }


  void test_clone_shrink2() {
    bufferlist bl;
    bl.append(std::string(100, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0, bl.length(), bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap"));

    std::string clone_name = get_temp_image_name();
    int order = m_src_ictx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));
    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    bufferlist bl1;
    bl1.append(std::string(1000, 'X'));

    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl1.length(), // Write 1000 bytes at offset 0 (object 0)
                               bufferlist{bl1}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap1"));

    deep_copy();
    compare();

    librbd::NoOpProgressContext no_op;
    auto new_size = m_src_ictx->size >> 1;
    ASSERT_EQ(0, m_src_ictx->operations->resize(new_size, true, no_op));

    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));
    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap2"));

    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap1", &m_src_snap_id_start));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap2", &m_src_snap_id_end));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_dst_ictx, "snap1", &m_dst_snap_id_start));
  }

  void test_clone_expand() {
    bufferlist bl;
    bl.append(std::string(100, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0, bl.length(), bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap"));

    std::string clone_name = get_temp_image_name();
    int order = m_src_ictx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));
    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    librbd::NoOpProgressContext no_op;
    auto new_size = m_src_ictx->size << 1;
    ASSERT_EQ(0, m_src_ictx->operations->resize(new_size, true, no_op));
  }

  void test_clone_expand2() {
    bufferlist bl;
    bl.append(std::string(100, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0, bl.length(), bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap"));

    std::string clone_name = get_temp_image_name();
    int order = m_src_ictx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));
    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    bufferlist bl1;
    bl1.append(std::string(1000, 'X'));

    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl1.length(), // Write 1000 bytes at offset 0 (object 0)
                               bufferlist{bl1}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap1"));

    deep_copy();
    compare();

    librbd::NoOpProgressContext no_op;
    auto new_size = m_src_ictx->size << 1;
    ASSERT_EQ(0, m_src_ictx->operations->resize(new_size, true, no_op));

    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));
    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap2"));

    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap1", &m_src_snap_id_start));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap2", &m_src_snap_id_end));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_dst_ictx, "snap1", &m_dst_snap_id_start));
  }

  void test_clone_hide_parent() {
    uint64_t object_size = 1 << m_src_ictx->order;
    bufferlist bl;
    bl.append(std::string(100, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, object_size, bl.length(),
                               bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap"));

    std::string clone_name = get_temp_image_name();
    int order = m_src_ictx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));
    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap1"));

    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::discard(*m_src_ictx, object_size, bl.length(), false));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap2"));

    librbd::NoOpProgressContext no_op;
    ASSERT_EQ(0, m_src_ictx->operations->resize(object_size, true, no_op));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap3"));

    ASSERT_EQ(0, m_src_ictx->operations->resize(2 * object_size, true, no_op));
  }

  void test_clone() {
    bufferlist bl;
    bl.append(std::string(((1 << m_src_ictx->order) * 2) + 1, '1'));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl.length(),
                               bufferlist{bl}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 2 * bl.length(), bl.length(),
                               bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap"));

    std::string clone_name = get_temp_image_name();
    int order = m_src_ictx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));
    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    bufferlist bl1;
    bl1.append(std::string(1000, 'X'));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl1.length(),
                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::discard(*m_src_ictx, bl1.length() + 10,
                                 bl1.length(), false));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap"));

    clone_name = get_temp_image_name();
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));
    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::write(*m_src_ictx, 1 * bl.length(), bl1.length(),
                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::discard(*m_src_ictx, 2 * bl1.length() + 10,
                                 bl1.length(), false));
  }

  void test_clone2() {
    bufferlist bl;
    bl.append(std::string(((1 << m_src_ictx->order) * 2) + 1, '1')); // 8MB + 1
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl.length(), // Writes 8MB + 1 at offset 0  (objects 0 and 1 and 2)
                               bufferlist{bl}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl.length()),
              api::Io<>::write(*m_src_ictx, 2 * bl.length(), bl.length(),  // Writes 8MB + 1 at offset 16MB + 2 (object 4 + 5 + 6)
                               bufferlist{bl}, 0));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap0"));
    ASSERT_EQ(0, snap_protect(*m_src_ictx, "snap0"));

    std::string clone_name = get_temp_image_name();
    int order = m_src_ictx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));
    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(), "snap0",
                               m_ioctx, clone_name.c_str(), features, &order, 0,
                               0));

    close_image(m_src_ictx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));

    bufferlist bl1;
    bl1.append(std::string(1000, 'X'));

    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::write(*m_src_ictx, 0 * bl.length(), bl1.length(), // Write 1000 bytes at offset 0 (object 0)
                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::discard(*m_src_ictx, bl1.length() + 10,        // Discard 1000 bytes at offset 1010 (oject 0)
                                 bl1.length(), false));
    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap1"));

    deep_copy();
    compare();

    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::write(*m_src_ictx, 1 * bl.length(), bl1.length(),  // Write 1000 bytes at offset 8MB + 1  (object 2)
                               bufferlist{bl1}, 0));
    ASSERT_EQ(static_cast<ssize_t>(bl1.length()),
              api::Io<>::discard(*m_src_ictx, 2 * bl1.length() + 10,        // Discard 1000 bytes at offset 2010  (object 0)
                                 bl1.length(), false));

    ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));
    ASSERT_EQ(0, snap_create(*m_src_ictx, "snap2"));

    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap1", &m_src_snap_id_start));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_src_ictx, "snap2", &m_src_snap_id_end));
    ASSERT_EQ(0, librbd::api::Snapshot<>::get_id(m_dst_ictx, "snap1", &m_dst_snap_id_start));

  }

  void test_stress() {
    uint64_t initial_size, size;
    {
      std::shared_lock src_locker{m_src_ictx->image_lock};
      size = initial_size = m_src_ictx->get_image_size(CEPH_NOSNAP);
    }

    int nsnaps = 4;
    const char *c = getenv("TEST_RBD_DEEPCOPY_STRESS_NSNAPS");
    if (c != NULL) {
      std::stringstream ss(c);
      ASSERT_TRUE(ss >> nsnaps);
    }

    int nwrites = 4;
    c = getenv("TEST_RBD_DEEPCOPY_STRESS_NWRITES");
    if (c != NULL) {
      std::stringstream ss(c);
      ASSERT_TRUE(ss >> nwrites);
    }

    for (int i = 0; i < nsnaps; i++) {
      for (int j = 0; j < nwrites; j++) {
        size_t len = rand() % ((1 << m_src_ictx->order) * 2);
        ASSERT_GT(size, len);
        bufferlist bl;
        bl.append(std::string(len, static_cast<char>('A' + i)));
        uint64_t off = std::min(static_cast<uint64_t>(rand() % size),
                                static_cast<uint64_t>(size - len));
        std::cout << "write: " << static_cast<char>('A' + i) << " " << off
                  << "~" << len << std::endl;
        ASSERT_EQ(static_cast<ssize_t>(bl.length()),
                  api::Io<>::write(*m_src_ictx, off, bl.length(),
                                   bufferlist{bl}, 0));
        len = rand() % ((1 << m_src_ictx->order) * 2);
        ASSERT_GT(size, len);
        off = std::min(static_cast<uint64_t>(rand() % size),
                       static_cast<uint64_t>(size - len));
        std::cout << "discard: " << off << "~" << len << std::endl;
        ASSERT_EQ(static_cast<ssize_t>(len),
                  api::Io<>::discard(*m_src_ictx, off, len, false));
      }

      ASSERT_EQ(0, api::Io<>::flush(*m_src_ictx));

      std::string snap_name = "snap" + stringify(i);
      std::cout << "snap: " << snap_name << std::endl;
      ASSERT_EQ(0, snap_create(*m_src_ictx, snap_name.c_str()));

      if (m_src_ictx->test_features(RBD_FEATURE_LAYERING) && rand() % 4) {
        ASSERT_EQ(0, snap_protect(*m_src_ictx, snap_name.c_str()));

        std::string clone_name = get_temp_image_name();
        int order = m_src_ictx->order;
        uint64_t features;
        ASSERT_EQ(0, librbd::get_features(m_src_ictx, &features));

        std::cout << "clone " << m_src_ictx->name << " -> " << clone_name
                  << std::endl;
        ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_ictx->name.c_str(),
                                   snap_name.c_str(), m_ioctx,
                                   clone_name.c_str(), features, &order,
                                   m_src_ictx->stripe_unit,
                                   m_src_ictx->stripe_count));
        close_image(m_src_ictx);
        ASSERT_EQ(0, open_image(clone_name, &m_src_ictx));
      }

      if (rand() % 2) {
        librbd::NoOpProgressContext no_op;
        uint64_t new_size =  initial_size + rand() % size;
        std::cout << "resize: " << new_size << std::endl;
        ASSERT_EQ(0, m_src_ictx->operations->resize(new_size, true, no_op));
        {
          std::shared_lock src_locker{m_src_ictx->image_lock};
          size = m_src_ictx->get_image_size(CEPH_NOSNAP);
        }
        ASSERT_EQ(new_size, size);
      }
    }
  }

  librbd::ImageCtx *m_src_ictx = nullptr;
  librbd::ImageCtx *m_dst_ictx = nullptr;
  librbd::ImageOptions m_opts;
  librados::snap_t m_src_snap_id_start = 0;
  librados::snap_t m_src_snap_id_end = CEPH_NOSNAP;
  librados::snap_t m_dst_snap_id_start = 0;
};

TEST_F(TestDeepCopy, Empty)
{
}

TEST_F(TestDeepCopy, NoSnaps)
{
  test_no_snaps();
}

TEST_F(TestDeepCopy, Snaps)
{
  test_snaps();
}

TEST_F(TestDeepCopy, SnapDiscard)
{
  test_snap_discard();
}

TEST_F(TestDeepCopy, CloneDiscard)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  test_clone_discard();
}

TEST_F(TestDeepCopy, CloneShrink)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  test_clone_shrink();
}

TEST_F(TestDeepCopy, CloneExpand)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  test_clone_expand();
}

TEST_F(TestDeepCopy, CloneHideParent)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  test_clone_hide_parent();
}

TEST_F(TestDeepCopy, Clone)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  test_clone();
}

TEST_F(TestDeepCopy, CloneFlatten)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  uint64_t flatten = 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_FLATTEN, flatten));

  test_clone();
}

TEST_F(TestDeepCopy, Stress)
{
  test_stress();
}

TEST_F(TestDeepCopy, NoSnaps_LargerDstObjSize)
{
  SKIP_IF_CRIMSON();
  uint64_t order = m_src_ictx->order + 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));

  test_no_snaps();
}

TEST_F(TestDeepCopy, Snaps_LargerDstObjSize)
{
  SKIP_IF_CRIMSON();
  uint64_t order = m_src_ictx->order + 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));

  test_snaps();
}

TEST_F(TestDeepCopy, Clone_LargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  uint64_t order = m_src_ictx->order + 1 + rand() % 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));

  test_clone();
}

TEST_F(TestDeepCopy, CloneFlatten_LargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  uint64_t order = m_src_ictx->order + 1 + rand() % 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t flatten = 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_FLATTEN, flatten));

  test_clone();
}

TEST_F(TestDeepCopy, Stress_LargerDstObjSize)
{
  SKIP_IF_CRIMSON();
  uint64_t order = m_src_ictx->order + 1 + rand() % 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));

  test_stress();
}

TEST_F(TestDeepCopy, NoSnaps_SmallerDstObjSize)
{
  uint64_t order = m_src_ictx->order - 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t stripe_unit = m_src_ictx->stripe_unit >> 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));

  test_no_snaps();
}

TEST_F(TestDeepCopy, Snaps_SmallerDstObjSize)
{
  uint64_t order = m_src_ictx->order - 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t stripe_unit = m_src_ictx->stripe_unit >> 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));

  test_snaps();
}

TEST_F(TestDeepCopy, Clone_SmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  uint64_t order = m_src_ictx->order - 1 - rand() % 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t stripe_unit = m_src_ictx->stripe_unit >> 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));

  test_clone();
}

TEST_F(TestDeepCopy, CloneFlatten_SmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  uint64_t order = m_src_ictx->order - 1 - rand() % 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t stripe_unit = m_src_ictx->stripe_unit >> 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  uint64_t flatten = 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_FLATTEN, flatten));

  test_clone();
}

TEST_F(TestDeepCopy, Stress_SmallerDstObjSize)
{
  uint64_t order = m_src_ictx->order - 1 - rand() % 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  uint64_t stripe_unit = m_src_ictx->stripe_unit >> 2;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));

  test_stress();
}

TEST_F(TestDeepCopy, NoSnaps_StrippingLargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order + 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_no_snaps();
}

TEST_F(TestDeepCopy, Snaps_StrippingLargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order + 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_snaps();
}

TEST_F(TestDeepCopy, Clone_StrippingLargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order + 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_clone();
}

TEST_F(TestDeepCopy, CloneFlatten_StrippingLargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order + 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));
  uint64_t flatten = 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_FLATTEN, flatten));

  test_clone();
}

TEST_F(TestDeepCopy, Stress_StrippingLargerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order + 1 + rand() % 2;
  uint64_t stripe_unit = 1 << (order - rand() % 4);
  uint64_t stripe_count = 2 + rand() % 14;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_stress();
}

TEST_F(TestDeepCopy, NoSnaps_StrippingSmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order - 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_no_snaps();
}

TEST_F(TestDeepCopy, Snaps_StrippingSmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order - 1;
  uint64_t stripe_unit = 1 << (order - 2);
  uint64_t stripe_count = 4;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_snaps();
}

TEST_F(TestDeepCopy, Clone_StrippingSmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order - 1 - rand() % 2;
  uint64_t stripe_unit = 1 << (order - rand() % 4);
  uint64_t stripe_count = 2 + rand() % 14;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_clone();
}

TEST_F(TestDeepCopy, CloneFlatten_StrippingSmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order - 1 - rand() % 2;
  uint64_t stripe_unit = 1 << (order - rand() % 4);
  uint64_t stripe_count = 2 + rand() % 14;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));
  uint64_t flatten = 1;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_FLATTEN, flatten));

  test_clone();
}

TEST_F(TestDeepCopy, Stress_StrippingSmallerDstObjSize)
{
  REQUIRE_FEATURE(RBD_FEATURE_STRIPINGV2);

  uint64_t order = m_src_ictx->order - 1 - rand() % 2;
  uint64_t stripe_unit = 1 << (order - rand() % 4);
  uint64_t stripe_count = 2 + rand() % 14;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_ORDER, order));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit));
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count));

  test_stress();
}

TEST_F(TestDeepCopy, SnapsIncremental)
{
  test_snaps2();
}

TEST_F(TestDeepCopy, SnapDiscardIncremental)
{
  test_snap_discard2();
}

TEST_F(TestDeepCopy, SnapDiscardIncremental2)
{
  test_snap_discard3();
}

TEST_F(TestDeepCopy, CloneShrinkIncremental)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  test_clone_shrink2();
}

TEST_F(TestDeepCopy, CloneExpandIncremental)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  test_clone_expand2();
}


TEST_F(TestDeepCopy, CloneIncremental)
{
  //Skipping until https://tracker.ceph.com/issues/61891 is fixed
  GTEST_SKIP();
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  test_clone2();
}

} // namespace librbd
