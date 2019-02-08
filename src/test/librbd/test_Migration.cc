// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados/test.h"
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/api/Group.h"
#include "librbd/api/Image.h"
#include "librbd/api/Migration.h"
#include "librbd/api/Mirror.h"
#include "librbd/api/Namespace.h"
#include "librbd/internal.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/io/ReadResult.h"

void register_test_migration() {
}

struct TestMigration : public TestFixture {
  static void SetUpTestCase() {
    TestFixture::SetUpTestCase();

    _other_pool_name = get_temp_pool_name("test-librbd-");
    ASSERT_EQ(0, _rados.pool_create(_other_pool_name.c_str()));
  }

  static void TearDownTestCase() {
    ASSERT_EQ(0, _rados.pool_delete(_other_pool_name.c_str()));

    TestFixture::TearDownTestCase();
  }

  void SetUp() override {
    TestFixture::SetUp();

    ASSERT_EQ(0, _rados.ioctx_create(_other_pool_name.c_str(),
                                     _other_pool_ioctx));

    open_image(m_ioctx, m_image_name, &m_ictx);
    m_image_id = m_ictx->id;

    std::string ref_image_name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(m_rbd, m_ioctx, ref_image_name, m_ictx->size));
    EXPECT_EQ(0, _rados.ioctx_create2(m_ioctx.get_id(), m_ref_ioctx));
    open_image(m_ref_ioctx, ref_image_name, &m_ref_ictx);

    resize(20 * (1 << 22));
  }

  void TearDown() override {
    if (m_ref_ictx != nullptr) {
      close_image(m_ref_ictx);
    }
    if (m_ictx != nullptr) {
      close_image(m_ictx);
    }

    _other_pool_ioctx.close();

    TestFixture::TearDown();
  }

  void compare(const std::string &description = "") {
    vector<librbd::snap_info_t> src_snaps, dst_snaps;

    EXPECT_EQ(m_ref_ictx->size, m_ictx->size);
    EXPECT_EQ(0, librbd::snap_list(m_ref_ictx, src_snaps));
    EXPECT_EQ(0, librbd::snap_list(m_ictx, dst_snaps));
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
                     m_ref_ictx, cls::rbd::UserSnapshotNamespace(),
                     src_snap_name));
      EXPECT_EQ(0, librbd::api::Image<>::snap_set(
                     m_ictx, cls::rbd::UserSnapshotNamespace(),
                     dst_snap_name));
      compare_snaps(
        description + " snap: " + (src_snap_name ? src_snap_name : "null"),
        m_ref_ictx, m_ictx);
    }
  }

  void compare_snaps(const std::string &description, librbd::ImageCtx *src_ictx,
                     librbd::ImageCtx *dst_ictx) {
    uint64_t src_size, dst_size;
    {
      RWLock::RLocker src_locker(src_ictx->snap_lock);
      RWLock::RLocker dst_locker(dst_ictx->snap_lock);
      src_size = src_ictx->get_image_size(src_ictx->snap_id);
      dst_size = dst_ictx->get_image_size(dst_ictx->snap_id);
    }
    if (src_size != dst_size) {
      std::cout << description << ": size differs" << std::endl;
      EXPECT_EQ(src_size, dst_size);
    }

    if (dst_ictx->test_features(RBD_FEATURE_LAYERING)) {
      bool flags_set;
      RWLock::RLocker dst_locker(dst_ictx->snap_lock);
      EXPECT_EQ(0, dst_ictx->test_flags(dst_ictx->snap_id,
                                        RBD_FLAG_OBJECT_MAP_INVALID,
                                        dst_ictx->snap_lock, &flags_set));
      EXPECT_FALSE(flags_set);
    }

    ssize_t read_size = 1 << src_ictx->order;
    uint64_t offset = 0;
    while (offset < src_size) {
      read_size = std::min(read_size, static_cast<ssize_t>(src_size - offset));

      bufferptr src_ptr(read_size);
      bufferlist src_bl;
      src_bl.push_back(src_ptr);
      librbd::io::ReadResult src_result{&src_bl};
      EXPECT_EQ(read_size, src_ictx->io_work_queue->read(
                  offset, read_size, librbd::io::ReadResult{src_result}, 0));

      bufferptr dst_ptr(read_size);
      bufferlist dst_bl;
      dst_bl.push_back(dst_ptr);
      librbd::io::ReadResult dst_result{&dst_bl};
      EXPECT_EQ(read_size, dst_ictx->io_work_queue->read(
                  offset, read_size, librbd::io::ReadResult{dst_result}, 0));

      if (!src_bl.contents_equal(dst_bl)) {
        std::cout << description
                  << ", block " << offset << "~" << read_size << " differs"
                  << std::endl;
        char *c = getenv("TEST_RBD_MIGRATION_VERBOSE");
        if (c != NULL && *c != '\0') {
          std::cout << "src block: " << std::endl; src_bl.hexdump(std::cout);
          std::cout << "dst block: " << std::endl; dst_bl.hexdump(std::cout);
        }
      }
      EXPECT_TRUE(src_bl.contents_equal(dst_bl));
      offset += read_size;
    }
  }

  void open_image(librados::IoCtx& io_ctx, const std::string &name,
                  librbd::ImageCtx **ictx) {
    *ictx = new librbd::ImageCtx(name.c_str(), "", nullptr, io_ctx, false);
    m_ictxs.insert(*ictx);

    ASSERT_EQ(0, (*ictx)->state->open(0));
  }

  void migration_prepare(librados::IoCtx& dst_io_ctx,
                         const std::string &dst_name, int r = 0) {
    std::cout << __func__ << std::endl;

    close_image(m_ictx);
    m_ictx = nullptr;

    EXPECT_EQ(r, librbd::api::Migration<>::prepare(m_ioctx, m_image_name,
                                                   dst_io_ctx, dst_name,
                                                   m_opts));
    if (r == 0) {
      open_image(dst_io_ctx, dst_name, &m_ictx);
    } else {
      open_image(m_ioctx, m_image_name, &m_ictx);
    }
    compare("after prepare");
  }

  void migration_execute(librados::IoCtx& io_ctx, const std::string &name,
                         int r = 0) {
    std::cout << __func__ << std::endl;

    librbd::NoOpProgressContext no_op;
    EXPECT_EQ(r, librbd::api::Migration<>::execute(io_ctx, name, no_op));
  }

  void migration_abort(librados::IoCtx& io_ctx, const std::string &name,
                       int r = 0) {
    std::cout << __func__ << std::endl;

    std::string dst_name = m_ictx->name;
    close_image(m_ictx);
    m_ictx = nullptr;

    librbd::NoOpProgressContext no_op;
    EXPECT_EQ(r, librbd::api::Migration<>::abort(io_ctx, name, no_op));

    if (r == 0) {
      open_image(m_ioctx, m_image_name, &m_ictx);
    } else {
      open_image(m_ioctx, dst_name, &m_ictx);
    }

    compare("after abort");
  }

  void migration_commit(librados::IoCtx& io_ctx, const std::string &name) {
    std::cout << __func__ << std::endl;

    librbd::NoOpProgressContext no_op;
    EXPECT_EQ(0, librbd::api::Migration<>::commit(io_ctx, name, no_op));

    compare("after commit");
  }

  void migration_status(librbd::image_migration_state_t state) {
    librbd::image_migration_status_t status;
    EXPECT_EQ(0, librbd::api::Migration<>::status(m_ioctx, m_image_name,
                                                  &status));
    EXPECT_EQ(status.source_pool_id, m_ioctx.get_id());
    EXPECT_EQ(status.source_pool_namespace, m_ioctx.get_namespace());
    EXPECT_EQ(status.source_image_name, m_image_name);
    EXPECT_EQ(status.source_image_id, m_image_id);
    EXPECT_EQ(status.dest_pool_id, m_ictx->md_ctx.get_id());
    EXPECT_EQ(status.dest_pool_namespace, m_ictx->md_ctx.get_namespace());
    EXPECT_EQ(status.dest_image_name, m_ictx->name);
    EXPECT_EQ(status.dest_image_id, m_ictx->id);
    EXPECT_EQ(status.state, state);
  }

  void migrate(librados::IoCtx& dst_io_ctx, const std::string &dst_name) {
    migration_prepare(dst_io_ctx, dst_name);
    migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);
    migration_execute(dst_io_ctx, dst_name);
    migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
    migration_commit(dst_io_ctx, dst_name);
  }

  void write(uint64_t off, uint64_t len, char c) {
    std::cout << "write: " << c << " " << off << "~" << len << std::endl;

    bufferlist ref_bl;
    ref_bl.append(std::string(len, c));
    ASSERT_EQ(static_cast<ssize_t>(len),
              m_ref_ictx->io_work_queue->write(off, len, std::move(ref_bl), 0));
    bufferlist bl;
    bl.append(std::string(len, c));
    ASSERT_EQ(static_cast<ssize_t>(len),
              m_ictx->io_work_queue->write(off, len, std::move(bl), 0));
  }

  void discard(uint64_t off, uint64_t len) {
    std::cout << "discard: " << off << "~" << len << std::endl;

    ASSERT_EQ(static_cast<ssize_t>(len),
              m_ref_ictx->io_work_queue->discard(off, len, false));
    ASSERT_EQ(static_cast<ssize_t>(len),
              m_ictx->io_work_queue->discard(off, len, false));
  }

  void flush() {
    ASSERT_EQ(0, m_ref_ictx->io_work_queue->flush());
    ASSERT_EQ(0, m_ictx->io_work_queue->flush());
  }

  void snap_create(const std::string &snap_name) {
    std::cout << "snap_create: " << snap_name << std::endl;

    flush();

    ASSERT_EQ(0, TestFixture::snap_create(*m_ref_ictx, snap_name));
    ASSERT_EQ(0, TestFixture::snap_create(*m_ictx, snap_name));
  }

  void snap_protect(const std::string &snap_name) {
    std::cout << "snap_protect: " << snap_name << std::endl;

    ASSERT_EQ(0, TestFixture::snap_protect(*m_ref_ictx, snap_name));
    ASSERT_EQ(0, TestFixture::snap_protect(*m_ictx, snap_name));
  }

  void clone(const std::string &snap_name) {
    snap_protect(snap_name);

    int order = m_ref_ictx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_ref_ictx, &features));
    features &= ~RBD_FEATURES_IMPLICIT_ENABLE;

    std::string ref_clone_name = get_temp_image_name();
    std::string clone_name = get_temp_image_name();

    std::cout << "clone " << m_ictx->name << " -> " << clone_name
              << std::endl;

    ASSERT_EQ(0, librbd::clone(m_ref_ictx->md_ctx, m_ref_ictx->name.c_str(),
                               snap_name.c_str(), m_ref_ioctx,
                               ref_clone_name.c_str(), features, &order,
                               m_ref_ictx->stripe_unit,
                               m_ref_ictx->stripe_count));

    ASSERT_EQ(0, librbd::clone(m_ictx->md_ctx, m_ictx->name.c_str(),
                               snap_name.c_str(), m_ioctx,
                               clone_name.c_str(), features, &order,
                               m_ictx->stripe_unit,
                               m_ictx->stripe_count));

    close_image(m_ref_ictx);
    open_image(m_ref_ioctx, ref_clone_name, &m_ref_ictx);

    close_image(m_ictx);
    open_image(m_ioctx, clone_name, &m_ictx);
    m_image_name = m_ictx->name;
    m_image_id = m_ictx->id;
  }

  void resize(uint64_t size) {
    std::cout << "resize: " << size << std::endl;

    librbd::NoOpProgressContext no_op;
    ASSERT_EQ(0, m_ref_ictx->operations->resize(size, true, no_op));
    ASSERT_EQ(0, m_ictx->operations->resize(size, true, no_op));
  }

  void test_no_snaps() {
    uint64_t len = (1 << m_ictx->order) * 2 + 1;
    write(0 * len, len, '1');
    write(2 * len, len, '1');
    flush();
  }

  void test_snaps() {
    uint64_t len = (1 << m_ictx->order) * 2 + 1;
    write(0 * len, len, '1');
    snap_create("snap1");
    write(1 * len, len, '1');

    write(0 * len, 1000, 'X');
    discard(1000 + 10, 1000);

    snap_create("snap2");

    write(1 * len, 1000, 'X');
    discard(2 * len + 10, 1000);

    uint64_t size = m_ictx->size;

    resize(size << 1);

    write(size - 1, len, '2');

    snap_create("snap3");

    resize(size);

    discard(size - 1, 1);

    flush();
  }

  void test_clone() {
    uint64_t len = (1 << m_ictx->order) * 2 + 1;
    write(0 * len, len, 'X');
    write(2 * len, len, 'X');

    snap_create("snap");
    clone("snap");

    write(0, 1000, 'X');
    discard(1010, 1000);

    snap_create("snap");
    clone("snap");

    write(1000, 1000, 'X');
    discard(2010, 1000);

    flush();
  }

  void test_stress(const std::string &snap_name_prefix = "snap",
                   char start_char = 'A') {
    uint64_t initial_size = m_ictx->size;

    int nsnaps = 4;
    const char *c = getenv("TEST_RBD_MIGRATION_STRESS_NSNAPS");
    if (c != NULL) {
      std::stringstream ss(c);
      ASSERT_TRUE(ss >> nsnaps);
    }

    int nwrites = 4;
    c = getenv("TEST_RBD_MIGRATION_STRESS_NWRITES");
    if (c != NULL) {
      std::stringstream ss(c);
      ASSERT_TRUE(ss >> nwrites);
    }

    for (int i = 0; i < nsnaps; i++) {
      for (int j = 0; j < nwrites; j++) {
        size_t len = rand() % ((1 << m_ictx->order) * 2);
        ASSERT_GT(m_ictx->size, len);
        uint64_t off = std::min(static_cast<uint64_t>(rand() % m_ictx->size),
                                static_cast<uint64_t>(m_ictx->size - len));
        write(off, len, start_char + i);

        len = rand() % ((1 << m_ictx->order) * 2);
        ASSERT_GT(m_ictx->size, len);
        off = std::min(static_cast<uint64_t>(rand() % m_ictx->size),
                       static_cast<uint64_t>(m_ictx->size - len));
        discard(off, len);
      }

      std::string snap_name = snap_name_prefix + stringify(i);
      snap_create(snap_name);

      if (m_ictx->test_features(RBD_FEATURE_LAYERING) &&
          !m_ictx->test_features(RBD_FEATURE_MIGRATING) &&
          rand() % 4) {
        clone(snap_name);
      }

      if (rand() % 2) {
        librbd::NoOpProgressContext no_op;
        uint64_t new_size = initial_size + rand() % m_ictx->size;
        resize(new_size);
        ASSERT_EQ(new_size, m_ictx->size);
      }
    }
    flush();
  }

  void test_stress2(bool concurrent) {
    test_stress();

    migration_prepare(m_ioctx, m_image_name);
    migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

    thread user([this]() {
        test_stress("user", 'a');
        for (int i = 0; i < 5; i++) {
          uint64_t off = (i + 1) * m_ictx->size / 10;
          uint64_t len = m_ictx->size / 40;
          write(off, len, '1' + i);

          off += len / 4;
          len /= 2;
          discard(off, len);
        }
        flush();
      });

    if (concurrent) {
      librados::IoCtx io_ctx;
      EXPECT_EQ(0, _rados.ioctx_create2(m_ioctx.get_id(), io_ctx));
      migration_execute(io_ctx, m_image_name);
      io_ctx.close();
      user.join();
    } else {
      user.join();
      compare("before execute");
      migration_execute(m_ioctx, m_image_name);
    }

    migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
    migration_commit(m_ioctx, m_image_name);
  }

  static std::string _other_pool_name;
  static librados::IoCtx _other_pool_ioctx;

  std::string m_image_id;
  librbd::ImageCtx *m_ictx = nullptr;
  librados::IoCtx m_ref_ioctx;
  librbd::ImageCtx *m_ref_ictx = nullptr;
  librbd::ImageOptions m_opts;
};

std::string TestMigration::_other_pool_name;
librados::IoCtx TestMigration::_other_pool_ioctx;

TEST_F(TestMigration, Empty)
{
  uint64_t features = m_ictx->features ^ RBD_FEATURE_LAYERING;
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_FEATURES, features));

  migrate(m_ioctx, m_image_name);

  ASSERT_EQ(features, m_ictx->features);
}

TEST_F(TestMigration, OtherName)
{
  std::string name = get_temp_image_name();

  migrate(m_ioctx, name);

  ASSERT_EQ(name, m_ictx->name);
}

TEST_F(TestMigration, OtherPool)
{
  migrate(_other_pool_ioctx, m_image_name);

  ASSERT_EQ(_other_pool_ioctx.get_id(), m_ictx->md_ctx.get_id());
}

TEST_F(TestMigration, OtherNamespace)
{
  ASSERT_EQ(0, librbd::api::Namespace<>::create(_other_pool_ioctx, "ns1"));
  _other_pool_ioctx.set_namespace("ns1");

  migrate(_other_pool_ioctx, m_image_name);

  ASSERT_EQ(_other_pool_ioctx.get_id(), m_ictx->md_ctx.get_id());
  ASSERT_EQ(_other_pool_ioctx.get_namespace(), m_ictx->md_ctx.get_namespace());
  _other_pool_ioctx.set_namespace("");
}

TEST_F(TestMigration, DataPool)
{
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_DATA_POOL,
                          _other_pool_ioctx.get_pool_name().c_str()));

  migrate(m_ioctx, m_image_name);

  ASSERT_EQ(_other_pool_ioctx.get_id(), m_ictx->data_ctx.get_id());
}

TEST_F(TestMigration, AbortAfterPrepare)
{
  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);
  migration_abort(m_ioctx, m_image_name);
}

TEST_F(TestMigration, AbortAfterFailedPrepare)
{
  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_DATA_POOL, "INVALID_POOL"));

  migration_prepare(m_ioctx, m_image_name, -ENOENT);

  // Migration is automatically aborted if prepare failed
}

TEST_F(TestMigration, AbortAfterExecute)
{
  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);
  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_abort(m_ioctx, m_image_name);
}

TEST_F(TestMigration, OtherPoolAbortAfterExecute)
{
  migration_prepare(_other_pool_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);
  migration_execute(_other_pool_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_abort(_other_pool_ioctx, m_image_name);
}

TEST_F(TestMigration, OtherNamespaceAbortAfterExecute)
{
  ASSERT_EQ(0, librbd::api::Namespace<>::create(_other_pool_ioctx, "ns2"));
  _other_pool_ioctx.set_namespace("ns2");

  migration_prepare(_other_pool_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);
  migration_execute(_other_pool_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_abort(_other_pool_ioctx, m_image_name);

  _other_pool_ioctx.set_namespace("");
  ASSERT_EQ(0, librbd::api::Namespace<>::remove(_other_pool_ioctx, "ns2"));
}

TEST_F(TestMigration, MirroringSamePool)
{
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  ASSERT_EQ(0, librbd::api::Mirror<>::mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));

  ASSERT_EQ(0, librbd::api::Mirror<>::image_enable(m_ictx, false));
  librbd::mirror_image_info_t info;
  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, info.state);

  migrate(m_ioctx, m_image_name);

  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, info.state);
}

TEST_F(TestMigration, MirroringAbort)
{
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  ASSERT_EQ(0, librbd::api::Mirror<>::mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));

  ASSERT_EQ(0, librbd::api::Mirror<>::image_enable(m_ictx, false));
  librbd::mirror_image_info_t info;
  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, info.state);

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);
  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_DISABLED, info.state);

  migration_abort(m_ioctx, m_image_name);

  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, info.state);
}

TEST_F(TestMigration, MirroringOtherPoolDisabled)
{
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  ASSERT_EQ(0, librbd::api::Mirror<>::mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));

  ASSERT_EQ(0, librbd::api::Mirror<>::image_enable(m_ictx, false));
  librbd::mirror_image_info_t info;
  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, info.state);

  migrate(_other_pool_ioctx, m_image_name);

  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_DISABLED, info.state);
}

TEST_F(TestMigration, MirroringOtherPoolEnabled)
{
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  ASSERT_EQ(0, librbd::api::Mirror<>::mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  ASSERT_EQ(0, librbd::api::Mirror<>::mode_set(_other_pool_ioctx,
                                               RBD_MIRROR_MODE_IMAGE));

  ASSERT_EQ(0, librbd::api::Mirror<>::image_enable(m_ictx, false));
  librbd::mirror_image_info_t info;
  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, info.state);

  migrate(_other_pool_ioctx, m_image_name);

  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, info.state);
}

TEST_F(TestMigration, MirroringPool)
{
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  ASSERT_EQ(0, librbd::api::Mirror<>::mode_set(_other_pool_ioctx,
                                               RBD_MIRROR_MODE_POOL));
  librbd::mirror_image_info_t info;
  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_DISABLED, info.state);

  migrate(_other_pool_ioctx, m_image_name);

  ASSERT_EQ(0, librbd::api::Mirror<>::image_get_info(m_ictx, &info));
  ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, info.state);
}

TEST_F(TestMigration, Group)
{
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, librbd::api::Group<>::create(m_ioctx, "123"));
  ASSERT_EQ(0, librbd::api::Group<>::image_add(m_ioctx, "123", m_ioctx,
                                               m_image_name.c_str()));
  librbd::group_info_t info;
  ASSERT_EQ(0, librbd::api::Group<>::image_get_group(m_ictx, &info));

  std::string name = get_temp_image_name();

  migrate(m_ioctx, name);

  ASSERT_EQ(0, librbd::api::Group<>::image_get_group(m_ictx, &info));
  ASSERT_EQ(info.name, "123");

  ASSERT_EQ(0, librbd::api::Group<>::image_remove(m_ioctx, "123", m_ioctx,
                                                  name.c_str()));
  ASSERT_EQ(0, librbd::api::Group<>::remove(m_ioctx, "123"));
}

TEST_F(TestMigration, GroupAbort)
{
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, librbd::api::Group<>::create(m_ioctx, "123"));
  ASSERT_EQ(0, librbd::api::Group<>::image_add(m_ioctx, "123", m_ioctx,
                                               m_image_name.c_str()));
  librbd::group_info_t info;
  ASSERT_EQ(0, librbd::api::Group<>::image_get_group(m_ictx, &info));

  std::string name = get_temp_image_name();

  migration_prepare(m_ioctx, name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  ASSERT_EQ(0, librbd::api::Group<>::image_get_group(m_ictx, &info));
  ASSERT_EQ(info.name, "123");

  migration_abort(m_ioctx, m_image_name);

  ASSERT_EQ(0, librbd::api::Group<>::image_get_group(m_ictx, &info));
  ASSERT_EQ(info.name, "123");

  ASSERT_EQ(0, librbd::api::Group<>::image_remove(m_ioctx, "123", m_ioctx,
                                                  m_image_name.c_str()));
  ASSERT_EQ(0, librbd::api::Group<>::remove(m_ioctx, "123"));
}

TEST_F(TestMigration, NoSnaps)
{
  test_no_snaps();
  migrate(m_ioctx, m_image_name);
}

TEST_F(TestMigration, NoSnapsOtherPool)
{
  test_no_snaps();

  test_no_snaps();
  migrate(_other_pool_ioctx, m_image_name);
}

TEST_F(TestMigration, NoSnapsDataPool)
{
  test_no_snaps();

  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_DATA_POOL,
                          _other_pool_ioctx.get_pool_name().c_str()));
  migrate(m_ioctx, m_image_name);

  EXPECT_EQ(_other_pool_ioctx.get_id(), m_ictx->data_ctx.get_id());
}

TEST_F(TestMigration, NoSnapsShrinkAfterPrepare)
{
  test_no_snaps();

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  resize(m_ictx->size >> 1);

  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, NoSnapsShrinkToZeroBeforePrepare)
{
  test_no_snaps();
  resize(0);

  migrate(m_ioctx, m_image_name);
}

TEST_F(TestMigration, NoSnapsShrinkToZeroAfterPrepare)
{
  test_no_snaps();

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  resize(0);

  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, NoSnapsExpandAfterPrepare)
{
  test_no_snaps();

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  resize(m_ictx->size << 1);

  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, NoSnapsSnapAfterPrepare)
{
  test_no_snaps();

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  snap_create("after_prepare_snap");
  resize(m_ictx->size >> 1);
  write(0, 1000, '*');

  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, Snaps)
{
  test_snaps();
  migrate(m_ioctx, m_image_name);
}

TEST_F(TestMigration, SnapsOtherPool)
{
  test_snaps();

  test_no_snaps();
  migrate(_other_pool_ioctx, m_image_name);

  EXPECT_EQ(_other_pool_ioctx.get_id(), m_ictx->md_ctx.get_id());
}

TEST_F(TestMigration, SnapsDataPool)
{
  test_snaps();

  ASSERT_EQ(0, m_opts.set(RBD_IMAGE_OPTION_DATA_POOL,
                          _other_pool_ioctx.get_pool_name().c_str()));
  migrate(m_ioctx, m_image_name);

  EXPECT_EQ(_other_pool_ioctx.get_id(), m_ictx->data_ctx.get_id());
}

TEST_F(TestMigration, SnapsShrinkAfterPrepare)
{
  test_snaps();

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  resize(m_ictx->size >> 1);

  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, SnapsShrinkToZeroBeforePrepare)
{
  test_snaps();
  resize(0);

  migrate(m_ioctx, m_image_name);
}

TEST_F(TestMigration, SnapsShrinkToZeroAfterPrepare)
{
  test_snaps();

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  resize(0);

  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, SnapsExpandAfterPrepare)
{
  test_snaps();

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  auto size = m_ictx->size;
  resize(size << 1);
  write(size, 1000, '*');

  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, SnapsExpandAfterPrepare2)
{
  auto size = m_ictx->size;

  write(size >> 1, 10, 'X');
  snap_create("snap1");
  resize(size >> 1);

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  resize(size);
  write(size >> 1, 5, 'Y');

  compare("before execute");

  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, SnapsSnapAfterPrepare)
{
  test_snaps();

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  auto ictx = new librbd::ImageCtx(m_ictx->name.c_str(), "", "snap3", m_ioctx,
                                   false);
  ASSERT_EQ(0, ictx->state->open(0));
  EXPECT_EQ(0, librbd::api::Image<>::snap_set(
              m_ref_ictx, cls::rbd::UserSnapshotNamespace(), "snap3"));
  compare_snaps("opened after prepare snap3", m_ref_ictx, ictx);
  EXPECT_EQ(0, librbd::api::Image<>::snap_set(
              m_ref_ictx, cls::rbd::UserSnapshotNamespace(), nullptr));
  EXPECT_EQ(0, ictx->state->close());

  snap_create("after_prepare_snap");
  resize(m_ictx->size >> 1);
  write(0, 1000, '*');

  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, SnapsSnapExpandAfterPrepare)
{
  test_snaps();

  migration_prepare(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_PREPARED);

  snap_create("after_prepare_snap");
  auto size = m_ictx->size;
  resize(size << 1);
  write(size, 1000, '*');

  migration_execute(m_ioctx, m_image_name);
  migration_status(RBD_IMAGE_MIGRATION_STATE_EXECUTED);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, Clone)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  test_clone();
  migrate(m_ioctx, m_image_name);
}

TEST_F(TestMigration, CloneParent) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  snap_create("snap");

  librbd::linked_image_spec_t expected_parent_image;
  expected_parent_image.image_id = m_ictx->id;
  expected_parent_image.image_name = m_ictx->name;

  auto it = m_ictx->snap_ids.find({cls::rbd::UserSnapshotNamespace{}, "snap"});
  ASSERT_TRUE(it != m_ictx->snap_ids.end());

  librbd::snap_spec_t expected_parent_snap;
  expected_parent_snap.id = it->second;

  clone("snap");
  migration_prepare(m_ioctx, m_image_name);

  librbd::linked_image_spec_t parent_image;
  librbd::snap_spec_t parent_snap;
  ASSERT_EQ(0, librbd::api::Image<>::get_parent(m_ictx, &parent_image,
                                                &parent_snap));
  ASSERT_EQ(expected_parent_image.image_id, parent_image.image_id);
  ASSERT_EQ(expected_parent_image.image_name, parent_image.image_name);
  ASSERT_EQ(expected_parent_snap.id, parent_snap.id);

  migration_abort(m_ioctx, m_image_name);
}


TEST_F(TestMigration, CloneUpdateAfterPrepare)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  write(0, 10, 'X');
  snap_create("snap");
  clone("snap");

  migration_prepare(m_ioctx, m_image_name);

  write(0, 1, 'Y');

  migration_execute(m_ioctx, m_image_name);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, TriggerAssertSnapcSeq)
{
  auto size = m_ictx->size;

  write((size >> 1) + 0, 10, 'A');
  snap_create("snap1");
  write((size >> 1) + 1, 10, 'B');

  migration_prepare(m_ioctx, m_image_name);

  // copyup => deep copy (first time)
  write((size >> 1) + 2, 10, 'C');

  // preserve data before resizing
  snap_create("snap2");

  // decrease head overlap
  resize(size >> 1);

  // migrate object => deep copy (second time) => assert_snapc_seq => -ERANGE
  migration_execute(m_ioctx, m_image_name);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, SnapTrimBeforePrepare)
{
  auto size = m_ictx->size;

  write(size >> 1, 10, 'A');
  snap_create("snap1");
  resize(size >> 1);

  migration_prepare(m_ioctx, m_image_name);

  resize(size);
  snap_create("snap3");
  write(size >> 1, 10, 'B');
  snap_create("snap4");
  resize(size >> 1);

  migration_execute(m_ioctx, m_image_name);
  migration_commit(m_ioctx, m_image_name);
}

TEST_F(TestMigration, StressNoMigrate)
{
  test_stress();

  compare();
}

TEST_F(TestMigration, Stress)
{
  test_stress();

  migrate(m_ioctx, m_image_name);
}

TEST_F(TestMigration, Stress2)
{
  test_stress2(false);
}

TEST_F(TestMigration, StressLive)
{
  test_stress2(true);
}
