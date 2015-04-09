// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "librbd/AioCompletion.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include <boost/scope_exit.hpp>
#include <utility>
#include <vector>

void register_test_internal() {
}

class TestInternal : public TestFixture {
public:

  TestInternal() {}

  typedef std::vector<std::pair<std::string, bool> > Snaps;

  virtual void TearDown() {
    for (Snaps::iterator iter = m_snaps.begin(); iter != m_snaps.end(); ++iter) {
      librbd::ImageCtx *ictx;
      EXPECT_EQ(0, open_image(m_image_name, &ictx));
      if (iter->second) {
	EXPECT_EQ(0, librbd::snap_unprotect(ictx, iter->first.c_str()));
      }
      EXPECT_EQ(0, librbd::snap_remove(ictx, iter->first.c_str()));
    }

    TestFixture::TearDown();
  }

  int create_snapshot(const char *snap_name, bool snap_protect) {
    librbd::ImageCtx *ictx;
    int r = open_image(m_image_name, &ictx);
    if (r < 0) {
      return r;
    }

    r = librbd::snap_create(ictx, snap_name);
    if (r < 0) {
      return r;
    }

    m_snaps.push_back(std::make_pair(snap_name, snap_protect));
    if (snap_protect) {
      r = librbd::snap_protect(ictx, snap_name);
      if (r < 0) {
	return r;
      }
    }
    close_image(ictx);
    return 0;
  }

  Snaps m_snaps;
};

class DummyContext : public Context {
public:
  virtual void finish(int r) {
  }
};

TEST_F(TestInternal, IsExclusiveLockOwner) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_FALSE(is_owner);

  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->try_lock());
  }

  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, ResizeLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, librbd::resize(ictx, m_image_size >> 1, no_op));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, ResizeFailsToLockImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(-EROFS, librbd::resize(ictx, m_image_size >> 1, no_op));
}

TEST_F(TestInternal, SnapCreateLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  BOOST_SCOPE_EXIT( (ictx) ) {
    ASSERT_EQ(0, librbd::snap_remove(ictx, "snap1"));
  } BOOST_SCOPE_EXIT_END;

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, SnapCreateFailsToLockImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  ASSERT_EQ(-EROFS, librbd::snap_create(ictx, "snap1"));
}

TEST_F(TestInternal, SnapRollbackLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  ASSERT_EQ(0, create_snapshot("snap1", false));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, librbd::snap_rollback(ictx, "snap1", no_op));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, SnapRollbackFailsToLockImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);


  ASSERT_EQ(0, create_snapshot("snap1", false));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(-EROFS, librbd::snap_rollback(ictx, "snap1", no_op));
}

TEST_F(TestInternal, SnapSetReleasesLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  ASSERT_EQ(0, create_snapshot("snap1", false));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, librbd::snap_set(ictx, "snap1"));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_FALSE(is_owner);
}

TEST_F(TestInternal, FlattenLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snapshot("snap1", true));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  std::string clone_name = get_temp_image_name();
  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, librbd::flatten(ictx2, no_op));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx2, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, FlattenFailsToLockImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snapshot("snap1", true));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  std::string clone_name = get_temp_image_name();
  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), features, &order, 0, 0));

  TestInternal *parent = this;
  librbd::ImageCtx *ictx2 = NULL;
  BOOST_SCOPE_EXIT( (&m_ioctx) (clone_name) (parent) (&ictx2) ) {
    if (ictx2 != NULL) {
      parent->close_image(ictx2);
      parent->unlock_image();
    }
    librbd::NoOpProgressContext no_op;
    ASSERT_EQ(0, librbd::remove(m_ioctx, clone_name.c_str(), no_op));
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, open_image(clone_name, &ictx2));
  ASSERT_EQ(0, lock_image(*ictx2, LOCK_EXCLUSIVE, "manually locked"));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(-EROFS, librbd::flatten(ictx2, no_op));
}

TEST_F(TestInternal, AioWriteRequestsLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  std::string buffer(256, '1');
  DummyContext *ctx = new DummyContext();
  librbd::AioCompletion *c =
    librbd::aio_create_completion_internal(ctx, librbd::rbd_ctx_cb);
  c->get();
  aio_write(ictx, 0, buffer.size(), buffer.c_str(), c, 0);

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_FALSE(is_owner);
  ASSERT_FALSE(c->is_complete());
  c->put();
}

TEST_F(TestInternal, AioDiscardRequestsLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  DummyContext *ctx = new DummyContext();
  librbd::AioCompletion *c =
    librbd::aio_create_completion_internal(ctx, librbd::rbd_ctx_cb);
  c->get();
  aio_discard(ictx, 0, 256, c);

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_FALSE(is_owner);
  ASSERT_FALSE(c->is_complete());
  c->put();
}

TEST_F(TestInternal, CancelAsyncResize) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  {
    RWLock::WLocker l(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->try_lock());
    ASSERT_TRUE(ictx->image_watcher->is_lock_owner());
  }

  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));

  uint32_t attempts = 0;
  while (attempts++ < 20 && size > 0) {
    C_SaferCond ctx;
    librbd::NoOpProgressContext prog_ctx;

    size -= MIN(size, 1<<18);
    {
      RWLock::RLocker l(ictx->owner_lock);
      ASSERT_EQ(0, librbd::async_resize(ictx, &ctx, size, prog_ctx));
    }

    // try to interrupt the in-progress resize
    ictx->cancel_async_requests();

    int r = ctx.wait();
    if (r == -ERESTART) {
      std::cout << "detected canceled async request" << std::endl;
      break;
    }
    ASSERT_EQ(0, r);
  }
}

TEST_F(TestInternal, MultipleResize) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  {
    RWLock::WLocker l(ictx->owner_lock);
    if (ictx->image_watcher->is_lock_supported()) {
      ASSERT_EQ(0, ictx->image_watcher->try_lock());
      ASSERT_TRUE(ictx->image_watcher->is_lock_owner());
    }
  }

  uint64_t size;
  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  uint64_t original_size = size;

  std::vector<C_SaferCond*> contexts;

  uint32_t attempts = 0;
  librbd::NoOpProgressContext prog_ctx;
  while (size > 0) {
    uint64_t new_size = original_size;
    if (attempts++ % 2 == 0) {
      size -= MIN(size, 1<<18);
      new_size = size;
    }

    RWLock::RLocker l(ictx->owner_lock);
    contexts.push_back(new C_SaferCond());
    ASSERT_EQ(0, librbd::async_resize(ictx, contexts.back(), new_size,
                                      prog_ctx));
  }

  for (uint32_t i = 0; i < contexts.size(); ++i) {
    ASSERT_EQ(0, contexts[i]->wait());
    delete contexts[i];
  }

  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(0U, size);
}
