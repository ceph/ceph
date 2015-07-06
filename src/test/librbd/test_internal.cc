// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/AioCompletion.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include <boost/scope_exit.hpp>
#include <boost/assign/list_of.hpp>
#include <utility>
#include <vector>

void register_test_internal() {
}

class TestInternal : public TestFixture {
public:

  TestInternal() {}

  typedef std::vector<std::pair<std::string, bool> > Snaps;

  virtual void TearDown() {
    unlock_image();
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

  unlock_image();
  ASSERT_EQ(0, c->wait_for_complete());
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

  unlock_image();
  ASSERT_EQ(0, c->wait_for_complete());
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

TEST_F(TestInternal, MetadatConfig) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  map<string, bool> test_confs = boost::assign::map_list_of(
    "aaaaaaa", false)(
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", false)(
    "cccccccccccccc", false);
  map<string, bool>::iterator it = test_confs.begin();
  const string prefix = "test_config_";
  bool is_continue;
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::Image image1;
  map<string, bufferlist> pairs, res;
  pairs[prefix+it->first].append("value1");
  ++it;
  pairs[prefix+it->first].append("value2");
  ++it;
  pairs[prefix+it->first].append("value3");
  pairs[prefix+"asdfsdaf"].append("value6");
  pairs[prefix+"zxvzxcv123"].append("value5");

  is_continue = ictx->_filter_metadata_confs(prefix, test_confs, pairs, &res);
  ASSERT_TRUE(is_continue);
  ASSERT_TRUE(res.size() == 3U);
  it = test_confs.begin();
  ASSERT_TRUE(res.count(it->first));
  ASSERT_TRUE(it->second);
  ++it;
  ASSERT_TRUE(res.count(it->first));
  ASSERT_TRUE(it->second);
  ++it;
  ASSERT_TRUE(res.count(it->first));
  ASSERT_TRUE(it->second);
  res.clear();

  pairs["zzzzzzzz"].append("value7");
  is_continue = ictx->_filter_metadata_confs(prefix, test_confs, pairs, &res);
  ASSERT_FALSE(is_continue);
  ASSERT_TRUE(res.size() == 3U);
}

TEST_F(TestInternal, SnapshotCopyup)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  bufferlist bl;
  bl.append(std::string(256, '1'));
  ASSERT_EQ(256, librbd::write(ictx, 0, bl.length(), bl.c_str(), 0));

  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::snap_protect(ictx, "snap1"));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  std::string clone_name = get_temp_image_name();
  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  ASSERT_EQ(0, librbd::snap_create(ictx2, "snap1"));
  ASSERT_EQ(0, librbd::snap_create(ictx2, "snap2"));

  ASSERT_EQ(256, librbd::write(ictx2, 256, bl.length(), bl.c_str(), 0));

  librados::IoCtx snap_ctx;
  snap_ctx.dup(m_ioctx);
  snap_ctx.snap_set_read(CEPH_SNAPDIR);

  librados::snap_set_t snap_set;
  ASSERT_EQ(0, snap_ctx.list_snaps(ictx2->get_object_name(0), &snap_set));

  std::vector< std::pair<uint64_t,uint64_t> > expected_overlap =
    boost::assign::list_of(
      std::make_pair(0, 256))(
      std::make_pair(512, 2096640));
  ASSERT_EQ(2U, snap_set.clones.size());
  ASSERT_NE(CEPH_NOSNAP, snap_set.clones[0].cloneid);
  ASSERT_EQ(2U, snap_set.clones[0].snaps.size());
  ASSERT_EQ(expected_overlap, snap_set.clones[0].overlap);
  ASSERT_EQ(CEPH_NOSNAP, snap_set.clones[1].cloneid);

  bufferptr read_ptr(256);
  bufferlist read_bl;
  read_bl.push_back(read_ptr);

  std::list<std::string> snaps = boost::assign::list_of(
    "snap1")("snap2")("");
  for (std::list<std::string>::iterator it = snaps.begin();
       it != snaps.end(); ++it) {
    const char *snap_name = it->empty() ? NULL : it->c_str();
    ASSERT_EQ(0, librbd::snap_set(ictx2, snap_name));

    ASSERT_EQ(256, librbd::read(ictx2, 0, 256, read_bl.c_str(), 0));
    ASSERT_TRUE(bl.contents_equal(read_bl));

    ASSERT_EQ(256, librbd::read(ictx2, 256, 256, read_bl.c_str(), 0));
    if (snap_name == NULL) {
      ASSERT_TRUE(bl.contents_equal(read_bl));
    } else {
      ASSERT_TRUE(read_bl.is_zero());
    }

    // verify the object map was properly updated
    if ((ictx2->features & RBD_FEATURE_OBJECT_MAP) != 0) {
      uint8_t state = OBJECT_EXISTS;
      if ((ictx2->features & RBD_FEATURE_FAST_DIFF) != 0 &&
          it != snaps.begin() && snap_name != NULL) {
        state = OBJECT_EXISTS_CLEAN;
      }
      RWLock::WLocker object_map_locker(ictx2->object_map_lock);
      ASSERT_EQ(state, ictx2->object_map[0]);
    }
  }
}

TEST_F(TestInternal, ResizeCopyup)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  m_image_name = get_temp_image_name();
  m_image_size = 1 << 14;

  uint64_t features = 0;
  get_features(&features);
  int order = 12;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, m_image_name.c_str(), m_image_size,
                             features, &order));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  for (size_t i = 0; i < m_image_size; i += bl.length()) {
    ASSERT_EQ(bl.length(), librbd::write(ictx, i, bl.length(), bl.c_str(), 0));
  }

  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::snap_protect(ictx, "snap1"));

  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  ASSERT_EQ(0, librbd::snap_create(ictx2, "snap1"));

  bufferptr read_ptr(bl.length());
  bufferlist read_bl;
  read_bl.push_back(read_ptr);

  // verify full / partial object removal properly copyup
  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, librbd::resize(ictx2, m_image_size - (1 << order) - 32, no_op));
  ASSERT_EQ(0, librbd::snap_set(ictx2, "snap1"));

  {
    // hide the parent from the snapshot
    RWLock::WLocker snap_locker(ictx2->snap_lock);
    ictx2->snap_info.begin()->second.parent = librbd::parent_info();
  }

  for (size_t i = 2 << order; i < m_image_size; i += bl.length()) {
    ASSERT_EQ(bl.length(), librbd::read(ictx2, i, bl.length(), read_bl.c_str(),
                                        0));
    ASSERT_TRUE(bl.contents_equal(read_bl));
  }
}

TEST_F(TestInternal, DiscardCopyup)
{
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  m_image_name = get_temp_image_name();
  m_image_size = 1 << 14;

  uint64_t features = 0;
  get_features(&features);
  int order = 12;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, m_image_name.c_str(), m_image_size,
                             features, &order));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  for (size_t i = 0; i < m_image_size; i += bl.length()) {
    ASSERT_EQ(bl.length(), librbd::write(ictx, i, bl.length(), bl.c_str(), 0));
  }

  ASSERT_EQ(0, librbd::snap_create(ictx, "snap1"));
  ASSERT_EQ(0, librbd::snap_protect(ictx, "snap1"));

  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  ASSERT_EQ(0, librbd::snap_create(ictx2, "snap1"));

  bufferptr read_ptr(bl.length());
  bufferlist read_bl;
  read_bl.push_back(read_ptr);

  ASSERT_EQ(static_cast<int>(m_image_size - 64),
            librbd::discard(ictx2, 32, m_image_size - 64));
  ASSERT_EQ(0, librbd::snap_set(ictx2, "snap1"));

  {
    // hide the parent from the snapshot
    RWLock::WLocker snap_locker(ictx2->snap_lock);
    ictx2->snap_info.begin()->second.parent = librbd::parent_info();
  }

  for (size_t i = 0; i < m_image_size; i += bl.length()) {
    ASSERT_EQ(bl.length(), librbd::read(ictx2, i, bl.length(), read_bl.c_str(),
                                        0));
    ASSERT_TRUE(bl.contents_equal(read_bl));
  }
}

TEST_F(TestInternal, ShrinkFlushesCache) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  {
    RWLock::WLocker owner_locker(ictx->owner_lock);
    ASSERT_EQ(0, ictx->image_watcher->try_lock());
  }

  std::string buffer(4096, '1');
  C_SaferCond cond_ctx;
  librbd::AioCompletion *c =
    librbd::aio_create_completion_internal(&cond_ctx, librbd::rbd_ctx_cb);
  c->get();
  aio_write(ictx, 0, buffer.size(), buffer.c_str(), c, 0);

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, librbd::resize(ictx, m_image_size >> 1, no_op));

  ASSERT_TRUE(c->is_complete());
  ASSERT_EQ(0, c->wait_for_complete());
  ASSERT_EQ(0, cond_ctx.wait());
  c->put();
}
