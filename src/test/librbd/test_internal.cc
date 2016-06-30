// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequest.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
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
	EXPECT_EQ(0, ictx->operations->snap_unprotect(iter->first.c_str()));
      }
      EXPECT_EQ(0, ictx->operations->snap_remove(iter->first.c_str()));
    }

    TestFixture::TearDown();
  }

  int create_snapshot(const char *snap_name, bool snap_protect) {
    librbd::ImageCtx *ictx;
    int r = open_image(m_image_name, &ictx);
    if (r < 0) {
      return r;
    }

    r = snap_create(*ictx, snap_name);
    if (r < 0) {
      return r;
    }

    m_snaps.push_back(std::make_pair(snap_name, snap_protect));
    if (snap_protect) {
      r = ictx->operations->snap_protect(snap_name);
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

TEST_F(TestInternal, OpenByID) {
   REQUIRE_FORMAT_V2();

   librbd::ImageCtx *ictx;
   ASSERT_EQ(0, open_image(m_image_name, &ictx));
   std::string id = ictx->id;
   close_image(ictx);

   ictx = new librbd::ImageCtx("", id, nullptr, m_ioctx, true);
   ASSERT_EQ(0, ictx->state->open());
   ASSERT_EQ(ictx->name, m_image_name);
   close_image(ictx);
}

TEST_F(TestInternal, IsExclusiveLockOwner) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  bool is_owner;
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_FALSE(is_owner);

  C_SaferCond ctx;
  {
    RWLock::WLocker l(ictx->owner_lock);
    ictx->exclusive_lock->try_lock(&ctx);
  }
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, librbd::is_exclusive_lock_owner(ictx, &is_owner));
  ASSERT_TRUE(is_owner);
}

TEST_F(TestInternal, ResizeLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx->operations->resize(m_image_size >> 1, no_op));

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
  ASSERT_EQ(-EROFS, ictx->operations->resize(m_image_size >> 1, no_op));
}

TEST_F(TestInternal, SnapCreateLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  BOOST_SCOPE_EXIT( (ictx) ) {
    ASSERT_EQ(0, ictx->operations->snap_remove("snap1"));
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

  ASSERT_EQ(-EROFS, snap_create(*ictx, "snap1"));
}

TEST_F(TestInternal, SnapRollbackLocksImage) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  ASSERT_EQ(0, create_snapshot("snap1", false));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx->operations->snap_rollback("snap1", no_op));

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
  ASSERT_EQ(-EROFS, ictx->operations->snap_rollback("snap1", no_op));
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
  ASSERT_EQ(0, ictx2->operations->flatten(no_op));

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
  ASSERT_EQ(-EROFS, ictx2->operations->flatten(no_op));
}

TEST_F(TestInternal, AioWriteRequestsLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, lock_image(*ictx, LOCK_EXCLUSIVE, "manually locked"));

  std::string buffer(256, '1');
  Context *ctx = new DummyContext();
  librbd::AioCompletion *c = librbd::AioCompletion::create(ctx);
  c->get();
  ictx->aio_work_queue->aio_write(c, 0, buffer.size(), buffer.c_str(), 0);

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

  Context *ctx = new DummyContext();
  librbd::AioCompletion *c = librbd::AioCompletion::create(ctx);
  c->get();
  ictx->aio_work_queue->aio_discard(c, 0, 256);

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

  C_SaferCond ctx;
  {
    RWLock::WLocker l(ictx->owner_lock);
    ictx->exclusive_lock->try_lock(&ctx);
  }

  ASSERT_EQ(0, ctx.wait());
  {
    RWLock::RLocker owner_locker(ictx->owner_lock);
    ASSERT_TRUE(ictx->exclusive_lock->is_lock_owner());
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
      ictx->operations->execute_resize(size, prog_ctx, &ctx, 0);
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

  if (ictx->exclusive_lock != nullptr) {
    C_SaferCond ctx;
    {
      RWLock::WLocker l(ictx->owner_lock);
      ictx->exclusive_lock->try_lock(&ctx);
    }

    RWLock::RLocker owner_locker(ictx->owner_lock);
    ASSERT_EQ(0, ctx.wait());
    ASSERT_TRUE(ictx->exclusive_lock->is_lock_owner());
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
    ictx->operations->execute_resize(new_size, prog_ctx, contexts.back(), 0);
  }

  for (uint32_t i = 0; i < contexts.size(); ++i) {
    ASSERT_EQ(0, contexts[i]->wait());
    delete contexts[i];
  }

  ASSERT_EQ(0, librbd::get_size(ictx, &size));
  ASSERT_EQ(0U, size);
}

TEST_F(TestInternal, Metadata) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  map<string, bool> test_confs = boost::assign::map_list_of(
    "aaaaaaa", false)(
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", false)(
    "cccccccccccccc", false);
  map<string, bool>::iterator it = test_confs.begin();
  int r;
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  r = librbd::metadata_set(ictx, it->first, "value1");
  ASSERT_EQ(0, r);
  ++it;
  r = librbd::metadata_set(ictx, it->first, "value2");
  ASSERT_EQ(0, r);
  ++it;
  r = librbd::metadata_set(ictx, it->first, "value3");
  ASSERT_EQ(0, r);
  r = librbd::metadata_set(ictx, "abcd", "value4");
  ASSERT_EQ(0, r);
  r = librbd::metadata_set(ictx, "xyz", "value5");
  ASSERT_EQ(0, r);
  map<string, bufferlist> pairs;
  r = librbd::metadata_list(ictx, "", 0, &pairs);
  ASSERT_EQ(0, r);
  ASSERT_EQ(5u, pairs.size());
  r = librbd::metadata_remove(ictx, "abcd");
  ASSERT_EQ(0, r);
  r = librbd::metadata_remove(ictx, "xyz");
  ASSERT_EQ(0, r);
  pairs.clear();
  r = librbd::metadata_list(ictx, "", 0, &pairs);
  ASSERT_EQ(0, r);
  ASSERT_EQ(3u, pairs.size());
  string val;
  r = librbd::metadata_get(ictx, it->first, &val);
  ASSERT_EQ(0, r);
  ASSERT_STREQ(val.c_str(), "value3");
}

TEST_F(TestInternal, MetadataFilter) {
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
  pairs["abc"].append("value");
  pairs["abcabc"].append("value");
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
  ASSERT_EQ(256, ictx->aio_work_queue->write(0, bl.length(), bl.c_str(), 0));

  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->operations->snap_protect("snap1"));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  std::string clone_name = get_temp_image_name();
  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  ASSERT_EQ(0, snap_create(*ictx2, "snap1"));
  ASSERT_EQ(0, snap_create(*ictx2, "snap2"));

  ASSERT_EQ(256, ictx2->aio_work_queue->write(256, bl.length(), bl.c_str(), 0));

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

  std::list<std::string> snaps = {"snap1", "snap2", ""};
  for (std::list<std::string>::iterator it = snaps.begin();
       it != snaps.end(); ++it) {
    const char *snap_name = it->empty() ? NULL : it->c_str();
    ASSERT_EQ(0, librbd::snap_set(ictx2, snap_name));

    ASSERT_EQ(256, ictx2->aio_work_queue->read(0, 256, read_bl.c_str(), 0));
    ASSERT_TRUE(bl.contents_equal(read_bl));

    ASSERT_EQ(256, ictx2->aio_work_queue->read(256, 256, read_bl.c_str(), 0));
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

      librbd::ObjectMap object_map(*ictx2, ictx2->snap_id);
      C_SaferCond ctx;
      object_map.open(&ctx);
      ASSERT_EQ(0, ctx.wait());

      RWLock::WLocker object_map_locker(ictx2->object_map_lock);
      ASSERT_EQ(state, object_map[0]);
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
    ASSERT_EQ(bl.length(), ictx->aio_work_queue->write(i, bl.length(),
                                                       bl.c_str(), 0));
  }

  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->operations->snap_protect("snap1"));

  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));
  ASSERT_EQ(0, snap_create(*ictx2, "snap1"));

  bufferptr read_ptr(bl.length());
  bufferlist read_bl;
  read_bl.push_back(read_ptr);

  // verify full / partial object removal properly copyup
  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx2->operations->resize(m_image_size - (1 << order) - 32,
                                         no_op));
  ASSERT_EQ(0, ictx2->operations->resize(m_image_size - (2 << order) - 32,
                                         no_op));
  ASSERT_EQ(0, librbd::snap_set(ictx2, "snap1"));

  {
    // hide the parent from the snapshot
    RWLock::WLocker snap_locker(ictx2->snap_lock);
    ictx2->snap_info.begin()->second.parent = librbd::parent_info();
  }

  for (size_t i = 2 << order; i < m_image_size; i += bl.length()) {
    ASSERT_EQ(bl.length(), ictx2->aio_work_queue->read(i, bl.length(),
                                                       read_bl.c_str(), 0));
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
    ASSERT_EQ(bl.length(), ictx->aio_work_queue->write(i, bl.length(),
                                                       bl.c_str(), 0));
  }

  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->operations->snap_protect("snap1"));

  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
			     clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx2;
  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  ASSERT_EQ(0, snap_create(*ictx2, "snap1"));

  bufferptr read_ptr(bl.length());
  bufferlist read_bl;
  read_bl.push_back(read_ptr);

  ASSERT_EQ(static_cast<int>(m_image_size - 64),
            ictx2->aio_work_queue->discard(32, m_image_size - 64));
  ASSERT_EQ(0, librbd::snap_set(ictx2, "snap1"));

  {
    // hide the parent from the snapshot
    RWLock::WLocker snap_locker(ictx2->snap_lock);
    ictx2->snap_info.begin()->second.parent = librbd::parent_info();
  }

  for (size_t i = 0; i < m_image_size; i += bl.length()) {
    ASSERT_EQ(bl.length(), ictx2->aio_work_queue->read(i, bl.length(),
                                                       read_bl.c_str(), 0));
    ASSERT_TRUE(bl.contents_equal(read_bl));
  }
}

TEST_F(TestInternal, ShrinkFlushesCache) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  std::string buffer(4096, '1');

  // ensure write-path is initialized
  ictx->aio_work_queue->write(0, buffer.size(), buffer.c_str(), 0);

  C_SaferCond cond_ctx;
  librbd::AioCompletion *c = librbd::AioCompletion::create(&cond_ctx);
  c->get();
  ictx->aio_work_queue->aio_write(c, 0, buffer.size(), buffer.c_str(), 0);

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx->operations->resize(m_image_size >> 1, no_op));

  ASSERT_TRUE(c->is_complete());
  ASSERT_EQ(0, c->wait_for_complete());
  ASSERT_EQ(0, cond_ctx.wait());
  c->put();
}

TEST_F(TestInternal, ImageOptions) {
  rbd_image_options_t opts1 = NULL, opts2 = NULL;
  uint64_t uint64_val1 = 10, uint64_val2 = 0;
  std::string string_val1;

  librbd::image_options_create(&opts1);
  ASSERT_NE((rbd_image_options_t)NULL, opts1);
  ASSERT_TRUE(librbd::image_options_is_empty(opts1));

  ASSERT_EQ(-EINVAL, librbd::image_options_get(opts1, RBD_IMAGE_OPTION_FEATURES,
	  &string_val1));
  ASSERT_EQ(-ENOENT, librbd::image_options_get(opts1, RBD_IMAGE_OPTION_FEATURES,
	  &uint64_val1));

  ASSERT_EQ(-EINVAL, librbd::image_options_set(opts1, RBD_IMAGE_OPTION_FEATURES,
	  string_val1));

  ASSERT_EQ(0, librbd::image_options_set(opts1, RBD_IMAGE_OPTION_FEATURES,
	  uint64_val1));
  ASSERT_FALSE(librbd::image_options_is_empty(opts1));
  ASSERT_EQ(0, librbd::image_options_get(opts1, RBD_IMAGE_OPTION_FEATURES,
	  &uint64_val2));
  ASSERT_EQ(uint64_val1, uint64_val2);

  librbd::image_options_create_ref(&opts2, opts1);
  ASSERT_NE((rbd_image_options_t)NULL, opts2);
  ASSERT_FALSE(librbd::image_options_is_empty(opts2));

  uint64_val2 = 0;
  ASSERT_NE(uint64_val1, uint64_val2);
  ASSERT_EQ(0, librbd::image_options_get(opts2, RBD_IMAGE_OPTION_FEATURES,
	  &uint64_val2));
  ASSERT_EQ(uint64_val1, uint64_val2);

  uint64_val2++;
  ASSERT_NE(uint64_val1, uint64_val2);
  ASSERT_EQ(-ENOENT, librbd::image_options_get(opts1, RBD_IMAGE_OPTION_ORDER,
	  &uint64_val1));
  ASSERT_EQ(-ENOENT, librbd::image_options_get(opts2, RBD_IMAGE_OPTION_ORDER,
	  &uint64_val2));
  ASSERT_EQ(0, librbd::image_options_set(opts2, RBD_IMAGE_OPTION_ORDER,
	  uint64_val2));
  ASSERT_EQ(0, librbd::image_options_get(opts1, RBD_IMAGE_OPTION_ORDER,
	  &uint64_val1));
  ASSERT_EQ(0, librbd::image_options_get(opts2, RBD_IMAGE_OPTION_ORDER,
	  &uint64_val2));
  ASSERT_EQ(uint64_val1, uint64_val2);

  librbd::image_options_destroy(opts1);

  uint64_val2++;
  ASSERT_NE(uint64_val1, uint64_val2);
  ASSERT_EQ(0, librbd::image_options_get(opts2, RBD_IMAGE_OPTION_ORDER,
	  &uint64_val2));
  ASSERT_EQ(uint64_val1, uint64_val2);

  ASSERT_EQ(0, librbd::image_options_unset(opts2, RBD_IMAGE_OPTION_ORDER));
  ASSERT_EQ(-ENOENT, librbd::image_options_unset(opts2, RBD_IMAGE_OPTION_ORDER));

  librbd::image_options_clear(opts2);
  ASSERT_EQ(-ENOENT, librbd::image_options_get(opts2, RBD_IMAGE_OPTION_FEATURES,
	  &uint64_val2));
  ASSERT_TRUE(librbd::image_options_is_empty(opts2));

  librbd::image_options_destroy(opts2);
}

TEST_F(TestInternal, WriteFullCopyup) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(0, ictx->operations->resize(1 << ictx->order, no_op));

  bufferlist bl;
  bl.append(std::string(1 << ictx->order, '1'));
  ASSERT_EQ(bl.length(),
            ictx->aio_work_queue->write(0, bl.length(), bl.c_str(), 0));
  ASSERT_EQ(0, librbd::flush(ictx));

  ASSERT_EQ(0, create_snapshot("snap1", true));

  std::string clone_name = get_temp_image_name();
  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), ictx->features, &order, 0, 0));

  TestInternal *parent = this;
  librbd::ImageCtx *ictx2 = NULL;
  BOOST_SCOPE_EXIT( (&m_ioctx) (clone_name) (parent) (&ictx2) ) {
    if (ictx2 != NULL) {
      ictx2->operations->snap_remove("snap1");
      parent->close_image(ictx2);
    }

    librbd::NoOpProgressContext remove_no_op;
    ASSERT_EQ(0, librbd::remove(m_ioctx, clone_name.c_str(), remove_no_op));
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, open_image(clone_name, &ictx2));
  ASSERT_EQ(0, ictx2->operations->snap_create("snap1"));

  bufferlist write_full_bl;
  write_full_bl.append(std::string(1 << ictx2->order, '2'));
  ASSERT_EQ(write_full_bl.length(),
            ictx2->aio_work_queue->write(0, write_full_bl.length(),
            write_full_bl.c_str(), 0));

  ASSERT_EQ(0, ictx2->operations->flatten(no_op));

  bufferptr read_ptr(bl.length());
  bufferlist read_bl;
  read_bl.push_back(read_ptr);

  ASSERT_EQ(read_bl.length(), ictx2->aio_work_queue->read(0, read_bl.length(),
                                                          read_bl.c_str(), 0));
  ASSERT_TRUE(write_full_bl.contents_equal(read_bl));

  ASSERT_EQ(0, librbd::snap_set(ictx2, "snap1"));
  ASSERT_EQ(read_bl.length(), ictx2->aio_work_queue->read(0, read_bl.length(),
                                                          read_bl.c_str(), 0));
  ASSERT_TRUE(bl.contents_equal(read_bl));
}
