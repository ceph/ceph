// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2016 Mirantis Inc
 *
 * Author: Mykola Golub <mgolub@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "include/stringify.h"
#include "cls/journal/cls_journal_types.h"
#include "cls/journal/cls_journal_client.h"
#include "cls/rbd/cls_rbd_client.h"
#include "journal/Journaler.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/internal.h"
#include "tools/rbd_mirror/types.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/Threads.h"

#include "test/librados/test.h"
#include "gtest/gtest.h"

using rbd::mirror::RadosRef;

void register_test_rbd_mirror() {
}

#define TEST_IO_SIZE 512
#define TEST_IO_COUNT 11

class TestImageReplayer : public ::testing::Test {
public:
  struct C_WatchCtx : public librados::WatchCtx2 {
    TestImageReplayer *test;
    std::string oid;
    Mutex lock;
    Cond cond;
    bool notified;

    C_WatchCtx(TestImageReplayer *test, const std::string &oid)
      : test(test), oid(oid), lock("C_WatchCtx::lock"), notified(false) {
    }

    virtual void handle_notify(uint64_t notify_id, uint64_t cookie,
                               uint64_t notifier_id, bufferlist& bl_) {
      bufferlist bl;
      test->m_remote_ioctx.notify_ack(oid, notify_id, cookie, bl);

      Mutex::Locker locker(lock);
      notified = true;
      cond.Signal();
    }

    virtual void handle_error(uint64_t cookie, int err) {
      ASSERT_EQ(0, err);
    }
  };

  TestImageReplayer() : m_watch_handle(0)
  {
    EXPECT_EQ("", connect_cluster_pp(m_local_cluster));

    m_local_pool_name = get_temp_pool_name();
    EXPECT_EQ(0, m_local_cluster.pool_create(m_local_pool_name.c_str()));
    EXPECT_EQ(0, m_local_cluster.ioctx_create(m_local_pool_name.c_str(),
					      m_local_ioctx));

    EXPECT_EQ("", connect_cluster_pp(m_remote_cluster));

    m_remote_pool_name = get_temp_pool_name();
    EXPECT_EQ(0, m_remote_cluster.pool_create(m_remote_pool_name.c_str()));
    m_remote_pool_id = m_remote_cluster.pool_lookup(m_remote_pool_name.c_str());
    EXPECT_GE(m_remote_pool_id, 0);

    EXPECT_EQ(0, m_remote_cluster.ioctx_create(m_remote_pool_name.c_str(),
					       m_remote_ioctx));

    m_image_name = get_temp_image_name();
    uint64_t features = g_ceph_context->_conf->rbd_default_features;
    features |= RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING;
    int order = 0;
    EXPECT_EQ(0, librbd::create(m_remote_ioctx, m_image_name.c_str(), 1 << 22,
				false, features, &order, 0, 0));
    m_remote_image_id = get_image_id(m_remote_ioctx, m_image_name);

    m_threads = new rbd::mirror::Threads(reinterpret_cast<CephContext*>(
      m_local_ioctx.cct()));
  }

  ~TestImageReplayer()
  {
    if (m_watch_handle != 0) {
      m_remote_ioctx.unwatch2(m_watch_handle);
      delete m_watch_ctx;
      m_watch_ctx = nullptr;
      m_watch_handle = 0;
    }

    delete m_replayer;
    delete m_threads;

    EXPECT_EQ(0, m_remote_cluster.pool_delete(m_remote_pool_name.c_str()));
    EXPECT_EQ(0, m_local_cluster.pool_delete(m_local_pool_name.c_str()));
  }

  template <typename ImageReplayerT = rbd::mirror::ImageReplayer<> >
  void create_replayer() {
    m_replayer = new ImageReplayerT(m_threads,
      rbd::mirror::RadosRef(new librados::Rados(m_local_ioctx)),
      rbd::mirror::RadosRef(new librados::Rados(m_remote_ioctx)),
      m_local_mirror_uuid, m_remote_mirror_uuid, m_local_ioctx.get_id(),
      m_remote_pool_id, m_remote_image_id, "global image id");
  }

  void start(rbd::mirror::ImageReplayer<>::BootstrapParams *bootstap_params =
	     nullptr)
  {
    C_SaferCond cond;
    m_replayer->start(&cond, bootstap_params);
    ASSERT_EQ(0, cond.wait());

    ASSERT_EQ(0U, m_watch_handle);
    std::string oid = ::journal::Journaler::header_oid(m_remote_image_id);
    m_watch_ctx = new C_WatchCtx(this, oid);
    ASSERT_EQ(0, m_remote_ioctx.watch2(oid, &m_watch_handle, m_watch_ctx));
  }

  void stop()
  {
    if (m_watch_handle != 0) {
      m_remote_ioctx.unwatch2(m_watch_handle);
      delete m_watch_ctx;
      m_watch_ctx = nullptr;
      m_watch_handle = 0;
    }

    C_SaferCond cond;
    m_replayer->stop(&cond);
    ASSERT_EQ(0, cond.wait());
  }

  void bootstrap()
  {
    create_replayer<>();

    rbd::mirror::ImageReplayer<>::BootstrapParams
      bootstap_params(m_image_name);
    start(&bootstap_params);
    wait_for_replay_complete();
    stop();
  }

  std::string  get_temp_image_name()
  {
    return "image" + stringify(++_image_number);
  }

  std::string get_image_id(librados::IoCtx &ioctx, const string &image_name)
  {
    std::string obj = librbd::util::id_obj_name(image_name);
    std::string id;
    EXPECT_EQ(0, librbd::cls_client::get_id(&ioctx, obj, &id));
    return id;
  }

  void open_image(librados::IoCtx &ioctx, const std::string &image_name,
		  bool readonly, librbd::ImageCtx **ictxp)
  {
    librbd::ImageCtx *ictx = new librbd::ImageCtx(image_name.c_str(),
						  "", "", ioctx, readonly);
    EXPECT_EQ(0, ictx->state->open());
    *ictxp = ictx;
  }

  void open_local_image(librbd::ImageCtx **ictxp)
  {
    open_image(m_local_ioctx, m_image_name, true, ictxp);
  }

  void open_remote_image(librbd::ImageCtx **ictxp)
  {
    open_image(m_remote_ioctx, m_image_name, false, ictxp);
  }

  void close_image(librbd::ImageCtx *ictx)
  {
    ictx->state->close();
  }

  void get_commit_positions(cls::journal::ObjectPosition *master_position,
			    cls::journal::ObjectPosition *mirror_position)
  {
    std::string master_client_id = "";
    std::string mirror_client_id = m_local_mirror_uuid;

    C_SaferCond cond;
    uint64_t minimum_set;
    uint64_t active_set;
    std::set<cls::journal::Client> registered_clients;
    std::string oid = ::journal::Journaler::header_oid(m_remote_image_id);
    cls::journal::client::get_mutable_metadata(m_remote_ioctx, oid,
					       &minimum_set, &active_set,
					       &registered_clients, &cond);
    ASSERT_EQ(0, cond.wait());

    *master_position = cls::journal::ObjectPosition();
    *mirror_position = cls::journal::ObjectPosition();

    std::set<cls::journal::Client>::const_iterator c;
    for (c = registered_clients.begin(); c != registered_clients.end(); c++) {
      std::cout << __func__ << ": client: " << *c << std::endl;
      cls::journal::ObjectPositions object_positions =
	c->commit_position.object_positions;
      cls::journal::ObjectPositions::const_iterator p =
	object_positions.begin();
      if (p != object_positions.end()) {
	if (c->id == master_client_id) {
	  ASSERT_EQ(cls::journal::ObjectPosition(), *master_position);
	  *master_position = *p;
	} else if (c->id == mirror_client_id) {
	  ASSERT_EQ(cls::journal::ObjectPosition(), *mirror_position);
	  *mirror_position = *p;
	}
      }
    }
  }

  bool wait_for_watcher_notify(int seconds)
  {
    if (m_watch_handle == 0) {
      return false;
    }

    Mutex::Locker locker(m_watch_ctx->lock);
    while (!m_watch_ctx->notified) {
      if (m_watch_ctx->cond.WaitInterval(g_ceph_context, m_watch_ctx->lock,
					 utime_t(seconds, 0)) != 0) {
        return false;
      }
    }
    m_watch_ctx->notified = false;
    return true;
  }

  void wait_for_replay_complete()
  {
    cls::journal::ObjectPosition master_position;
    cls::journal::ObjectPosition mirror_position;

    for (int i = 0; i < 100; i++) {
      printf("m_replayer->flush()\n");
      C_SaferCond cond;
      m_replayer->flush(&cond);
      ASSERT_EQ(0, cond.wait());
      get_commit_positions(&master_position, &mirror_position);
      if (master_position == mirror_position) {
	break;
      }
      wait_for_watcher_notify(1);
    }

    ASSERT_EQ(master_position, mirror_position);
  }

  void write_test_data(librbd::ImageCtx *ictx, const char *test_data, off_t off,
                       size_t len)
  {
    size_t written;
    written = ictx->aio_work_queue->write(off, len, test_data, 0);
    printf("wrote: %d\n", (int)written);
    ASSERT_EQ(len, written);
  }

  void read_test_data(librbd::ImageCtx *ictx, const char *expected, off_t off,
                      size_t len)
  {
    ssize_t read;
    char *result = (char *)malloc(len + 1);

    ASSERT_NE(static_cast<char *>(NULL), result);
    read = ictx->aio_work_queue->read(off, len, result, 0);
    printf("read: %d\n", (int)read);
    ASSERT_EQ(len, static_cast<size_t>(read));
    result[len] = '\0';
    if (memcmp(result, expected, len)) {
      printf("read: %s\nexpected: %s\n", result, expected);
      ASSERT_EQ(0, memcmp(result, expected, len));
    }
    free(result);
  }

  void generate_test_data() {
    for (int i = 0; i < TEST_IO_SIZE; ++i) {
      m_test_data[i] = (char) (rand() % (126 - 33) + 33);
    }
    m_test_data[TEST_IO_SIZE] = '\0';
  }

  void flush(librbd::ImageCtx *ictx)
  {
    C_SaferCond aio_flush_ctx;
    librbd::AioCompletion *c = librbd::AioCompletion::create(&aio_flush_ctx);
    c->get();
    ictx->aio_work_queue->aio_flush(c);
    ASSERT_EQ(0, c->wait_for_complete());
    c->put();

    C_SaferCond journal_flush_ctx;
    ictx->journal->flush_commit_position(&journal_flush_ctx);
    ASSERT_EQ(0, journal_flush_ctx.wait());

    printf("flushed\n");
  }

  static int _image_number;

  rbd::mirror::Threads *m_threads = nullptr;
  librados::Rados m_local_cluster, m_remote_cluster;
  std::string m_local_mirror_uuid = "local mirror uuid";
  std::string m_remote_mirror_uuid = "remote mirror uuid";
  std::string m_local_pool_name, m_remote_pool_name;
  librados::IoCtx m_local_ioctx, m_remote_ioctx;
  std::string m_image_name;
  int64_t m_remote_pool_id;
  std::string m_remote_image_id;
  rbd::mirror::ImageReplayer<> *m_replayer;
  C_WatchCtx *m_watch_ctx;
  uint64_t m_watch_handle;
  char m_test_data[TEST_IO_SIZE + 1];
};

int TestImageReplayer::_image_number;

TEST_F(TestImageReplayer, Bootstrap)
{
  bootstrap();
}

TEST_F(TestImageReplayer, BootstrapErrorLocalImageExists)
{
  int order = 0;
  EXPECT_EQ(0, librbd::create(m_local_ioctx, m_image_name.c_str(), 1 << 22,
			      false, 0, &order, 0, 0));

  create_replayer<>();
  rbd::mirror::ImageReplayer<>::BootstrapParams
    bootstap_params(m_image_name);
  C_SaferCond cond;
  m_replayer->start(&cond, &bootstap_params);
  ASSERT_EQ(-EEXIST, cond.wait());
}

TEST_F(TestImageReplayer, BootstrapErrorNoJournal)
{
  // disable remote journal journaling
  librbd::ImageCtx *ictx;
  open_remote_image(&ictx);
  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));
  ASSERT_EQ(0, librbd::update_features(ictx, RBD_FEATURE_JOURNALING, false));
  close_image(ictx);

  create_replayer<>();
  rbd::mirror::ImageReplayer<>::BootstrapParams
    bootstap_params(m_image_name);
  C_SaferCond cond;
  m_replayer->start(&cond, &bootstap_params);
  ASSERT_EQ(-ENOENT, cond.wait());
}

TEST_F(TestImageReplayer, StartInterrupted)
{
  create_replayer<>();
  rbd::mirror::ImageReplayer<>::BootstrapParams
    bootstap_params(m_image_name);
  C_SaferCond start_cond, stop_cond;
  m_replayer->start(&start_cond, &bootstap_params);
  m_replayer->stop(&stop_cond);
  int r = start_cond.wait();
  printf("start returned %d\n", r);
  // TODO: improve the test to avoid this race  // TODO: improve the test to avoid this race
  ASSERT_TRUE(r == -EINTR || r == 0);
  ASSERT_EQ(0, stop_cond.wait());
}

TEST_F(TestImageReplayer, ErrorJournalReset)
{
  bootstrap();

  ASSERT_EQ(0, librbd::Journal<>::reset(m_remote_ioctx, m_remote_image_id));

  C_SaferCond cond;
  m_replayer->start(&cond);
  ASSERT_EQ(-EEXIST, cond.wait());
}

TEST_F(TestImageReplayer, ErrorNoJournal)
{
  bootstrap();

  // disable remote journal journaling
  // (reset before disabling, so it does not fail with EBUSY)
  ASSERT_EQ(0, librbd::Journal<>::reset(m_remote_ioctx, m_remote_image_id));
  librbd::ImageCtx *ictx;
  open_remote_image(&ictx);
  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));
  ASSERT_EQ(0, librbd::update_features(ictx, RBD_FEATURE_JOURNALING, false));
  close_image(ictx);

  rbd::mirror::ImageReplayer<>::BootstrapParams
    bootstap_params(m_image_name);
  C_SaferCond cond;
  m_replayer->start(&cond, &bootstap_params);
  ASSERT_EQ(-ENOENT, cond.wait());
}

TEST_F(TestImageReplayer, StartStop)
{
  bootstrap();

  start();
  wait_for_replay_complete();
  stop();
}

TEST_F(TestImageReplayer, WriteAndStartReplay)
{
  bootstrap();

  // Write to remote image and start replay

  librbd::ImageCtx *ictx;

  generate_test_data();
  open_remote_image(&ictx);
  for (int i = 0; i < TEST_IO_COUNT; ++i) {
    write_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }
  flush(ictx);
  close_image(ictx);

  start();
  wait_for_replay_complete();
  stop();

  open_local_image(&ictx);
  for (int i = 0; i < TEST_IO_COUNT; ++i) {
    read_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }
  close_image(ictx);
}

TEST_F(TestImageReplayer, StartReplayAndWrite)
{
  bootstrap();

  // Start replay and write to remote image

  librbd::ImageCtx *ictx;

  start();

  generate_test_data();
  open_remote_image(&ictx);
  for (int i = 0; i < TEST_IO_COUNT; ++i) {
    write_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }
  flush(ictx);

  wait_for_replay_complete();

  for (int i = TEST_IO_COUNT; i < 2 * TEST_IO_COUNT; ++i) {
    write_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }
  flush(ictx);
  close_image(ictx);

  wait_for_replay_complete();

  open_local_image(&ictx);
  for (int i = 0; i < 2 * TEST_IO_COUNT; ++i) {
    read_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }
  close_image(ictx);

  stop();
}

TEST_F(TestImageReplayer, NextTag)
{
  bootstrap();

  // write, reopen, and write again to test switch to the next tag

  librbd::ImageCtx *ictx;

  start();

  generate_test_data();

  const int N = 10;

  for (int j = 0; j < N; j++) {
    open_remote_image(&ictx);
    for (int i = j * TEST_IO_COUNT; i < (j + 1) * TEST_IO_COUNT; ++i) {
      write_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
    }
    close_image(ictx);
  }

  wait_for_replay_complete();

  open_local_image(&ictx);
  for (int i = 0; i < N * TEST_IO_COUNT; ++i) {
    read_test_data(ictx, m_test_data, TEST_IO_SIZE * i, TEST_IO_SIZE);
  }
  close_image(ictx);

  stop();
}

class ImageReplayer : public rbd::mirror::ImageReplayer<> {
public:
  ImageReplayer(rbd::mirror::Threads *threads,
		rbd::mirror::RadosRef local, rbd::mirror::RadosRef remote,
		const std::string &local_mirror_uuid,
                const std::string &remote_mirror_uuid,
                int64_t local_pool_id,
		int64_t remote_pool_id,	const std::string &remote_image_id,
                const std::string &global_image_id)
    : rbd::mirror::ImageReplayer<>(threads, local, remote, local_mirror_uuid,
				   remote_mirror_uuid, local_pool_id,
                                   remote_pool_id, remote_image_id,
                                   global_image_id)
    {}

  void set_error(const std::string &state, int r) {
    m_errors[state] = r;
  }

  int get_error(const std::string &state) const {
    std::map<std::string, int>::const_iterator i = m_errors.find(state);
    return i == m_errors.end() ? 0 : i->second;
  }

protected:
  virtual void on_stop_journal_replay_shut_down_finish(int r) {
    ASSERT_EQ(0, r);
    rbd::mirror::ImageReplayer<>::on_stop_journal_replay_shut_down_finish(
      get_error("on_stop_journal_replay_shut_down"));
  }

  virtual void on_stop_local_image_close_finish(int r) {
    ASSERT_EQ(0, r);
    rbd::mirror::ImageReplayer<>::on_stop_local_image_close_finish(
      get_error("on_stop_local_image_close"));
  }

private:
  std::map<std::string, int> m_errors;
};

#define TEST_ON_START_ERROR(state) \
TEST_F(TestImageReplayer, Error_on_start_##state)			\
{									\
  create_replayer<ImageReplayer>();					\
  reinterpret_cast<ImageReplayer *>(m_replayer)->			\
    set_error("on_start_" #state, -1);					\
  rbd::mirror::ImageReplayer<>::BootstrapParams				\
    bootstap_params(m_image_name);			                \
  C_SaferCond cond;							\
  m_replayer->start(&cond, &bootstap_params);				\
  ASSERT_EQ(-1, cond.wait());						\
}

#define TEST_ON_STOP_ERROR(state) \
TEST_F(TestImageReplayer, Error_on_stop_##state)			\
{									\
  create_replayer<ImageReplayer>();					\
  reinterpret_cast<ImageReplayer *>(m_replayer)->			\
    set_error("on_stop_" #state, -1);					\
  rbd::mirror::ImageReplayer<>::BootstrapParams				\
    bootstap_params(m_image_name);			                \
  start(&bootstap_params);						\
  /* TODO: investigate: without wait below I observe: */		\
  /* librbd/journal/Replay.cc: 70: FAILED assert(m_op_events.empty()) */\
  wait_for_replay_complete();						\
  C_SaferCond cond;							\
  m_replayer->stop(&cond);						\
  ASSERT_EQ(0, cond.wait());						\
}

TEST_ON_STOP_ERROR(journal_replay_shut_down);
TEST_ON_STOP_ERROR(no_error);

