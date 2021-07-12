// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 LINE
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include "mds/MDSDmclockScheduler.h"
#include "gtest/gtest.h"

#define dout_subsys ceph_subsys_mds
#define dout_context g_ceph_context

MDSRank *mds= nullptr;

TEST(MDSDmclockScheduler, SchedulerConstructor)
{
  MDSDmclockScheduler *scheduler = new MDSDmclockScheduler(mds);
  delete scheduler;

  scheduler = new MDSDmclockScheduler(mds);
  scheduler->enable_qos_feature();
  scheduler->disable_qos_feature();
  delete scheduler;

  scheduler = new MDSDmclockScheduler(mds);
  scheduler->enable_qos_feature();
  delete scheduler;

  scheduler = new MDSDmclockScheduler(mds);
  scheduler->enable_qos_feature();
  scheduler->disable_qos_feature();
  scheduler->disable_qos_feature();
  delete scheduler;
}

TEST(MDSDmclockScheduler, CephConfDmclockCheck)
{
  g_ceph_context->_conf.set_val("debug mds", "0/20");

  g_ceph_context->_conf.set_val("mds_dmclock_reservation", "10000");
  g_ceph_context->_conf.set_val("mds_dmclock_weight", "10000");
  g_ceph_context->_conf.set_val("mds_dmclock_limit", "10000");
  g_ceph_context->_conf.set_val("mds_dmclock_enable", "true");

  MDSDmclockScheduler *scheduler = new MDSDmclockScheduler(mds);

  ASSERT_EQ(scheduler->get_default_conf().is_enabled(), true);
  ASSERT_EQ(scheduler->get_default_conf().get_reservation(), 10000);
  ASSERT_EQ(scheduler->get_default_conf().get_weight(), 10000);
  ASSERT_EQ(scheduler->get_default_conf().get_limit(), 10000);

  delete scheduler;

  g_ceph_context->_conf.set_val("mds_dmclock_enable", "false");

  scheduler = new MDSDmclockScheduler(mds);

  ASSERT_FALSE(scheduler->get_default_conf().is_enabled());

  delete scheduler;
}

TEST(MDSDmclockScheduler, BasicTest)
{
  MDSDmclockScheduler *scheduler = new MDSDmclockScheduler(mds);

  scheduler->enable_qos_feature();
  ASSERT_TRUE(scheduler->get_default_conf().is_enabled());

  SessionId sid = "10024";
  VolumeId vid = "/volumes/_nogroup/subvol";
  VolumeInfo vi;
  {
    ClientInfo client_info(10.0, 20.0, 30.0);
    bool use_default = false;

    scheduler->create_volume_info(vid, client_info, use_default);
    scheduler->add_session_to_volume_info(vid, sid);

    ASSERT_TRUE(scheduler->copy_volume_info(vid, vi));
    ASSERT_EQ(vi.get_reservation(), client_info.reservation);
    ASSERT_EQ(vi.get_weight(), client_info.weight);
    ASSERT_EQ(vi.get_limit(), client_info.limit);
    ASSERT_EQ(vi.is_use_default(), use_default);
  }

  {
    ClientInfo client_info(100.0, 200.0, 300.0);
    bool use_default = true;

    scheduler->update_volume_info(vid, client_info, use_default);

    ASSERT_TRUE(scheduler->copy_volume_info(vid, vi));
    ASSERT_EQ(vi.get_reservation(), client_info.reservation);
    ASSERT_EQ(vi.get_weight(), client_info.weight);
    ASSERT_EQ(vi.get_limit(), client_info.limit);
    ASSERT_EQ(vi.is_use_default(), use_default);
  }

  {
    ClientInfo client_info(100.0, 200.0, 300.0);
    bool use_default = false;
    scheduler->update_volume_info(vid, client_info, use_default);
    ASSERT_TRUE(scheduler->copy_volume_info(vid, vi));
    ASSERT_FALSE(vi.is_use_default());
  }

  {
    vi.set_reservation(1000.0);
    ASSERT_EQ(vi.get_reservation(), 1000.0);
    vi.set_weight(2000.0);
    ASSERT_EQ(vi.get_weight(), 2000.0);
    vi.set_limit(3000.0);
    ASSERT_EQ( vi.get_limit(), 3000.0);
    vi.set_use_default(true);
    ASSERT_TRUE(vi.is_use_default());
  }

  scheduler->set_default_volume_info(vid);
  ASSERT_TRUE(scheduler->copy_volume_info(vid, vi));
  ASSERT_TRUE(vi.is_use_default());

  scheduler->delete_session_from_volume_info(vid, sid);
  ASSERT_FALSE(scheduler->copy_volume_info(vid, vi));

  scheduler->disable_qos_feature();
  ASSERT_FALSE(scheduler->get_default_conf().is_enabled());

  delete scheduler;
}

TEST(MDSDmclockScheduler, VolumeSessionInfo)
{
  MDSDmclockScheduler *scheduler = new MDSDmclockScheduler(mds);
  scheduler->enable_qos_feature();

  SessionId sid = "10024";
  VolumeId vid = "/volumes/_nogroup/subvol";
  VolumeInfo vi;

  ClientInfo client_info(10.0, 20.0, 30.0);
  bool use_default = false;

  scheduler->create_volume_info(vid, client_info, use_default);
  scheduler->add_session_to_volume_info(vid, sid);
  scheduler->add_session_to_volume_info(vid, sid);

  ASSERT_TRUE(scheduler->copy_volume_info(vid, vi));
  ASSERT_EQ(vi.get_session_cnt(), 1);

  scheduler->delete_session_from_volume_info(vid, sid);
  scheduler->delete_session_from_volume_info(vid, sid);
  ASSERT_FALSE(scheduler->copy_volume_info(vid, vi));

  scheduler->create_volume_info(vid, client_info, use_default);
  scheduler->add_session_to_volume_info(vid, "1020");
  scheduler->add_session_to_volume_info(vid, "1020");
  scheduler->add_session_to_volume_info(vid, "1020");
  scheduler->create_volume_info(vid, client_info, use_default);
  scheduler->add_session_to_volume_info(vid, "3021");
  scheduler->add_session_to_volume_info(vid, "3021");
  scheduler->create_volume_info(vid, client_info, use_default);
  scheduler->add_session_to_volume_info(vid, "9028");
  ASSERT_TRUE(scheduler->copy_volume_info(vid, vi));
  ASSERT_EQ(vi.get_session_cnt(), 3);

  scheduler->delete_session_from_volume_info(vid, "9028");
  scheduler->delete_session_from_volume_info(vid, "1020");
  ASSERT_TRUE(scheduler->copy_volume_info(vid, vi));
  ASSERT_EQ(vi.get_session_cnt(), 1);

  scheduler->delete_session_from_volume_info(vid, "3021");
  ASSERT_FALSE(scheduler->copy_volume_info(vid, vi));

  delete scheduler;
}

Session *make_session(VolumeId vid, int sid)
{
  Session *session = new Session(nullptr);
  client_metadata_t meta;
  meta.kv_map = {{"root", vid}};
  session->set_client_metadata(meta);
  session->info.inst.name._num = sid;
  return session;
}

void put_session(Session *session)
{
  session->put();
}

TEST(MDSDmclockScheduler, SessionSanity)
{
  MDSDmclockScheduler *scheduler = new MDSDmclockScheduler(mds);
  scheduler->enable_qos_feature();

  /* nullptr session sanity check */
  scheduler->add_session(nullptr);
  ASSERT_EQ(scheduler->get_volume_info_map().size(), 0);

  VolumeId vid = "/";
  VolumeInfo vi;
  scheduler->create_volume_info(vid, ClientInfo(100.0, 300.0, 400.0), false);
  Session *session = make_session(vid, 10000);

  scheduler->add_session(session);
  ASSERT_EQ(scheduler->get_volume_info_map().size(), 1);
  ASSERT_TRUE(scheduler->copy_volume_info(vid, vi));
  ASSERT_EQ(vi.get_session_cnt(), 1);

  scheduler->remove_session(session);
  ASSERT_EQ(scheduler->get_volume_info_map().size(), 0);
  ASSERT_FALSE(scheduler->copy_volume_info(vid, vi));

  /* duplicate */
  scheduler->remove_session(session);
  put_session(session);

  VolumeId mount_root_path = "/volumes/_nogroup/4c55ad20-9c44-4a5e-9233-8ac64340b98c/subdir";
  VolumeId subvol_root_path = scheduler->convert_subvol_root(mount_root_path);
  scheduler->create_volume_info(subvol_root_path, ClientInfo(100.0, 300.0, 400.0), false);
  session = make_session(mount_root_path, 10000);

  scheduler->add_session(session);
  ASSERT_EQ(scheduler->get_volume_info_map().size(), 1);
  ASSERT_TRUE(scheduler->copy_volume_info(subvol_root_path, vi));
  ASSERT_EQ(vi.get_session_cnt(), 1);

  scheduler->remove_session(session);
  ASSERT_EQ(scheduler->get_volume_info_map().size(), 0);
  ASSERT_FALSE(scheduler->copy_volume_info(vid, vi));
  put_session(session);

  #define SESSION_NUM 10
  Session *session_a[SESSION_NUM];

  vid = "/volumes/_nogroup/4c55ad20-9c44-4a5e-9233-8ac64340b98c";
  scheduler->create_volume_info(vid, ClientInfo(100.0, 300.0, 400.0), false);

  for (int i = 0; i < SESSION_NUM; i++) {
    session_a[i] = make_session(vid, 10000 + i);
    scheduler->add_session(session_a[i]);
  }
  ASSERT_EQ(scheduler->get_volume_info_map().size(), 1);
  ASSERT_TRUE(scheduler->copy_volume_info(vid, vi)==true);
  ASSERT_EQ(vi.get_session_cnt(), SESSION_NUM);

  for (int i = 0; i < SESSION_NUM; i++) {
    ASSERT_TRUE(scheduler->copy_volume_info(vid, vi));
    ASSERT_EQ(vi.get_session_cnt(), (SESSION_NUM - i));
    ASSERT_EQ(scheduler->get_volume_info_map().size(), 1);
    scheduler->remove_session(session_a[i]);
  }

  ASSERT_FALSE(scheduler->copy_volume_info(vid, vi));
  ASSERT_EQ(scheduler->get_volume_info_map().size(), 0);

  for (int i = 0; i < SESSION_NUM; i++) {
    put_session(session_a[i]);
  }

  #define VOLUME_NUM 10
  VolumeId vid_a[VOLUME_NUM];
  VolumeInfo vi_a[VOLUME_NUM];
  Session *session_b[VOLUME_NUM][SESSION_NUM];

  for (int i = 0; i < VOLUME_NUM; i++) {
    vid_a[i]  = "/volumes/_nogroup/4c55ad20-9c44-4a5e-9233-8ac64340b98" + i;
    scheduler->create_volume_info(vid_a[i], ClientInfo(100.0, 300.0, 400.0), false);

    for (int j = 0; j < SESSION_NUM; j++) {
      session_b[i][j] = make_session(vid_a[i], 10000 + j);
      scheduler->add_session(session_b[i][j]);
    }
    ASSERT_TRUE(scheduler->copy_volume_info(vid_a[i], vi_a[i]));
    ASSERT_EQ(vi_a[i].get_session_cnt(), SESSION_NUM);
  }
  ASSERT_EQ(scheduler->get_volume_info_map().size(), VOLUME_NUM);

  for (int i = 0; i < VOLUME_NUM; i++) {
    for (int j = 0; j < SESSION_NUM; j++) {
      scheduler->remove_session(session_b[i][j]);
    }
    ASSERT_FALSE(scheduler->copy_volume_info(vid_a[i], vi_a[i]));
  }
  ASSERT_EQ(scheduler->get_volume_info_map().size(), 0);

  /* nagative test: delete again */
  for (int i = 0; i < VOLUME_NUM; i++) {
    for (int j = 0; j < SESSION_NUM; j++) {
      scheduler->remove_session(session_b[i][j]);
    }
  }
  ASSERT_EQ(scheduler->get_volume_info_map().size(), 0);

  for (int i = 0; i < VOLUME_NUM; i++) {
    for (int j = 0; j < SESSION_NUM; j++) {
      put_session(session_b[i][j]);
    }
  }

  delete scheduler;
}

std::atomic_int request_count = 0;
std::atomic_int complete_count = 0;

Queue::ClientInfoFunc client_info_f;
Queue::CanHandleRequestFunc can_handle_f;
Queue::HandleRequestFunc handle_request_f = [] /* &complete_count, &request_count*/
(const VolumeId& id, std::unique_ptr<ClientRequest>&& request, const PhaseType& phase_type, const uint64_t cost)
{
  complete_count++;
};

MDSDmclockScheduler *create_dmclock_scheduler()
{
  MDSDmclockScheduler *scheduler = new MDSDmclockScheduler(mds, client_info_f, can_handle_f, handle_request_f);
  request_count = 0;
  complete_count = 0;
  return scheduler;
}

void cleanup_dmclock_scheduler(MDSDmclockScheduler *scheduler)
{
  delete scheduler;
}

TEST(MDSDmclockScheduler, IssueClientRequest)
{
  MDSDmclockScheduler *scheduler = create_dmclock_scheduler();
  scheduler->enable_qos_feature();

  SessionId sid = "323400";
  VolumeId vid = "/";
  ClientInfo client_info(10.0, 20.0, 30.0);
  bool use_default = false;
  scheduler->create_volume_info(vid, client_info, use_default);
  scheduler->add_session_to_volume_info(vid, sid);

  for (int i = 0; i < 20; i++) {
    request_count++;
    MDSReqRef req;
    scheduler->enqueue_client_request<MDSReqRef>(req, vid);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  for (int i = 0; i < 20; i++) {
    request_count++;
    MDSReqRef req;
    scheduler->enqueue_client_request<MDSReqRef>(req, vid);

    if (i / 5 % 2 == 0) {
      scheduler->enable_qos_feature();
    } else {
      scheduler->disable_qos_feature();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  for (int i = 0; i < 100; i++) {
    request_count++;
    MDSReqRef req;
    scheduler->enqueue_client_request<MDSReqRef>(req, vid);
  }

  scheduler->disable_qos_feature();
  cleanup_dmclock_scheduler(scheduler);

  ASSERT_EQ(request_count, complete_count);
}

TEST(MDSDmclockScheduler, CancelClientRequest)
{
  MDSDmclockScheduler *scheduler = create_dmclock_scheduler();
  scheduler->enable_qos_feature();

  SessionId sid = "23423";
  VolumeId vid = "/volumes/_nogroup/subvol";
  bool use_default = false;
  ClientInfo client_info(10.0, 20.0, 30.0);
  scheduler->create_volume_info(vid, client_info, use_default);
  scheduler->add_session_to_volume_info(vid, sid);

  for (int i = 0; i < 1000; i++) {
    request_count++;
    MDSReqRef req;
    scheduler->enqueue_client_request<MDSReqRef>(req, vid);
  }

  scheduler->disable_qos_feature();
  cleanup_dmclock_scheduler(scheduler);

  ASSERT_EQ(request_count, complete_count);
}

TEST(MDSDmclockScheduler, IssueUpdateRequest)
{
  MDSDmclockScheduler *scheduler = create_dmclock_scheduler();
  scheduler->enable_qos_feature();
  atomic_int update_count = 0;

  SessionId sid = "323423";
  VolumeId vid = "/volumes/_nogroup/subvol";
  ClientInfo client_info(100.0, 200.0, 300.0);
  bool use_default = false;
  scheduler->create_volume_info(vid, client_info, use_default);
  scheduler->add_session_to_volume_info(vid, sid);

  std::mutex m;
  std::unique_lock<std::mutex> lk(m);
  std::condition_variable cv;

  RequestCB cb_func = [&update_count, &cv]
  {
    update_count++;
    cv.notify_one();
  };

  int sync_total_count = 100;
  for (int i = 0; i < sync_total_count; i++) {
    double reservation = (std::rand() % 1000) + 1;
    double weight = (std::rand() % 1000) + 1;
    double limit = (std::rand() % 1000) + 1;
    client_info.update(reservation, weight, limit);
    scheduler->update_volume_info(vid, client_info, use_default);
    scheduler->enqueue_update_request(vid, cb_func);
    cv.wait(lk, []{return true;});
  }

  int async_total_count = 100;
  for (int i = 0; i < async_total_count; i++) {
    double reservation = (std::rand() % 1000) + 1;
    double weight = (std::rand() % 1000) + 1;
    double limit = (std::rand() % 1000) + 1;
    client_info.update(reservation, weight, limit);
    scheduler->update_volume_info(vid, client_info, use_default);
    scheduler->enqueue_update_request(vid, cb_func);
  }

  scheduler->disable_qos_feature();
  cleanup_dmclock_scheduler(scheduler);
  ASSERT_EQ(update_count, (sync_total_count+async_total_count));
}

TEST(MDSDmclockScheduler, IssueClientUpdateMixedRequest)
{
  MDSDmclockScheduler *scheduler = create_dmclock_scheduler();
  scheduler->enable_qos_feature();
  atomic_int update_count = 0;

  SessionId sid = "33343";
  VolumeId vid = "/volumes/_nogroup/subvol";
  ClientInfo client_info(100.0, 200.0, 300.0);
  bool use_default = false;
  scheduler->create_volume_info(vid, client_info, use_default);
  scheduler->add_session_to_volume_info(vid, sid);

  std::mutex m;
  std::unique_lock<std::mutex> lk(m);
  std::condition_variable cv;

  RequestCB cb_func = [&update_count, &cv]
  {
    update_count++;
    cv.notify_one();
  };

  int sync_total_count = 100;
  for (int i = 0; i < sync_total_count; i++) {
    if (i % 2 == 0) {
      double reservation = (std::rand() % 1000) + 1;
      double weight = (std::rand() % 1000) + 1;
      double limit = (std::rand() % 1000) + 1;
      client_info.update(reservation, weight, limit);
      scheduler->update_volume_info(vid, client_info, use_default);
      scheduler->enqueue_update_request(vid, cb_func);
      cv.wait(lk, []{return true;});
    } else {
      MDSReqRef req;
      scheduler->enqueue_client_request<MDSReqRef>(req, vid);
    }
  }

  scheduler->disable_qos_feature();
  cleanup_dmclock_scheduler(scheduler);
  ASSERT_EQ(update_count+complete_count, sync_total_count);
}
