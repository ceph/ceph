// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"

#include "common/async/context_pool.h"
#include "global/global_context.h"

#include "msg/Messenger.h"
#include "mon/MonClient.h"
#include "osdc/ObjectCacher.h"
#include "client/MetaRequest.h"
#include "client/Client.h"
#include "messages/MClientReclaim.h"
#include "messages/MClientSession.h"
#include "common/async/blocked_completion.h"

#define dout_subsys ceph_subsys_client

namespace bs = boost::system;
namespace ca = ceph::async;

struct FileHandleInfo{
    Inode *file;
    const char *filename;
    Fh *fh;
    struct ceph_statx stx;
};

class ClientScaffold : public Client {  
public:
    ClientScaffold(Messenger *m, MonClient *mc, Objecter *objecter_) : Client(m, mc, objecter_) {}
    virtual ~ClientScaffold()
    { }
    int check_dummy_op(const UserPerm& perms){
      RWRef_t mref_reader(mount_state, CLIENT_MOUNTING);
      if (!mref_reader.is_state_satisfied()) {
        return -CEPHFS_ENOTCONN;
      }
      std::scoped_lock l(client_lock);
      MetaRequest *req = new MetaRequest(CEPH_MDS_OP_DUMMY);
      int res = make_request(req, perms);
      ldout(cct, 10) << __func__ << " result=" << res << dendl;
      return res;
    }
    int send_unknown_session_op(int op) {
      RWRef_t mref_reader(mount_state, CLIENT_MOUNTING);
      if (!mref_reader.is_state_satisfied()) {
        return -CEPHFS_ENOTCONN;
      }
      std::scoped_lock l(client_lock);
      auto session = _get_or_open_mds_session(0);
      auto msg = make_message<MClientSession>(op, session->seq);
      int res = session->con->send_message2(std::move(msg));
      ldout(cct, 10) << __func__ << " result=" << res << dendl;
      return res;
    }
    bool check_client_blocklisted() {
      RWRef_t mref_reader(mount_state, CLIENT_MOUNTING);
      if (!mref_reader.is_state_satisfied()) {
        return -CEPHFS_ENOTCONN;
      }
      std::scoped_lock l(client_lock);
      bs::error_code ec;
      ldout(cct, 20) << __func__ << ": waiting for latest osdmap" << dendl;
      objecter->wait_for_latest_osdmap(ca::use_blocked[ec]);
      ldout(cct, 20) << __func__ << ": got latest osdmap: " << ec << dendl;
      const auto myaddrs = messenger->get_myaddrs();
      return objecter->with_osdmap([&](const OSDMap& o) {return o.is_blocklisted(myaddrs);});
    }
    bool check_unknown_reclaim_flag(uint32_t flag) {
      RWRef_t mref_reader(mount_state, CLIENT_MOUNTING);
      if (!mref_reader.is_state_satisfied()) {
        return -CEPHFS_ENOTCONN;
      }
      std::scoped_lock l(client_lock);
      char uuid[256];
      sprintf(uuid, "unknownreclaimflag:%x", getpid());
      auto session = _get_or_open_mds_session(0);
      auto m = make_message<MClientReclaim>(uuid, flag);
      ceph_assert(session->con->send_message2(std::move(m)) == 0);
      wait_on_list(waiting_for_reclaim);
      return session->reclaim_state == MetaSession::RECLAIM_FAIL ? true : false;
    }

    void write_n_bytes_async(struct Fh *fh, size_t to_write, size_t block_size,
                            int iov_cnt, off_t *offset) {
      /// @brief Write N bytes of data asynchronously.
      /// @param fh - File handle
      /// @param to_write - bytes to write
      /// @param block_size - Total size of each iovec structs array
      /// @param iov_cnt - Number of elements in iovec structs array
      /// @param offset - location to start writing from

      const size_t DATA_PER_BLOCK = size_t(block_size / iov_cnt);
      // to persist memory address of out_buf after every loop iteration
      std::vector<std::unique_ptr<char[]>> buffers;
      buffers.reserve(iov_cnt);
      struct iovec iov_buf_out[iov_cnt];

      for (int i = 0; i < iov_cnt; ++i) {
        auto out_buf = std::make_unique<char[]>(DATA_PER_BLOCK);
        memset(out_buf.get(), 'a' + i, DATA_PER_BLOCK);
        iov_buf_out[i].iov_base = out_buf.get();
        iov_buf_out[i].iov_len = DATA_PER_BLOCK;
        buffers.push_back(std::move(out_buf));
      }

      std::unique_ptr<C_SaferCond> writefinish = nullptr;
      int64_t rc = 0, bytes_written = 0;

      while (to_write > 0) {
        writefinish.reset(new C_SaferCond("nonblocking-writefinish-n-mb"));
        if (to_write >= block_size) {
          rc = ll_preadv_pwritev(fh, iov_buf_out, iov_cnt, *offset,
                                true, writefinish.get(), nullptr);
          ASSERT_EQ(rc, 0);
          bytes_written = writefinish->wait();
          ASSERT_EQ(bytes_written, block_size);
          *offset += bytes_written;
          to_write -= bytes_written;
        } else {
          // Allocate a smaller buffer for the remaining bytes
          auto small_buf = std::make_unique<char[]>(to_write);
          memset(small_buf.get(), 'z', to_write);
          struct iovec iov_small_buf_out[1] = {
              {small_buf.get(), to_write}
          };
          rc = ll_preadv_pwritev(fh, iov_small_buf_out, 1, *offset,
                                true, writefinish.get(), nullptr);
          ASSERT_EQ(rc, 0);
          bytes_written = writefinish->wait();
          ASSERT_EQ(bytes_written, to_write);
          *offset += bytes_written;
          to_write -= bytes_written;
          ASSERT_EQ(to_write, 0);
          break;
        }
      }
    }

    bool is_data_pool_full(int64_t data_pool) {
      /* check if data pool is reported as full with a specified timeout 
      and period to sleep */
      return objecter->with_osdmap([&](const OSDMap &o) {
        for (const auto& kv : o.get_pools()) {
          if (kv.first == data_pool && kv.second.has_flag(pg_pool_t::FLAG_FULL)) {
            return true;
          }
        }
        return false;
      });
    }

    bool wait_until_true(std::function<bool()> condition, int32_t timeout = 600,
                        int32_t period = 5) {
      // wait for the condition to be true until timeout

      while(true) {
        if (condition()) {
          return true;
        }
        timeout -= period;
        if (!timeout) {
          return false;
        }
        sleep(period);
      }
    }

    bool wait_for_osdmap_epoch_update(const epoch_t initial_osd_epoch,
                                      int32_t timeout = 10,
                                      int32_t period = 5) {
      // wait for timeout and check for osdmap epoch increment

      bs::error_code ec;
      objecter->wait_for_latest_osdmap(ca::use_blocked[ec]);
      ldout(cct, 20) << __func__ << ": latest osdmap: " << ec << dendl;

      while(timeout) {
        if (objecter->with_osdmap(std::mem_fn(&OSDMap::get_epoch)) > initial_osd_epoch) {
          return true;
        } else {
          sleep(period);
          timeout -= period;
        }
      }
      return false;
    }

    FileHandleInfo get_file(Inode *root, const char* filename, UserPerm myperm) {
      // open a file and return inode, filename, file handle and fs stats

      Inode *file;
      Fh *fh;
      struct ceph_statx stx;

      assert (ll_createx(root, filename, 0666, O_RDWR | O_CREAT | O_TRUNC,
              &file, &fh, &stx, 0, 0, myperm) == 0);
      FileHandleInfo result;
      result.file = file;
      result.filename = filename;
      result.fh = fh;
      result.stx = stx;
      return result;
    }
};

class TestClient : public ::testing::Test {
public:
    static void SetUpTestSuite() {
      icp.start(g_ceph_context->_conf.get_val<std::uint64_t>("client_asio_thread_count"));
    }
    static void TearDownTestSuite() {
      icp.stop();
    }
    void SetUp() override {
      messenger = Messenger::create_client_messenger(g_ceph_context, "client");
      if (messenger->start() != 0) {
        throw std::runtime_error("failed to start messenger");
      }

      mc = new MonClient(g_ceph_context, icp);
      if (mc->build_initial_monmap() < 0) {
        throw std::runtime_error("build monmap");
      }
      mc->set_messenger(messenger);
      mc->set_want_keys(CEPH_ENTITY_TYPE_MDS | CEPH_ENTITY_TYPE_OSD);
      if (mc->init() < 0) {
        throw std::runtime_error("init monclient");
      }

      objecter = new Objecter(g_ceph_context, messenger, mc, icp);
      objecter->set_client_incarnation(0);
      objecter->init();
      messenger->add_dispatcher_tail(objecter);
      objecter->start();

      client = new ClientScaffold(messenger, mc, objecter);
      client->init();
      client->mount("/", myperm, true);
    }
    void TearDown() override {
      if (client->is_mounted())
        client->unmount();
      client->shutdown();
      objecter->shutdown();
      mc->shutdown();
      messenger->shutdown();
      messenger->wait();

      delete client;
      client = nullptr;
      delete objecter;
      objecter = nullptr;
      delete mc;
      mc = nullptr;
      delete messenger;
      messenger = nullptr;
    }
protected:
    static inline ceph::async::io_context_pool icp;
    static inline UserPerm myperm{0,0};
    MonClient* mc = nullptr;
    Messenger* messenger = nullptr;
    Objecter* objecter = nullptr;
    ClientScaffold* client = nullptr;
};
