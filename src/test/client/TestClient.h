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
#include "client/FSCrypt.h"
#include "messages/MClientReclaim.h"
#include "messages/MClientSession.h"
#include "common/async/blocked_completion.h"

#include "fscrypt_conf.h"

#define dout_subsys ceph_subsys_client

namespace bs = boost::system;
namespace ca = ceph::async;


struct fscrypt_env {
  bool encrypted = false;
  std::string name;
  std::string dir;
  char key[32];
};

class ClientScaffold : public Client {  
  fscrypt_env *fse;
public:
    ClientScaffold(Messenger *m, MonClient *mc, Objecter *objecter_, fscrypt_env *fse) : Client(m, mc, objecter_), fse(fse) {}
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

    bool encrypt(const UserPerm& myperm) {
      if (!fscrypt_enabled || fse->encrypted) {
        return false;
      }

      pid_t mypid = getpid();
      fse->name = std::string("ceph_test_client_fscrypt.") + stringify(mypid) + "." + stringify(rand());
      fse->dir = std::string("/") + fse->name;

      int r = mount("/", myperm, true);
      if (r < 0) {
        std::clog << __func__ << "(): mount() r=" << r << std::endl;
        throw std::runtime_error("mount() returned error");
      }

      r = mkdir(fse->name.c_str(), 0755, myperm);
      if (r < 0) {
        std::clog << __func__ << "(): mkdir(" << fse->name << ") r=" << r << std::endl;
        throw std::runtime_error("get_root() returned error");
      }

      for (size_t i = 0; i < sizeof(fse->key); ++i) {
        fse->key[i] = (char)rand();
      }

      std::string key_fname = fse->name + ".key";
      int key_fd = open(key_fname.c_str(), O_RDWR|O_CREAT|O_TRUNC, myperm, 0600);
      if (key_fd < 0) {
        std::clog << __func__ << "(): open() fd=" << key_fd << std::endl;
        throw std::runtime_error("open() returned error");
      }

      r = write(key_fd, fse->key, sizeof(fse->key), 0);
      if (r < 0) {
        std::clog << __func__ << "(): write() r=" << r << std::endl;
        throw std::runtime_error("write() returned error");
      }

      close(key_fd);

      struct ceph_fscrypt_key_identifier kid;

      r = add_fscrypt_key(fse->key, sizeof(fse->key), &kid);
      if (r < 0) {
        std::clog << __func__ << "(): add_fscrypt_key() r=" << r << std::endl;
        throw std::runtime_error("add_fscrypt_key() returned error");
      }

      struct fscrypt_policy_v2 policy;
      policy.version = 2;
      policy.contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
      policy.filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
      policy.flags = FSCRYPT_POLICY_FLAGS_PAD_32;
      memcpy(policy.master_key_identifier, kid.raw, FSCRYPT_KEY_IDENTIFIER_SIZE);

      int fd = open(fse->name.c_str(), O_DIRECTORY, myperm, 0);
      if (fd < 0) {
        std::clog << __func__ << "(): open() r=" << r << std::endl;
        throw std::runtime_error("open() returned error");
      }

      r = set_fscrypt_policy_v2(fd, policy);
      if (r < 0) {
        std::clog << __func__ << "(): set_fscrypt_policy() r=" << r << std::endl;
        throw std::runtime_error("set_fscrypt_policy() returned error");
      }

      fse->encrypted = true;

      return true;
    }

    int do_mount(const std::string &mount_root, const UserPerm& perms,
                 bool require_mds=false, const std::string &fs_name="") {
      int r;

      if (fse->dir.empty()) {
        r = mount(mount_root, perms, require_mds, fs_name);
      } else {
        std::string new_root = fse->dir + mount_root;

        r = mount(new_root, perms, require_mds, fs_name);
      }
      if (r < 0) {
        std::clog << __func__ << "() do_mount r=" << r << std::endl;
        return r;
      }

      struct ceph_fscrypt_key_identifier kid;

      r = add_fscrypt_key(fse->key, sizeof(fse->key), &kid);
      if (r < 0) {
        std::clog << __func__ << "() ceph_mount add_fscrypt_key r=" << r << std::endl;
        return r;
      }

      return 0;
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

    bool encrypt() {
      bool encrypted = client->encrypt(myperm);
      if (encrypted) {
        client->unmount();
        client->shutdown();
        delete client;
      }
      return encrypted;
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

      do {
        client = new ClientScaffold(messenger, mc, objecter, &fse);
        client->init();
      } while (encrypt());

      client->do_mount("/", myperm, true);
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
    static inline fscrypt_env fse;
    MonClient* mc = nullptr;
    Messenger* messenger = nullptr;
    Objecter* objecter = nullptr;
    ClientScaffold* client = nullptr;
};
