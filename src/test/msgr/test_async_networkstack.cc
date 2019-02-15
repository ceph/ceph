// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <algorithm>
#include <atomic>
#include <iostream>
#include <list>
#include <random>
#include <string>
#include <set>
#include <vector>
#include <gtest/gtest.h>

#include "acconfig.h"
#include "common/config_obs.h"
#include "include/Context.h"
#include "msg/async/Event.h"
#include "msg/async/Stack.h"


#if GTEST_HAS_PARAM_TEST

class NoopConfigObserver : public md_config_obs_t {
  std::list<std::string> options;
  const char **ptrs = 0;

public:
  NoopConfigObserver(std::list<std::string> l) : options(l) {
    ptrs = new const char*[options.size() + 1];
    unsigned j = 0;
    for (auto& i : options) {
      ptrs[j++] = i.c_str();
    }
    ptrs[j] = 0;
  }
  ~NoopConfigObserver() {
    delete[] ptrs;
  }

  const char** get_tracked_conf_keys() const override {
    return ptrs;
  }
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set <std::string> &changed) override {
  }
};

class NetworkWorkerTest : public ::testing::TestWithParam<const char*> {
 public:
  std::shared_ptr<NetworkStack> stack;
  string addr, port_addr;

  NoopConfigObserver fake_obs = {{"ms_type",
				 "ms_dpdk_coremask",
				 "ms_dpdk_host_ipv4_addr",
				 "ms_dpdk_gateway_ipv4_addr",
				 "ms_dpdk_netmask_ipv4_addr"}};

  NetworkWorkerTest() {}
  void SetUp() override {
    cerr << __func__ << " start set up " << GetParam() << std::endl;
    if (strncmp(GetParam(), "dpdk", 4)) {
      g_ceph_context->_conf.set_val("ms_type", "async+posix");
      addr = "127.0.0.1:15000";
      port_addr = "127.0.0.1:15001";
    } else {
      g_ceph_context->_conf.set_val_or_die("ms_type", "async+dpdk");
      g_ceph_context->_conf.set_val_or_die("ms_dpdk_debug_allow_loopback", "true");
      g_ceph_context->_conf.set_val_or_die("ms_async_op_threads", "2");
      g_ceph_context->_conf.set_val_or_die("ms_dpdk_coremask", "0x7");
      g_ceph_context->_conf.set_val_or_die("ms_dpdk_host_ipv4_addr", "172.16.218.3");
      g_ceph_context->_conf.set_val_or_die("ms_dpdk_gateway_ipv4_addr", "172.16.218.2");
      g_ceph_context->_conf.set_val_or_die("ms_dpdk_netmask_ipv4_addr", "255.255.255.0");
      addr = "172.16.218.3:15000";
      port_addr = "172.16.218.3:15001";
    }
    stack = NetworkStack::create(g_ceph_context, GetParam());
    stack->start();
  }
  void TearDown() override {
    stack->stop();
  }
  string get_addr() const {
    return addr;
  }
  string get_ip_different_port() const {
    return port_addr;
  }
  string get_different_ip() const {
    return "10.0.123.100:4323";
  }
  EventCenter *get_center(unsigned i) {
    return &stack->get_worker(i)->center;
  }
  Worker *get_worker(unsigned i) {
    return stack->get_worker(i);
  }
  template<typename func>
  class C_dispatch : public EventCallback {
    Worker *worker;
    func f;
    std::atomic_bool done;
   public:
    C_dispatch(Worker *w, func &&_f): worker(w), f(std::move(_f)), done(false) {}
    void do_request(uint64_t id) override {
      f(worker);
      done = true;
    }
    void wait() {
      int us = 1000 * 1000 * 1000;
      while (!done) {
        ASSERT_TRUE(us > 0);
        usleep(100);
        us -= 100;
      }
    }
  };
  template<typename func>
  void exec_events(func &&f) {
    std::vector<C_dispatch<func>*> dis;
    for (unsigned i = 0; i < stack->get_num_worker(); ++i) {
      Worker *w = stack->get_worker(i);
      C_dispatch<func> *e = new C_dispatch<func>(w, std::move(f));
      stack->get_worker(i)->center.dispatch_event_external(e);
      dis.push_back(e);
    }

    for (auto &&e : dis) {
      e->wait();
      delete e;
    }
  }
};

class C_poll : public EventCallback {
  EventCenter *center;
  std::atomic<bool> woken;
  static const int sleepus = 500;

 public:
  explicit C_poll(EventCenter *c): center(c), woken(false) {}
  void do_request(uint64_t r) override {
    woken = true;
  }
  bool poll(int milliseconds) {
    auto start = ceph::coarse_real_clock::now();
    while (!woken) {
      center->process_events(sleepus);
      usleep(sleepus);
      auto r = std::chrono::duration_cast<std::chrono::milliseconds>(
              ceph::coarse_real_clock::now() - start);
      if (r >= std::chrono::milliseconds(milliseconds))
        break;
    }
    return woken;
  }
  void reset() {
    woken = false;
  }
};

TEST_P(NetworkWorkerTest, SimpleTest) {
  entity_addr_t bind_addr;
  ASSERT_TRUE(bind_addr.parse(get_addr().c_str()));
  std::atomic_bool accepted(false);
  std::atomic_bool *accepted_p = &accepted;

  exec_events([this, accepted_p, bind_addr](Worker *worker) mutable {
    entity_addr_t cli_addr;
    SocketOptions options;
    ServerSocket bind_socket;
    EventCenter *center = &worker->center;
    ssize_t r = 0;
    if (stack->support_local_listen_table() || worker->id == 0)
      r = worker->listen(bind_addr, 0, options, &bind_socket);
    ASSERT_EQ(0, r);

    ConnectedSocket cli_socket, srv_socket;
    if (worker->id == 0) {
      r = worker->connect(bind_addr, options, &cli_socket);
      ASSERT_EQ(0, r);
    }

    bool is_my_accept = false;
    if (bind_socket) {
      C_poll cb(center);
      center->create_file_event(bind_socket.fd(), EVENT_READABLE, &cb);
      if (cb.poll(500)) {
        *accepted_p = true;
        is_my_accept = true;
      }
      ASSERT_TRUE(*accepted_p);
      center->delete_file_event(bind_socket.fd(), EVENT_READABLE);
    }

    if (is_my_accept) {
      r = bind_socket.accept(&srv_socket, options, &cli_addr, worker);
      ASSERT_EQ(0, r);
      ASSERT_TRUE(srv_socket.fd() > 0);
    }

    if (worker->id == 0) {
      C_poll cb(center);
      center->create_file_event(cli_socket.fd(), EVENT_READABLE, &cb);
      r = cli_socket.is_connected();
      if (r == 0) {
        ASSERT_EQ(true, cb.poll(500));
        r = cli_socket.is_connected();
      }
      ASSERT_EQ(1, r);
      center->delete_file_event(cli_socket.fd(), EVENT_READABLE);
    }

    const char *message = "this is a new message";
    int len = strlen(message);
    bufferlist bl;
    bl.append(message, len);
    if (worker->id == 0) {
      r = cli_socket.send(bl, false);
      ASSERT_EQ(len, r);
    }

    char buf[1024];
    C_poll cb(center);
    if (is_my_accept) {
      center->create_file_event(srv_socket.fd(), EVENT_READABLE, &cb);
      {
        r = srv_socket.read(buf, sizeof(buf));
        while (r == -EAGAIN) {
          ASSERT_TRUE(cb.poll(500));
          r = srv_socket.read(buf, sizeof(buf));
          cb.reset();
        }
        ASSERT_EQ(len, r);
        ASSERT_EQ(0, memcmp(buf, message, len));
      }
      bind_socket.abort_accept();
    }
    if (worker->id == 0) {
      cli_socket.shutdown();
      // ack delay is 200 ms
    }

    bl.clear();
    bl.append(message, len);
    if (worker->id == 0) {
      r = cli_socket.send(bl, false);
      ASSERT_EQ(-EPIPE, r);
    }
    if (is_my_accept) {
      cb.reset();
      ASSERT_TRUE(cb.poll(500));
      r = srv_socket.read(buf, sizeof(buf));
      if (r == -EAGAIN) {
        cb.reset();
        ASSERT_TRUE(cb.poll(1000*500));
        r = srv_socket.read(buf, sizeof(buf));
      }
      ASSERT_EQ(0, r);
      center->delete_file_event(srv_socket.fd(), EVENT_READABLE);
      srv_socket.close();
    }
  });
}

TEST_P(NetworkWorkerTest, ConnectFailedTest) {
  entity_addr_t bind_addr;
  ASSERT_TRUE(bind_addr.parse(get_addr().c_str()));

  exec_events([this, bind_addr](Worker *worker) mutable {
    EventCenter *center = &worker->center;
    entity_addr_t cli_addr;
    SocketOptions options;
    ServerSocket bind_socket;
    int r = 0;
    if (stack->support_local_listen_table() || worker->id == 0)
      r = worker->listen(bind_addr, 0, options, &bind_socket);
    ASSERT_EQ(0, r);

    ConnectedSocket cli_socket1, cli_socket2;
    if (worker->id == 0) {
      ASSERT_TRUE(cli_addr.parse(get_ip_different_port().c_str()));
      r = worker->connect(cli_addr, options, &cli_socket1);
      ASSERT_EQ(0, r);
      C_poll cb(center);
      center->create_file_event(cli_socket1.fd(), EVENT_READABLE, &cb);
      r = cli_socket1.is_connected();
      if (r == 0) {
        ASSERT_TRUE(cb.poll(500));
        r = cli_socket1.is_connected();
      }
      ASSERT_TRUE(r == -ECONNREFUSED || r == -ECONNRESET);
    }

    if (worker->id == 1) {
      ASSERT_TRUE(cli_addr.parse(get_different_ip().c_str()));
      r = worker->connect(cli_addr, options, &cli_socket2);
      ASSERT_EQ(0, r);
      C_poll cb(center);
      center->create_file_event(cli_socket2.fd(), EVENT_READABLE, &cb);
      r = cli_socket2.is_connected();
      if (r == 0) {
        cb.poll(500);
        r = cli_socket2.is_connected();
      }
      ASSERT_TRUE(r != 1);
      center->delete_file_event(cli_socket2.fd(), EVENT_READABLE);
    }
  });
}

TEST_P(NetworkWorkerTest, ListenTest) {
  Worker *worker = get_worker(0);
  entity_addr_t bind_addr;
  ASSERT_TRUE(bind_addr.parse(get_addr().c_str()));
  SocketOptions options;
  ServerSocket bind_socket1, bind_socket2;
  int r = worker->listen(bind_addr, 0, options, &bind_socket1);
  ASSERT_EQ(0, r);

  r = worker->listen(bind_addr, 0, options, &bind_socket2);
  ASSERT_EQ(-EADDRINUSE, r);
}

TEST_P(NetworkWorkerTest, AcceptAndCloseTest) {
  entity_addr_t bind_addr;
  ASSERT_TRUE(bind_addr.parse(get_addr().c_str()));
  std::atomic_bool accepted(false);
  std::atomic_bool *accepted_p = &accepted;
  std::atomic_int unbind_count(stack->get_num_worker());
  std::atomic_int *count_p = &unbind_count;
  exec_events([this, bind_addr, accepted_p, count_p](Worker *worker) mutable {
    SocketOptions options;
    EventCenter *center = &worker->center;
    entity_addr_t cli_addr;
    int r = 0;
    {
      ServerSocket bind_socket;
      if (stack->support_local_listen_table() || worker->id == 0)
        r = worker->listen(bind_addr, 0, options, &bind_socket);
      ASSERT_EQ(0, r);

      ConnectedSocket srv_socket, cli_socket;
      if (bind_socket) {
        r = bind_socket.accept(&srv_socket, options, &cli_addr, worker);
        ASSERT_EQ(-EAGAIN, r);
      }

      C_poll cb(center);
      if (worker->id == 0) {
        center->create_file_event(bind_socket.fd(), EVENT_READABLE, &cb);
        r = worker->connect(bind_addr, options, &cli_socket);
        ASSERT_EQ(0, r);
        ASSERT_TRUE(cb.poll(500));
      }

      if (bind_socket) {
        cb.reset();
        cb.poll(500);
        ConnectedSocket srv_socket2;
        do {
          r = bind_socket.accept(&srv_socket2, options, &cli_addr, worker);
          usleep(100);
        } while (r == -EAGAIN && !*accepted_p);
        if (r == 0)
          *accepted_p = true;
        ASSERT_TRUE(*accepted_p);
        // srv_socket2 closed
        center->delete_file_event(bind_socket.fd(), EVENT_READABLE);
      }

      if (worker->id == 0) {
        char buf[100];
        cb.reset();
        center->create_file_event(cli_socket.fd(), EVENT_READABLE, &cb);
        int i = 3;
        while (!i--) {
          ASSERT_TRUE(cb.poll(500));
          r = cli_socket.read(buf, sizeof(buf));
          if (r == 0)
            break;
        }
        ASSERT_EQ(0, r);
        center->delete_file_event(cli_socket.fd(), EVENT_READABLE);
      }

      if (bind_socket)
        center->create_file_event(bind_socket.fd(), EVENT_READABLE, &cb);
      if (worker->id == 0) {
        *accepted_p = false;
        r = worker->connect(bind_addr, options, &cli_socket);
        ASSERT_EQ(0, r);
        cb.reset();
        ASSERT_TRUE(cb.poll(500));
        cli_socket.close();
      }

      if (bind_socket) {
        do {
          r = bind_socket.accept(&srv_socket, options, &cli_addr, worker);
          usleep(100);
        } while (r == -EAGAIN && !*accepted_p);
        if (r == 0)
          *accepted_p = true;
        ASSERT_TRUE(*accepted_p);
        center->delete_file_event(bind_socket.fd(), EVENT_READABLE);
      }
      // unbind
    }

    --*count_p;
    while (*count_p > 0)
      usleep(100);

    ConnectedSocket cli_socket;
    r = worker->connect(bind_addr, options, &cli_socket);
    ASSERT_EQ(0, r);
    {
      C_poll cb(center);
      center->create_file_event(cli_socket.fd(), EVENT_READABLE, &cb);
      r = cli_socket.is_connected();
      if (r == 0) {
        ASSERT_TRUE(cb.poll(500));
        r = cli_socket.is_connected();
      }
      ASSERT_TRUE(r == -ECONNREFUSED || r == -ECONNRESET);
    }
  });
}

TEST_P(NetworkWorkerTest, ComplexTest) {
  entity_addr_t bind_addr;
  std::atomic_bool listen_done(false);
  std::atomic_bool *listen_p = &listen_done;
  std::atomic_bool accepted(false);
  std::atomic_bool *accepted_p = &accepted;
  std::atomic_bool done(false);
  std::atomic_bool *done_p = &done;
  ASSERT_TRUE(bind_addr.parse(get_addr().c_str()));
  exec_events([this, bind_addr, listen_p, accepted_p, done_p](Worker *worker) mutable {
    entity_addr_t cli_addr;
    EventCenter *center = &worker->center;
    SocketOptions options;
    ServerSocket bind_socket;
    int r = 0;
    if (stack->support_local_listen_table() || worker->id == 0) {
      r = worker->listen(bind_addr, 0, options, &bind_socket);
      ASSERT_EQ(0, r);
      *listen_p = true;
    }
    ConnectedSocket cli_socket, srv_socket;
    if (worker->id == 1) {
      while (!*listen_p) {
        usleep(50);
        r = worker->connect(bind_addr, options, &cli_socket);
        ASSERT_EQ(0, r);
      }
    }

    if (bind_socket) {
      C_poll cb(center);
      center->create_file_event(bind_socket.fd(), EVENT_READABLE, &cb);
      int count = 3;
      while (count--) {
        if (cb.poll(500)) {
          r = bind_socket.accept(&srv_socket, options, &cli_addr, worker);
          ASSERT_EQ(0, r);
          *accepted_p = true;
          break;
        }
      }
      ASSERT_TRUE(*accepted_p);
      center->delete_file_event(bind_socket.fd(), EVENT_READABLE);
    }

    if (worker->id == 1) {
      C_poll cb(center);
      center->create_file_event(cli_socket.fd(), EVENT_WRITABLE, &cb);
      r = cli_socket.is_connected();
      if (r == 0) {
        ASSERT_TRUE(cb.poll(500));
        r = cli_socket.is_connected();
      }
      ASSERT_EQ(1, r);
      center->delete_file_event(cli_socket.fd(), EVENT_WRITABLE);
    }

    const size_t message_size = 10240;
    size_t count = 100;
    string message(message_size, '!');
    for (size_t i = 0; i < message_size; i += 100)
      message[i] = ',';
    size_t len = message_size * count;
    C_poll cb(center);
    if (worker->id == 1)
      center->create_file_event(cli_socket.fd(), EVENT_WRITABLE, &cb);
    if (srv_socket)
      center->create_file_event(srv_socket.fd(), EVENT_READABLE, &cb);
    size_t left = len;
    len *= 2;
    string read_string;
    int again_count = 0;
    int c = 2;
    bufferlist bl;
    for (size_t i = 0; i < count; ++i)
      bl.push_back(bufferptr((char*)message.data(), message_size));
    while (!*done_p) {
      again_count = 0;
      if (worker->id == 1) {
        if (c > 0) {
          ssize_t r = 0;
          usleep(100);
          if (left > 0) {
            r = cli_socket.send(bl, false);
            ASSERT_TRUE(r >= 0 || r == -EAGAIN);
            if (r > 0)
              left -= r;
            if (r == -EAGAIN)
              ++again_count;
          }
          if (left == 0) {
            --c;
            left = message_size * count;
            ASSERT_EQ(0U, bl.length());
            for (size_t i = 0; i < count; ++i)
              bl.push_back(bufferptr((char*)message.data(), message_size));
          }
        }
      }

      if (srv_socket) {
        char buf[1000];
        if (len > 0) {
          r = srv_socket.read(buf, sizeof(buf));
          ASSERT_TRUE(r > 0 || r == -EAGAIN);
          if (r > 0) {
            read_string.append(buf, r);
            len -= r;
          } else if (r == -EAGAIN) {
            ++again_count;
          }
        }
        if (len == 0) {
          for (size_t i = 0; i < read_string.size(); i += message_size)
            ASSERT_EQ(0, memcmp(read_string.c_str()+i, message.c_str(), message_size));
          *done_p = true;
        }
      }
      if (again_count) {
        cb.reset();
        cb.poll(500);
      }
    }
    if (worker->id == 1)
      center->delete_file_event(cli_socket.fd(), EVENT_WRITABLE);
    if (srv_socket)
      center->delete_file_event(srv_socket.fd(), EVENT_READABLE);

    if (bind_socket)
      bind_socket.abort_accept();
    if (srv_socket)
      srv_socket.close();
    if (worker->id == 1)
      cli_socket.close();
  });
}

class StressFactory {
  struct Client;
  struct Server;
  struct ThreadData {
    Worker *worker;
    std::set<Client*> clients;
    std::set<Server*> servers;
    ~ThreadData() {
      for (auto && i : clients)
        delete i;
      for (auto && i : servers)
        delete i;
    }
  };

  struct RandomString {
    size_t slen;
    vector<std::string> strs;
    std::random_device rd;
    std::default_random_engine rng;

    explicit RandomString(size_t s): slen(s), rng(rd()) {}
    void prepare(size_t n) {
      static const char alphabet[] =
          "abcdefghijklmnopqrstuvwxyz"
          "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          "0123456789";

      std::uniform_int_distribution<> dist(
              0, sizeof(alphabet) / sizeof(*alphabet) - 2);

      strs.reserve(n);
      std::generate_n(
        std::back_inserter(strs), strs.capacity(), [&] {
          std::string str;
          str.reserve(slen);
          std::generate_n(std::back_inserter(str), slen, [&]() {
            return alphabet[dist(rng)];
          });
         return str;
        }
      );
    }
    std::string &get_random_string() {
      std::uniform_int_distribution<> dist(
              0, strs.size() - 1);
      return strs[dist(rng)];
    }
  };
  struct Message {
    size_t idx;
    size_t len;
    std::string content;

    explicit Message(RandomString &rs, size_t i, size_t l): idx(i) {
      size_t slen = rs.slen;
      len = std::max(slen, l);

      std::vector<std::string> strs;
      strs.reserve(len / slen);
      std::generate_n(
        std::back_inserter(strs), strs.capacity(), [&] {
          return rs.get_random_string();
        }
      );
      len = slen * strs.size();
      content.reserve(len);
      for (auto &&s : strs)
        content.append(s);
    }
    bool verify(const char *b, size_t len = 0) const {
      return content.compare(0, len, b, 0, len) == 0;
    }
  };

  template <typename T>
  class C_delete : public EventCallback {
    T *ctxt;
   public:
    explicit C_delete(T *c): ctxt(c) {}
    void do_request(uint64_t id) override {
      delete ctxt;
      delete this;
    }
  };

  class Client {
    StressFactory *factory;
    EventCenter *center;
    ConnectedSocket socket;
    std::deque<StressFactory::Message*> acking;
    std::deque<StressFactory::Message*> writings;
    std::string buffer;
    size_t index = 0;
    size_t left;
    bool write_enabled = false;
    size_t read_offset = 0, write_offset = 0;
    bool first = true;
    bool dead = false;
    StressFactory::Message homeless_message;

    class Client_read_handle : public EventCallback {
      Client *c;
     public:
      explicit Client_read_handle(Client *_c): c(_c) {}
      void do_request(uint64_t id) override {
        c->do_read_request();
      }
    } read_ctxt;

    class Client_write_handle : public EventCallback {
      Client *c;
     public:
      explicit Client_write_handle(Client *_c): c(_c) {}
      void do_request(uint64_t id) override {
        c->do_write_request();
      }
    } write_ctxt;

   public:
    Client(StressFactory *f, EventCenter *cen, ConnectedSocket s, size_t c)
        : factory(f), center(cen), socket(std::move(s)), left(c), homeless_message(factory->rs, -1, 1024),
          read_ctxt(this), write_ctxt(this) {
      center->create_file_event(
              socket.fd(), EVENT_READABLE, &read_ctxt);
      center->dispatch_event_external(&read_ctxt);
    }
    void close() {
      ASSERT_FALSE(write_enabled);
      dead = true;
      socket.shutdown();
      center->delete_file_event(socket.fd(), EVENT_READABLE);
      center->dispatch_event_external(new C_delete<Client>(this));
    }

    void do_read_request() {
      if (dead)
        return ;
      ASSERT_TRUE(socket.is_connected() >= 0);
      if (!socket.is_connected())
        return ;
      ASSERT_TRUE(!acking.empty() || first);
      if (first) {
        first = false;
        center->dispatch_event_external(&write_ctxt);
        if (acking.empty())
          return ;
      }
      StressFactory::Message *m = acking.front();
      int r = 0;
      if (buffer.empty())
        buffer.resize(m->len);
      bool must_no = false;
      while (true) {
        r = socket.read((char*)buffer.data() + read_offset,
                        m->len - read_offset);
        ASSERT_TRUE(r == -EAGAIN || r > 0);
        if (r == -EAGAIN)
          break;
        read_offset += r;

        std::cerr << " client " << this << " receive " << m->idx << " len " << r << " content: "  << std::endl;
        ASSERT_FALSE(must_no);
        if ((m->len - read_offset) == 0) {
          ASSERT_TRUE(m->verify(buffer.data(), 0));
          delete m;
          acking.pop_front();
          read_offset = 0;
          buffer.clear();
          if (acking.empty()) {
            m = &homeless_message;
            must_no = true;
          } else {
            m = acking.front();
            buffer.resize(m->len);
          }
        }
      }
      if (acking.empty()) {
        center->dispatch_event_external(&write_ctxt);
        return ;
      }
    }

    void do_write_request() {
      if (dead)
        return ;
      ASSERT_TRUE(socket.is_connected() > 0);

      while (left > 0 && factory->queue_depth > writings.size() + acking.size()) {
        StressFactory::Message *m = new StressFactory::Message(
                factory->rs, ++index,
                factory->rd() % factory->max_message_length);
        std::cerr << " client " << this << " generate message " << m->idx << " length " << m->len << std::endl;
        ASSERT_EQ(m->len, m->content.size());
        writings.push_back(m);
        --left;
        --factory->message_left;
      }

      while (!writings.empty()) {
        StressFactory::Message *m = writings.front();
        bufferlist bl;
        bl.append(m->content.data() + write_offset, m->content.size() - write_offset);
        ssize_t r = socket.send(bl, false);
        if (r == 0)
          break;
        std::cerr << " client " << this << " send " << m->idx << " len " << r << " content: " << std::endl;
        ASSERT_TRUE(r >= 0);
        write_offset += r;
        if (write_offset == m->content.size()) {
          write_offset = 0;
          writings.pop_front();
          acking.push_back(m);
        }
      }
      if (writings.empty() && write_enabled) {
        center->delete_file_event(socket.fd(), EVENT_WRITABLE);
        write_enabled = false;
      } else if (!writings.empty() && !write_enabled) {
        ASSERT_EQ(0, center->create_file_event(
                  socket.fd(), EVENT_WRITABLE, &write_ctxt));
        write_enabled = true;
      }
    }

    bool finish() const {
      return left == 0 && acking.empty() && writings.empty();
    }
  };
  friend class Client;

  class Server {
    StressFactory *factory;
    EventCenter *center;
    ConnectedSocket socket;
    std::deque<std::string> buffers;
    bool write_enabled = false;
    bool dead = false;

    class Server_read_handle : public EventCallback {
      Server *s;
     public:
      explicit Server_read_handle(Server *_s): s(_s) {}
      void do_request(uint64_t id) override {
        s->do_read_request();
      }
    } read_ctxt;

    class Server_write_handle : public EventCallback {
      Server *s;
     public:
      explicit Server_write_handle(Server *_s): s(_s) {}
      void do_request(uint64_t id) override {
        s->do_write_request();
      }
    } write_ctxt;

   public:
    Server(StressFactory *f, EventCenter *c, ConnectedSocket s):
        factory(f), center(c), socket(std::move(s)), read_ctxt(this), write_ctxt(this) {
      center->create_file_event(socket.fd(), EVENT_READABLE, &read_ctxt);
      center->dispatch_event_external(&read_ctxt);
    }
    void close() {
      ASSERT_FALSE(write_enabled);
      socket.shutdown();
      center->delete_file_event(socket.fd(), EVENT_READABLE);
      center->dispatch_event_external(new C_delete<Server>(this));
    }
    void do_read_request() {
      if (dead)
        return ;
      int r = 0;
      while (true) {
        char buf[4096];
        bufferptr data;
        if (factory->zero_copy_read) {
          r = socket.zero_copy_read(data);
        } else {
          r = socket.read(buf, sizeof(buf));
        }
        ASSERT_TRUE(r == -EAGAIN || (r >= 0 && (size_t)r <= sizeof(buf)));
        if (r == 0) {
          ASSERT_TRUE(buffers.empty());
          dead = true;
          return ;
        } else if (r == -EAGAIN)
          break;
        if (factory->zero_copy_read) {
          buffers.emplace_back(data.c_str(), 0, data.length());
        } else {
          buffers.emplace_back(buf, 0, r);
        }
        std::cerr << " server " << this << " receive " << r << " content: " << std::endl;
      }
      if (!buffers.empty() && !write_enabled)
        center->dispatch_event_external(&write_ctxt);
    }

    void do_write_request() {
      if (dead)
        return ;

      while (!buffers.empty()) {
        bufferlist bl;
        auto it = buffers.begin();
        for (size_t i = 0; i < buffers.size(); ++i) {
          bl.push_back(bufferptr((char*)it->data(), it->size()));
          ++it;
        }

        ssize_t r = socket.send(bl, false);
        std::cerr << " server " << this << " send " << r << std::endl;
        if (r == 0)
          break;
        ASSERT_TRUE(r >= 0);
        while (r > 0) {
          ASSERT_TRUE(!buffers.empty());
          string &buffer = buffers.front();
          if (r >= (int)buffer.size()) {
            r -= (int)buffer.size();
            buffers.pop_front();
          } else {
           std::cerr << " server " << this << " sent " << r << std::endl;
            buffer = buffer.substr(r, buffer.size());
            break;
          }
        }
      }
      if (buffers.empty()) {
        if (write_enabled) {
          center->delete_file_event(socket.fd(), EVENT_WRITABLE);
          write_enabled = false;
        }
      } else if (!write_enabled) {
        ASSERT_EQ(0, center->create_file_event(
                  socket.fd(), EVENT_WRITABLE, &write_ctxt));
        write_enabled = true;
      }
    }

    bool finish() {
     return dead;
    }
  };
  friend class Server;

  class C_accept : public EventCallback {
    StressFactory *factory;
    ServerSocket bind_socket;
    ThreadData *t_data;
    Worker *worker;

   public:
    C_accept(StressFactory *f, ServerSocket s, ThreadData *data, Worker *w)
        : factory(f), bind_socket(std::move(s)), t_data(data), worker(w) {}
    void do_request(uint64_t id) override {
      while (true) {
        entity_addr_t cli_addr;
        ConnectedSocket srv_socket;
        SocketOptions options;
        int r = bind_socket.accept(&srv_socket, options, &cli_addr, worker);
        if (r == -EAGAIN) {
          break;
        }
        ASSERT_EQ(0, r);
        ASSERT_TRUE(srv_socket.fd() > 0);
        Server *cb = new Server(factory, &t_data->worker->center, std::move(srv_socket));
        t_data->servers.insert(cb);
      }
    }
  };
  friend class C_accept;

 public:
  static const size_t min_client_send_messages = 100;
  static const size_t max_client_send_messages = 1000;
  std::shared_ptr<NetworkStack> stack;
  RandomString rs;
  std::random_device rd;
  const size_t client_num, queue_depth, max_message_length;
  atomic_int message_count, message_left;
  entity_addr_t bind_addr;
  std::atomic_bool already_bind = {false};
  bool zero_copy_read;
  SocketOptions options;

  explicit StressFactory(const std::shared_ptr<NetworkStack> &s, const string &addr,
                         size_t cli, size_t qd, size_t mc, size_t l, bool zero_copy)
      : stack(s), rs(128), client_num(cli), queue_depth(qd),
        max_message_length(l), message_count(mc), message_left(mc),
        zero_copy_read(zero_copy) {
    bind_addr.parse(addr.c_str());
    rs.prepare(100);
  }
  ~StressFactory() {
  }

  void add_client(ThreadData *t_data) {
    static Mutex lock("add_client_lock");
    Mutex::Locker l(lock);
    ConnectedSocket sock;
    int r = t_data->worker->connect(bind_addr, options, &sock);
    std::default_random_engine rng(rd());
    std::uniform_int_distribution<> dist(
            min_client_send_messages, max_client_send_messages);
    ASSERT_EQ(0, r);
    int c = dist(rng);
    if (c > message_count.load())
      c = message_count.load();
    Client *cb = new Client(this, &t_data->worker->center, std::move(sock), c);
    t_data->clients.insert(cb);
    message_count -= c;
  }

  void drop_client(ThreadData *t_data, Client *c) {
    c->close();
    ASSERT_EQ(1U, t_data->clients.erase(c));
  }

  void drop_server(ThreadData *t_data, Server *s) {
    s->close();
    ASSERT_EQ(1U, t_data->servers.erase(s));
  }

  void start(Worker *worker) {
    int r = 0;
    ThreadData t_data;
    t_data.worker = worker;
    ServerSocket bind_socket;
    if (stack->support_local_listen_table() || worker->id == 0) {
      r = worker->listen(bind_addr, 0, options, &bind_socket);
      ASSERT_EQ(0, r);
      already_bind = true;
    }
    while (!already_bind)
      usleep(50);
    C_accept *accept_handler = nullptr;
    int bind_fd = 0;
    if (bind_socket) {
      bind_fd = bind_socket.fd();
      accept_handler = new C_accept(this, std::move(bind_socket), &t_data, worker);
      ASSERT_EQ(0, worker->center.create_file_event(
                  bind_fd, EVENT_READABLE, accept_handler));
    }

    int echo_throttle = message_count;
    while (message_count > 0 || !t_data.clients.empty() || !t_data.servers.empty()) {
      if (message_count > 0  && t_data.clients.size() < client_num && t_data.servers.size() < client_num)
        add_client(&t_data);
      for (auto &&c : t_data.clients) {
        if (c->finish()) {
          drop_client(&t_data, c);
          break;
        }
      }
      for (auto &&s : t_data.servers) {
        if (s->finish()) {
          drop_server(&t_data, s);
          break;
        }
      }

      worker->center.process_events(1);
      if (echo_throttle > message_left) {
        std::cerr << " clients " << t_data.clients.size() << " servers " << t_data.servers.size()
                  << " message count " << message_left << std::endl;
        echo_throttle -= 100;
      }
    }
    if (bind_fd)
      worker->center.delete_file_event(bind_fd, EVENT_READABLE);
    delete accept_handler;
  }
};

TEST_P(NetworkWorkerTest, StressTest) {
  StressFactory factory(stack, get_addr(), 16, 16, 10000, 1024,
                        strncmp(GetParam(), "dpdk", 4) == 0);
  StressFactory *f = &factory;
  exec_events([f](Worker *worker) mutable {
    f->start(worker);
  });
  ASSERT_EQ(0, factory.message_left);
}


INSTANTIATE_TEST_CASE_P(
  NetworkStack,
  NetworkWorkerTest,
  ::testing::Values(
#ifdef HAVE_DPDK
    "dpdk",
#endif
    "posix"
  )
);

#else

// Google Test may not support value-parameterized tests with some
// compilers. If we use conditional compilation to compile out all
// code referring to the gtest_main library, MSVC linker will not link
// that library at all and consequently complain about missing entry
// point defined in that library (fatal error LNK1561: entry point
// must be defined). This dummy test keeps gtest_main linked in.
TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}

#endif


/*
 * Local Variables:
 * compile-command: "cd ../.. ; make ceph_test_async_networkstack &&
 *    ./ceph_test_async_networkstack
 *
 * End:
 */
