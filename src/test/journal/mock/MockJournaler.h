// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef TEST_RBD_MIRROR_MOCK_JOURNALER_H
#define TEST_RBD_MIRROR_MOCK_JOURNALER_H

#include <gmock/gmock.h>
#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "cls/journal/cls_journal_types.h"
#include "journal/Journaler.h"
#include <iosfwd>
#include <string>

class Context;
class ContextWQ;
class Mutex;
class SafeTimer;

namespace journal {

struct ReplayHandler;

struct MockFuture {
  static MockFuture *s_instance;
  static MockFuture &get_instance() {
    assert(s_instance != nullptr);
    return *s_instance;
  }

  MockFuture() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(is_valid, bool());
  MOCK_METHOD1(flush, void(Context *));
  MOCK_METHOD1(wait, void(Context *));
};

struct MockFutureProxy {
  bool is_valid() const {
    return MockFuture::get_instance().is_valid();
  }

  void flush(Context *on_safe) {
    MockFuture::get_instance().flush(on_safe);
  }

  void wait(Context *on_safe) {
    MockFuture::get_instance().wait(on_safe);
  }
};

struct MockReplayEntry {
  static MockReplayEntry *s_instance;
  static MockReplayEntry &get_instance() {
    assert(s_instance != nullptr);
    return *s_instance;
  }

  MockReplayEntry() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(get_commit_tid, uint64_t());
  MOCK_METHOD0(get_data, bufferlist());
};

struct MockReplayEntryProxy {
  uint64_t get_commit_tid() const {
    return MockReplayEntry::get_instance().get_commit_tid();
  }

  bufferlist get_data() {
    return MockReplayEntry::get_instance().get_data();
  }
};

struct MockJournaler {
  static MockJournaler *s_instance;
  static MockJournaler &get_instance() {
    assert(s_instance != nullptr);
    return *s_instance;
  }

  MockJournaler() {
    s_instance = this;
  }

  MOCK_METHOD0(construct, void());

  MOCK_METHOD1(init, void(Context *));
  MOCK_METHOD0(shut_down, void());
  MOCK_CONST_METHOD0(is_initialized, bool());

  MOCK_METHOD3(get_metadata, void(uint8_t *order, uint8_t *splay_width,
                                  int64_t *pool_id));
  MOCK_METHOD4(get_mutable_metadata, void(uint64_t*, uint64_t*,
                                          std::set<cls::journal::Client> *,
                                          Context*));

  MOCK_METHOD2(register_client, void(const bufferlist &, Context *));
  MOCK_METHOD3(get_client, void(const std::string &, cls::journal::Client *,
                                Context *));
  MOCK_METHOD2(get_cached_client, int(const std::string&, cls::journal::Client*));
  MOCK_METHOD2(update_client, void(const bufferlist &, Context *));

  MOCK_METHOD3(get_tag, void(uint64_t, cls::journal::Tag *, Context *));
  MOCK_METHOD3(get_tags, void(uint64_t, journal::Journaler::Tags*, Context*));

  MOCK_METHOD1(start_replay, void(::journal::ReplayHandler *replay_handler));
  MOCK_METHOD2(start_live_replay, void(ReplayHandler *, double));
  MOCK_METHOD1(try_pop_front, bool(MockReplayEntryProxy *));
  MOCK_METHOD2(try_pop_front, bool(MockReplayEntryProxy *, uint64_t *));
  MOCK_METHOD0(stop_replay, void());

  MOCK_METHOD3(start_append, void(int flush_interval, uint64_t flush_bytes,
                                  double flush_age));
  MOCK_CONST_METHOD0(get_max_append_size, uint64_t());
  MOCK_METHOD2(append, MockFutureProxy(uint64_t tag_id,
                                       const bufferlist &bl));
  MOCK_METHOD1(flush, void(Context *on_safe));
  MOCK_METHOD1(stop_append, void(Context *on_safe));

  MOCK_METHOD1(committed, void(const MockReplayEntryProxy &));
  MOCK_METHOD1(committed, void(const MockFutureProxy &future));
  MOCK_METHOD1(flush_commit_position, void(Context*));

};

struct MockJournalerProxy {
  template <typename IoCtxT>
  MockJournalerProxy(IoCtxT &header_ioctx, const std::string &,
                     const std::string &, double) {
    MockJournaler::get_instance().construct();
  }

  MockJournalerProxy(ContextWQ *work_queue, SafeTimer *timer, Mutex *timer_lock,
                     librados::IoCtx &header_ioctx, const std::string &journal_id,
                     const std::string &client_id, double commit_interval) {
    MockJournaler::get_instance().construct();
  }

  int exists(bool *header_exists) const {
    return -EINVAL;
  }
  int create(uint8_t order, uint8_t splay_width, int64_t pool_id) {
    return -EINVAL;
  }
  int remove(bool force) {
    return -EINVAL;
  }
  int register_client(const bufferlist &data) {
    return -EINVAL;
  }
  void allocate_tag(uint64_t, const bufferlist &,
                    cls::journal::Tag*, Context *on_finish) {
    on_finish->complete(-EINVAL);
  }

  void init(Context *on_finish) {
    MockJournaler::get_instance().init(on_finish);
  }
  void shut_down() {
    MockJournaler::get_instance().shut_down();
  }
  bool is_initialized() const {
    return MockJournaler::get_instance().is_initialized();
  }

  void get_metadata(uint8_t *order, uint8_t *splay_width, int64_t *pool_id) {
    MockJournaler::get_instance().get_metadata(order, splay_width, pool_id);
  }

  void get_mutable_metadata(uint64_t *min, uint64_t *active,
                            std::set<cls::journal::Client> *clients,
                            Context *on_finish) {
    MockJournaler::get_instance().get_mutable_metadata(min, active, clients,
                                                       on_finish);
  }

  void register_client(const bufferlist &data, Context *on_finish) {
    MockJournaler::get_instance().register_client(data, on_finish);
  }

  void get_client(const std::string &client_id, cls::journal::Client *client,
                  Context *on_finish) {
    MockJournaler::get_instance().get_client(client_id, client, on_finish);
  }

  int get_cached_client(const std::string& client_id,
                        cls::journal::Client* client) {
    return MockJournaler::get_instance().get_cached_client(client_id, client);
  }

  void update_client(const bufferlist &client_data, Context *on_finish) {
    MockJournaler::get_instance().update_client(client_data, on_finish);
  }

  void get_tag(uint64_t tag_tid, cls::journal::Tag *tag, Context *on_finish) {
    MockJournaler::get_instance().get_tag(tag_tid, tag, on_finish);
  }

  void get_tags(uint64_t tag_class, journal::Journaler::Tags *tags,
                Context *on_finish) {
    MockJournaler::get_instance().get_tags(tag_class, tags, on_finish);
  }

  void start_replay(::journal::ReplayHandler *replay_handler) {
    MockJournaler::get_instance().start_replay(replay_handler);
  }

  void start_live_replay(ReplayHandler *handler, double interval) {
    MockJournaler::get_instance().start_live_replay(handler, interval);
  }

  bool try_pop_front(MockReplayEntryProxy *replay_entry) {
    return MockJournaler::get_instance().try_pop_front(replay_entry);
  }

  bool try_pop_front(MockReplayEntryProxy *entry, uint64_t *tag_tid) {
    return MockJournaler::get_instance().try_pop_front(entry, tag_tid);
  }

  void stop_replay() {
    MockJournaler::get_instance().stop_replay();
  }

  void start_append(int flush_interval, uint64_t flush_bytes, double flush_age) {
    MockJournaler::get_instance().start_append(flush_interval, flush_bytes,
                                               flush_age);
  }

  uint64_t get_max_append_size() const {
    return MockJournaler::get_instance().get_max_append_size();
  }

  MockFutureProxy append(uint64_t tag_id, const bufferlist &bl) {
    return MockJournaler::get_instance().append(tag_id, bl);
  }

  void flush(Context *on_safe) {
    MockJournaler::get_instance().flush(on_safe);
  }

  void stop_append(Context *on_safe) {
    MockJournaler::get_instance().stop_append(on_safe);
  }

  void committed(const MockReplayEntryProxy &entry) {
    MockJournaler::get_instance().committed(entry);
  }

  void committed(const MockFutureProxy &future) {
    MockJournaler::get_instance().committed(future);
  }

  void flush_commit_position(Context *on_finish) {
    MockJournaler::get_instance().flush_commit_position(on_finish);
  }
};

std::ostream &operator<<(std::ostream &os, const MockJournalerProxy &);

} // namespace journal

#endif // TEST_RBD_MIRROR_MOCK_JOURNALER_H
