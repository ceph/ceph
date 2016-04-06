// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef TEST_RBD_MIRROR_MOCK_JOURNALER_H
#define TEST_RBD_MIRROR_MOCK_JOURNALER_H

#include <gmock/gmock.h>
#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "cls/journal/cls_journal_types.h"
#include "librbd/Journal.h"
#include "librbd/journal/TypeTraits.h"
#include <iosfwd>
#include <string>

class Context;
class ContextWQ;
class Mutex;
class SafeTimer;

namespace journal {

struct ReplayHandler;

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

  MOCK_METHOD1(init, void(Context *));
  MOCK_METHOD0(shut_down, void());
  MOCK_CONST_METHOD0(is_initialized, bool());

  MOCK_METHOD4(get_mutable_metadata, void(uint64_t*, uint64_t*,
                                          std::set<cls::journal::Client> *,
                                          Context*));

  MOCK_METHOD2(try_pop_front, bool(MockReplayEntryProxy *, uint64_t *));
  MOCK_METHOD2(start_live_replay, void(ReplayHandler *, double));
  MOCK_METHOD0(stop_replay, void());

  MOCK_METHOD1(committed, void(const MockReplayEntryProxy &));
  MOCK_METHOD1(flush_commit_position, void(Context*));

  MOCK_METHOD2(update_client, void(const bufferlist&, Context *on_safe));

  MOCK_METHOD3(get_tag, void(uint64_t, cls::journal::Tag *, Context *));

  MOCK_METHOD2(get_cached_client, int(const std::string &,
				      cls::journal::Client *));
};

struct MockJournalerProxy {
  MockJournalerProxy(ContextWQ *work_queue, SafeTimer *timer, Mutex *timer_lock,
                     librados::IoCtx &header_ioctx, const std::string &journal_id,
                     const std::string &client_id, double commit_interval) {
    MockJournaler::get_instance();
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

  void get_mutable_metadata(uint64_t *min, uint64_t *active,
                            std::set<cls::journal::Client> *clients,
                            Context *on_finish) {
    MockJournaler::get_instance().get_mutable_metadata(min, active, clients,
                                                       on_finish);
  }

  bool try_pop_front(MockReplayEntryProxy *entry, uint64_t *tag_tid) {
    return MockJournaler::get_instance().try_pop_front(entry, tag_tid);
  }
  void start_live_replay(ReplayHandler *handler, double interval) {
    MockJournaler::get_instance().start_live_replay(handler, interval);
  }
  void stop_replay() {
    MockJournaler::get_instance().stop_replay();
  }

  void committed(const MockReplayEntryProxy &entry) {
    MockJournaler::get_instance().committed(entry);
  }
  void flush_commit_position(Context *on_finish) {
    MockJournaler::get_instance().flush_commit_position(on_finish);
  }

  void update_client(const bufferlist& data, Context *on_safe) {
    MockJournaler::get_instance().update_client(data, on_safe);
  }

  void get_tag(uint64_t tag_tid, cls::journal::Tag *tag, Context *on_finish) {
    MockJournaler::get_instance().get_tag(tag_tid, tag, on_finish);
  }

  int get_cached_client(const std::string &client_id,
			 cls::journal::Client *client) {
    return MockJournaler::get_instance().get_cached_client(client_id, client);
  }
};

std::ostream &operator<<(std::ostream &os, const MockJournalerProxy &);

} // namespace journal

namespace librbd {

struct MockImageCtx;

namespace journal {

template <>
struct TypeTraits<MockImageCtx> {
  typedef ::journal::MockJournaler Journaler;
};

} // namespace journal
} // namespace librbd

#endif // TEST_RBD_MIRROR_MOCK_JOURNALER_H
