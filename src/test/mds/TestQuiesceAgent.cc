/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, RedHat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/Cond.h"
#include "mds/QuiesceAgent.h"
#include "gtest/gtest.h"
#include <algorithm>
#include <functional>
#include <queue>
#include <ranges>
#include <system_error>
#include <thread>
#include <future>

class QuiesceAgentTest : public testing::Test {
  using RequestHandle = QuiesceInterface::RequestHandle;
  using QuiescingRoot = std::pair<RequestHandle, Context*>;
  protected:
    template< class _Rep = std::chrono::seconds::rep, class _Period = std::chrono::seconds::period, typename D = std::chrono::duration<_Rep, _Period>, class Function, class... Args >
    static bool timed_run(D timeout, Function&& f, Args&&... args ) {
      std::promise<void> done;
      auto future = done.get_future();

      auto job = std::bind(f, args...);

      auto tt = std::thread([job=std::move(job)](std::promise<void> done) {
        job();
        done.set_value();
      }, std::move(done));

      tt.detach();

      return future.wait_for(timeout) != std::future_status::timeout;
    }

    struct TestQuiesceAgent : public QuiesceAgent {
      using QuiesceAgent::QuiesceAgent;
      AgentThread& get_agent_thread() {
        return agent_thread;
      }

      QuiesceDbVersion get_latest_version()
      {
        std::lock_guard l(agent_mutex);
        return std::max({current.db_version, working.db_version, pending.db_version});
      }
      TrackedRoots& mutable_tracked_roots() {
        return current.roots;
      }

      QuiesceDbVersion await_idle() {
        std::unique_lock l(agent_mutex);
        return await_idle_locked(l);
      }
    };
    QuiesceMap latest_ack;
    std::unordered_map<QuiesceRoot, QuiescingRoot> quiesce_requests;
    ceph_tid_t last_tid;
    std::mutex mutex;

    std::unique_ptr<TestQuiesceAgent> agent;

    bool complete_quiesce(QuiesceRoot root, int rc = 0) {
      std::lock_guard l(mutex);
      if (auto it = quiesce_requests.find(root); it != quiesce_requests.end()) {
        if (it->second.second) {
          it->second.second->complete(rc);
          it->second.second = nullptr;
          if (rc != 0) {
            // there was an error, no need to keep this request anymore
            quiesce_requests.erase(it);
          }
          return true;
        }
      }
      return false;
    }

    void SetUp() override {
      
      QuiesceAgent::ControlInterface ci;
      quiesce_requests.clear();

      ci.submit_request = [this](QuiesceRoot r, Context* c) {
        std::lock_guard l(mutex);

        // always create a new request id
        auto req_id = metareqid_t(entity_name_t::MDS(0), ++last_tid);

        auto [it, inserted] = quiesce_requests.try_emplace(r, req_id, c);

        if (!inserted) {
          // we must update the request id so that old one can't cancel this request.
          it->second.first = req_id;
          if (it->second.second) {
            it->second.second->complete(-EINTR);
            it->second.second = c;
          } else {
            // if we have no context, it means we've completed it
            // since we weren't inserted, we must have successfully quiesced
            c->complete(0);
          }
        }

        return it->second.first;
      };
      
      ci.cancel_request = [this](RequestHandle h) {
        std::lock_guard l(mutex);
        
        for (auto it = quiesce_requests.cbegin(); it != quiesce_requests.cend(); it++) {
          if (it->second.first == h) {
            if (it->second.second) {
              it->second.second->complete(-ECANCELED);
            }
            quiesce_requests.erase(it);
            return 0;
          }
        }

        return ENOENT;
      };

      ci.agent_ack = [this](QuiesceMap const& update) {
        std::lock_guard l(mutex);
        latest_ack = update;
        return 0;
      };

      agent = std::make_unique<TestQuiesceAgent>(ci);
    }

    void TearDown() override {
      if (agent) {
        agent->shutdown();
        agent.reset();
      }
    }


    using R = QuiesceMap::Roots::value_type;
    using RootInitList = std::initializer_list<R>;

    std::optional<QuiesceMap> update(QuiesceDbVersion v, RootInitList roots)
    {
      QuiesceMap map(v, QuiesceMap::Roots { roots });

      if (agent->db_update(map)) {
        return map;
      }

      return std::nullopt;
    }

    std::optional<QuiesceMap> update(QuiesceSetVersion v, RootInitList roots)
    {
      return update(QuiesceDbVersion { 1, v }, roots);
    }

    std::optional<QuiesceMap> update(RootInitList roots)
    {
      return update(agent->get_latest_version() + 1, roots);
    }

    template <class _Rep = std::chrono::seconds::rep, class _Period = std::chrono::seconds::period, typename D = std::chrono::duration<_Rep, _Period>>
    bool await_idle_v(QuiesceDbVersion version, D timeout = std::chrono::duration_cast<D>(std::chrono::seconds(10)))
    {
      return timed_run(timeout, [this, version] {
        while (version > agent->await_idle()) { };
      });
    }

    template <class _Rep = std::chrono::seconds::rep, class _Period = std::chrono::seconds::period, typename D = std::chrono::duration<_Rep, _Period>>
    bool await_idle(D timeout = std::chrono::duration_cast<D>(std::chrono::seconds(10)))
    {
      return timed_run(timeout, [this] {
        agent->await_idle();
      });
    }
};

TEST_F(QuiesceAgentTest, ThreadManagement) {
  EXPECT_TRUE(agent->get_agent_thread().is_started());

  EXPECT_TRUE(await_idle());

  EXPECT_TRUE(update({ { "root1", QS_QUIESCING } }).has_value());

  EXPECT_TRUE(await_idle());

  EXPECT_TRUE(update({ { "root2", QS_QUIESCING } }).has_value());

  agent->reset();

  EXPECT_TRUE(await_idle());

  EXPECT_TRUE(update({ { "root3", QS_QUIESCING } }).has_value());

  // make sure that the agent thread completes in a timely fashion
  EXPECT_TRUE(timed_run(std::chrono::seconds(1), [this] { agent->shutdown(); agent.reset(); }));
}

TEST_F(QuiesceAgentTest, DbUpdates) {
  {
    auto ack = update(1, { 
      { "root0", QS_QUIESCING }, // this shouldn't be reported because its state isn't different from QUIESCING
      { "root1", QS_QUIESCING }, // ditto
      { "root2", QS_QUIESCED }, // this should be reported back as quiescing
      { "root3", QS_RELEASING }, // this should be reported back as expired
      { "root4", QS_RELEASED }, // this should be reported back as expired
      { "root5", QS_EXPIRED }, // this should be ignored
    });

    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(1, ack->db_version);
    EXPECT_EQ(3, ack->roots.size());
    EXPECT_EQ(QS_QUIESCING, ack->roots.at("root2").state);
    EXPECT_EQ(QS_EXPIRED, ack->roots.at("root3").state);
    EXPECT_EQ(QS_EXPIRED, ack->roots.at("root4").state);
    EXPECT_TRUE(await_idle());
  }

  EXPECT_EQ(1, agent->get_current_version());

  {
    auto roots = agent->tracked_roots();
    EXPECT_EQ(5, roots.size());
    EXPECT_EQ(QS_QUIESCING, roots.at("root0")->committed_state);
    EXPECT_EQ(QS_QUIESCING, roots.at("root1")->committed_state);
    EXPECT_EQ(QS_QUIESCED, roots.at("root2")->committed_state);
    EXPECT_EQ(QS_RELEASING, roots.at("root3")->committed_state);
    EXPECT_EQ(QS_RELEASED, roots.at("root4")->committed_state);
  
    // manipulate root0 and root1 as if they were quiesced and root2 as if it was released
    auto& root0 = *roots.at("root0");
    root0.quiesce_result = 0;
    EXPECT_EQ(QS_QUIESCED, root0.get_actual_state());

    auto& root1 = *roots.at("root1");
    root1.quiesce_result = 0;
    EXPECT_EQ(QS_QUIESCED, root1.get_actual_state());

    auto& root2 = *roots.at("root2");
    root2.quiesce_result = 0;
    root2.cancel_result = 0;
    EXPECT_EQ(QS_RELEASED, root2.get_actual_state());
  }

  {
    auto ack = update(2, { 
      { "root0", QS_RELEASING }, // this should be reported back as quiesced
      { "root1", QS_QUIESCING }, // this should be reported back as quiesced
      { "root2", QS_RELEASING }, // this should be reported back as released
    });

    EXPECT_EQ(2, ack->db_version);
    EXPECT_EQ(3, ack->roots.size());
    EXPECT_EQ(QS_QUIESCED, ack->roots.at("root0").state);
    EXPECT_EQ(QS_QUIESCED, ack->roots.at("root1").state);
    EXPECT_EQ(QS_RELEASED, ack->roots.at("root2").state);
  }

  EXPECT_TRUE(await_idle());
  {
    auto roots = agent->tracked_roots();
    EXPECT_EQ(3, roots.size());
    EXPECT_EQ(QS_RELEASING, roots.at("root0")->committed_state);
    EXPECT_EQ(QS_QUIESCING, roots.at("root1")->committed_state);
    EXPECT_EQ(QS_RELEASING, roots.at("root2")->committed_state);
  }

  {
    // we should be able to set pending version to anything
    // and the agent should follow, including rolling back to 0
    auto ack = update({200, 0}, {});

    EXPECT_TRUE(await_idle());
    EXPECT_EQ(0, ack->db_version);
    EXPECT_EQ(0, ack->roots.size());
    EXPECT_EQ((QuiesceDbVersion {200, 0}), agent->get_current_version());
  }
}

TEST_F(QuiesceAgentTest, QuiesceProtocol) {

  {
    auto ack = update(1, { 
      { "root1", QS_QUIESCING },
      { "root2", QS_QUIESCING },
      { "root3", QS_QUIESCING },
    });

    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(1, ack->db_version);
    EXPECT_EQ(0, ack->roots.size());
  }

  EXPECT_TRUE(await_idle());

  {
    auto tracked = agent->tracked_roots();
    EXPECT_EQ(3, tracked.size());
    EXPECT_EQ(QS_QUIESCING, tracked.at("root1")->committed_state);
    EXPECT_EQ(QS_QUIESCING, tracked.at("root2")->committed_state);
    EXPECT_EQ(QS_QUIESCING, tracked.at("root3")->committed_state);

    EXPECT_EQ(QS_QUIESCING, tracked.at("root1")->get_actual_state());
    EXPECT_EQ(QS_QUIESCING, tracked.at("root2")->get_actual_state());
    EXPECT_EQ(QS_QUIESCING, tracked.at("root3")->get_actual_state());

    // we should have seen the quiesce requests for all roots
    EXPECT_EQ(tracked.at("root1")->quiesce_request.value(), quiesce_requests.at("root1").first);
    EXPECT_EQ(tracked.at("root2")->quiesce_request.value(), quiesce_requests.at("root2").first);
    EXPECT_EQ(tracked.at("root3")->quiesce_request.value(), quiesce_requests.at("root3").first);
  }

  EXPECT_EQ(3, quiesce_requests.size());

  // complete one root with success
  EXPECT_TRUE(complete_quiesce("root1"));

  EXPECT_TRUE(await_idle());
  // we should have seen an ack sent
  EXPECT_EQ(1, latest_ack.db_version);
  EXPECT_EQ(1, latest_ack.roots.size());
  EXPECT_EQ(QS_QUIESCED, latest_ack.roots.at("root1").state);

  latest_ack.clear();

  // complete the other root with failure
  EXPECT_TRUE(complete_quiesce("root2", -1));

  EXPECT_TRUE(await_idle());
  EXPECT_EQ(1, latest_ack.db_version);
  ASSERT_EQ(2, latest_ack.roots.size());
  EXPECT_EQ(QS_QUIESCED, latest_ack.roots.at("root1").state);
  EXPECT_EQ(QS_FAILED, latest_ack.roots.at("root2").state);

  latest_ack.clear();

  // complete the third root with success
  // complete one root with success
  EXPECT_TRUE(complete_quiesce("root3"));

  EXPECT_TRUE(await_idle());

  // we should see the two quiesced roots in the ack,
  EXPECT_EQ(1, latest_ack.db_version);
  ASSERT_EQ(3, latest_ack.roots.size());
  EXPECT_EQ(QS_QUIESCED, latest_ack.roots.at("root1").state);
  EXPECT_EQ(QS_FAILED, latest_ack.roots.at("root2").state);
  EXPECT_EQ(QS_QUIESCED, latest_ack.roots.at("root3").state);

  {
    auto ack = update(2, {
      { "root2", QS_QUIESCING },
      { "root3", QS_RELEASING },
    });

    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(2, ack->db_version);
    // this update doesn't have root1, so it should be untracked and cancelled
    // root2 is still quiescing, no updates for it
    // root3 is released asyncrhonously so for now it should be QUIESCED
    ASSERT_EQ(2, ack->roots.size());
    EXPECT_EQ(QS_FAILED, latest_ack.roots.at("root2").state);
    EXPECT_EQ(QS_QUIESCED, ack->roots.at("root3").state);
  }

  EXPECT_TRUE(await_idle());

  {
    // make sure that root1 isn't tracked
    auto tracked = agent->tracked_roots();
    EXPECT_EQ(2, agent->get_current_version());
    ASSERT_EQ(2, tracked.size());
    EXPECT_EQ(QS_QUIESCING, tracked.at("root2")->committed_state);
    EXPECT_EQ(QS_RELEASING, tracked.at("root3")->committed_state);
  }

  // we should have also seen cancelations for root1 and root3.
  // We observe this by missing them from the quiesce_requests
  // NB: root2 shouldn't be part of requests either since it was completed with failure
  EXPECT_EQ(0, quiesce_requests.size());
}

TEST_F(QuiesceAgentTest, DuplicateQuiesceRequest) {
  {
    auto ack = update(1, { 
      { "root1", QS_QUIESCING },
      { "root2", QS_QUIESCING },
      { "root3", QS_QUIESCING },
    });

    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(1, ack->db_version);
    EXPECT_EQ(0, ack->roots.size());
  }

  EXPECT_TRUE(await_idle());

  QuiesceAgent::TrackedRootRef pinned1, pinned2;

  {
    auto tracked = agent->tracked_roots();
    ASSERT_EQ(3, tracked.size());
    EXPECT_EQ(tracked.at("root1")->quiesce_request.value(), quiesce_requests.at("root1").first);
    EXPECT_EQ(tracked.at("root2")->quiesce_request.value(), quiesce_requests.at("root2").first);
    EXPECT_EQ(tracked.at("root3")->quiesce_request.value(), quiesce_requests.at("root3").first);

    // copying the shared ref will keep the object alive
    pinned1 = tracked.at("root1");
    pinned2 = tracked.at("root2");
  }

  // root 1 should be quiesced now
  EXPECT_TRUE(complete_quiesce("root1"));

  EXPECT_EQ(QS_QUIESCED, pinned1->get_actual_state());
  EXPECT_EQ(QS_QUIESCING, pinned2->get_actual_state());

  // imagine that we lost our root records for a moment
  {
    auto ack = update(2, {
      { "root3", QS_QUIESCING },
    });

    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(2, ack->db_version);
    EXPECT_EQ(0, ack->roots.size());
  }

  EXPECT_TRUE(await_idle());

  {
    auto tracked = agent->tracked_roots();
    EXPECT_EQ(1, tracked.size());
    EXPECT_EQ(tracked.at("root3")->quiesce_request.value(), quiesce_requests.at("root3").first);
  }

  // since we have those pinned, they should still be live

  EXPECT_TRUE(pinned1.unique());
  EXPECT_TRUE(pinned2.unique());

  EXPECT_EQ(QS_QUIESCED, pinned1->get_actual_state());
  EXPECT_EQ(QS_QUIESCING, pinned2->get_actual_state());

  EXPECT_TRUE(quiesce_requests.contains("root1"));
  EXPECT_TRUE(quiesce_requests.contains("root2"));

  latest_ack.clear();
  // now, bring the roots back
  {
    auto ack = update(3, { 
      { "root1", QS_QUIESCING },
      { "root2", QS_QUIESCING },
      { "root3", QS_QUIESCING },
    });

    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(3, ack->db_version);
    // even though root1 is already quiesced,
    // we should not know about it synchronously
    EXPECT_EQ(0, ack->roots.size());
  }

  EXPECT_TRUE(await_idle());

  // now we should have seen the ack with root2 quiesced
  EXPECT_EQ(3, latest_ack.db_version);
  EXPECT_EQ(1, latest_ack.roots.size());
  EXPECT_EQ(QS_QUIESCED, latest_ack.roots.at("root1").state);

  // the actual state of the pinned objects shouldn't have changed
  EXPECT_EQ(QS_QUIESCED, pinned1->get_actual_state());
  EXPECT_EQ(QS_FAILED, pinned2->get_actual_state());

  EXPECT_EQ(0, *pinned1->quiesce_result);
  EXPECT_EQ(-EINTR, *pinned2->quiesce_result);

  // releasing the pinned objects will attempt to cancel, but that shouldn't interfere with the current state
  pinned1.reset();
  pinned2.reset();

  EXPECT_TRUE(quiesce_requests.contains("root1"));
  EXPECT_TRUE(quiesce_requests.contains("root2"));

  EXPECT_TRUE(complete_quiesce("root2"));

  EXPECT_TRUE(await_idle());
  EXPECT_EQ(3, latest_ack.db_version);
  EXPECT_EQ(2, latest_ack.roots.size());
  EXPECT_EQ(QS_QUIESCED, latest_ack.roots.at("root1").state);
  EXPECT_EQ(QS_QUIESCED, latest_ack.roots.at("root2").state);
}

TEST_F(QuiesceAgentTest, TimeoutBeforeComplete)
{
  {
    auto ack = update(1, {
                             { "root1", QS_QUIESCING },
                         });

    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(1, ack->db_version);
    EXPECT_EQ(0, ack->roots.size());
  }

  EXPECT_TRUE(await_idle());

  // QuiesceAgent::TrackedRootRef pinned1, pinned2;

  {
    auto tracked = agent->tracked_roots();
    EXPECT_EQ(1, tracked.size());
    EXPECT_EQ(tracked.at("root1")->quiesce_request.value(), quiesce_requests.at("root1").first);
  }

  // with a new update we got our root 1 timedout (this is the same as not listing it at all)
  {
    auto ack = update(2, {
                             { "root1", QS_TIMEDOUT },
                         });

    ASSERT_TRUE(ack.has_value());
    EXPECT_EQ(2, ack->db_version);
    EXPECT_EQ(0, ack->roots.size());
  }

  EXPECT_TRUE(await_idle());

  {
    auto tracked = agent->tracked_roots();
    EXPECT_EQ(0, tracked.size());
  }
}
