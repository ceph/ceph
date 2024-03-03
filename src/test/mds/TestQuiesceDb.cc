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
#include "mds/QuiesceDbManager.h"
#include "mds/QuiesceDbEncoding.h"
#include "gtest/gtest.h"
#include "common/Cond.h"
#include <ranges>
#include <system_error>
#include <thread>
#include <queue>
#include <functional>
#include <algorithm>
#include <iostream>
#include <future>
#include <list>
#include <array>
#include <utility>
#include <cstdlib>
#include "fmt/format.h"
#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_quiesce
#undef dout_prefix
#define dout_prefix *_dout << "== test == "

struct GenericVerboseErrorCode {
  int error_code;
  GenericVerboseErrorCode(int error_code) : error_code(std::abs(error_code)) {}
  auto operator<=>(const GenericVerboseErrorCode&) const = default;
};

template <class CharT, class Traits>
static std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os, const GenericVerboseErrorCode& ec)
{
  if (0 == ec.error_code) {
    return os << "Success(0)";
  } else {
    return os << std::generic_category().message(ec.error_code) << "(" << ec.error_code << ")";
  }
};

class QuiesceDbTest: public testing::Test {
  protected:
    template <class _Rep = std::chrono::seconds::rep, class _Period = std::chrono::seconds::period, typename D = std::chrono::duration<_Rep, _Period>, class Function, class... Args>
    static bool timed_run(D timeout, Function&& f, Args&&... args)
    {
      std::promise<void> done;
      auto future = done.get_future();

      auto job = std::bind(f, args...);

      auto tt = std::thread([job = std::move(job)](std::promise<void> done) {
        job();
        done.set_value();
      },
          std::move(done));

      tt.detach();

      return future.wait_for(timeout) != std::future_status::timeout;
    }
    struct TestQuiesceDbManager: public QuiesceDbManager
    {
      using QuiesceDbManager::QuiesceDbManager;
      using QuiesceDbManager::Db;
      Db& internal_db() {
        return db;
      }
      QuiesceClusterMembership& internal_membership() {
        return membership;
      }
      decltype(pending_requests)& internal_pending_requests() {
        return pending_requests;
      }
      decltype(awaits)& internal_awaits() {
        return awaits;
      }
      decltype(peers)& internal_peers() {
        return peers;
      }
    };

    epoch_t epoch = 0;
    std::map<QuiesceInterface::PeerId, std::unique_ptr<TestQuiesceDbManager>> managers;

    std::mutex comms_mutex;
    std::condition_variable comms_cond;

    fs_cluster_id_t fs_id = 1;
    std::string fs_name = "a";

    std::unordered_map<QuiesceInterface::PeerId, QuiesceMap> latest_acks;
    using AckHook = std::function<bool(QuiesceInterface::PeerId, QuiesceMap&)>;
    std::list<std::pair<AckHook, std::promise<void>>> ack_hooks;

    std::future<void> add_ack_hook(AckHook&& predicate)
    {
      std::lock_guard l(comms_mutex);
      auto &&[_, promise] = ack_hooks.emplace_back(predicate, std::promise<void> {});
      return promise.get_future();
    }

    void SetUp() override {
      for (QuiesceInterface::PeerId r = mds_gid_t(1); r < mds_gid_t(11); r++) {
        managers[r].reset(new TestQuiesceDbManager());
      }
    }

    void TearDown() override
    {
      dout(6) << "\n tearing down the cluster" << dendl;
      // We want to cause the managers to destruct
      // before we have the last_request destructed.
      // We should remove entries from `managers` under the comms lock
      // to avoid race with attempts of messaging between the managers.
      // Then we actually clear the map, destructing the managers,
      // outside the lock: the destruction will join the db threads
      // which in turn migh attempt to send a message
      std::unique_lock l(comms_mutex);
      auto mgrs = std::move(managers);
      l.unlock();
      mgrs.clear();
    }

    void configure_cluster(std::vector<QuiesceInterface::PeerId> leader_and_replicas = { mds_gid_t(1), mds_gid_t(2), mds_gid_t(3) })
    {
      ++epoch;
      ASSERT_GE(leader_and_replicas.size(), 1);
      std::unordered_set<QuiesceInterface::PeerId> members(leader_and_replicas.begin(), leader_and_replicas.end());
      auto leader = leader_and_replicas[0];
      for (const auto &[this_peer, mgr] : managers) {
        QuiesceClusterMembership mem = {
          epoch,
          fs_id,
          fs_name,
          this_peer,
          leader,
          members,
          [epoch = this->epoch, this, leader, me = this_peer](auto recipient, auto listing) {
            std::unique_lock l(comms_mutex);
            if (epoch == this->epoch) {
              if (this->managers.contains(recipient)) {
                dout(10) << "listing from " << me << " (leader=" << leader << ") to " << recipient << " for version " << listing.db_version << " with " << listing.sets.size() << " sets" << dendl;

                ceph::bufferlist bl;
                encode(listing, bl);
                listing.clear();
                auto p = bl.cbegin();
                decode(listing, p);

                this->managers[recipient]->submit_peer_listing({me, std::move(listing)});
                comms_cond.notify_all();
                return 0;
              }
            }
            return -1;
          },
          [epoch = this->epoch, this, leader, me = this_peer](auto diff_map) {
            std::unique_lock l(comms_mutex);
            if (epoch == this->epoch) {
              if (this->managers.contains(leader)) {
                std::queue<std::promise<void>> done_hooks;
                dout(10) << "ack from " << me << " to the leader (" << leader << ") for version " << diff_map.db_version << " with " << diff_map.roots.size() << " roots" << dendl;
                auto [it, inserted] = latest_acks.insert({me, diff_map});
                if (!inserted) {
                  if (it->second.db_version == diff_map.db_version) {
                    if (it->second.roots == diff_map.roots) {
                      dout(1) << "WARNING: detected a potentialy redundant ack" << dendl;
                    }
                  }
                  it->second = diff_map;
                }
                for (auto it = ack_hooks.begin(); it != ack_hooks.end();) {
                  if (it->first(me, diff_map)) {
                    done_hooks.emplace(std::move(it->second));
                    it = ack_hooks.erase(it);
                  } else {
                    it++;
                  }
                }

                ceph::bufferlist bl;
                encode(diff_map, bl);
                diff_map.clear();
                auto p = bl.cbegin();
                decode(diff_map, p);

                this->managers[leader]->submit_peer_ack({me, std::move(diff_map)});
                comms_cond.notify_all();
                l.unlock();
                while(!done_hooks.empty()) {
                  done_hooks.front().set_value();
                  done_hooks.pop();
                }
                return 0;
              }
            }
            return -1;
          }
        };
        mgr->update_membership(mem);
      }
      dout(6) << "\n === configured cluster with the following members, starting with the leader: " << leader_and_replicas << dendl;
    }

    struct TestRequestContext: public QuiesceDbManager::RequestContext, public C_SaferCond {
      void finish(int r) override { C_SaferCond::finish(r); }
      void complete(int r) override { C_SaferCond::complete(r); }

      const QuiesceDbTest& parent;
      TestRequestContext(const QuiesceDbTest& parent) : parent(parent) {}
      ~TestRequestContext() {
        wait();
      }

      bool start(std::invocable<QuiesceDbRequest&> auto const & c)
      {
        done = false;
        response.clear();
        request.reset(c);

        int rr = -1;

        for (auto& [rank, mgr] : parent.managers) {
          if (!(rr = mgr->submit_request(this))) {
            break;
          }
        }

        if (rr == EPERM) {
          // change the error to something never returned for a request
          // EPIPE seems reasonable as we couldn't find the leader to send the command to
          complete(EPIPE);
          return false;
        }

        return true;
      }

      GenericVerboseErrorCode check_result() {
        std::unique_lock l{lock};
        if (done) {
          return ERR(rval);
        }
        // this error is never returned by the manager
        return NA();
      }

      GenericVerboseErrorCode wait_result() {
        return ERR(wait());
      }

      GenericVerboseErrorCode wait_result_for(double seconds)
      {
        return ERR(wait_for(seconds));
      }
    };

    std::deque<std::unique_ptr<TestRequestContext>> requests;
    std::unique_ptr<TestRequestContext> last_request;

    const QuiesceDbManager::AgentCallback::Notify QUIESCING_AGENT_CB = [](QuiesceMap& quiesce_map) {
      dout(15) << "QUIESCING_AGENT_CB: notified with " << quiesce_map.roots.size() << " roots for version " << quiesce_map.db_version << dendl;
      for (auto it = quiesce_map.roots.begin(); it != quiesce_map.roots.end();) {
        switch (it->second.state) {
        case QS_QUIESCING:
          it->second.state = QS_QUIESCED;
          dout(10) << "QUIESCING_AGENT_CB: reporting '" << it->first << "' as " << it->second.state << dendl;
          it++;
          break;
        default:
          it = quiesce_map.roots.erase(it);
          break;
        }
      }
      return true;
    };

    const QuiesceDbManager::AgentCallback::Notify FAILING_AGENT_CB = [](QuiesceMap& quiesce_map) {
      dout(15) << "FAILING_AGENT_CB: notified with " << quiesce_map.roots.size() << " roots for version " << quiesce_map.db_version << dendl;
      for (auto it = quiesce_map.roots.begin(); it != quiesce_map.roots.end();) {
        switch (it->second.state) {
        case QS_QUIESCING:
          it->second.state = QS_FAILED;
          dout(10) << "FAILING_AGENT_CB: reporting '" << it->first << "' as " << it->second.state << dendl;
          it++;
          break;
        default:
          it = quiesce_map.roots.erase(it);
          break;
        }
      }
      return true;
    };

    const QuiesceDbManager::AgentCallback::Notify SILENT_AGENT_CB = [](QuiesceMap& quiesce_map) {
      dout(15) << "SILENT_AGENT_CB: nacking quiesce map version " << quiesce_map.db_version << " with " << quiesce_map.roots.size() << " roots" << dendl;
      return false;
    };

    GenericVerboseErrorCode
    run_request(std::invocable<QuiesceDbRequest&> auto const& c)
    {
      last_request.reset(new TestRequestContext(*this));
      last_request->start(c);
      return ERR(last_request->wait());
    }

    GenericVerboseErrorCode
    run_request_for(double seconds, std::invocable<QuiesceDbRequest&> auto const& c)
    {
      last_request.reset(new TestRequestContext(*this));
      last_request->start(c);
      return ERR(last_request->wait_for(seconds));
    }

    TestRequestContext& start_request(std::invocable<QuiesceDbRequest&> auto const& c)
    {
      auto &ptr = requests.emplace_back(new TestRequestContext(*this));
      ptr->start(c);
      return *ptr;
    }

    TestQuiesceDbManager::Db& db(QuiesceInterface::PeerId peer) {
      return managers[peer]->internal_db();
    }

    static GenericVerboseErrorCode ERR(int val) {
      return GenericVerboseErrorCode(val);
    }
    static GenericVerboseErrorCode OK()
    {
      return ERR(0);
    }
    static GenericVerboseErrorCode NA() {
      return ERR(EBUSY);
    }

    static QuiesceTimeInterval sec(double val) {
      return std::chrono::duration_cast<QuiesceTimeInterval>(std::chrono::duration<double>(val));
    }
};

/* ================================================================ */
TEST_F(QuiesceDbTest, ManagerStartup) {
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1) }));
  ASSERT_EQ(OK(), run_request_for(100, [](auto& r) {}));
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(2) }));
  ASSERT_EQ(OK(), run_request_for(100, [](auto& r) {}));
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1), mds_gid_t(2) }));
  ASSERT_EQ(OK(), run_request_for(100, [](auto& r) {}));
}

/* ================================================================ */
TEST_F(QuiesceDbTest, SetCreation) {
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1) }));

  // create a named set by resetting roots
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set0";
    r.reset_roots({"root1"});
  }));

  // the set must have timed out immediately since we haven't configured
  // the expiration timeout.
  ASSERT_TRUE(last_request->response.sets.contains("set0"));
  EXPECT_EQ(QS_TIMEDOUT, last_request->response.sets.at("set0").rstate.state);
  EXPECT_TRUE(db(mds_gid_t(1)).sets.contains(*last_request->request.set_id));

  // create a named set by including roots
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.include_roots({"root1"});
  }));

  // the set must have timed out immediately since we haven't configured
  // the expiration timeout. 
  ASSERT_TRUE(last_request->response.sets.contains("set1"));
  EXPECT_EQ(QS_TIMEDOUT, last_request->response.sets.at("set1").rstate.state);
  EXPECT_TRUE(db(mds_gid_t(1)).sets.contains(*last_request->request.set_id));

  // create a new unique set by including roots
  EXPECT_EQ(OK(), run_request([](auto& r) {
    r.include_roots({"root2"});
  }));

  // the manager must have filled the set id with a unique value
  ASSERT_TRUE(last_request->request.set_id.has_value());
  EXPECT_TRUE(db(mds_gid_t(1)).sets.contains(*last_request->request.set_id));

  // create a new unique set by resetting roots
  EXPECT_EQ(OK(), run_request([](auto& r) {
    r.reset_roots({"root2"});
  }));

  // the manager must have filled the set id with a unique value
  ASSERT_TRUE(last_request->request.set_id.has_value());
  EXPECT_TRUE(db(mds_gid_t(1)).sets.contains(*last_request->request.set_id));

  // prevent modification of a named set when a new set is desired
  EXPECT_EQ(ERR(ESTALE), run_request([](auto& r) {
    r.set_id = "set1";
    r.if_version = 0;
    r.roots.emplace("root3");
  }));
  EXPECT_EQ(1, last_request->response.sets.size());
  EXPECT_TRUE(last_request->response.sets.contains("set1"));

  EXPECT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set2";
    r.if_version = 0;
    r.roots.emplace("root4");
  }));

  EXPECT_EQ(1, last_request->response.sets.size());
  EXPECT_TRUE(last_request->response.sets.contains("set2"));
  EXPECT_EQ(QS_TIMEDOUT, last_request->response.sets.at("set2").rstate.state);

  // let's try to create a new named but expect it to have non-zero version
  EXPECT_EQ(ERR(ENOENT), run_request([](auto& r) {
    r.set_id = "set3";
    r.if_version = 1;
    r.roots.emplace("root4");
  }));

  EXPECT_EQ(0, last_request->response.sets.size());

  // let's try to create a new anonymous but expect it to have non-zero version
  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.if_version = 2;
    r.roots.emplace("root4");
  }));

  EXPECT_EQ(0, last_request->response.sets.size());

  // an empty string is a valid set id.
  EXPECT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "";
    r.roots.emplace("root1");
  }));
}

template<class T>
constexpr
std::array<std::optional<T>, 2> nullopt_and_default() {
  return {std::nullopt, T{}};
}

template<class F, class... V, size_t... S>
  requires std::invocable<F, V...>
void cartesian_apply(F func, std::array<V, S> const & ... array_args) {
  // inspired by https://stackoverflow.com/a/31169617/5171225

  // the iteration count is a product of all array sizes
  const long long N = (S * ...);

  for (long long n = 0; n < N; ++n) {
    std::lldiv_t q { n, 0 };

    // we use parameter pack expansion as part of the brace initializer
    // to perform sequential calculation of the 
    auto apply_tuple = std::tuple<V const &...> { 
      (q = div(q.quot, array_args.size()), array_args.at(q.rem)) 
      ... 
    };

    if (!std::apply(func, apply_tuple)) {
      return;
    }
  }
}

template<class... Args>
void coutall(Args&&... args) {
  int dummy[sizeof...(args)] = { (std::cout << args, std::cout << " ", 0)... };
  std::cout << std::endl;
}

TEST_F(QuiesceDbTest, QuiesceRequestValidation)
{

  auto checkRequest = [](
    decltype(std::declval<QuiesceDbRequest>().control.roots_op) const& op,
    decltype(std::declval<QuiesceDbRequest>().set_id)           const& set_id,
    decltype(std::declval<QuiesceDbRequest>().if_version)       const& if_version,
    decltype(std::declval<QuiesceDbRequest>().timeout)          const& timeout,
    decltype(std::declval<QuiesceDbRequest>().expiration)       const& expiration,
    decltype(std::declval<QuiesceDbRequest>().await)            const& await,
    decltype(std::declval<QuiesceDbRequest>().roots)            const& roots) {
      QuiesceDbRequest r;
      r.control.roots_op = op;
      r.set_id = set_id;
      r.if_version = if_version;
      r.timeout = timeout;
      r.expiration = expiration;
      r.await = await;
      r.roots = roots;

      if (op >= QuiesceDbRequest::RootsOp::__INVALID) {
        EXPECT_FALSE(r.is_valid())
          << "op: " << r.op_string() << ", set_id: " << bool(set_id) 
          << ", if_version: " << bool(if_version) 
          << ", timeout: " << bool(timeout) << ", expiration: " 
          << bool(expiration) << ", await: " 
          << bool(await) << ", roots.size(): " << roots.size();
      } else {
        // if set id is provided, all goes
        if (set_id) {
          EXPECT_TRUE(r.is_valid())
            << "op: " << r.op_string() << ", set_id: " << bool(set_id) 
            << ", if_version: " << bool(if_version) 
            << ", timeout: " << bool(timeout) << ", expiration: " 
            << bool(expiration) << ", await: " 
            << bool(await) << ", roots.size(): " << roots.size();
        } else {
          // without the set id we can create a new set
          // or perform operations on all sets
          if (roots.size() > 0) {
            // if roots are provided, we assume creation
            // all combinations are valid unless it's an exclude,
            // which doesn't make sense without a set id
            EXPECT_NE(r.is_exclude(), r.is_valid())
              << "op: " << r.op_string() << ", set_id: " << bool(set_id) 
              << ", if_version: " << bool(if_version) 
              << ", timeout: " << bool(timeout) << ", expiration: " 
              << bool(expiration) << ", await: " 
              << bool(await) << ", roots.size(): " << roots.size();
          } else {
            // means it's a query or a "cancel all"
            // no other parameters should be set
            if (if_version || timeout || expiration || await) {
              EXPECT_FALSE(r.is_valid())
                << "op: " << r.op_string() << ", set_id: " << bool(set_id) 
                << ", if_version: " << bool(if_version) 
                << ", timeout: " << bool(timeout) << ", expiration: " 
                << bool(expiration) << ", await: " 
                << bool(await) << ", roots.size(): " << roots.size();
            } else {
              EXPECT_NE(r.is_release(), r.is_valid())
                << "op: " << r.op_string() << ", set_id: " << bool(set_id) 
                << ", if_version: " << bool(if_version) 
                << ", timeout: " << bool(timeout) << ", expiration: " 
                << bool(expiration) << ", await: " 
                << bool(await) << ", roots.size(): " << roots.size();
            }
          }
        }
      }

      return !testing::Test::HasFailure();
  };

  const auto ops = std::array { QuiesceDbRequest::RootsOp::INCLUDE_OR_QUERY, QuiesceDbRequest::RootsOp::EXCLUDE_OR_RELEASE, QuiesceDbRequest::RootsOp::RESET_OR_CANCEL, QuiesceDbRequest::RootsOp::__INVALID };
  const auto strings = nullopt_and_default<std::string>();
  const auto versions = nullopt_and_default<QuiesceSetVersion>();
  const auto intervals = nullopt_and_default<QuiesceTimeInterval>();
  const auto roots = std::array { QuiesceDbRequest::Roots {}, QuiesceDbRequest::Roots { "root1" } };

  cartesian_apply(checkRequest,
      ops, strings, versions, intervals, intervals, intervals, roots);
}

/* ================================================================ */
TEST_F(QuiesceDbTest, RootSanitization)
{
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1) }));
  // a positive test with all kinds of expected fixes
  ASSERT_EQ(OK(), run_request([this](auto& r) {
    r.set_id = "set1";
    r.include_roots({
      "file:root1",
      fmt::format("file://{}/root2", fs_id),
      fmt::format("//{}/root3", fs_name),
      fmt::format("inode://{}/4", fs_id),
      fmt::format("inode://{}/5", fs_name),
      "inode:18446744073709551615",
      "inode:/18446744073709551614",
      "inode:/18446744073709551613/",
      "root6/.///./..////root6//"
    });
  }));

  EXPECT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("file:/root1"));
  EXPECT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("file:/root2"));
  EXPECT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("file:/root3"));
  EXPECT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("inode:4"));
  EXPECT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("inode:5"));
  EXPECT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("inode:18446744073709551615"));
  EXPECT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("inode:18446744073709551614"));
  EXPECT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("inode:18446744073709551613"));
  EXPECT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("file:/root6/root6"));

  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.include_roots({
      "//10/root1"
    });
  }));
  EXPECT_EQ(0, last_request->response.sets.size());

  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.set_id = "badset";
    r.include_roots({
      "//badfsname/root1"
    });
  }));
  EXPECT_EQ(0, last_request->response.sets.size());

  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.set_id = "badset";
    r.include_roots({
      "inode://badfsname/1"
    });
  }));
  EXPECT_EQ(0, last_request->response.sets.size());

  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.set_id = "badset";
    r.include_roots({
      "inode:-4"
    });
  }));
  EXPECT_EQ(0, last_request->response.sets.size());

  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.set_id = "badset";
    r.include_roots({
      "inode:18446744073709551616" // too big to fit a uint64_t
    });
  }));
  EXPECT_EQ(0, last_request->response.sets.size());

  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.set_id = "badset";
    r.include_roots({
      "inode:1/2/3/4"
    });
  }));
  EXPECT_EQ(0, last_request->response.sets.size());

  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.set_id = "badset";
    r.include_roots({
      "inode:abcd"
    });
  }));
  EXPECT_EQ(0, last_request->response.sets.size());

  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.set_id = "badset";
    r.include_roots({
      "inode:123-456"
    });
  }));
  EXPECT_EQ(0, last_request->response.sets.size());

  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.set_id = "badset";
    r.include_roots({
      "inode:"
    });
  }));
  EXPECT_EQ(0, last_request->response.sets.size());

  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.set_id = "badset";
    r.include_roots({
      "inode:0" // zero is an invalid inodeno
    });
  }));
  EXPECT_EQ(0, last_request->response.sets.size());
}

/* ================================================================ */
TEST_F(QuiesceDbTest, SetModification)
{
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1) }));

  // create a named set by including roots
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.timeout = sec(60);
    r.expiration = sec(60);
    r.include_roots({"root1"});
  }));

  ASSERT_TRUE(db(mds_gid_t(1)).sets.contains("set1"));
  ASSERT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("file:/root1"));

  // include more roots
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.include_roots({"root2", "root3"});
  }));

  ASSERT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("file:/root2"));
  ASSERT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("file:/root3"));
  ASSERT_EQ(db(mds_gid_t(1)).sets.at("set1").members.size(), 3);

  auto latest_v = last_request->response.sets.at("set1").version;

  // including present roots shouldn't bump the version
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.include_roots({ "root2", "root3" });
  }));

  ASSERT_EQ(latest_v, last_request->response.sets.at("set1").version);
  ASSERT_EQ(latest_v, db(mds_gid_t(1)).sets.at("set1").version);

  // resetting to the same roots shouldn't bump the version
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.reset_roots({ "root1","root2", "root3" });
  }));

  ASSERT_EQ(latest_v, last_request->response.sets.at("set1").version);
  ASSERT_EQ(latest_v, db(mds_gid_t(1)).sets.at("set1").version);

  // exclude roots
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.exclude_roots({ "root1", "root4" }); // root4 wasn't included, noop
  }));

  // the db doesn't delete set memebers, only marks them as excluded
  ASSERT_EQ(db(mds_gid_t(1)).sets.at("set1").members.size(), 3);
  ASSERT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.at("file:/root1").excluded);
  ASSERT_FALSE(db(mds_gid_t(1)).sets.at("set1").members.at("file:/root2").excluded);
  ASSERT_FALSE(db(mds_gid_t(1)).sets.at("set1").members.at("file:/root3").excluded);
  ASSERT_FALSE(db(mds_gid_t(1)).sets.at("set1").members.contains("file:/root4"));

  // reset roots
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.reset_roots({"root4"});
  }));

  ASSERT_EQ(db(mds_gid_t(1)).sets.at("set1").members.size(), 4);
  ASSERT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.at("file:/root1").excluded);
  ASSERT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.at("file:/root2").excluded);
  ASSERT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.at("file:/root3").excluded);
  ASSERT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.contains("file:/root4"));
  ASSERT_FALSE(db(mds_gid_t(1)).sets.at("set1").members.at("file:/root4").excluded);

  // reset is an including op, should allow creating a set with it
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set2";
    r.timeout = sec(60);
    r.expiration = sec(60);
    r.reset_roots({"root5"});
  }));

  ASSERT_FALSE(db(mds_gid_t(1)).sets.at("set2").members.at("file:/root5").excluded);

  // cancel with no set_id should cancel all active sets
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.control.roots_op = QuiesceDbRequest::RootsOp::RESET_OR_CANCEL;
  }));

  ASSERT_TRUE(db(mds_gid_t(1)).sets.at("set1").members.at("file:/root4").excluded);
  ASSERT_TRUE(db(mds_gid_t(1)).sets.at("set2").members.at("file:/root5").excluded);

  ASSERT_EQ(QuiesceState::QS_CANCELED, db(mds_gid_t(1)).sets.at("set1").rstate.state);
  ASSERT_EQ(QuiesceState::QS_CANCELED, db(mds_gid_t(1)).sets.at("set2").rstate.state);

  // reset can be used to resurrect a set from a terminal state
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.timeout = sec(60);
    r.expiration = sec(60);
    r.reset_roots({ "root5" });
  }));

  ASSERT_EQ(QuiesceState::QS_QUIESCING, db(mds_gid_t(1)).sets.at("set1").rstate.state);
}

/* ================================================================ */
TEST_F(QuiesceDbTest, Timeouts) {
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1) }));

  // install the agent callback to reach the QUIESCED state
  managers.at(mds_gid_t(1))->reset_agent_callback(QUIESCING_AGENT_CB);

  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.timeout = sec(0.1);
    r.expiration = sec(0.1);
    r.include_roots({"root1"});
    r.await = sec(1);
  }));

  ASSERT_EQ(QuiesceState::QS_QUIESCED, last_request->response.sets.at("set1").rstate.state);

  std::this_thread::sleep_for(sec(0.15));

  ASSERT_EQ(QuiesceState::QS_EXPIRED, db(mds_gid_t(1)).sets.at("set1").rstate.state);

  // reset can be used to resurrect a set from a terminal state
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.reset_roots({ "root5" });
  }));
  ASSERT_EQ(QuiesceState::QS_QUIESCING, last_request->response.sets.at("set1").rstate.state);

  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set2";
    r.timeout = sec(0.1);
    r.expiration = sec(0.1);
    r.include_roots({ "root1" });
    r.await = sec(1);
  }));

  ASSERT_EQ(QuiesceState::QS_QUIESCED, last_request->response.sets.at("set2").rstate.state);

  // prevent the db agent from acking the roots
  managers.at(mds_gid_t(1))->reset_agent_callback(SILENT_AGENT_CB);

  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set2";
    r.release_roots();
  }));

  ASSERT_EQ(QuiesceState::QS_RELEASING, last_request->response.sets.at("set2").rstate.state);

  std::this_thread::sleep_for(sec(0.15));

  ASSERT_EQ(QuiesceState::QS_EXPIRED, db(mds_gid_t(1)).sets.at("set2").rstate.state);

  // reset can be used to resurrect a set from a terminal state
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set2";
    r.reset_roots({ "root1" });
  }));
  ASSERT_EQ(QuiesceState::QS_QUIESCING, last_request->response.sets.at("set2").rstate.state);

  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set3";
    r.timeout = sec(0.1);
    r.include_roots({ "root1" });
  }));

  ASSERT_EQ(QuiesceState::QS_QUIESCING, db(mds_gid_t(1)).sets.at("set3").rstate.state);

  std::this_thread::sleep_for(sec(0.15));

  ASSERT_EQ(QuiesceState::QS_TIMEDOUT, db(mds_gid_t(1)).sets.at("set3").rstate.state);  // reset can be used to resurrect a set from a terminal state
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set3";
    r.reset_roots({ "root1" });
  }));
  ASSERT_EQ(QuiesceState::QS_QUIESCING, last_request->response.sets.at("set3").rstate.state);
}

/* ================================================================ */
TEST_F(QuiesceDbTest, Failures) {
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1) }));

  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.timeout = sec(0.1);
    r.expiration = sec(0.1);
    r.include_roots({"root1"});
  }));

  EXPECT_EQ(QuiesceState::QS_QUIESCING, last_request->response.sets.at("set1").rstate.state);

  {
    // wait for the agent to ack root1 as failed
    auto did_ack = add_ack_hook([](auto rank, auto const& ack) {
      return ack.roots.contains("file:/root1") && ack.roots.at("file:/root1").state == QS_FAILED;
    });

    // allow acks
    managers.at(mds_gid_t(1))->reset_agent_callback(FAILING_AGENT_CB);

    EXPECT_EQ(std::future_status::ready, did_ack.wait_for(std::chrono::milliseconds(100)));
  }

  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
  }));

  EXPECT_EQ(QuiesceState::QS_FAILED, db(mds_gid_t(1)).sets.at("set1").rstate.state);
  EXPECT_EQ(QuiesceState::QS_FAILED, last_request->response.sets.at("set1").rstate.state);

  ASSERT_EQ(ERR(EBADF), run_request([](auto& r) {
    r.set_id = "set2";
    r.timeout = sec(0.1);
    r.expiration = sec(0.1);
    r.include_roots({ "root1" });
    r.await = sec(1);
  }));
}

/* ================================================================ */
TEST_F(QuiesceDbTest, InterruptedQuiesceAwait)
{
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1) }));

  auto then = QuiesceClock::now();

  // await timeout should result in a EINPROGRESS given that the set
  // isn't modified in the meantime
  ASSERT_EQ(ERR(EINPROGRESS), run_request([](auto& r) {
    r.set_id = "set1";
    r.timeout = sec(100);
    r.roots.emplace("root1");
    r.await = sec(0.1);
  }));

  ASSERT_EQ(QuiesceState::QS_QUIESCING, db(mds_gid_t(1)).sets.at("set1").rstate.state);
  ASSERT_GE(QuiesceClock::now() - then, *last_request->request.await);

  // start an asyncrhonous await request
  auto & await = start_request([](auto& r) {
    r.set_id = "set1";
    r.await = sec(100);
  });

  // flush the pending requests by running a simple query
  EXPECT_EQ(OK(), run_request([](auto& r) { r.query("set1"); }));

  // still running
  EXPECT_EQ(NA(), await.check_result());

  // modify the set but don't change roots 
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.expiration = sec(100);
    r.timeout = sec(10);
    r.roots.emplace("root1");
  }));

  // should still be running
  EXPECT_EQ(NA(), await.check_result());

  // add another set
  then = QuiesceClock::now();
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set2";
    r.timeout = sec(0.1);
    r.roots.emplace("root1");
  }));

  // should still be running
  EXPECT_EQ(NA(), await.check_result());

  // modify roots
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.roots.emplace("root2");
  }));

  EXPECT_EQ(ERR(EINTR), await.wait_result());

  // start async await on set2
  auto & await2 = start_request([](auto& r) {
    r.set_id = "set2";
    r.await = sec(100);
  });

  // should be running
  EXPECT_EQ(NA(), await2.check_result());

  // and another one, this time wait for it to finish
  ASSERT_EQ(ERR(ETIMEDOUT), run_request([](auto& r) {
    r.set_id = "set2";
    r.await = sec(100);
  }));

  // the other await on the same set must have finished with the same result
  EXPECT_EQ(ERR(ETIMEDOUT), await2.wait_result());

  // shouldn't have taken much longer than the timeout configured on the set
  auto epsilon = sec(0.01);
  ASSERT_LE(QuiesceClock::now() - then - epsilon, last_request->response.sets.at("set2").timeout);

  // let's cancel set 1 while awaiting it a few times

  // start async await on set1
  auto& await3 = start_request([](auto& r) {
    r.set_id = "set1";
    r.await = sec(100);
  });

  auto& await4 = start_request([](auto& r) {
    r.set_id = "set1";
    r.await = sec(100);
  });

  // should be running
  EXPECT_EQ(NA(), await3.check_result());
  EXPECT_EQ(NA(), await4.check_result());

  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.reset_roots({});
  }));

  EXPECT_EQ(ERR(ECANCELED), await3.wait_result());
  EXPECT_EQ(ERR(ECANCELED), await4.wait_result());

  // awaiting a set in a terminal state should immediately
  // complete with the corresponding error
  ASSERT_EQ(ERR(ECANCELED), run_request([](auto& r) {
    r.set_id = "set1";
    r.await = sec(100);
  }));
  ASSERT_EQ(ERR(ETIMEDOUT), run_request([](auto& r) {
    r.set_id = "set2";
    r.await = sec(100);
  }));
}

/* ================================================================ */
TEST_F(QuiesceDbTest, RepeatedQuiesceAwait) {
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1) }));

  // let us reach quiescing
  managers.at(mds_gid_t(1))->reset_agent_callback(QUIESCING_AGENT_CB);

  // pick an expiration timeout
  auto expiration = sec(0.1);

  // create a set and let it quiesce
  ASSERT_EQ(OK(), run_request([=](auto& r) {
    r.set_id = "set1";
    r.timeout = sec(0.1);
    r.expiration = expiration;
    r.roots.emplace("root1");
    r.await = QuiesceTimeInterval::max();
  }));

  EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at("set1").rstate.state);

  // sleep for half the expiration interval multiple times
  // each time sending another await request
  // the expectation is that every time we call await
  // the expiration timer is reset, hence we should be able to
  // sustain the loop for arbitrarily long
  for (int i = 0; i < 10; i++) {
    std::this_thread::sleep_for(expiration/2);
    ASSERT_EQ(OK(), run_request([i](auto& r) {
      r.set_id = "set1";
      if (i % 2) {
        // this shouldn't affect anything
        r.reset_roots({"root1"});
      }
      r.await = sec(0);
    }));
  }

  // Prevent the set from reaching the RELEASED state
  managers.at(mds_gid_t(1))->reset_agent_callback(SILENT_AGENT_CB);

  // start releasing and observe that the timer isn't reset in this case,
  // so after a few EINPROGRESS we eventually reach timeout due to expiration
  for (int i = 0; i < 2; i++) {
    ASSERT_EQ(ERR(EINPROGRESS), run_request([=](auto& r) {
      r.set_id = "set1";
      r.release_roots();
      r.await = (expiration*2)/5;
    }));
  }

  // NB: the ETIMEDOUT is the await result, while the set itself should be EXPIRED
  EXPECT_EQ(ERR(ETIMEDOUT), run_request([=](auto& r) {
    r.set_id = "set1";
    r.release_roots();
    r.await = expiration;
  }));

  EXPECT_EQ(QS_EXPIRED, last_request->response.sets.at("set1").rstate.state);

  EXPECT_EQ(ERR(ETIMEDOUT), run_request([](auto& r) {
    r.set_id = "set1";
    r.await = sec(0.1);
  }));

  EXPECT_EQ(ERR(EPERM), run_request([](auto& r) {
    r.set_id = "set1";
    r.release_roots();
  }));

}

/* ================================================================ */
TEST_F(QuiesceDbTest, ReleaseAwait)
{
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1) }));

  // create some sets
  for (auto&& set_id : { "set1", "set2", "set3" }) {
    ASSERT_EQ(OK(), run_request([set_id](auto& r) {
      r.set_id = set_id;
      r.timeout = sec(100);
      r.expiration = sec(100);
      r.include_roots({ "root1", "root2"});
    })) << "creating " << set_id;
    EXPECT_EQ(QS_QUIESCING, last_request->response.sets.at(set_id).rstate.state);
  }

  // we shouldn't be able to release-await a quiescing set
  for (auto&& set_id : { "set1", "set2" }) {
    ASSERT_EQ(ERR(EPERM), run_request([set_id](auto& r) {
      r.set_id = set_id;
      r.release_roots();
      r.await = sec(1);
    })) << "bad release-await " << set_id;
  }

  managers.at(mds_gid_t(1))->reset_agent_callback(QUIESCING_AGENT_CB);

  for (auto&& set_id : { "set1", "set2", "set3" }) {
    ASSERT_EQ(OK(), run_request([set_id](auto& r) {
      r.set_id = set_id;
      r.await = sec(0.1);
    })) << "quiesce-await " << set_id;
    EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at(set_id).rstate.state);
  }

  managers.at(mds_gid_t(1))->reset_agent_callback(SILENT_AGENT_CB);

  auto & release_await1 = start_request([](auto &r) {
    r.set_id = "set1";
    r.release_roots();
    r.await = sec(100);
  });

  auto& release_await2 = start_request([](auto& r) {
    r.set_id = "set2";
    r.release_roots();
    r.await = sec(100);
  });

  EXPECT_EQ(OK(), run_request([](auto &r){}));
  // releasing should be in progress
  EXPECT_EQ(NA(), release_await1.check_result());
  EXPECT_EQ(NA(), release_await2.check_result());
  EXPECT_EQ(QS_RELEASING, last_request->response.sets.at("set1").rstate.state);
  EXPECT_EQ(QS_RELEASING, last_request->response.sets.at("set2").rstate.state);
  auto releasing_v1 = last_request->response.sets.at("set1").version;

  // we can request release again without any version bump
  EXPECT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.release_roots();
  }));

  EXPECT_EQ(releasing_v1, last_request->response.sets.at("set1").version );

  // we can release-await with a short await timeout
  EXPECT_EQ(ERR(EINPROGRESS), run_request([](auto& r) {
    r.set_id = "set1";
    r.release_roots();
    r.await = sec(0.1);
  }));

  // we can't quiesce-await a set that's releasing
  EXPECT_EQ(ERR(EPERM), run_request([](auto& r) {
    r.set_id = "set1";
    r.await = sec(0.1);
  }));

  // shouldn't be able to add roots to a releasing set
  EXPECT_EQ(ERR(EPERM), run_request([](auto &r) {
    r.set_id = "set1";
    r.include_roots({"root3"});
  }));

  // still on the same set version
  EXPECT_EQ(releasing_v1, last_request->response.sets.at("set1").version );

  // it should be allowed to exclude roots from a releasing set
  EXPECT_EQ(OK(), run_request([](auto &r) {
    r.set_id = "set2";
    r.exclude_roots({"root2"});
  }));

  // the corresponding await must have been interrupted due to the change to the members
  EXPECT_EQ(ERR(EINTR), release_await2.wait_result_for(0.1));

  // still releasing
  EXPECT_EQ(QS_RELEASING, last_request->response.sets.at("set2").rstate.state);

  // await again
  auto& release_await22 = start_request([](auto& r) {
    r.set_id = "set2";
    r.release_roots();
    r.await = sec(100);
  });

  EXPECT_EQ(NA(), release_await22.check_result());

  // excluding the last root should cancel the set
  EXPECT_EQ(OK(), run_request([](auto &r) {
    r.set_id = "set2";
    r.exclude_roots({"root1"});
  }));

  EXPECT_EQ(ERR(ECANCELED), release_await22.wait_result_for(0.1));

  std::atomic<QuiesceState> root1_state(QS__INVALID);
  managers.at(mds_gid_t(1))->reset_agent_callback([&](auto &map){
    if (map.roots.contains("file:/root1")) {
      root1_state = map.roots.at("file:/root1").state;
      root1_state.notify_all();
    }
    return false;
  });

  // validate that root1 is still reported to the agents as QUIESCING
  // even though we are already releasing set1
  // this is because there is another set with this root which is not releasing
  EXPECT_TRUE(timed_run(sec(0.1), [&](){root1_state.wait(QS__INVALID);}));
  EXPECT_EQ(QS_QUIESCING, root1_state.load());

  // allow acks
  managers.at(mds_gid_t(1))->reset_agent_callback(QUIESCING_AGENT_CB);
  EXPECT_EQ(OK(), release_await1.wait_result_for(0.1));

  EXPECT_EQ(QS_RELEASED, release_await1.response.sets.at("set1").rstate.state);

  // it should be OK to request release or release-await on a RELEASED set
  EXPECT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.release_roots();
  }));

  EXPECT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.release_roots();
    r.await = sec(0.1);
  }));

  // it's invalid to send a release without a set id
  EXPECT_EQ(ERR(EINVAL), run_request([](auto& r) {
    r.release_roots();
  }));
}

/* ================================================================ */
TEST_F(QuiesceDbTest, LeaderShutdown)
{
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1) }));

  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.timeout = sec(60);
    r.expiration = sec(60);
    r.include_roots({ "root1" });
  }));

  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set2";
    r.timeout = sec(60);
    r.expiration = sec(60);
    r.include_roots({ "root2", "root3"});
  }));

  std::queue<TestRequestContext*> outstanding_awaits;
  std::queue<TestRequestContext*> pending_requests;

  // let's have several awaits pending
  for(auto&& set_id: {"set1", "set2"}) {
    for (int i=0; i<2; i++) {
      outstanding_awaits.emplace(&start_request([set_id](auto&r) {
        r.set_id = set_id;
        r.await = sec(100);
      }));
      EXPECT_EQ(NA(), outstanding_awaits.front()->check_result());
    }
  }

  // flush the pending requests by running a simple query
  EXPECT_EQ(OK(), run_request([](auto& r) { r.query("set1"); }));

  ASSERT_EQ(outstanding_awaits.size(), managers.at(mds_gid_t(1))->internal_awaits().size());

  std::mutex agent_mutex;
  std::condition_variable agent_cond;
  bool callback_reached = false;

  // block the db thread with a malicious agent callback
  managers.at(mds_gid_t(1))->reset_agent_callback([&](auto& map) {
    std::unique_lock l(agent_mutex);
    callback_reached = true;
    agent_cond.notify_all();
    l.unlock();
    std::this_thread::sleep_for(sec(0.1));
    return false;
  });

  {
    std::unique_lock l(agent_mutex);
    agent_cond.wait(l, [&]{return callback_reached;});
  }

  // now that the db thread is sleeping we can pile up some pending requests
  pending_requests.emplace(&start_request([](auto& r) {
    r.set_id = "set3";
    r.include_roots({"root4"});
  }));
  EXPECT_EQ(NA(), pending_requests.front()->check_result());

  pending_requests.emplace(&start_request([](auto& r) {
    r.set_id = "set4";
    r.include_roots({"root5"});
  }));
  EXPECT_EQ(NA(), pending_requests.front()->check_result());

  pending_requests.emplace(&start_request([](auto& r) {
    r.set_id = "set1";
    r.await = sec(100);
  }));
  EXPECT_EQ(NA(), pending_requests.front()->check_result());

  ASSERT_EQ(managers.at(mds_gid_t(1))->internal_pending_requests().size(), pending_requests.size());

  // reset the membership of the manager
  // this will block until the db thread exits
  managers.at(mds_gid_t(1))->update_membership({});

  // as of now all requests must have finished
  while(!outstanding_awaits.empty()) {
    auto& r = *outstanding_awaits.front();
    EXPECT_EQ(ERR(EINPROGRESS), r.check_result());
    outstanding_awaits.pop();
  }

  while (!pending_requests.empty()) {
    auto& r = *pending_requests.front();
    EXPECT_EQ(ERR(EPERM), r.check_result());
    pending_requests.pop();
  }
}

/* ================================================================ */
TEST_F(QuiesceDbTest, MultiRankQuiesce)
{
  ASSERT_NO_FATAL_FAILURE(configure_cluster({  mds_gid_t(1), mds_gid_t(2), mds_gid_t(3) }));

  std::vector<TestRequestContext*> awaits;

  // create and await several sets
  // we deliberately avoid setting the expiration timeout in this test
  for (auto&& set_id: {"set1", "set2", "set3"}) {
    awaits.emplace_back(&start_request([set_id](auto& r) {
      r.set_id = set_id;
      r.timeout = sec(100);
      r.include_roots({"root1"});
      r.await = sec(100);
    }));
  }

  // flush the pending requests by running a simple query
  ASSERT_EQ(OK(), run_request([](auto&r){r.query("set1");}));

  ASSERT_EQ(awaits.size(), managers.at(mds_gid_t(1))->internal_awaits().size());

  for (auto&& await: awaits) {
    EXPECT_EQ(NA(), await->check_result()) << await->request.set_id.value();
  }

  {
    std::unordered_set<QuiesceInterface::PeerId> peers_quiesced;
    auto did_ack = add_ack_hook([&](auto p, auto const &m) {
      if (m.roots.contains("file:/root1") && (m.roots.at("file:/root1").state == QS_QUIESCED)) {
        peers_quiesced.insert(p);
      }
      return peers_quiesced.size() >= 2;
    });

    // let two of the three peers ack quiescing of the root
    managers.at(mds_gid_t(1))->reset_agent_callback(QUIESCING_AGENT_CB);
    managers.at(mds_gid_t(2))->reset_agent_callback(QUIESCING_AGENT_CB);

    ASSERT_EQ(std::future_status::ready, did_ack.wait_for(std::chrono::milliseconds(100)));
  }

  // kick the db queue with a simple query
  ASSERT_EQ(OK(), run_request([](auto& r) { r.query("set1"); }));

  // should still be waiting for the last agent
  EXPECT_EQ(QS_QUIESCING, last_request->response.sets.at("set1").rstate.state);
  for (auto&& await: awaits) {
    EXPECT_EQ(NA(), await->check_result()) << await->request.set_id.value();
  }

  {
    // wait for the late peer to ack root1 as released
    auto did_ack = add_ack_hook([](auto gid, auto const& ack) {
      return gid == mds_gid_t(3) && ack.roots.contains("file:/root1") && ack.roots.at("file:/root1").state == QS_QUIESCED;
    });

    // allow acks
    managers.at(mds_gid_t(3))->reset_agent_callback(QUIESCING_AGENT_CB);

    EXPECT_EQ(std::future_status::ready, did_ack.wait_for(std::chrono::milliseconds(100)));
  }

  // kick the db queue with a simple query
  ASSERT_EQ(OK(), run_request([](auto& r) {}));

  // first three sets must be expired because they had 0 expiration
  EXPECT_EQ(QS_EXPIRED, last_request->response.sets.at("set1").rstate.state);
  EXPECT_EQ(QS_EXPIRED, last_request->response.sets.at("set2").rstate.state);
  EXPECT_EQ(QS_EXPIRED, last_request->response.sets.at("set3").rstate.state);

  // pending quiesce requests must have all completed successfully
  // even though some of the sets got expired immediately
  for (auto&& await : awaits) {
    EXPECT_EQ(OK(), await->check_result()) << await->request.set_id.value();
  }
}

/* ================================================================ */
TEST_F(QuiesceDbTest, MultiRankRelease)
{
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1), mds_gid_t(2), mds_gid_t(3) }));
  managers.at(mds_gid_t(1))->reset_agent_callback(QUIESCING_AGENT_CB);
  managers.at(mds_gid_t(2))->reset_agent_callback(QUIESCING_AGENT_CB);
  managers.at(mds_gid_t(3))->reset_agent_callback(QUIESCING_AGENT_CB);

  // quiesce two sets
  for (auto&& set_id : { "set1", "set2" }) {
    ASSERT_EQ(OK(), run_request([set_id](auto& r) {
      r.set_id = set_id;
      r.timeout = sec(60);
      r.expiration = sec(60);
      r.await = sec(100);
      r.include_roots({ "root1" });
    }));
    EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at(set_id).rstate.state);
  }

  auto quiesced_v = db(mds_gid_t(1)).sets.at("set1").version;

  // prevent one of the acks
  managers.at(mds_gid_t(2))->reset_agent_callback(SILENT_AGENT_CB);

  // release roots
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.release_roots();
  }));

  EXPECT_EQ(QS_RELEASING, last_request->response.sets.at("set1").rstate.state);
  auto releasing_v = last_request->response.sets.at("set1").version;
  ASSERT_NE(quiesced_v, releasing_v);

  auto &async_release = start_request([](auto& r) {
    r.set_id = "set2";
    r.await = sec(100);
    r.release_roots();
  });

  EXPECT_EQ(NA(), async_release.check_result());

  // shouldn't hurt to run release twice for set 1
  ASSERT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.release_roots();
  }));

  EXPECT_EQ(releasing_v, last_request->response.sets.at("set1").version);

  // we shouldn't be able to quiesce-await a releasing set
  ASSERT_EQ(ERR(EPERM), run_request_for(1, [](auto& r) {
    r.set_id = "set1";
    r.await = sec(100);
  }));

  auto latest_v = db(mds_gid_t(1)).set_version;

  // wait for all peers to sync version 
  {
    std::unique_lock l(comms_mutex);
    auto result = comms_cond.wait_for(l, std::chrono::milliseconds(100), [&] {
      auto min_v = std::min({ db(mds_gid_t(1)).set_version, db(mds_gid_t(2)).set_version, db(mds_gid_t(3)).set_version });
      return min_v >= latest_v;
    });
    ASSERT_TRUE(result);
  }

  // all replicas must agree
  for (auto&& gid : {mds_gid_t(1), mds_gid_t(2), mds_gid_t(3)}) {
    EXPECT_EQ(QS_RELEASING, db(gid).sets.at("set1").rstate.state) << "db of gid " << gid;
    EXPECT_EQ(QS_RELEASING, db(gid).sets.at("set2").rstate.state) << "db of gid " << gid;
  }

  // wait for the late peer to ack back
  auto did_ack = add_ack_hook([](auto gid, auto const &ack){
    return gid == mds_gid_t(2);
  });

  // allow acks
  managers.at(mds_gid_t(2))->reset_agent_callback(QUIESCING_AGENT_CB);

  EXPECT_EQ(std::future_status::ready, did_ack.wait_for(std::chrono::milliseconds(100)));

  ASSERT_EQ(OK(), run_request([](auto& r) { }));

  EXPECT_EQ(QS_RELEASED, last_request->response.sets.at("set1").rstate.state);
  EXPECT_EQ(QS_RELEASED, last_request->response.sets.at("set2").rstate.state);
  EXPECT_EQ(OK(), async_release.check_result());

  // validate that we can release-await RELEASED sets
  // but can't quiesce-await the same
  for (auto&& set_id : { "set1", "set2" }) {
    ASSERT_EQ(OK(), run_request([set_id](auto& r) {
      r.set_id = set_id;
      r.await = sec(100);
      r.release_roots();
    }));
    ASSERT_EQ(ERR(EPERM), run_request([set_id](auto& r) {
      r.set_id = set_id;
      r.await = sec(100);
    }));
  }
}

/* ================================================================ */
TEST_F(QuiesceDbTest, MultiRankRecovery)
{
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1), mds_gid_t(2), mds_gid_t(3) }));
  managers.at(mds_gid_t(1))->reset_agent_callback(QUIESCING_AGENT_CB);
  managers.at(mds_gid_t(2))->reset_agent_callback(QUIESCING_AGENT_CB);
  managers.at(mds_gid_t(3))->reset_agent_callback(QUIESCING_AGENT_CB);

  // quiesce two sets
  for (auto&& set_id : { "set1", "set2" }) {
    ASSERT_EQ(OK(), run_request([set_id](auto& r) {
      r.set_id = set_id;
      r.timeout = sec(60);
      r.expiration = sec(60);
      r.await = sec(100);
      r.include_roots({ "root1" });
    }));
    EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at(set_id).rstate.state);
  }


  auto did_ack41 = add_ack_hook([](auto gid, auto const &ack){
    return gid == mds_gid_t(4) && ack.db_version.set_version > 0;
  });

  // reconfigure the cluster so that a new member is assigned leader
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(4), mds_gid_t(2), mds_gid_t(3) }));

  EXPECT_EQ(std::future_status::ready, did_ack41.wait_for(std::chrono::milliseconds(2000)));

  // we expect the db to be populated since the new leader must have discovered newer versions
  // we expect the sets to become quiescing since there's at least one member that's not acking (the new one)
  EXPECT_EQ(OK(), run_request([](auto& r) {}));
  ASSERT_EQ(2, last_request->response.sets.size());
  EXPECT_EQ(QS_QUIESCING, last_request->response.sets.at("set1").rstate.state);
  EXPECT_EQ(QS_QUIESCING, last_request->response.sets.at("set2").rstate.state);

  // reconfigure the cluster back to quiescing members
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1), mds_gid_t(2), mds_gid_t(3) }));

  // we expect the db to be populated since the new leader must have discovered newer versions
  // we expect the sets to become quiescing since there's at least one member that's not acking (the new one)
  EXPECT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set1";
    r.await = sec(1);
  }));
  ASSERT_EQ(1, last_request->response.sets.size());
  EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at("set1").rstate.state);
  EXPECT_EQ(OK(), run_request([](auto& r) {
    r.set_id = "set2";
    r.await = sec(1);
  }));
  ASSERT_EQ(1, last_request->response.sets.size());
  EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at("set2").rstate.state);

  // lose a non-leader node
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1), mds_gid_t(2) }));

  EXPECT_EQ(OK(), run_request([](auto& r) {}));
  ASSERT_EQ(2, last_request->response.sets.size());
  EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at("set1").rstate.state);
  EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at("set2").rstate.state);

  auto did_ack3 = add_ack_hook([](auto gid, auto const &ack){
    return gid == mds_gid_t(3) && ack.db_version.set_version > 0;
  });

  // add back a quiescing peer
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1), mds_gid_t(2), mds_gid_t(3)}));

  EXPECT_EQ(OK(), run_request([](auto& r) {}));
  ASSERT_EQ(2, last_request->response.sets.size());
  EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at("set1").rstate.state);
  EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at("set2").rstate.state);

  EXPECT_EQ(std::future_status::ready, did_ack3.wait_for(std::chrono::milliseconds(2000)));

  EXPECT_EQ(OK(), run_request([](auto& r) {}));
  ASSERT_EQ(2, last_request->response.sets.size());
  EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at("set1").rstate.state);
  EXPECT_EQ(QS_QUIESCED, last_request->response.sets.at("set2").rstate.state);

  auto did_ack42 = add_ack_hook([](auto gid, auto const &ack){
    return gid == mds_gid_t(4) && ack.db_version.set_version > 0;
  });

  // add a non-quiescing peer
  ASSERT_NO_FATAL_FAILURE(configure_cluster({ mds_gid_t(1), mds_gid_t(2), mds_gid_t(3), mds_gid_t(4) }));

  EXPECT_EQ(std::future_status::ready, did_ack42.wait_for(std::chrono::milliseconds(2000)));
  EXPECT_EQ(OK(), run_request([](auto& r) {}));
  ASSERT_EQ(2, last_request->response.sets.size());
  EXPECT_EQ(QS_QUIESCING, last_request->response.sets.at("set1").rstate.state);
  EXPECT_EQ(QS_QUIESCING, last_request->response.sets.at("set2").rstate.state);
}