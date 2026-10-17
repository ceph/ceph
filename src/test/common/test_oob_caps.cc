// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "common/oob_config.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string>
#include <thread>

#include <sched.h>
#include <sys/capability.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/intrusive_ptr.hpp>

#include "common/ceph_context.h"
#include "common/config.h"
#include "include/msgr.h"
#include "gtest/gtest.h"

using ceph::oob::keep_caps;
using ceph::oob::keep_only_net_raw;
using ceph::oob::needs_net_raw;
using ceph::oob::osd_keeps_net_raw;
using ceph::oob::with_net_raw;
using ceph::rdma::parse_oob_transports;

// The kernel keeps capabilities for each thread. A test that changes
// them does so on a thread of its own, or in a child process (as_root(),
// or for a switch of user), so that the others see the test process as
// it started.

namespace {

/// whether the calling thread holds CAP_NET_RAW in a set
bool holds(cap_flag_t set)
{
  cap_t c = cap_get_proc();
  cap_flag_value_t v = CAP_CLEAR;
  if (c) {
    cap_get_flag(c, CAP_NET_RAW, set, &v);
    cap_free(c);
  }
  return v == CAP_SET;
}

/// the calling thread's capabilities, as cap_to_text() writes them
std::string caps_text()
{
  cap_t c = cap_get_proc();
  if (!c) {
    return "?";
  }
  char* t = cap_to_text(c, nullptr);
  std::string s = t ? t : "?";
  cap_free(t);
  cap_free(c);
  return s;
}

/// set CAP_NET_RAW in the calling thread's effective set, or clear it
/// there, or with all = true clear it in every set
int set_net_raw(cap_flag_value_t v, bool all = false)
{
  cap_t c = cap_get_proc();
  if (!c) {
    return -errno;
  }
  const cap_value_t raw[1] = {CAP_NET_RAW};
  int r = 0;
  if (cap_set_flag(c, CAP_EFFECTIVE, 1, raw, v) < 0 ||
      (all && (cap_set_flag(c, CAP_PERMITTED, 1, raw, v) < 0 ||
	       cap_set_flag(c, CAP_INHERITABLE, 1, raw, v) < 0)) ||
      cap_set_proc(c) < 0) {
    r = -errno;
  }
  cap_free(c);
  return r;
}

/// what as_root()'s child exits with when it holds no capability
constexpr int NO_ROOT = 78;

constexpr const char* NO_ROOT_SKIP =
  "needs root, or an unprivileged user namespace, which this system "
  "refuses";

/// Run body in a child process that holds root's capabilities: every
/// one, in a user namespace of its own, which needs no privilege, or
/// those of the test process when the kernel refuses the namespace and
/// they include CAP_NET_RAW. The child reports body's failures, and the
/// test fails with them. Returns false when the child holds none, as
/// where unprivileged user namespaces are disabled and the test does
/// not run as root.
bool as_root(const std::function<void()>& body)
{
  // nothing that is buffered now is written twice
  fflush(stdout);
  fflush(stderr);
  const pid_t pid = fork();
  if (pid < 0) {
    ADD_FAILURE() << "fork: " << strerror(errno);
    return true;
  }
  if (pid == 0) {
    // the child has one thread, which unshare() needs
    if (unshare(CLONE_NEWUSER) < 0) {
      const int e = errno;
      if (!holds(CAP_PERMITTED)) {
        fprintf(stderr, "unshare(CLONE_NEWUSER): %s\n", strerror(e));
        _exit(NO_ROOT);
      }
    }
    body();
    fflush(stdout);
    _exit(::testing::Test::HasFailure() ? 1 : 0);
  }
  int status = 0;
  EXPECT_EQ(pid, waitpid(pid, &status, 0));
  EXPECT_TRUE(WIFEXITED(status)) << status;
  if (WIFEXITED(status) && WEXITSTATUS(status) == NO_ROOT) {
    return false;
  }
  EXPECT_EQ(0, WEXITSTATUS(status)) << "the child process failed";
  return true;
}

/// what switch_user() returns when the switch itself fails, as in a
/// user namespace that maps no other user
constexpr int NO_SWITCH = 77;

/// In a child process: keep the capabilities across a switch to
/// nobody, keep only CAP_NET_RAW, permitted and not effective, then
/// raise it with with_net_raw() and check that it is effective only
/// there. Returns 0, or the step that failed.
int switch_user()
{
  std::string err;
  if (keep_caps(&err) < 0) {
    return 1;
  }
  if (setuid(65534) != 0) {
    return NO_SWITCH;
  }
  bool kept = false;
  if (keep_only_net_raw(&kept, &err) < 0 || !kept) {
    return 2;
  }
  // nothing else survives, and nothing is effective
  if (caps_text() != "cap_net_raw=p" || prctl(PR_GET_KEEPCAPS) != 0) {
    return 3;
  }
  bool raised = false;
  if (with_net_raw([&] { raised = holds(CAP_EFFECTIVE); }, &err) != 0 ||
      !raised) {
    return 4;
  }
  if (holds(CAP_EFFECTIVE) || !holds(CAP_PERMITTED)) {
    return 5;
  }
  return 0;
}

/// a context for a daemon of type that switches to uid
boost::intrusive_ptr<CephContext> context(uint32_t type, uid_t uid)
{
  boost::intrusive_ptr<CephContext> cct{new CephContext(type), false};
  cct->_conf._clear_safe_to_start_threads();  // so we can set configs
  cct->set_uid_gid(uid, uid);
  return cct;
}

} // anonymous namespace

TEST(OobCaps, NeedsNetRaw)
{
  EXPECT_TRUE(needs_net_raw("uet"));
  EXPECT_FALSE(needs_net_raw("tcp"));
  EXPECT_FALSE(needs_net_raw("verbs;ofi_rxm"));
  EXPECT_FALSE(needs_net_raw("shm"));
  EXPECT_FALSE(needs_net_raw(""));
  // the provider of the daemon's one endpoint, which the cluster
  // transport names before the client list does
  EXPECT_TRUE(needs_net_raw(parse_oob_transports("tcp", "uet").ofi_provider));
  EXPECT_FALSE(needs_net_raw(
    parse_oob_transports("tcp, uet", "").ofi_provider));
}

// global_init(): an OSD keeps it through --setuser when uet is the
// provider of its endpoint
TEST(OobCaps, OsdKeepsNetRaw)
{
  auto cct = context(CEPH_ENTITY_TYPE_OSD, 167);
  auto& conf = cct->_conf;
  conf.set_val("oob_client_transports", "tcp");
  conf.set_val("oob_cluster_transport", "uet");
  EXPECT_TRUE(osd_keeps_net_raw(cct.get()));
  // an OSD that stays root, or that keeps every capability through the
  // switch (set_keepcaps), has nothing to keep
  cct->set_uid_gid(0, 0);
  EXPECT_FALSE(osd_keeps_net_raw(cct.get()));
  cct->set_uid_gid(167, 167);
  conf.set_val("set_keepcaps", "true");
  EXPECT_FALSE(osd_keeps_net_raw(cct.get()));
  conf.set_val("set_keepcaps", "false");
  // without a cluster transport, the client list's first libfabric
  // provider
  conf.set_val("oob_cluster_transport", "");
  conf.set_val("oob_client_transports", "cuobj, uet");
  EXPECT_TRUE(osd_keeps_net_raw(cct.get()));
  conf.set_val("oob_client_transports", "tcp, uet");
  EXPECT_FALSE(osd_keeps_net_raw(cct.get()));
  // the cluster transport's provider, before the client list's
  conf.set_val("oob_client_transports", "uet");
  conf.set_val("oob_cluster_transport", "verbs");
  EXPECT_FALSE(osd_keeps_net_raw(cct.get()));
  conf.set_val("oob_client_transports", "");
  conf.set_val("oob_cluster_transport", "");
  EXPECT_FALSE(osd_keeps_net_raw(cct.get()));
  // transports that only the mon config selects: oob_keep_net_raw
  // keeps it all the same, unless set_keepcaps keeps everything
  conf.set_val("oob_keep_net_raw", "true");
  EXPECT_TRUE(osd_keeps_net_raw(cct.get()));
  conf.set_val("set_keepcaps", "true");
  EXPECT_FALSE(osd_keeps_net_raw(cct.get()));
  conf.set_val("set_keepcaps", "false");
  cct->set_uid_gid(0, 0);
  EXPECT_FALSE(osd_keeps_net_raw(cct.get()));
  cct->set_uid_gid(167, 167);
  conf.set_val("oob_keep_net_raw", "false");
  // another daemon
  auto other = context(CEPH_ENTITY_TYPE_CLIENT, 167);
  other->_conf.set_val("oob_cluster_transport", "uet");
  EXPECT_FALSE(osd_keeps_net_raw(other.get()));
  other->_conf.set_val("oob_keep_net_raw", "true");
  EXPECT_FALSE(osd_keeps_net_raw(other.get()));
}

// whatever the test process holds: fn runs once, the result says
// whether it had the capability, and the caller ends as it began. A
// thread that has nothing to raise runs fn itself
TEST(OobCaps, RunsOnce)
{
  const auto before = caps_text();
  const bool runs_here = holds(CAP_EFFECTIVE) || !holds(CAP_PERMITTED);
  int calls = 0;
  bool raised = false;
  std::thread::id ran_on;
  std::string err;
  const int r = with_net_raw([&] {
    ++calls;
    raised = holds(CAP_EFFECTIVE);
    ran_on = std::this_thread::get_id();
  }, &err);
  EXPECT_EQ(1, calls);
  EXPECT_EQ(r == 0, raised);
  EXPECT_EQ(r == 0, err.empty()) << err;
  EXPECT_EQ(runs_here, ran_on == std::this_thread::get_id());
  EXPECT_EQ(before, caps_text());
}

// a thread that does not hold it, as one of an OSD that switched user
// without keeping it: fn runs on it, without the capability
TEST(OobCaps, WithoutNetRaw)
{
  std::thread([] {
    ASSERT_EQ(0, set_net_raw(CAP_CLEAR, true));
    const auto self = std::this_thread::get_id();
    std::thread::id ran_on;
    bool raised = true;
    std::string err;
    EXPECT_EQ(-EPERM, with_net_raw([&] {
      ran_on = std::this_thread::get_id();
      raised = holds(CAP_EFFECTIVE);
    }, &err));
    EXPECT_EQ(self, ran_on);
    EXPECT_FALSE(raised);
    EXPECT_FALSE(err.empty());
    EXPECT_FALSE(holds(CAP_PERMITTED));
  }).join();
}

// a thread that holds it in its permitted set only, as one of an OSD
// that kept it through --setuser: fn runs on a thread that raises it,
// and the caller's effective set stays without it
TEST(OobCaps, RaisesFromPermitted)
{
  const bool ran = as_root([] {
    ASSERT_EQ(0, set_net_raw(CAP_CLEAR));
    ASSERT_TRUE(holds(CAP_PERMITTED));
    const auto before = caps_text();
    const auto self = std::this_thread::get_id();
    std::thread::id ran_on;
    bool raised = false;
    std::string err;
    EXPECT_EQ(0, with_net_raw([&] {
      ran_on = std::this_thread::get_id();
      raised = holds(CAP_EFFECTIVE);
    }, &err)) << err;
    EXPECT_NE(self, ran_on);
    EXPECT_TRUE(raised);
    EXPECT_EQ(before, caps_text());
    EXPECT_FALSE(holds(CAP_EFFECTIVE));
  });
  if (!ran) {
    GTEST_SKIP() << NO_ROOT_SKIP;
  }
}

TEST(OobCaps, Rethrows)
{
  std::string err;
  EXPECT_THROW(with_net_raw([] { throw std::runtime_error("open"); }, &err),
	       std::runtime_error);
  // from the thread that raises it, too
  const bool ran = as_root([] {
    ASSERT_EQ(0, set_net_raw(CAP_CLEAR));
    ASSERT_TRUE(holds(CAP_PERMITTED));
    std::thread::id ran_on;
    std::string err;
    EXPECT_THROW(with_net_raw([&] {
      ran_on = std::this_thread::get_id();
      throw std::runtime_error("open");
    }, &err), std::runtime_error);
    EXPECT_NE(std::this_thread::get_id(), ran_on);
    EXPECT_FALSE(holds(CAP_EFFECTIVE));
  });
  if (!ran) {
    GTEST_SKIP() << NO_ROOT_SKIP;
  }
}

// without a switch of user, from every capability: the thread keeps
// CAP_NET_RAW in its permitted set, and nothing else, as do the threads
// it starts, and no longer keeps its capabilities through a later switch
TEST(OobCaps, KeepOnlyNetRaw)
{
  const bool ran = as_root([] {
    ASSERT_TRUE(holds(CAP_PERMITTED));
    std::string err;
    ASSERT_EQ(0, keep_caps(&err)) << err;
    EXPECT_EQ(1, prctl(PR_GET_KEEPCAPS));
    bool kept = false;
    ASSERT_EQ(0, keep_only_net_raw(&kept, &err)) << err;
    EXPECT_TRUE(kept);
    EXPECT_EQ("cap_net_raw=p", caps_text());
    EXPECT_EQ(0, prctl(PR_GET_KEEPCAPS));
    std::thread([] {
      EXPECT_EQ("cap_net_raw=p", caps_text());
    }).join();
  });
  if (!ran) {
    GTEST_SKIP() << NO_ROOT_SKIP;
  }
}

// every capability but CAP_NET_RAW, as root has in a container that
// leaves it out: nothing is kept
TEST(OobCaps, KeepOnlyNetRawWithout)
{
  const bool ran = as_root([] {
    ASSERT_EQ(0, set_net_raw(CAP_CLEAR, true));
    std::string err;
    bool kept = true;
    ASSERT_EQ(0, keep_only_net_raw(&kept, &err)) << err;
    EXPECT_FALSE(kept);
    EXPECT_EQ("=", caps_text());
  });
  if (!ran) {
    GTEST_SKIP() << NO_ROOT_SKIP;
  }
}

// root that switches to another user keeps CAP_NET_RAW, and only it,
// and with_net_raw() raises it
TEST(OobCaps, SwitchUser)
{
  if (geteuid() != 0 || !holds(CAP_PERMITTED)) {
    GTEST_SKIP() << "needs root";
  }
  const pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    _exit(switch_user());
  }
  int status = 0;
  ASSERT_EQ(pid, waitpid(pid, &status, 0));
  ASSERT_TRUE(WIFEXITED(status));
  if (WEXITSTATUS(status) == NO_SWITCH) {
    GTEST_SKIP() << "cannot switch to uid 65534";
  }
  EXPECT_EQ(0, WEXITSTATUS(status));
}
