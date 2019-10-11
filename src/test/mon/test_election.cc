// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "mon/ElectionLogic.h"
#include "common/dout.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"

using namespace std;

#define dout_subsys ceph_subsys_test
#undef dout_prefix
#define dout_prefix _prefix(_dout, prefix_name())
static ostream& _prefix(std::ostream *_dout, const char *prefix) {
  return *_dout << prefix;
}

const char* prefix_name() { return "test_election: "; }

int main(int argc, char **argv) {
  vector<const char*> args(argv, argv+argc);
  bool user_set_debug = false;
  for (auto& arg : args) {
    if (strncmp("--debug_mon", arg, 11) == 0) user_set_debug = true;
  }
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  if (!user_set_debug) g_ceph_context->_conf.set_val("debug mon", "0/20");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


class Owner;
struct Election {
  map<int, Owner*> electors;
  map<int, set<int> > blocked_messages;
  int count;
  ElectionLogic::election_strategy election_strategy;
  set<int> disallowed_leaders;

  vector< function<void()> > messages;

  Election(int c, ElectionLogic::election_strategy es=ElectionLogic::CLASSIC);
  ~Election();
  // ElectionOwner interfaces
  int get_paxos_size() { return count; }
  const set<int>& get_disallowed_leaders() { return disallowed_leaders; }
  void propose_to(int from, int to, epoch_t e);
  void defer_to(int from, int to, epoch_t e);
  void claim_victory(int from, int to, epoch_t e, const set<int>& members);
  void accept_victory(int from, int to, epoch_t e);
  void queue_message(int from, int to, function<void()> m);

  // test runner interfaces
  int run_timesteps(int max);
  void start_one(int who);
  void start_all();
  bool election_stable();
  bool check_leader_agreement();
  bool check_epoch_agreement();
  void block_messages(int from, int to);
  void block_bidirectional_messages(int a, int b);
  void unblock_messages(int from, int to);
  void unblock_bidirectional_messages(int a, int b);
  void add_disallowed_leader(int disallowed) { disallowed_leaders.insert(disallowed); }
  const char* prefix_name() { return "Election:      "; }
};
struct Owner : public ElectionOwner {
  Election *parent;
  int rank;
  epoch_t persisted_epoch;
  bool ever_joined;
  ElectionLogic logic;
  set<int> quorum;
  int victory_accepters;
  int timer_steps; // timesteps until we trigger timeout
  bool timer_election; // the timeout is for normal election, or victory

 Owner(int r, ElectionLogic::election_strategy es,
       Election *p) : parent(p), rank(r), persisted_epoch(0),
    ever_joined(false),
    logic(this, g_ceph_context),
    victory_accepters(0),
    timer_steps(-1), timer_election(true) {
        logic.set_election_strategy(es);
  }
    
  // in-memory store: just save to variable
  void persist_epoch(epoch_t e) { persisted_epoch = e; }
  // in-memory store: just return variable
  epoch_t read_persisted_epoch() const { return persisted_epoch; }
  // in-memory store: don't need to validate
  void validate_store() { return; }
  // don't need to do anything with our state right now
  void notify_bump_epoch() {}
  // pass back to ElectionLogic; we don't need this redirect ourselves
  void trigger_new_election() { logic.start(); }
  int get_my_rank() const { return rank; }
  // we don't need to persist scores as we don't reset and lose memory state
  void persist_connectivity_scores() {}
  void propose_to_peers(epoch_t e) {
    for (int i = 0; i < parent->get_paxos_size(); ++i) {
      if (i == rank) continue;
      parent->propose_to(rank, i, e);
    }
  }
  void reset_election() {
    _start();
    logic.start();
  }
  bool ever_participated() const { return ever_joined; }
  unsigned paxos_size() const { return parent->get_paxos_size(); }
  const set<int>& get_disallowed_leaders() const { return parent->get_disallowed_leaders(); }
  void cancel_timer() {
    timer_steps = -1;
  }
  void reset_timer(int steps) {
    cancel_timer();
    timer_steps = 3 + steps; // FIXME? magic number, current step + roundtrip
    timer_election = true;
  }
  void start_victory_timer() {
    cancel_timer();
    timer_election = false;
    timer_steps = 3; // FIXME? current step + roundtrip
  }
  void _start() {
    reset_timer(0);
    quorum.clear();
  }
  void _defer_to(int who) {
    parent->defer_to(rank, who, logic.get_epoch());
    reset_timer(0);
  }
  void message_victory(const std::set<int>& members) {
    for (auto i : members) {
      if (i == rank) continue;
      parent->claim_victory(rank, i, logic.get_epoch(), members);
    }
    start_victory_timer();
    quorum = members;
    victory_accepters = 1;
  }
  bool is_current_member(int rank) const { return quorum.count(rank) != 0; }
  void receive_propose(int from, epoch_t e) {
    logic.receive_propose(from, e);
  }
  void receive_ack(int from, epoch_t e) {
    if (e < logic.get_epoch())
      return;
    logic.receive_ack(from, e);
  }
  void receive_victory_claim(int from, epoch_t e, const set<int>& members) {
    if (e < logic.get_epoch())
      return;
    if (logic.receive_victory_claim(from, e)) {
      quorum = members;
      cancel_timer();
      parent->accept_victory(rank, from, e);
    }
  }
  void receive_victory_ack(int from, epoch_t e) {
    if (e < logic.get_epoch())
      return;
    ++victory_accepters;
    if (victory_accepters == static_cast<int>(quorum.size())) {
      cancel_timer();
      for (int i : quorum) {
	parent->electors[i]->ever_joined = true;
      }
    }
  }
  void election_timeout() {
    ldout(g_ceph_context, 2) << "election epoch " << logic.get_epoch()
	 << " timed out for " << rank
	 << ", electing me:" << logic.electing_me
	 << ", acked_me:" << logic.acked_me << dendl;
    logic.end_election_period();
  }
  void victory_timeout() {
    ldout(g_ceph_context, 2) << "victory epoch " << logic.get_epoch()
	 << " timed out for " << rank
	 << ", electing me:" << logic.electing_me
	 << ", acked_me:" << logic.acked_me << dendl;
    reset_election();
  }
  void notify_timestep() {
    assert(timer_steps != 0);
    if (timer_steps > 0) {
      --timer_steps;
    }
    if (timer_steps == 0) {
      if (timer_election) {
	election_timeout();
      } else {
	victory_timeout();
      }
    }
  }
  const char *prefix_name() { return "Owner:         "; }
};

Election::Election(int c, ElectionLogic::election_strategy es) : count(c), election_strategy(es)
{
  for (int i = 0; i < count; ++i) {
    electors[i] = new Owner(i, election_strategy, this);
  }
}

Election::~Election()
{
  {
    for (auto i : electors) {
      delete i.second;
    }
  }
}

void Election::queue_message(int from, int to, function<void()> m)
{
  if (!blocked_messages[from].count(to)) {
    messages.push_back(m);
  }
}
void Election::defer_to(int from, int to, epoch_t e)
{
  Owner *o = electors[to];
  queue_message(from, to, [o, from, e] {
    o->receive_ack(from, e);
    });
}

void Election::propose_to(int from, int to, epoch_t e)
{
  Owner *o = electors[to];
  queue_message(from, to, [o, from, e] {
      o->receive_propose(from, e);
    });
}

void Election::claim_victory(int from, int to, epoch_t e, const set<int>& members)
{
  Owner *o = electors[to];
  queue_message(from, to, [o, from, e, members] {
      o->receive_victory_claim(from, e, members);
    });
}

void Election::accept_victory(int from, int to, epoch_t e)
{
  Owner *o = electors[to];
  queue_message(from, to, [o, from, e] {
      o->receive_victory_ack(from, e);
    });
}

int Election::run_timesteps(int max)
{
  vector< function<void()> > current_m;
  int steps = 0;
  for (; (!max || steps < max) && // we have timesteps left AND ONE OF
	 (!messages.empty() || // there are messages pending.
	  !election_stable()); // somebody's not happy and will act in future
       ++steps) {
    current_m.clear();
    current_m.swap(messages);
    for (auto& m : current_m) {
      m();
    }
    for (auto o : electors) {
      o.second->notify_timestep();
    }
  }

  return steps;
}

void Election::start_one(int who)
{
  assert(who < static_cast<int>(electors.size()));
  electors[who]->logic.start();
}

void Election::start_all() {
  for (auto e : electors) {
    e.second->logic.start();
  }
}

bool Election::election_stable()
{
  // see if anybody has a timer running
  for (auto i : electors) {
    if (i.second->timer_steps != -1)
      return false;
  }
  return true;
}

bool Election::check_leader_agreement()
{
  int leader = electors[0]->logic.get_election_winner();
  ldout(g_ceph_context, 10) << "check_leader_agreement on " << leader << dendl;
  for (auto i: electors) {
    if (leader != i.second->logic.get_election_winner()) {
      ldout(g_ceph_context, 10) << "rank " << i.first << " has different leader "
				<< i.second->logic.get_election_winner() << dendl;
      return false;
    }
  }
  if (disallowed_leaders.count(leader)) {
    ldout(g_ceph_context, 10) << "that leader is disallowed! member of "
			      << disallowed_leaders << dendl;
    return false;
  }
  return true;
}

bool Election::check_epoch_agreement()
{
  epoch_t epoch = electors[0]->logic.get_epoch();
  for (auto i : electors) {
    if (epoch != i.second->logic.get_epoch()) {
      return false;
    }
  }
  return true;
}

void Election::block_messages(int from, int to)
{
  blocked_messages[from].insert(to);
}
void Election::block_bidirectional_messages(int a, int b)
{
  block_messages(a, b);
  block_messages(b, a);
}
void Election::unblock_messages(int from, int to)
{
  blocked_messages[from].erase(to);
}
void Election::unblock_bidirectional_messages(int a, int b)
{
  unblock_messages(a, b);
  unblock_messages(b, a);
}


TEST(election, single_startup_election_completes)
{
  for (int starter = 0; starter < 5; ++starter) {
    Election election(5);
    election.start_one(starter);
    // This test is not actually legit since you should start
    // all the ElectionLogics, but it seems to work
    int steps = election.run_timesteps(0);
    ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
    ASSERT_TRUE(election.election_stable());
    ASSERT_TRUE(election.check_leader_agreement());
    ASSERT_TRUE(election.check_epoch_agreement());
  }
}

TEST(election, everybody_starts_completes)
{
  Election election(5);
  election.start_all();
  int steps = election.run_timesteps(0);
  ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
}

TEST(election, blocked_connection_continues_election)
{
  Election election(5);
  election.block_bidirectional_messages(0, 1);
  election.start_all();
  int steps = election.run_timesteps(100);
  ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
  // This is a failure mode!
  ASSERT_FALSE(election.election_stable());
  election.unblock_bidirectional_messages(0, 1);
  steps = election.run_timesteps(100);
  ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
  ASSERT_TRUE(election.election_stable());
  ASSERT_TRUE(election.check_leader_agreement());
  ASSERT_TRUE(election.check_epoch_agreement());
}

TEST(election, disallowed_doesnt_win)
{
  int MON_COUNT = 5;
  for (int i = 0; i < MON_COUNT - 1; ++i) {
    Election election(MON_COUNT, ElectionLogic::DISALLOW);
    for (int j = 0; j <= i; ++j) {
      election.add_disallowed_leader(j);
    }
    election.start_all();
    int steps = election.run_timesteps(0);
    ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
    ASSERT_TRUE(election.election_stable());
    ASSERT_TRUE(election.check_leader_agreement());
    ASSERT_TRUE(election.check_epoch_agreement());
    int leader = election.electors[0]->logic.get_election_winner();
    for (int j = 0; j <= i; ++j) {
      ASSERT_NE(j, leader);
    }
  }
  for (int i = MON_COUNT - 1; i > 0; --i) {
    Election election(MON_COUNT);
    for (int j = i; j <= MON_COUNT - 1; ++j) {
      election.add_disallowed_leader(j);
    }
    election.start_all();
    int steps = election.run_timesteps(0);
    ldout(g_ceph_context, 1) << "ran in " << steps << " timesteps" << dendl;
    ASSERT_TRUE(election.election_stable());
    ASSERT_TRUE(election.check_leader_agreement());
    ASSERT_TRUE(election.check_epoch_agreement());
    int leader = election.electors[0]->logic.get_election_winner();
    for (int j = i; j < MON_COUNT; ++j) {
      ASSERT_NE(j, leader);
    }
  }
}

// TODO: Write a test that disallowing and disconnecting 0 is otherwise stable?
