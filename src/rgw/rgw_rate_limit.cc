// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fstream>
#include <yaml-cpp/yaml.h>
#include <sys/inotify.h>
#include <unistd.h>
#include <signal.h>

#include "common/debug.h"
#include "common/Timer.h"
#include "common/Thread.h"
#include "include/atomic.h"
#include "global/signal_handler.h"
#include "rgw_op.h"

#include "rgw_rate_limit.h"

#define dout_subsys ceph_subsys_rgw

#define RATE_LIMIT_CONFIG_FILE "/etc/ceph/rgw-rate-limit.yml"

enum Period { UNDEF=0, SECOND=1, MINUTE=2, HOUR=3 };


// Globals

typedef struct {
  atomic64_t recvd;
  atomic64_t limit;
  Period period;
  int accept_idx;
  int reject_idx;
} api_counter_t;

typedef unordered_map<string, api_counter_t *> RGW_api_ctr_t;
typedef RGW_api_ctr_t::iterator api_ctr_it;
typedef RGW_api_ctr_t::const_iterator api_ctr_cit;

// Maps of user counters
RGW_api_ctr_t *rgw_get_ctrs, *rgw_put_ctrs;


// Helpers

static Period parse_period_str(const string period_str)
{
  Period retval = UNDEF;
  if ( period_str == "sec" ) {
    retval = SECOND;
  } else if ( period_str == "min" ) {
    retval = MINUTE;
  } else if ( period_str == "hour" ) {
    retval = HOUR;
  }
  return retval;
}

static string get_period_str(const Period period)
{
  string retval = "UNDEF";
  if ( period == SECOND ) {
    retval = "sec";
  } else if ( period == MINUTE ) {
    retval = "min";
  } else if ( period == HOUR ) {
    retval = "hour";
  }
  return retval;
}

static void dump_limit_config(RGW_api_ctr_t *ctrs)
{
  for (api_ctr_cit iter = ctrs->begin(); iter != ctrs->end(); ++iter) {
    string user = iter->first;
    api_counter_t *ctr = iter->second;
    dout(0) << "User: " << user \
	    << "\tlimit: " << ctr->limit.read() << "/" \
	    << get_period_str(ctr->period) << dendl;
  }
}

static void dump_ctr_stats(RGW_api_ctr_t *ctrs)
{
  for (api_ctr_cit iter = ctrs->begin(); iter != ctrs->end(); ++iter) {
    string user = iter->first;
    api_counter_t *ctr = iter->second;
    dout(0) << "User: " << user << "\tlimit: " << ctr->limit.read() \
	    << "/" << get_period_str(ctr->period) \
	    << ", recvd: " << ctr->recvd.read() << dendl;
  }
}

static void rgw_api_ctr_dump(int signum)
{
  dout(0) << "*** GET stats dump" << dendl;
  dump_ctr_stats(rgw_get_ctrs);
  dout(0) << "*** PUT stats dump" << dendl;
  dump_ctr_stats(rgw_put_ctrs);
}

static api_counter_t *parse_limit(const YAML::Node& limit_data)
{
  api_counter_t *api_ctr = new api_counter_t();
  try {
    if ( !limit_data.size() ) {
      delete api_ctr;
      return NULL;
    }

    unsigned long long limit;
    limit_data["limit"] >> limit;
    api_ctr->limit.set(limit);
    string period_str;
    limit_data["period"] >> period_str;
    if ( (api_ctr->period = parse_period_str(period_str)) == UNDEF ) {
      delete api_ctr;
      return NULL;
    }

  } catch (...) {
    delete api_ctr;
    return NULL;
  }
  return api_ctr;
}

static void swap_ctr(api_counter_t *old_ctr, api_counter_t *new_ctr)
{
  old_ctr->limit.set(new_ctr->limit.read());
  old_ctr->period = new_ctr->period;
}

// Parse config file into tmp map & swap with current map
static int load_limit_config_file(bool init)
{
  YAML::Node doc;
  try {
    ifstream config_file(RATE_LIMIT_CONFIG_FILE);
    YAML::Parser parser(config_file);
    parser.GetNextDocument(doc);
  } catch (...) {
    dout(0) << "Error parsing rate limit config" << dendl;
    return -1;
  }

  dout(0) << "*** Rate limit config parsing ***" << dendl;

  for ( unsigned int i=0; i < doc.size(); i++ ) {
    string user;
    const YAML::Node& node = doc[i];

    try {
      node["user"] >> user;
    } catch (...) {
      dout(0) << "Key 'user' not found, skipping stanza" << dendl;
      continue;
    }

    api_counter_t *get_ctr;
    try {
      const YAML::Node& get = node["get"];
      if ( (get_ctr = parse_limit(get)) == NULL ) {
	throw;
      } else {
	if ( init ) {
	  (*rgw_get_ctrs)[user] = get_ctr;
	} else {
	  api_ctr_it iter = rgw_get_ctrs->find(user);
	  if ( iter != rgw_get_ctrs->end() ) {
	    api_counter_t *ctr = iter->second;
	    swap_ctr(ctr, get_ctr);
	    delete get_ctr;
	  }
	}
      }
    } catch (...) {
      dout(0) << "Error parsing GET limit for user: " << user << dendl;
    }

    api_counter_t *put_ctr;
    try {
      const YAML::Node& put = node["put"];
      if ( (put_ctr = parse_limit(put)) == NULL ) {
	throw;
      } else {
	if ( init ) {
	  (*rgw_put_ctrs)[user] = put_ctr;
	} else {
	  api_ctr_it iter = rgw_put_ctrs->find(user);
	  if ( iter != rgw_put_ctrs->end() ) {
	    api_counter_t *ctr = iter->second;
	    swap_ctr(ctr, put_ctr);
	    delete put_ctr;
	  }
	}
      }
    } catch (...) {
      dout(0) << "Error parsing PUT limit for user: " << user << dendl;
    }

    if ( get_ctr ) {
      dout(0) << "User: " << user \
	      << "\tGET: " << get_ctr->limit.read() \
	      << "/" << get_period_str(get_ctr->period) << dendl;
    }
    if ( put_ctr ) {
      dout(0) << "User: " << user \
	      << "\tPUT: " << put_ctr->limit.read() \
	      << "/" << get_period_str(put_ctr->period) << dendl;
    }
  }

  dout(0) << "*** Final GET rate limit config ***" << dendl;
  dump_limit_config(rgw_get_ctrs);
  dout(0) << "*** Final PUT rate limit config ***" << dendl;
  dump_limit_config(rgw_put_ctrs);

  return 0;
}


// Timer setup, callbacks, counter reset

#define ONE_SEC 1
#define ONE_MIN 60
#define ONE_HOUR (60 * 60)

Mutex *sec_timer_lock, *min_timer_lock, *hour_timer_lock;
SafeTimer *rate_ctr_timer_sec, *rate_ctr_timer_min, *rate_ctr_timer_hour;

static void reset_ctrs(RGW_api_ctr_t *ctrs, Period period)
{
  for (api_ctr_cit iter = ctrs->begin(); iter != ctrs->end(); ++iter) {
    string user = iter->first;
    api_counter_t *ctr = iter->second;
    if ( ctr->period == period ) {
      ctr->recvd.set(0);
    }
  }
}

class C_RateCtrSecTimeout : public Context {
public:
  C_RateCtrSecTimeout() {}
  void finish(int r) {
    reset_ctrs(rgw_get_ctrs, SECOND);
    reset_ctrs(rgw_put_ctrs, SECOND);
    rate_ctr_timer_sec->add_event_after(ONE_SEC, new C_RateCtrSecTimeout);
  }
};

class C_RateCtrMinTimeout : public Context {
public:
  C_RateCtrMinTimeout() {}
  void finish(int r) {
    reset_ctrs(rgw_get_ctrs, MINUTE);
    reset_ctrs(rgw_put_ctrs, MINUTE);
    rate_ctr_timer_min->add_event_after(ONE_MIN, new C_RateCtrMinTimeout);
  }
};

class C_RateCtrHourTimeout : public Context {
public:
  C_RateCtrHourTimeout() {}
  void finish(int r) {
    reset_ctrs(rgw_get_ctrs, HOUR);
    reset_ctrs(rgw_put_ctrs, HOUR);
    rate_ctr_timer_hour->add_event_after(ONE_HOUR, new C_RateCtrHourTimeout);
  }
};

static void setup_timers()
{
  sec_timer_lock = new Mutex("timer_sec");
  rate_ctr_timer_sec = new SafeTimer(g_ceph_context, *sec_timer_lock);
  rate_ctr_timer_sec->init();
  sec_timer_lock->Lock();
  rate_ctr_timer_sec->add_event_after(ONE_SEC, new C_RateCtrSecTimeout);
  sec_timer_lock->Unlock();

  min_timer_lock = new Mutex("timer_min");
  rate_ctr_timer_min = new SafeTimer(g_ceph_context, *min_timer_lock);
  rate_ctr_timer_min->init();
  min_timer_lock->Lock();
  rate_ctr_timer_min->add_event_after(ONE_MIN, new C_RateCtrMinTimeout);
  min_timer_lock->Unlock();

  hour_timer_lock = new Mutex("timer_hour");
  rate_ctr_timer_hour = new SafeTimer(g_ceph_context, *hour_timer_lock);
  rate_ctr_timer_hour->init();
  hour_timer_lock->Lock();
  rate_ctr_timer_hour->add_event_after(ONE_HOUR, new C_RateCtrHourTimeout);
  hour_timer_lock->Unlock();
}


// API calls 'perf' counters
// Per-account counters for accept + reject GET / PUT call
// Counter names: rgw_api_{accept,reject}_{get,put}_<user>
#define RGW_API_CTR_IDX   (17000)
#define RGW_API_ACCEPT_CTR_NAME_PREFIX string("rgw_api_accept_")
#define RGW_API_REJECT_CTR_NAME_PREFIX string("rgw_api_reject_")

PerfCountersBuilder *rgw_api_ctr_pcb;
PerfCounters *rgw_api_ctrs;

static void add_api_ctr(int ctr_idx, string ctr_name)
{
  char *ctr_name_str = new char[ctr_name.length() + 1];
  strcpy(ctr_name_str, ctr_name.c_str());
  rgw_api_ctr_pcb->add_u64_counter(ctr_idx, ctr_name_str);
}

static void add_api_accept_ctr(int ctr_idx, string ctr_name)
{
  ctr_name = RGW_API_ACCEPT_CTR_NAME_PREFIX + ctr_name;
  add_api_ctr(ctr_idx, ctr_name);
}

static void add_api_reject_ctr(int ctr_idx, string ctr_name)
{
  ctr_name = RGW_API_REJECT_CTR_NAME_PREFIX + ctr_name;
  add_api_ctr(ctr_idx, ctr_name);
}

static void rgw_init_api_ctrs(CephContext *cct)
{
  int num_rgw_api_ctrs = (rgw_get_ctrs->size() + rgw_put_ctrs->size()) * 2;

  // PerfCountersBuilder does a ()-style bounds check, hence the +1's
  rgw_api_ctr_pcb = new PerfCountersBuilder(cct, "rgw_api_counters",
	RGW_API_CTR_IDX, RGW_API_CTR_IDX + num_rgw_api_ctrs + 1);
  int next_ctr_idx = RGW_API_CTR_IDX + 1;

  for ( api_ctr_it iter = rgw_get_ctrs->begin();
	iter != rgw_get_ctrs->end();
	++iter ) {
    string user = iter->first;
    api_counter_t *ctr = iter->second;
    dout(20) << "Adding GET: " << user << " @ " << next_ctr_idx << dendl;
    add_api_accept_ctr(next_ctr_idx, string("get_") + user);
    ctr->accept_idx = next_ctr_idx;
    next_ctr_idx++;
    add_api_reject_ctr(next_ctr_idx, string("get_") + user);
    ctr->reject_idx = next_ctr_idx;
    next_ctr_idx++;
  }
  for ( api_ctr_cit iter = rgw_put_ctrs->begin();
	iter != rgw_put_ctrs->end();
	++iter ) {
    string user = iter->first;
    api_counter_t *ctr = iter->second;
    dout(20) << "Adding PUT: " << user << " @ " << next_ctr_idx << dendl;
    add_api_accept_ctr(next_ctr_idx, string("put_") + user);
    ctr->accept_idx = next_ctr_idx;
    next_ctr_idx++;
    add_api_reject_ctr(next_ctr_idx, string("put_") + user);
    ctr->reject_idx = next_ctr_idx;
    next_ctr_idx++;
  }

  rgw_api_ctrs = rgw_api_ctr_pcb->create_perf_counters();
  cct->get_perfcounters_collection()->add(rgw_api_ctrs);
}


// Actual rate limit / api count check
static bool rgw_op_rate_limit_ok(RGW_api_ctr_t *ctrs, string user)
{
  if ( !ctrs ) {
    return true;
  }
  api_ctr_cit iter;
  if ( (iter = ctrs->find(user)) != ctrs->end() ) {
    api_counter_t *ctr = iter->second;
    ctr->recvd.inc();
    dout(20) << user << " : recvd = " << ctr->recvd.read() \
	     << ", limit = " << ctr->limit.read() << dendl;
    if ( ctr->recvd.read() > ctr->limit.read() ) {
      rgw_api_ctrs->inc(ctr->reject_idx);
      return false;
    }
    rgw_api_ctrs->inc(ctr->accept_idx);
  }
  return true;
}

bool rgw_rate_limit_ok(string& user, RGWOp *op)
{
  RGW_api_ctr_t *ctrs = NULL;
  if ( op->get_type() == RGW_OP_GET_OBJ ) {
    ctrs = rgw_get_ctrs;
  }
  if ( op->get_type() == RGW_OP_PUT_OBJ ) {
    ctrs = rgw_put_ctrs;
  }
  return (ctrs ? rgw_op_rate_limit_ok(ctrs, user) : true);
}


// file watcher

static void watch_config_file()
{
  dout(0) << "file watcher thread" << dendl;

  int ifd;
  if ( (ifd = inotify_init()) == -1 ) {
    dout(0) << "inotify_init fail" << dendl;
    return;
  }
  int wfd;
  uint32_t mask = IN_DELETE_SELF | IN_MODIFY;
  if ( (wfd = inotify_add_watch(ifd, RATE_LIMIT_CONFIG_FILE, mask)) == -1 ) {
    dout(0) << "inotify add watch fail" << dendl;
    return;
  }

  while ( true ) {
    struct inotify_event event;
    if ( read(ifd, (void *)(&event), sizeof(event)) == -1 ) {
      if ( errno == EINTR ) {
	dout(0) << "inotify read intr" << dendl;
	break;
      }
    }
    // TODO: assert event.wd == wfd

    if ( event.mask & IN_MODIFY ) {
      dout(0) << "inotify: config file modified" << dendl;
      load_limit_config_file(false);
    }

    if ( event.mask & IN_IGNORED ) {
      dout(0) << "inotify watch rm'ed" << dendl;
      break;
    }
  }

  inotify_rm_watch(ifd, wfd);
  close(ifd);
}

class FileWatchThread : public Thread {
public:
  FileWatchThread() {}
  void *entry() {
    watch_config_file();
    return NULL;
  }
};


// Setup maps, timers, file-watch
int rgw_rate_limit_init(CephContext *cct)
{
  rgw_put_ctrs = new RGW_api_ctr_t;
  rgw_get_ctrs = new RGW_api_ctr_t;

  if ( load_limit_config_file(true) < 0 ) {
    return -1;
  }

  rgw_init_api_ctrs(cct);

  FileWatchThread *fwthd = new FileWatchThread();
  fwthd->create();

  setup_timers();

  register_async_signal_handler(SIGUSR2, rgw_api_ctr_dump);

  return 0;
}
