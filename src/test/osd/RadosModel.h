// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "common/Mutex.h"
#include "common/Cond.h"
#include "include/rados/librados.hpp"

#include <iostream>
#include <sstream>
#include <map>
#include <set>
#include <list>
#include <string>
#include <stdlib.h>
#include <time.h>

using namespace std;

/* Snap creation/removal tester
 */

struct RadosTestContext;
struct TestOpStat;

template <typename T>
typename T::iterator rand_choose(T &cont)
{
  if (cont.size() == 0) {
    return cont.end();
  }
  int index = rand() % cont.size();
  typename T::iterator retval = cont.begin();

  for (; index > 0; --index) retval++;
  return retval;
}

struct TestOp
{
  RadosTestContext *context;
  TestOpStat *stat;
  bool done;
  TestOp(RadosTestContext *context,
	 TestOpStat *stat = 0) :
    context(context),
    stat(stat),
    done(0)
  {}

  virtual ~TestOp();

  virtual void _begin() = 0;
  virtual void _finish() = 0;
  virtual string getType() = 0;
  virtual bool finished()
  {
    return true;
  }

  void begin();
  void finish();
};

struct TestOpStat
{
  Mutex stat_lock;

  TestOpStat() : stat_lock("TestOpStat lock") {}
    
  static uint64_t gettime()
  {
    timeval t;
    gettimeofday(&t,0);
    return (1000000*t.tv_sec) + t.tv_usec;
  }

  struct TypeStatus
  {
    map<TestOp*,uint64_t> inflight;
    multiset<uint64_t> latencies;
    void begin(TestOp *in)
    {
      assert(!inflight.count(in));
      inflight[in] = gettime();
    }

    void end(TestOp *in)
    {
      assert(inflight.count(in));
      uint64_t curtime = gettime();
      latencies.insert(curtime - inflight[in]);
      inflight.erase(in);
    }

    void export_latencies(map<double,uint64_t> &in) const
    {
      map<double,uint64_t>::iterator i = in.begin();
      multiset<uint64_t>::iterator j = latencies.begin();
      int count = 0;
      while (j != latencies.end() && i != in.end()) {
	count++;
	if ((((double)count)/((double)latencies.size())) * 100 >= i->first) {
	  i->second = *j;
	  ++i;
	}
	++j;
      }
    }
  };
  map<string,TypeStatus> stats;

  void begin(TestOp *in)
  {
    stat_lock.Lock();
    stats[in->getType()].begin(in);
    stat_lock.Unlock();
  }

  void end(TestOp *in)
  {
    stat_lock.Lock();
    stats[in->getType()].end(in);
    stat_lock.Unlock();
  }

  friend std::ostream & operator<<(std::ostream &, TestOpStat&);
};

std::ostream & operator<<(std::ostream &out, TestOpStat &rhs)
{
  rhs.stat_lock.Lock();
  for (map<string,TestOpStat::TypeStatus>::iterator i = rhs.stats.begin();
       i != rhs.stats.end();
       ++i) {
    map<double,uint64_t> latency;
    latency[10] = 0;
    latency[50] = 0;
    latency[90] = 0;
    latency[99] = 0;
    i->second.export_latencies(latency);
    
    out << i->first << " latency: " << std::endl;
    for (map<double,uint64_t>::iterator j = latency.begin();
	 j != latency.end();
	 ++j) {
      if (j->second == 0) break;
      out << "\t" << j->first << "th percentile: " 
	  << j->second / 1000 << "ms" << std::endl;
    }
  }
  rhs.stat_lock.Unlock();
  return out;
}

void TestOp::begin()
{
  if (stat) stat->begin(this);
  _begin();
}

void TestOp::finish()
{
  if (stat) stat->end(this);
  _finish();
}

struct TestOpGenerator
{
  virtual ~TestOpGenerator();
  virtual TestOp *next(RadosTestContext &context) = 0;
};

struct RadosTestContext
{
  Mutex state_lock;
  Cond wait_cond;
  map<int, map<string,string> > pool_obj_cont;
  set<int> snaps;
  set<string> oid_in_use;
  set<string> oid_not_in_use;
  int current_snap;
  string pool_name;
  librados::IoCtx io_ctx;
  librados::Rados rados;
  int next_oid;
  string prefix;
  int errors;
  int max_in_flight;
	
  RadosTestContext(const string &pool_name, 
		   int max_in_flight,
		   const char *id = 0) :
    state_lock("Context Lock"),
    pool_obj_cont(),
    current_snap(0),
    pool_name(pool_name),
    errors(0),
    max_in_flight(max_in_flight)
  {
    rados.init(id);
    rados.conf_read_file("ceph.conf");
    rados.connect();
    rados.ioctx_create(pool_name.c_str(), io_ctx);
    char hostname_cstr[100];
    gethostname(hostname_cstr, 100);
    stringstream hostpid;
    hostpid << hostname_cstr << getpid() << "-";
    prefix = hostpid.str();
  }

  void shutdown()
  {
    rados.shutdown();
  }

  void loop(TestOpGenerator *gen)
  {
    list<TestOp*> inflight;
    state_lock.Lock();

    TestOp *next = gen->next(*this);
    while (next || inflight.size()) {
      if (next) {
	inflight.push_back(next);
      }
      state_lock.Unlock();
      if (next) {
	(*inflight.rbegin())->begin();
      }
      state_lock.Lock();
      while (1) {
	for (list<TestOp*>::iterator i = inflight.begin();
	     i != inflight.end();) {
	  if ((*i)->finished()) {
	    delete *i;
	    inflight.erase(i++);
	  } else {
	    ++i;
	  }
	}
	
	if (inflight.size() >= (unsigned) max_in_flight || (!next && inflight.size())) {
	  cout << "Waiting on " << inflight.size() << std::endl;
	  wait();
	} else {
	  break;
	}
      }
      next = gen->next(*this);
    }
    state_lock.Unlock();
  }

  void wait()
  {
    wait_cond.Wait(state_lock);
  }

  void kick()
  {
    wait_cond.Signal();
  }

  bool find_object(string oid, string &contents, int snap = -1) const
  {
    for (map<int, map<string,string> >::const_reverse_iterator i = 
	   pool_obj_cont.rbegin();
	 i != pool_obj_cont.rend();
	 ++i) {
      if (snap != -1 && snap < i->first) continue;
      if (i->second.count(oid) != 0) {
	contents = i->second.find(oid)->second;
	return true;
      }
    }
    return false;
  }

  void remove_snap(int snap)
  {
    map<int, map<string,string> >::iterator next_iter = pool_obj_cont.find(snap);
    map<int, map<string,string> >::iterator current_iter = next_iter++;
    if (next_iter != pool_obj_cont.end()) {
      map<string,string> &current = current_iter->second;
      map<string,string> &next = next_iter->second;
      for (map<string,string>::iterator i = current.begin();
	   i != current.end();
	   ++i) {
	if (next.count(i->first) == 0) {
	  next[i->first] = i->second;
	}
      }
    }
    snaps.erase(snap);
    pool_obj_cont.erase(current_iter);
  }

  void add_snap()
  {
    current_snap++;
    pool_obj_cont[current_snap];
    snaps.insert(current_snap - 1);
  }

  void roll_back(string oid, int snap)
  {
    string contents;
    find_object(oid, contents, snap);
    pool_obj_cont.rbegin()->second[oid] = contents;
  }
};

void callback(librados::completion_t cb, void *arg) {
  TestOp *op = (TestOp *) arg;
  op->finish();
}

struct WriteOp : public TestOp
{
  string oid;
  string written;
  librados::AioCompletion *completion;

  WriteOp(RadosTestContext *context, 
	  const string &oid,
	  TestOpStat *stat = 0) : 
    TestOp(context, stat),
    oid(oid)
  {}
		
  void _begin()
  {
    context->state_lock.Lock();
    done = 0;
    completion = context->rados.aio_create_completion((void *) this, &callback, 0);
    stringstream to_write;
    to_write << context->prefix << "OID: " << oid << " snap " << context->current_snap << std::endl;
    written = to_write.str();
    context->pool_obj_cont[context->current_snap][oid] = written;

    context->oid_in_use.insert(oid);
    if (context->oid_not_in_use.count(oid) != 0) {
      context->oid_not_in_use.erase(oid);
    }

    bufferlist write_buffer;
    write_buffer.append(to_write.str());
    context->state_lock.Unlock();

    context->io_ctx.aio_write(context->prefix+oid, completion,
			     write_buffer, to_write.str().length(), 0);
  }

  void _finish()
  {
    context->state_lock.Lock();
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    if (int err = completion->get_return_value()) {
      cerr << "Error: oid " << oid << " write returned error code "
	   << err << std::endl;
    }
    context->kick();
    done = true;
    context->state_lock.Unlock();
  }

  bool finished()
  {
    return done && completion->is_complete();
  }

  string getType()
  {
    return "WriteOp";
  }
};

struct ReadOp : public TestOp
{
  librados::AioCompletion *completion;
  string oid;
  bufferlist result;
  string old_value;
  ReadOp(RadosTestContext *context, 
	 const string &oid,
	 TestOpStat *stat = 0) : 
    TestOp(context, stat),
    oid(oid)
  {}
		
  void _begin()
  {
    context->state_lock.Lock();
    done = 0;
    completion = context->rados.aio_create_completion((void *) this, &callback, 0);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    context->find_object(oid, old_value);

    context->state_lock.Unlock();
    context->io_ctx.aio_read(context->prefix+oid, completion,
			   &result, old_value.length(), 0);
  }

  void _finish()
  {
    context->state_lock.Lock();
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    if (int err = completion->get_return_value()) {
      cerr << "Error: oid " << oid << " read returned error code "
	   << err << std::endl;
    } else {
      string to_check;
      result.copy(0, result.length(), to_check);
      if (to_check != old_value) {
	context->errors++;
	cerr << "Error: oid " << oid << " read returned \n"
	     << to_check << "\nShould have returned\n"
	     << old_value << "\nCurrent snap is " << context->current_snap << std::endl;
      }
    }
    context->kick();
    done = true;
    context->state_lock.Unlock();
  }

  bool finished()
  {
    return done && completion->is_complete();
  }

  string getType()
  {
    return "ReadOp";
  }
};

struct SnapCreateOp : public TestOp
{
  SnapCreateOp(RadosTestContext *context,
	       TestOpStat *stat = 0) :
    TestOp(context, stat)
  {}

  void _begin()
  {
    context->state_lock.Lock();
    context->add_snap();
    context->state_lock.Unlock();

    stringstream snap_name;
    snap_name << context->prefix << context->current_snap - 1;
    context->io_ctx.snap_create(snap_name.str().c_str());
    finish();
  }

  void _finish()
  {
    return;
  }

  string getType()
  {
    return "SnapCreateOp";
  }
};

struct SnapRemoveOp : public TestOp
{
  int to_remove;
  SnapRemoveOp(RadosTestContext *context,
	       int snap,
	       TestOpStat *stat = 0) :
    TestOp(context, stat),
    to_remove(snap)
  {}

  void _begin()
  {
    context->state_lock.Lock();
    context->remove_snap(to_remove);
    context->state_lock.Unlock();

    stringstream snap_name;
    snap_name << context->prefix << to_remove;
    context->io_ctx.snap_remove(snap_name.str().c_str());
    finish();
  }

  void _finish()
  {
    return;
  }

  string getType()
  {
    return "SnapRemoveOp";
  }
};

struct RollbackOp : public TestOp
{
  string oid;
  int roll_back_to;
  RollbackOp(RadosTestContext *context,
	     const string &_oid,
	     int snap,
	     TestOpStat *stat = 0) :
    TestOp(context, stat),
    oid(_oid),
    roll_back_to(snap)
  {}

  void _begin()
  {
    context->state_lock.Lock();
    context->oid_in_use.insert(oid);
    context->roll_back(oid, roll_back_to);
    context->state_lock.Unlock();
	
    stringstream snap_name;
    snap_name << context->prefix << roll_back_to;
    context->io_ctx.rollback(context->prefix+oid, snap_name.str().c_str());
    finish();
  }

  void _finish()
  {
    return;
  }
    

  string getType()
  {
    return "RollBackOp";
  }
};
