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
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include "Object.h"
#include "TestOpStat.h"

#ifndef RADOSMODEL_H
#define RADOSMODEL_H

using namespace std;

class RadosTestContext;
class TestOpStat;

template <typename T>
typename T::iterator rand_choose(T &cont) {
  if (cont.size() == 0) {
    return cont.end();
  }
  int index = rand() % cont.size();
  typename T::iterator retval = cont.begin();

  for (; index > 0; --index) retval++;
  return retval;
}

class TestOp {
public:
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

class TestOpGenerator {
public:
  virtual ~TestOpGenerator();
  virtual TestOp *next(RadosTestContext &context) = 0;
};

class RadosTestContext {
public:
  Mutex state_lock;
  Cond wait_cond;
  map<int, map<string,ObjectDesc> > pool_obj_cont;
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
  ContentsGenerator &cont_gen;
  int seq_num;
  map<int,uint64_t> snaps;
  uint64_t seq;
  
	
  RadosTestContext(const string &pool_name, 
		   int max_in_flight,
		   ContentsGenerator &cont_gen,
		   const char *id = 0) :
    state_lock("Context Lock"),
    pool_obj_cont(),
    current_snap(0),
    pool_name(pool_name),
    errors(0),
    max_in_flight(max_in_flight),
    cont_gen(cont_gen), seq_num(0), seq(0)
  {
    rados.init(id);
    rados.conf_read_file(NULL);
    rados.conf_parse_env(NULL);
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

  void update_object(const string &oid, const ContDesc &contents)
  {
    ObjectDesc new_obj(&cont_gen);
    for (map<int, map<string,ObjectDesc> >::reverse_iterator i = 
	   pool_obj_cont.rbegin();
	 i != pool_obj_cont.rend();
	 ++i) {
      map<string,ObjectDesc>::iterator j = i->second.find(oid);
      if (j != i->second.end()) {
	new_obj = j->second;
	break;
      }
    }
    new_obj.update(contents);
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void remove_object(const string &oid)
  {
    ObjectDesc new_obj(&cont_gen);
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  bool find_object(const string &oid, ObjectDesc *contents, int snap = -1) const
  {
    for (map<int, map<string,ObjectDesc> >::const_reverse_iterator i = 
	   pool_obj_cont.rbegin();
	 i != pool_obj_cont.rend();
	 ++i) {
      if (snap != -1 && snap < i->first) continue;
      if (i->second.count(oid) != 0) {
	*contents = i->second.find(oid)->second;
	return true;
      }
    }
    return false;
  }

  void remove_snap(int snap)
  {
    map<int, map<string,ObjectDesc> >::iterator next_iter = pool_obj_cont.find(snap);
    assert(next_iter != pool_obj_cont.end());
    map<int, map<string,ObjectDesc> >::iterator current_iter = next_iter++;
    assert(current_iter != pool_obj_cont.end());
    map<string,ObjectDesc> &current = current_iter->second;
    map<string,ObjectDesc> &next = next_iter->second;
    for (map<string,ObjectDesc>::iterator i = current.begin();
	 i != current.end();
	 ++i) {
      if (next.count(i->first) == 0) {
	next.insert(pair<string,ObjectDesc>(i->first, i->second));
      }
    }
    pool_obj_cont.erase(current_iter);
    snaps.erase(snap);
  }

  void add_snap(uint64_t snap)
  {
    snaps[current_snap] = snap;
    current_snap++;
    pool_obj_cont[current_snap];
    seq = snap;
  }

  void roll_back(const string &oid, int snap)
  {
    ObjectDesc contents(&cont_gen);
    find_object(oid, &contents, snap);
    pool_obj_cont.rbegin()->second.erase(oid);
    pool_obj_cont.rbegin()->second.insert(pair<string,ObjectDesc>(oid, contents));
  }
};

void callback(librados::completion_t cb, void *arg);

class WriteOp : public TestOp {
public:
  string oid;
  ContDesc cont;
  set<librados::AioCompletion *> waiting;
  int waiting_on;

  WriteOp(RadosTestContext *context, 
	  const string &oid,
	  TestOpStat *stat = 0) : 
    TestOp(context, stat),
    oid(oid), waiting_on(0)
  {}
		
  void _begin()
  {
    context->state_lock.Lock();
    done = 0;
    stringstream acc;
    acc << context->prefix << "OID: " << oid << " snap " << context->current_snap << std::endl;
    string prefix = acc.str();

    cont = ContDesc(context->seq_num, context->current_snap, context->seq_num, prefix);

    context->update_object(oid, cont);

    context->oid_in_use.insert(oid);
    if (context->oid_not_in_use.count(oid) != 0) {
      context->oid_not_in_use.erase(oid);
    }

    context->seq_num++;

    vector<uint64_t> snapset(context->snaps.size());
    int j = 0;
    for (map<int,uint64_t>::reverse_iterator i = context->snaps.rbegin();
	 i != context->snaps.rend();
	 ++i, ++j) {
      snapset[j] = i->second;
    }
    interval_set<uint64_t> ranges;
    context->cont_gen.get_ranges(cont, ranges);
    for (interval_set<uint64_t>::iterator i = ranges.begin();
	 i != ranges.end();
	 ++i) {
      ++waiting_on;
      librados::AioCompletion *completion = 
	context->rados.aio_create_completion((void *) this, &callback, 0);
      waiting.insert(completion);
    }
    context->state_lock.Unlock();

    int r = context->io_ctx.selfmanaged_snap_set_write_ctx(context->seq, snapset);
    if (r) {
      cerr << "r is " << r << " snapset is " << snapset << " seq is " << context->seq << std::endl;
      assert(0);
    }

    ContentsGenerator::iterator gen_pos = context->cont_gen.get_iterator(cont);
    set<librados::AioCompletion*>::iterator l = waiting.begin();
    for (interval_set<uint64_t>::iterator i = ranges.begin(); 
	 i != ranges.end();
	 ++i, ++l) {
      bufferlist to_write;
      gen_pos.seek(i.get_start());
      for (uint64_t k = 0; k != i.get_len(); ++k, ++gen_pos) {
	to_write.append(*gen_pos);
      }
      assert(to_write.length() == i.get_len());
      assert(to_write.length() > 0);
      std::cout << "Writing " << context->prefix+oid << " from " << i.get_start()
		<< " to " << i.get_len() + i.get_start() << " ranges are " 
		<< ranges << std::endl;
      context->io_ctx.aio_write(context->prefix+oid, *l,
				to_write, i.get_len(), i.get_start());
    }
  }

  void _finish()
  {
    context->state_lock.Lock();
    assert(!done);
    waiting_on--;
    if (waiting_on == 0) {
      for (set<librados::AioCompletion *>::iterator i = waiting.begin();
	   i != waiting.end();
	   ) {
	assert((*i)->is_complete());
	if (int err = (*i)->get_return_value()) {
	  cerr << "Error: oid " << oid << " write returned error code "
	       << err << std::endl;
	}
	(*i)->release();
	waiting.erase(i++);
      }
      
      context->oid_in_use.erase(oid);
      context->oid_not_in_use.insert(oid);
      context->kick();
      done = true;
    }
    context->state_lock.Unlock();
  }

  bool finished()
  {
    return waiting.empty();
  }

  string getType()
  {
    return "WriteOp";
  }
};

class DeleteOp : public TestOp {
public:
  string oid;

  DeleteOp(RadosTestContext *context,
	   const string &oid,
	   TestOpStat *stat = 0) :
    TestOp(context, stat), oid(oid)
  {}

  void _begin()
  {
    context->state_lock.Lock();
    done = 0;
    stringstream acc;

    ObjectDesc contents(&context->cont_gen);
    bool present = context->find_object(oid, &contents);
    if (present) {
      present = !contents.deleted();
    }

    context->oid_in_use.insert(oid);
    if (context->oid_not_in_use.count(oid) != 0) {
      context->oid_not_in_use.erase(oid);
    }
    context->seq_num++;

    context->remove_object(oid);

    vector<uint64_t> snapset(context->snaps.size());
    int j = 0;
    for (map<int,uint64_t>::reverse_iterator i = context->snaps.rbegin();
	 i != context->snaps.rend();
	 ++i, ++j) {
      snapset[j] = i->second;
    }
    interval_set<uint64_t> ranges;
    context->state_lock.Unlock();

    int r = context->io_ctx.selfmanaged_snap_set_write_ctx(context->seq, snapset);
    if (r) {
      cerr << "r is " << r << " snapset is " << snapset << " seq is " << context->seq << std::endl;
      assert(0);
    }

    r = context->io_ctx.remove(context->prefix+oid);
    if (r && !(r == -ENOENT && !present)) {
      cerr << "r is " << r << " while deleting " << oid << " and present is " << present << std::endl;
      assert(0);
    }

    context->state_lock.Lock();
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->state_lock.Unlock();
    finish();
  }

  void _finish()
  {
    return;
  }

  bool finished()
  {
    return true;
  }

  string getType()
  {
    return "DeleteOp";
  }
};

class ReadOp : public TestOp {
public:
  librados::AioCompletion *completion;
  string oid;
  bufferlist result;
  ObjectDesc old_value;
  int snap;
  ReadOp(RadosTestContext *context, 
	 const string &oid,
	 TestOpStat *stat = 0) : 
    TestOp(context, stat),
    oid(oid),
    old_value(&context->cont_gen)
  {}
		
  void _begin()
  {
    context->state_lock.Lock();
    if (0 && !(rand() % 4) && !context->snaps.empty()) {
      snap = rand_choose(context->snaps)->first;
    } else {
      snap = -1;
    }
    done = 0;
    completion = context->rados.aio_create_completion((void *) this, &callback, 0);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    assert(context->find_object(oid, &old_value, snap));

    context->state_lock.Unlock();
    if (snap >= 0) {
      context->io_ctx.snap_set_read(context->snaps[snap]);
    }
    context->io_ctx.aio_read(context->prefix+oid, completion,
			     &result,
			     old_value.deleted() ? 0 : context->cont_gen.get_length(old_value.most_recent()), 0);
    if (snap >= 0) {
      context->io_ctx.snap_set_read(0);
    }
  }

  void _finish()
  {
    context->state_lock.Lock();
    assert(!done);
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    if (int err = completion->get_return_value()) {
      if (!(err == -ENOENT && old_value.deleted())) {
	cerr << "Error: oid " << oid << " read returned error code "
	     << err << std::endl;
      }
    } else {
      assert(!old_value.deleted());
      ContDesc to_check;
      bufferlist::iterator p = result.begin();
      if (!context->cont_gen.read_header(p, to_check)) {
	cerr << "Unable to decode oid " << oid << " at snap " << context->current_snap << std::endl;
	context->errors++;
      }
      if (to_check != old_value.most_recent()) {
	cerr << "Found incorrect object contents " << to_check 
	     << ", expected " << old_value.most_recent() << " oid " << oid << std::endl;
	context->errors++;
      }
      if (!old_value.check(result)) {
	cerr << "Object " << oid << " contents " << to_check << " corrupt" << std::endl;
	context->errors++;
      }
      if (context->errors) assert(0);
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

class SnapCreateOp : public TestOp {
public:
  SnapCreateOp(RadosTestContext *context,
	       TestOpStat *stat = 0) :
    TestOp(context, stat)
  {}

  void _begin()
  {
    uint64_t snap;
    assert(!context->io_ctx.selfmanaged_snap_create(&snap));

    context->state_lock.Lock();
    context->add_snap(snap);
    context->state_lock.Unlock();

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

class SnapRemoveOp : public TestOp {
public:
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
    uint64_t snap = context->snaps[to_remove];
    context->remove_snap(to_remove);
    context->state_lock.Unlock();

    assert(!context->io_ctx.selfmanaged_snap_remove(snap));
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

class RollbackOp : public TestOp {
public:
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
    uint64_t snap = context->snaps[roll_back_to];

    vector<uint64_t> snapset(context->snaps.size());
    int j = 0;
    for (map<int,uint64_t>::reverse_iterator i = context->snaps.rbegin();
	 i != context->snaps.rend();
	 ++i, ++j) {
      snapset[j] = i->second;
    }
    context->state_lock.Unlock();
    assert(!context->io_ctx.selfmanaged_snap_set_write_ctx(context->seq, snapset));

	
    int r = context->io_ctx.selfmanaged_snap_rollback(context->prefix+oid, 
						      snap);
    if (r) {
      cerr << "r is " << r << std::endl;
      assert(0);
    }
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

#endif
