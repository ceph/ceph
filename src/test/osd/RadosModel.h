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
#include "inttypes.h"
#include "test/librados/test.h"

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

  for (; index > 0; --index) ++retval;
  return retval;
}

enum TestOpType {
  TEST_OP_READ,
  TEST_OP_WRITE,
  TEST_OP_DELETE,
  TEST_OP_SNAP_CREATE,
  TEST_OP_SNAP_REMOVE,
  TEST_OP_ROLLBACK,
  TEST_OP_SETATTR,
  TEST_OP_RMATTR,
  TEST_OP_TMAPPUT,
  TEST_OP_WATCH
};

class TestWatchContext : public librados::WatchCtx {
  TestWatchContext(const TestWatchContext&);
public:
  Cond cond;
  uint64_t handle;
  bool waiting;
  Mutex lock;
  TestWatchContext() : handle(0), waiting(false),
		       lock("watch lock") {}
  void notify(uint8_t opcode, uint64_t ver, bufferlist &bl) {
    Mutex::Locker l(lock);
    waiting = false;
    cond.SignalAll();
  }
  void start() {
    Mutex::Locker l(lock);
    waiting = true;
  }
  void wait() {
    Mutex::Locker l(lock);
    while (waiting)
      cond.Wait(lock);
  }
  uint64_t &get_handle() {
    return handle;
  }
};

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

  virtual ~TestOp() {};

  /**
   * This struct holds data to be passed by a callback
   * to a TestOp::finish method.
   */
  struct CallbackInfo {
    uint64_t id;
    CallbackInfo(uint64_t id) : id(id) {}
    virtual ~CallbackInfo() {};
  };

  virtual void _begin() = 0;

  /**
   * Called when the operation completes.
   * This should be overridden by asynchronous operations.
   *
   * @param info information stored by a callback, or NULL -
   *             useful for multi-operation TestOps
   */
  virtual void _finish(CallbackInfo *info)
  {
    return;
  }
  virtual string getType() = 0;
  virtual bool finished()
  {
    return true;
  }

  void begin();
  void finish(CallbackInfo *info);
};

class TestOpGenerator {
public:
  virtual ~TestOpGenerator() {};
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
  const char *rados_id;
  bool initialized;
  map<string, TestWatchContext*> watches;
  
	
  RadosTestContext(const string &pool_name, 
		   int max_in_flight,
		   ContentsGenerator &cont_gen,
		   const char *id = 0) :
    state_lock("Context Lock"),
    pool_obj_cont(),
    current_snap(0),
    pool_name(pool_name),
    next_oid(0),
    errors(0),
    max_in_flight(max_in_flight),
    cont_gen(cont_gen), seq_num(0), seq(0),
    rados_id(id), initialized(false)
  {
  }

  int init()
  {
    int r = rados.init(rados_id);
    if (r < 0)
      return r;
    r = rados.conf_read_file(NULL);
    if (r < 0)
      return r;
    r = rados.conf_parse_env(NULL);
    if (r < 0)
      return r;
    r = rados.connect();
    if (r < 0)
      return r;
    r = rados.ioctx_create(pool_name.c_str(), io_ctx);
    if (r < 0) {
      rados.shutdown();
      return r;
    }
    char hostname_cstr[100];
    gethostname(hostname_cstr, 100);
    stringstream hostpid;
    hostpid << hostname_cstr << getpid() << "-";
    prefix = hostpid.str();
    assert(!initialized);
    initialized = true;
    return 0;
  }

  void shutdown()
  {
    if (initialized) {
      rados.shutdown();
    }
  }

  void loop(TestOpGenerator *gen)
  {
    assert(initialized);
    list<TestOp*> inflight;
    state_lock.Lock();

    TestOp *next = gen->next(*this);
    while (next || !inflight.empty()) {
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
	
	if (inflight.size() >= (unsigned) max_in_flight || (!next && !inflight.empty())) {
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

  TestWatchContext *get_watch_context(const string &oid) {
    return watches.count(oid) ? watches[oid] : 0;
  }

  TestWatchContext *watch(const string &oid) {
    assert(!watches.count(oid));
    return (watches[oid] = new TestWatchContext);
  }

  void unwatch(const string &oid) {
    assert(watches.count(oid));
    delete watches[oid];
    watches.erase(oid);
  }

  void rm_object_attrs(const string &oid, const set<string> &attrs)
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
    for (set<string>::const_iterator i = attrs.begin();
	 i != attrs.end();
	 ++i) {
      new_obj.attrs.erase(*i);
    }
    new_obj.tmap = false;
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void remove_object_header(const string &oid)
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
    new_obj.header = bufferlist();
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }


  void update_object_header(const string &oid, const bufferlist &bl)
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
    new_obj.header = bl;
    new_obj.tmap = false;
    new_obj.exists = true;
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void set_object_tmap(const string &oid, const map<string, ContDesc> &attrs,
		       bufferlist header,
		       bufferlist tmap_contents)
  {
    ObjectDesc new_obj(&cont_gen);
    pool_obj_cont[current_snap].erase(oid);
    new_obj.attrs = attrs;
    new_obj.tmap_contents = tmap_contents;
    new_obj.header = header;
    new_obj.tmap = true;
    new_obj.exists = true;
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void update_object_attrs(const string &oid, const map<string, ContDesc> &attrs)
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
    for (map<string, ContDesc>::const_iterator i = attrs.begin();
	 i != attrs.end();
	 ++i) {
      new_obj.attrs[i->first] = i->second;
    }
    new_obj.tmap = false;
    new_obj.exists = true;
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
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
    new_obj.exists = true;
    if (new_obj.tmap) {
      new_obj.tmap = false;
      new_obj.attrs.clear();
      new_obj.header = bufferlist();
    }
    new_obj.update(contents);
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void remove_object(const string &oid)
  {
    assert(!get_watch_context(oid));
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
    assert(!get_watch_context(oid));
    ObjectDesc contents(&cont_gen);
    find_object(oid, &contents, snap);
    pool_obj_cont.rbegin()->second.erase(oid);
    pool_obj_cont.rbegin()->second.insert(pair<string,ObjectDesc>(oid, contents));
  }
};

void read_callback(librados::completion_t comp, void *arg);
void write_callback(librados::completion_t comp, void *arg);

class RemoveAttrsOp : public TestOp {
public:
  string oid;
  librados::ObjectWriteOperation op;
  librados::AioCompletion *comp;
  bool done;
  RemoveAttrsOp(RadosTestContext *context,
	       const string &oid,
	       TestOpStat *stat) :
    TestOp(context, stat), oid(oid), comp(NULL), done(false)
    {}

  void _begin()
  {
    ContDesc cont;
    set<string> to_remove;
    {
      Mutex::Locker l(context->state_lock);
      ObjectDesc obj(&context->cont_gen);
      if (!context->find_object(oid, &obj)) {
	context->kick();
	done = true;
	return;
      }
      cont = ContDesc(context->seq_num, context->current_snap,
		      context->seq_num, "");
      context->oid_in_use.insert(oid);
      context->oid_not_in_use.erase(oid);

      if (rand() % 30) {
	ContentsGenerator::iterator iter = context->cont_gen.get_iterator(cont);
	for (map<string, ContDesc>::iterator i = obj.attrs.begin();
	     i != obj.attrs.end();
	     ++i, ++iter) {
	  if (!(*iter % 3)) {
	    //op.rmxattr(i->first.c_str());
	    to_remove.insert(i->first);
	    op.rmxattr(i->first.c_str());
	  }
	}
	if (to_remove.empty()) {
	  context->kick();
	  context->oid_in_use.erase(oid);
	  context->oid_not_in_use.insert(oid);
	  done = true;
	  return;
	}
	op.omap_rm_keys(to_remove);
      } else {
	op.omap_clear();
	for (map<string, ContDesc>::iterator i = obj.attrs.begin();
	     i != obj.attrs.end();
	     ++i) {
	  op.rmxattr(i->first.c_str());
	  to_remove.insert(i->first);
	}
	context->remove_object_header(oid);
      }
      context->rm_object_attrs(oid, to_remove);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, &write_callback,
						NULL);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op);
  }

  void _finish(CallbackInfo *info)
  {
    Mutex::Locker l(context->state_lock);
    done = true;
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "RemoveAttrsOp";
  }
};

class TmapPutOp : public TestOp {
public:
  string oid;
  librados::ObjectWriteOperation op;
  librados::AioCompletion *comp;
  bool done;
  TmapPutOp(RadosTestContext *context,
	       const string &oid,
	       TestOpStat *stat) :
    TestOp(context, stat), oid(oid), comp(NULL), done(false)
    {}

  void _begin()
  {
    ContDesc cont;
    {
      Mutex::Locker l(context->state_lock);
      cont = ContDesc(context->seq_num, context->current_snap,
		      context->seq_num, "");
      context->oid_in_use.insert(oid);
      context->oid_not_in_use.erase(oid);
    }

    op.remove();
    map<string, bufferlist> omap_contents;
    map<string, ContDesc> omap;
    bufferlist header;
    ContentsGenerator::iterator keygen = context->cont_gen.get_iterator(cont);
    while (!*keygen) ++keygen;
    while (*keygen) {
      header.append(*keygen);
      ++keygen;
    }
    for (int i = 0; i < 20; ++i) {
      string key;
      while (!*keygen) ++keygen;
      while (*keygen && key.size() < 40) {
	key.push_back((*keygen % 20) + 'a');
	++keygen;
      }
      ContDesc val(cont);
      val.seqnum += context->cont_gen.get_length(cont);
      val.prefix = ("oid: " + oid);
      omap[key] = val;
      bufferlist val_buffer = context->cont_gen.gen_attribute(val);
      omap_contents[key] = val_buffer;
      op.setxattr(key.c_str(), val_buffer);
    }

    bufferlist tmap_contents;
    ::encode(header, tmap_contents);
    ::encode(omap_contents, tmap_contents);
    op.tmap_put(tmap_contents);
    {
      Mutex::Locker l(context->state_lock);
      context->set_object_tmap(oid, omap, header,
			       tmap_contents);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, &write_callback,
						NULL);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op);
  }

  void _finish(CallbackInfo *info)
  {
    Mutex::Locker l(context->state_lock);
    int r;
    if ((r = comp->get_return_value())) {
      cerr << "err " << r << std::endl;
      assert(0);
    }
    done = true;
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "TmapPutOp";
  }
};

class SetAttrsOp : public TestOp {
public:
  string oid;
  librados::ObjectWriteOperation op;
  librados::AioCompletion *comp;
  bool done;
  SetAttrsOp(RadosTestContext *context,
	       const string &oid,
	       TestOpStat *stat) :
    TestOp(context, stat), oid(oid), comp(NULL), done(false)
    {}

  void _begin()
  {
    ContDesc cont;
    {
      Mutex::Locker l(context->state_lock);
      cont = ContDesc(context->seq_num, context->current_snap,
		      context->seq_num, "");
      context->oid_in_use.insert(oid);
      context->oid_not_in_use.erase(oid);
    }

    map<string, bufferlist> omap_contents;
    map<string, ContDesc> omap;
    bufferlist header;
    ContentsGenerator::iterator keygen = context->cont_gen.get_iterator(cont);
    op.create(false);
    while (!*keygen) ++keygen;
    while (*keygen) {
      header.append(*keygen);
      ++keygen;
    }
    for (int i = 0; i < 20; ++i) {
      string key;
      while (!*keygen) ++keygen;
      while (*keygen && key.size() < 40) {
	key.push_back((*keygen % 20) + 'a');
	++keygen;
      }
      ContDesc val(cont);
      val.seqnum += context->cont_gen.get_length(cont);
      val.prefix = ("oid: " + oid);
      omap[key] = val;
      bufferlist val_buffer = context->cont_gen.gen_attribute(val);
      omap_contents[key] = val_buffer;
      op.setxattr(key.c_str(), val_buffer);
    }
    op.omap_set_header(header);
    op.omap_set(omap_contents);

    {
      Mutex::Locker l(context->state_lock);
      context->update_object_header(oid, header);
      context->update_object_attrs(oid, omap);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, &write_callback,
						NULL);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op);
  }

  void _finish(CallbackInfo *info)
  {
    Mutex::Locker l(context->state_lock);
    int r;
    if ((r = comp->get_return_value())) {
      cerr << "err " << r << std::endl;
      assert(0);
    }
    done = true;
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "SetAttrsOp";
  }
};

class WriteOp : public TestOp {
public:
  string oid;
  ContDesc cont;
  set<librados::AioCompletion *> waiting;
  uint64_t waiting_on;
  uint64_t last_acked_tid;

  WriteOp(RadosTestContext *context, 
	  const string &oid,
	  TestOpStat *stat = 0) : 
    TestOp(context, stat),
    oid(oid), waiting_on(0), last_acked_tid(0)
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
    context->oid_not_in_use.erase(oid);

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
    context->state_lock.Unlock();

    int r = context->io_ctx.selfmanaged_snap_set_write_ctx(context->seq, snapset);
    if (r) {
      cerr << "r is " << r << " snapset is " << snapset << " seq is " << context->seq << std::endl;
      assert(0);
    }

    waiting_on = ranges.num_intervals();
    cout << "waiting_on = " << waiting_on << std::endl;
    ContentsGenerator::iterator gen_pos = context->cont_gen.get_iterator(cont);
    uint64_t tid = 1;
    for (interval_set<uint64_t>::iterator i = ranges.begin(); 
	 i != ranges.end();
	 ++i, ++tid) {
      bufferlist to_write;
      gen_pos.seek(i.get_start());
      for (uint64_t k = 0; k != i.get_len(); ++k, ++gen_pos) {
	to_write.append(*gen_pos);
      }
      assert(to_write.length() == i.get_len());
      assert(to_write.length() > 0);
      std::cout << "Writing " << context->prefix+oid << " from " << i.get_start()
		<< " to " << i.get_len() + i.get_start() << " tid " << tid
		<< " ranges are " << ranges << std::endl;
      pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
	new pair<TestOp*, TestOp::CallbackInfo*>(this,
						 new TestOp::CallbackInfo(tid));
      librados::AioCompletion *completion =
	context->rados.aio_create_completion((void*) cb_arg, &write_callback, NULL);
      waiting.insert(completion);
      context->io_ctx.aio_write(context->prefix+oid, completion,
				to_write, i.get_len(), i.get_start());
    }
  }

  void _finish(CallbackInfo *info)
  {
    assert(info);
    context->state_lock.Lock();
    uint64_t tid = info->id;

    cout << "finishing write tid " << tid << " to " << context->prefix + oid << std::endl;

    if (tid <= last_acked_tid) {
      cerr << "Error: finished tid " << tid
	   << " when last_acked_tid was " << last_acked_tid << std::endl;
      assert(0);
    }
    last_acked_tid = tid;

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
    return done;
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
    if (context->get_watch_context(oid)) {
      context->kick();
      context->state_lock.Unlock();
      return;
    }

    ObjectDesc contents(&context->cont_gen);
    context->find_object(oid, &contents);
    bool present = !contents.deleted();

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
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
    context->kick();
    context->state_lock.Unlock();
  }

  string getType()
  {
    return "DeleteOp";
  }
};

class ReadOp : public TestOp {
public:
  librados::AioCompletion *completion;
  librados::ObjectReadOperation op;
  string oid;
  ObjectDesc old_value;
  int snap;

  bufferlist result;
  int retval;

  map<string, bufferlist> attrs;
  int attrretval;

  set<string> omap_requested_keys;
  map<string, bufferlist> omap_returned_values;
  set<string> omap_keys;
  map<string, bufferlist> omap;
  bufferlist header;

  map<string, bufferlist> xattrs;
  ReadOp(RadosTestContext *context, 
	 const string &oid,
	 TestOpStat *stat = 0) : 
    TestOp(context, stat),
    completion(NULL),
    oid(oid),
    old_value(&context->cont_gen),
    snap(0),
    retval(0),
    attrretval(0)
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
    completion = context->rados.aio_create_completion((void *) this, &read_callback, 0);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    assert(context->find_object(oid, &old_value, snap));

    TestWatchContext *ctx = context->get_watch_context(oid);
    context->state_lock.Unlock();
    if (ctx) {
      assert(old_value.exists);
      TestAlarm alarm;
      std::cerr << "about to start" << std::endl;
      ctx->start();
      std::cerr << "started" << std::endl;
      bufferlist bl;
      context->io_ctx.set_notify_timeout(600);
      int r = context->io_ctx.notify(context->prefix+oid, 0, bl);
      if (r < 0) {
	std::cerr << "r is " << r << std::endl;
	assert(0);
      }
      std::cerr << "notified, waiting" << std::endl;
      ctx->wait();
    }
    if (snap >= 0) {
      context->io_ctx.snap_set_read(context->snaps[snap]);
    }

    if (!old_value.tmap) {
      op.read(0,
	      !old_value.has_contents() ? 0 :
	      context->cont_gen.get_length(old_value.most_recent()),
	      &result,
	      &retval);
    }

    for (map<string, ContDesc>::iterator i = old_value.attrs.begin();
	 i != old_value.attrs.end();
	 ++i) {
      if (rand() % 2) {
	string key = i->first;
	if (rand() % 2)
	  key.push_back((rand() % 26) + 'a');
	omap_requested_keys.insert(key);
      }
    }
    op.omap_get_vals_by_keys(omap_requested_keys, &omap_returned_values, 0);

    op.omap_get_keys("", -1, &omap_keys, 0);
    op.omap_get_vals("", -1, &omap, 0);
    op.getxattrs(&xattrs, 0);
    op.omap_get_header(&header, 0);
    assert(!context->io_ctx.aio_operate(context->prefix+oid, completion, &op, 0));
    if (snap >= 0) {
      context->io_ctx.snap_set_read(0);
    }
  }

  void _finish(CallbackInfo *info)
  {
    context->state_lock.Lock();
    assert(!done);
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    assert(completion->is_complete());
    if (int err = completion->get_return_value()) {
      if (!(err == -ENOENT && old_value.deleted())) {
	cerr << "Error: oid " << oid << " read returned error code "
	     << err << std::endl;
      }
    } else {
      assert(!old_value.deleted());
      if (old_value.has_contents()) {
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

      // Attributes
      if (!(old_value.header == header)) {
	cerr << "oid: " << oid << " header does not match, old size: "
	     << old_value.header.length() << " new size " << header.length()
	     << std::endl;
	assert(old_value.header == header);
      }
      if (omap.size() != old_value.attrs.size()) {
	cerr << "oid: " << oid << " tmap.size() is " << omap.size()
	     << " and old is " << old_value.attrs.size() << std::endl;
	assert(omap.size() == old_value.attrs.size());
      }
      if (omap_keys.size() != old_value.attrs.size()) {
	cerr << "oid: " << oid << " tmap.size() is " << omap_keys.size()
	     << " and old is " << old_value.attrs.size() << std::endl;
	assert(omap_keys.size() == old_value.attrs.size());
      }
      if (xattrs.size() != old_value.attrs.size()) {
	cerr << "oid: " << oid << " xattrs.size() is " << xattrs.size()
	     << " and old is " << old_value.attrs.size() << std::endl;
	assert(xattrs.size() == old_value.attrs.size());
      }
      for (map<string, bufferlist>::iterator omap_iter = omap.begin();
	   omap_iter != omap.end();
	   ++omap_iter) {
	assert(old_value.attrs.count(omap_iter->first));
	assert(xattrs.count(omap_iter->first));
	bufferlist bl = context->cont_gen.gen_attribute(
	  old_value.attrs[omap_iter->first]);
	assert(bl.length() == omap_iter->second.length());
	assert(bl.length() == xattrs[omap_iter->first].length());
	bufferlist::iterator k = bl.begin();
	bufferlist::iterator j = xattrs[omap_iter->first].begin();
	for(bufferlist::iterator l = omap_iter->second.begin();
	    !k.end() && !l.end() && !j.end();
	    ++k, ++l, ++j) {
	  assert(*l == *k);
	  assert(*j == *k);
	}
      }
      for (set<string>::iterator i = omap_requested_keys.begin();
	   i != omap_requested_keys.end();
	   ++i) {
	if (!omap_returned_values.count(*i))
	  assert(!old_value.attrs.count(*i));
	if (!old_value.attrs.count(*i))
	  assert(!omap_returned_values.count(*i));
      }
      for (map<string, bufferlist>::iterator i = omap_returned_values.begin();
	   i != omap_returned_values.end();
	   ++i) {
	assert(omap_requested_keys.count(i->first));
	assert(omap.count(i->first));
	assert(old_value.attrs.count(i->first));
	assert(i->second == omap[i->first]);
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

    vector<uint64_t> snapset(context->snaps.size());
    int j = 0;
    for (map<int,uint64_t>::reverse_iterator i = context->snaps.rbegin();
	 i != context->snaps.rend();
	 ++i, ++j) {
      snapset[j] = i->second;
    }

    context->state_lock.Unlock();
    int r = context->io_ctx.selfmanaged_snap_set_write_ctx(context->seq, snapset);
    if (r) {
      cerr << "r is " << r << " snapset is " << snapset << " seq is " << context->seq << std::endl;
      assert(0);
    }
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

    vector<uint64_t> snapset(context->snaps.size());
    int j = 0;
    for (map<int,uint64_t>::reverse_iterator i = context->snaps.rbegin();
	 i != context->snaps.rend();
	 ++i, ++j) {
      snapset[j] = i->second;
    }

    int r = context->io_ctx.selfmanaged_snap_set_write_ctx(context->seq, snapset);
    if (r) {
      cerr << "r is " << r << " snapset is " << snapset << " seq is " << context->seq << std::endl;
      assert(0);
    }
  }

  string getType()
  {
    return "SnapRemoveOp";
  }
};

class WatchOp : public TestOp {
  string oid;
public:
  WatchOp(RadosTestContext *context,
	     const string &_oid,
	     TestOpStat *stat = 0) :
    TestOp(context, stat),
    oid(_oid)
  {}

  void _begin()
  {
    context->state_lock.Lock();
    ObjectDesc contents(&context->cont_gen);
    context->find_object(oid, &contents);
    if (contents.deleted()) {
      context->kick();
      context->state_lock.Unlock();
      return;
    }
    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    vector<uint64_t> snapset(context->snaps.size());
    int j = 0;
    for (map<int,uint64_t>::reverse_iterator i = context->snaps.rbegin();
	 i != context->snaps.rend();
	 ++i, ++j) {
      snapset[j] = i->second;
    }

    TestWatchContext *ctx = context->get_watch_context(oid);
    context->state_lock.Unlock();
    assert(!context->io_ctx.selfmanaged_snap_set_write_ctx(context->seq, snapset));
    int r;
    if (!ctx) {
      {
	Mutex::Locker l(context->state_lock);
	ctx = context->watch(oid);
      }

      r = context->io_ctx.watch(context->prefix+oid,
				0,
				&ctx->get_handle(),
				ctx);
    } else {
      r = context->io_ctx.unwatch(context->prefix+oid,
				  ctx->get_handle());
      {
	Mutex::Locker l(context->state_lock);
	context->unwatch(oid);
      }
    }

    if (r) {
      cerr << "r is " << r << std::endl;
      assert(0);
    }

    {
      Mutex::Locker l(context->state_lock);
      context->oid_in_use.erase(oid);
      context->oid_not_in_use.insert(oid);
    }
  }

  string getType()
  {
    return "WatchOp";
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
    if (context->get_watch_context(oid)) {
      context->kick();
      context->state_lock.Unlock();
      return;
    }
    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
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

    {
      Mutex::Locker l(context->state_lock);
      context->oid_in_use.erase(oid);
      context->oid_not_in_use.insert(oid);
    }
  }

  string getType()
  {
    return "RollBackOp";
  }
};

#endif
