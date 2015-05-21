// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#include "include/int_types.h"

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
#include "test/librados/test.h"
#include "include/memory.h"
#include "common/sharedptr_registry.hpp"
#include "common/errno.h"
#include "osd/HitSet.h"

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
  TEST_OP_WRITE_EXCL,
  TEST_OP_DELETE,
  TEST_OP_SNAP_CREATE,
  TEST_OP_SNAP_REMOVE,
  TEST_OP_ROLLBACK,
  TEST_OP_SETATTR,
  TEST_OP_RMATTR,
  TEST_OP_WATCH,
  TEST_OP_COPY_FROM,
  TEST_OP_HIT_SET_LIST,
  TEST_OP_UNDIRTY,
  TEST_OP_IS_DIRTY,
  TEST_OP_CACHE_FLUSH,
  TEST_OP_CACHE_TRY_FLUSH,
  TEST_OP_CACHE_EVICT,
  TEST_OP_APPEND,
  TEST_OP_APPEND_EXCL
};

class TestWatchContext : public librados::WatchCtx2 {
  TestWatchContext(const TestWatchContext&);
public:
  Cond cond;
  uint64_t handle;
  bool waiting;
  Mutex lock;
  TestWatchContext() : handle(0), waiting(false),
		       lock("watch lock") {}
  void handle_notify(uint64_t notify_id, uint64_t cookie,
		     uint64_t notifier_id,
		     bufferlist &bl) {
    Mutex::Locker l(lock);
    waiting = false;
    cond.SignalAll();
  }
  void handle_error(uint64_t cookie, int err) {
    Mutex::Locker l(lock);
    cout << "watch handle_error " << err << std::endl;
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
  int num;
  RadosTestContext *context;
  TestOpStat *stat;
  bool done;
  TestOp(int n, RadosTestContext *context,
	 TestOpStat *stat = 0)
    : num(n),
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
  virtual bool must_quiesce_other_ops() { return false; }
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
  set<string> oid_flushing;
  set<string> oid_not_flushing;
  SharedPtrRegistry<int, int> snaps_in_use;
  int current_snap;
  string pool_name;
  librados::IoCtx io_ctx;
  librados::Rados rados;
  int next_oid;
  string prefix;
  int errors;
  int max_in_flight;
  int seq_num;
  map<int,uint64_t> snaps;
  uint64_t seq;
  const char *rados_id;
  bool initialized;
  map<string, TestWatchContext*> watches;
  const uint64_t max_size;
  const uint64_t min_stride_size;
  const uint64_t max_stride_size;
  AttrGenerator attr_gen;
  const bool no_omap;
  bool pool_snaps;
  bool write_fadvise_dontneed;
  int snapname_num;

  RadosTestContext(const string &pool_name, 
		   int max_in_flight,
		   uint64_t max_size,
		   uint64_t min_stride_size,
		   uint64_t max_stride_size,
		   bool no_omap,
		   bool pool_snaps,
		   bool write_fadvise_dontneed,
		   const char *id = 0) :
    state_lock("Context Lock"),
    pool_obj_cont(),
    current_snap(0),
    pool_name(pool_name),
    next_oid(0),
    errors(0),
    max_in_flight(max_in_flight),
    seq_num(0), seq(0),
    rados_id(id), initialized(false),
    max_size(max_size), 
    min_stride_size(min_stride_size), max_stride_size(max_stride_size),
    attr_gen(2000, 20000),
    no_omap(no_omap),
    pool_snaps(pool_snaps),
    write_fadvise_dontneed(write_fadvise_dontneed),
    snapname_num(0)
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
    bufferlist inbl;
    r = rados.mon_command(
      "{\"prefix\": \"osd pool set\", \"pool\": \"" + pool_name +
      "\", \"var\": \"write_fadvise_dontneed\", \"val\": \"" + (write_fadvise_dontneed ? "true" : "false") + "\"}",
      inbl, NULL, NULL);
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
    TestOp *waiting = NULL;

    while (next || !inflight.empty()) {
      if (next && next->must_quiesce_other_ops() && !inflight.empty()) {
	waiting = next;
	next = NULL;   // Force to wait for inflight to drain
      }
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
	    cout << (*i)->num << ": done (" << (inflight.size()-1) << " left)" << std::endl;
	    delete *i;
	    inflight.erase(i++);
	  } else {
	    ++i;
	  }
	}
	
	if (inflight.size() >= (unsigned) max_in_flight || (!next && !inflight.empty())) {
	  cout << " waiting on " << inflight.size() << std::endl;
	  wait();
	} else {
	  break;
	}
      }
      if (waiting) {
	next = waiting;
	waiting = NULL;
      } else {
	next = gen->next(*this);
      }
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

  ObjectDesc get_most_recent(const string &oid) {
    ObjectDesc new_obj;
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
    return new_obj;
  }

  void rm_object_attrs(const string &oid, const set<string> &attrs)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    for (set<string>::const_iterator i = attrs.begin();
	 i != attrs.end();
	 ++i) {
      new_obj.attrs.erase(*i);
    }
    new_obj.dirty = true;
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void remove_object_header(const string &oid)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    new_obj.header = bufferlist();
    new_obj.dirty = true;
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }


  void update_object_header(const string &oid, const bufferlist &bl)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    new_obj.header = bl;
    new_obj.exists = true;
    new_obj.dirty = true;
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void update_object_attrs(const string &oid, const map<string, ContDesc> &attrs)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    for (map<string, ContDesc>::const_iterator i = attrs.begin();
	 i != attrs.end();
	 ++i) {
      new_obj.attrs[i->first] = i->second;
    }
    new_obj.exists = true;
    new_obj.dirty = true;
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void update_object(ContentsGenerator *cont_gen,
		     const string &oid, const ContDesc &contents)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    new_obj.exists = true;
    new_obj.dirty = true;
    new_obj.update(cont_gen,
		   contents);
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void update_object_full(const string &oid, const ObjectDesc &contents)
  {
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, contents));
    pool_obj_cont[current_snap][oid].dirty = true;
  }

  void update_object_undirty(const string &oid)
  {
    ObjectDesc new_obj = get_most_recent(oid);
    new_obj.dirty = false;
    pool_obj_cont[current_snap].erase(oid);
    pool_obj_cont[current_snap].insert(pair<string,ObjectDesc>(oid, new_obj));
  }

  void update_object_version(const string &oid, uint64_t version,
			     int snap = -1)
  {
    for (map<int, map<string,ObjectDesc> >::reverse_iterator i = 
	   pool_obj_cont.rbegin();
	 i != pool_obj_cont.rend();
	 ++i) {
      if (snap != -1 && snap < i->first)
	continue;
      map<string,ObjectDesc>::iterator j = i->second.find(oid);
      if (j != i->second.end()) {
	if (version)
	  j->second.version = version;
	cout << __func__ << " oid " << oid
	     << " v " << version << " " << j->second.most_recent()
	     << " " << (j->second.dirty ? "dirty" : "clean")
	     << " " << (j->second.exists ? "exists" : "dne")
	     << std::endl;
	break;
      }
    }
  }

  void remove_object(const string &oid)
  {
    assert(!get_watch_context(oid));
    ObjectDesc new_obj;
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
    ObjectDesc contents;
    find_object(oid, &contents, snap);
    contents.dirty = true;
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
  RemoveAttrsOp(int n, RadosTestContext *context,
	       const string &oid,
	       TestOpStat *stat)
    : TestOp(n, context, stat), oid(oid), comp(NULL), done(false)
  {}

  void _begin()
  {
    ContDesc cont;
    set<string> to_remove;
    {
      Mutex::Locker l(context->state_lock);
      ObjectDesc obj;
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
	ContentsGenerator::iterator iter = context->attr_gen.get_iterator(cont);
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
	if (!context->no_omap) {
	  op.omap_rm_keys(to_remove);
	}
      } else {
	if (!context->no_omap) {
	  op.omap_clear();
	}
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
    comp = context->rados.aio_create_completion((void*) cb_arg, NULL,
						&write_callback);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op);
  }

  void _finish(CallbackInfo *info)
  {
    Mutex::Locker l(context->state_lock);
    done = true;
    context->update_object_version(oid, comp->get_version64());
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

class SetAttrsOp : public TestOp {
public:
  string oid;
  librados::ObjectWriteOperation op;
  librados::AioCompletion *comp;
  bool done;
  SetAttrsOp(int n,
	     RadosTestContext *context,
	     const string &oid,
	     TestOpStat *stat)
    : TestOp(n, context, stat),
      oid(oid), comp(NULL), done(false)
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
    ContentsGenerator::iterator keygen = context->attr_gen.get_iterator(cont);
    op.create(false);
    while (!*keygen) ++keygen;
    while (*keygen) {
      if (*keygen != '_')
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
      val.seqnum += (unsigned)(*keygen);
      val.prefix = ("oid: " + oid);
      omap[key] = val;
      bufferlist val_buffer = context->attr_gen.gen_bl(val);
      omap_contents[key] = val_buffer;
      op.setxattr(key.c_str(), val_buffer);
    }
    if (!context->no_omap) {
      op.omap_set_header(header);
      op.omap_set(omap_contents);
    }

    {
      Mutex::Locker l(context->state_lock);
      context->update_object_header(oid, header);
      context->update_object_attrs(oid, omap);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, NULL,
						&write_callback);
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
    context->update_object_version(oid, comp->get_version64());
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
  librados::AioCompletion *rcompletion;
  uint64_t waiting_on;
  uint64_t last_acked_tid;

  librados::ObjectReadOperation read_op;
  librados::ObjectWriteOperation write_op;
  bufferlist rbuffer;

  bool do_append;
  bool do_excl;

  WriteOp(int n,
	  RadosTestContext *context,
	  const string &oid,
	  bool do_append,
	  bool do_excl,
	  TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      oid(oid), waiting_on(0), last_acked_tid(0), do_append(do_append),
      do_excl(do_excl)
  {}
		
  void _begin()
  {
    context->state_lock.Lock();
    done = 0;
    stringstream acc;
    acc << context->prefix << "OID: " << oid << " snap " << context->current_snap << std::endl;
    string prefix = acc.str();

    cont = ContDesc(context->seq_num, context->current_snap, context->seq_num, prefix);

    ContentsGenerator *cont_gen;
    if (do_append) {
      ObjectDesc old_value;
      bool found = context->find_object(oid, &old_value);
      uint64_t prev_length = found && old_value.has_contents() ?
	old_value.most_recent_gen()->get_length(old_value.most_recent()) :
	0;
      cont_gen = new AppendGenerator(
	prev_length,
	(context->io_ctx.pool_requires_alignment() ?
	 context->io_ctx.pool_required_alignment() : 0),
	context->min_stride_size,
	context->max_stride_size,
	3);
    } else {
      cont_gen = new VarLenGenerator(
	context->max_size, context->min_stride_size, context->max_stride_size);
    }
    context->update_object(cont_gen, oid, cont);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    map<uint64_t, uint64_t> ranges;

    cont_gen->get_ranges_map(cont, ranges);
    std::cout << num << ":  seq_num " << context->seq_num << " ranges " << ranges << std::endl;
    context->seq_num++;

    waiting_on = ranges.size();
    //cout << " waiting_on = " << waiting_on << std::endl;
    ContentsGenerator::iterator gen_pos = cont_gen->get_iterator(cont);
    uint64_t tid = 1;
    for (map<uint64_t, uint64_t>::iterator i = ranges.begin(); 
	 i != ranges.end();
	 ++i, ++tid) {
      bufferlist to_write;
      gen_pos.seek(i->first);
      for (uint64_t k = 0; k != i->second; ++k, ++gen_pos) {
	to_write.append(*gen_pos);
      }
      assert(to_write.length() == i->second);
      assert(to_write.length() > 0);
      std::cout << num << ":  writing " << context->prefix+oid
		<< " from " << i->first
		<< " to " << i->first + i->second << " tid " << tid << std::endl;
      pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
	new pair<TestOp*, TestOp::CallbackInfo*>(this,
						 new TestOp::CallbackInfo(tid));
      librados::AioCompletion *completion =
	context->rados.aio_create_completion((void*) cb_arg, NULL,
					     &write_callback);
      waiting.insert(completion);
      librados::ObjectWriteOperation op;
      if (do_append) {
	op.append(to_write);
      } else {
	op.write(i->first, to_write);
      }
      if (do_excl && tid == 1)
	op.assert_exists();
      context->io_ctx.aio_operate(
	context->prefix+oid, completion,
	&op);
    }

    bufferlist contbl;
    ::encode(cont, contbl);
    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(
	this,
	new TestOp::CallbackInfo(++tid));
    librados::AioCompletion *completion = context->rados.aio_create_completion(
      (void*) cb_arg, NULL, &write_callback);
    waiting.insert(completion);
    waiting_on++;
    write_op.setxattr("_header", contbl);
    if (!do_append) {
      write_op.truncate(cont_gen->get_length(cont));
    }
    context->io_ctx.aio_operate(
      context->prefix+oid, completion, &write_op);

    cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(
	this,
	new TestOp::CallbackInfo(++tid));
    rcompletion = context->rados.aio_create_completion(
         (void*) cb_arg, NULL, &write_callback);
    waiting_on++;
    read_op.read(0, 1, &rbuffer, 0);
    context->io_ctx.aio_operate(
      context->prefix+oid, rcompletion,
      &read_op,
      librados::OPERATION_ORDER_READS_WRITES,  // order wrt previous write/update
      0);
    context->state_lock.Unlock();
  }

  void _finish(CallbackInfo *info)
  {
    assert(info);
    context->state_lock.Lock();
    uint64_t tid = info->id;

    cout << num << ":  finishing write tid " << tid << " to " << context->prefix + oid << std::endl;

    if (tid <= last_acked_tid) {
      cerr << "Error: finished tid " << tid
	   << " when last_acked_tid was " << last_acked_tid << std::endl;
      assert(0);
    }
    last_acked_tid = tid;

    assert(!done);
    waiting_on--;
    if (waiting_on == 0) {
      uint64_t version = 0;
      for (set<librados::AioCompletion *>::iterator i = waiting.begin();
	   i != waiting.end();
	   ) {
	assert((*i)->is_complete());
	if (int err = (*i)->get_return_value()) {
	  cerr << "Error: oid " << oid << " write returned error code "
	       << err << std::endl;
	}
	if ((*i)->get_version64() > version)
	  version = (*i)->get_version64();
	(*i)->release();
	waiting.erase(i++);
      }
      
      context->update_object_version(oid, version);
      if (rcompletion->get_version64() != version) {
	cerr << "Error: racing read on " << oid << " returned version "
	     << rcompletion->get_version64() << " rather than version "
	     << version << std::endl;
	assert(0 == "racing read got wrong version");
      }

      {
	ObjectDesc old_value;
	assert(context->find_object(oid, &old_value, -1));
	if (old_value.deleted())
	  std::cout << num << ":  left oid " << oid << " deleted" << std::endl;
	else
	  std::cout << num << ":  left oid " << oid << " "
		    << old_value.most_recent() << std::endl;
      }

      rcompletion->release();
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

  DeleteOp(int n,
	   RadosTestContext *context,
	   const string &oid,
	   TestOpStat *stat = 0)
    : TestOp(n, context, stat), oid(oid)
  {}

  void _begin()
  {
    context->state_lock.Lock();
    if (context->get_watch_context(oid)) {
      context->kick();
      context->state_lock.Unlock();
      return;
    }

    ObjectDesc contents;
    context->find_object(oid, &contents);
    bool present = !contents.deleted();

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    context->seq_num++;

    context->remove_object(oid);

    interval_set<uint64_t> ranges;
    context->state_lock.Unlock();

    int r = context->io_ctx.remove(context->prefix+oid);
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

  ceph::shared_ptr<int> in_use;

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
  ReadOp(int n,
	 RadosTestContext *context,
	 const string &oid,
	 TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      completion(NULL),
      oid(oid),
      snap(0),
      retval(0),
      attrretval(0)
  {}
		
  void _begin()
  {
    context->state_lock.Lock();
    if (!(rand() % 4) && !context->snaps.empty()) {
      snap = rand_choose(context->snaps)->first;
      in_use = context->snaps_in_use.lookup_or_create(snap, snap);
    } else {
      snap = -1;
    }
    std::cout << num << ": read oid " << oid << " snap " << snap << std::endl;
    done = 0;
    completion = context->rados.aio_create_completion((void *) this, &read_callback, 0);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    assert(context->find_object(oid, &old_value, snap));
    if (old_value.deleted())
      std::cout << num << ":  expect deleted" << std::endl;
    else
      std::cout << num << ":  expect " << old_value.most_recent() << std::endl;

    TestWatchContext *ctx = context->get_watch_context(oid);
    context->state_lock.Unlock();
    if (ctx) {
      assert(old_value.exists);
      TestAlarm alarm;
      std::cerr << num << ":  about to start" << std::endl;
      ctx->start();
      std::cerr << num << ":  started" << std::endl;
      bufferlist bl;
      context->io_ctx.set_notify_timeout(600);
      int r = context->io_ctx.notify2(context->prefix+oid, bl, 0, NULL);
      if (r < 0) {
	std::cerr << "r is " << r << std::endl;
	assert(0);
      }
      std::cerr << num << ":  notified, waiting" << std::endl;
      ctx->wait();
    }
    if (snap >= 0) {
      context->io_ctx.snap_set_read(context->snaps[snap]);
    }

    op.read(0,
	    !old_value.has_contents() ? 0 :
	    old_value.most_recent_gen()->get_length(old_value.most_recent()),
	    &result,
	    &retval);

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
    if (!context->no_omap) {
      op.omap_get_vals_by_keys(omap_requested_keys, &omap_returned_values, 0);

      op.omap_get_keys("", -1, &omap_keys, 0);
      op.omap_get_vals("", -1, &omap, 0);
      op.omap_get_header(&header, 0);
    }
    op.getxattrs(&xattrs, 0);
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
    uint64_t version = completion->get_version64();
    if (int err = completion->get_return_value()) {
      if (!(err == -ENOENT && old_value.deleted())) {
	cerr << num << ": Error: oid " << oid << " read returned error code "
	     << err << std::endl;
	assert(0);
      }
    } else {
      map<string, bufferlist>::iterator iter = xattrs.find("_header");
      bufferlist headerbl;
      if (iter == xattrs.end()) {
	if (old_value.has_contents()) {
	  cerr << num << ": Error: did not find header attr, has_contents: "
	       << old_value.has_contents()
	       << std::endl;
	  assert(!old_value.has_contents());
	}
      } else {
	headerbl = iter->second;
	xattrs.erase(iter);
      }
      if (old_value.deleted()) {
	std::cout << num << ":  expect deleted" << std::endl;
	assert(0 == "expected deleted");
      } else {
	std::cout << num << ":  expect " << old_value.most_recent() << std::endl;
      }
      if (old_value.has_contents()) {
	ContDesc to_check;
	bufferlist::iterator p = headerbl.begin();
	::decode(to_check, p);
	if (to_check != old_value.most_recent()) {
	  cerr << num << ": oid " << oid << " found incorrect object contents " << to_check
	       << ", expected " << old_value.most_recent() << std::endl;
	  context->errors++;
	}
	if (!old_value.check(result)) {
	  cerr << num << ": oid " << oid << " contents " << to_check << " corrupt" << std::endl;
	  context->errors++;
	}
	if (context->errors) assert(0);
      }

      // Attributes
      if (!context->no_omap) {
	if (!(old_value.header == header)) {
	  cerr << num << ": oid " << oid << " header does not match, old size: "
	       << old_value.header.length() << " new size " << header.length()
	       << std::endl;
	  assert(old_value.header == header);
	}
	if (omap.size() != old_value.attrs.size()) {
	  cerr << num << ": oid " << oid << " omap.size() is " << omap.size()
	       << " and old is " << old_value.attrs.size() << std::endl;
	  assert(omap.size() == old_value.attrs.size());
	}
	if (omap_keys.size() != old_value.attrs.size()) {
	  cerr << num << ": oid " << oid << " omap.size() is " << omap_keys.size()
	       << " and old is " << old_value.attrs.size() << std::endl;
	  assert(omap_keys.size() == old_value.attrs.size());
	}
      }
      if (xattrs.size() != old_value.attrs.size()) {
	cerr << num << ": oid " << oid << " xattrs.size() is " << xattrs.size()
	     << " and old is " << old_value.attrs.size() << std::endl;
	assert(xattrs.size() == old_value.attrs.size());
      }
      if (version != old_value.version) {
	cerr << num << ": oid " << oid << " version is " << version
	     << " and expected " << old_value.version << std::endl;
	assert(version == old_value.version);
      }
      for (map<string, ContDesc>::iterator iter = old_value.attrs.begin();
	   iter != old_value.attrs.end();
	   ++iter) {
	bufferlist bl = context->attr_gen.gen_bl(
	  iter->second);
	if (!context->no_omap) {
	  map<string, bufferlist>::iterator omap_iter = omap.find(iter->first);
	  assert(omap_iter != omap.end());
	  assert(bl.length() == omap_iter->second.length());
	  bufferlist::iterator k = bl.begin();
	  for(bufferlist::iterator l = omap_iter->second.begin();
	      !k.end() && !l.end();
	      ++k, ++l) {
	    assert(*l == *k);
	  }
	}
	map<string, bufferlist>::iterator xattr_iter = xattrs.find(iter->first);
	assert(xattr_iter != xattrs.end());
	assert(bl.length() == xattr_iter->second.length());
	bufferlist::iterator k = bl.begin();
	for (bufferlist::iterator j = xattr_iter->second.begin();
	     !k.end() && !j.end();
	     ++j, ++k) {
	  assert(*j == *k);
	}
      }
      if (!context->no_omap) {
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
  SnapCreateOp(int n,
	       RadosTestContext *context,
	       TestOpStat *stat = 0)
    : TestOp(n, context, stat)
  {}

  void _begin()
  {
    uint64_t snap;
    string snapname;

    if (context->pool_snaps) {
      stringstream ss;

      ss << context->prefix << "snap" << ++context->snapname_num;
      snapname = ss.str();

      int ret = context->io_ctx.snap_create(snapname.c_str());
      if (ret) {
	cerr << "snap_create returned " << ret << std::endl;
	assert(0);
      }
      assert(!context->io_ctx.snap_lookup(snapname.c_str(), &snap));

    } else {
      assert(!context->io_ctx.selfmanaged_snap_create(&snap));
    }

    context->state_lock.Lock();
    context->add_snap(snap);

    if (context->pool_snaps) {
      context->state_lock.Unlock();
    } else {
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
  }

  string getType()
  {
    return "SnapCreateOp";
  }
  bool must_quiesce_other_ops() { return context->pool_snaps; }
};

class SnapRemoveOp : public TestOp {
public:
  int to_remove;
  SnapRemoveOp(int n, RadosTestContext *context,
	       int snap,
	       TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      to_remove(snap)
  {}

  void _begin()
  {
    context->state_lock.Lock();
    uint64_t snap = context->snaps[to_remove];
    context->remove_snap(to_remove);

    if (context->pool_snaps) {
      string snapname;

      assert(!context->io_ctx.snap_get_name(snap, &snapname));
      assert(!context->io_ctx.snap_remove(snapname.c_str()));
     } else {
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
    context->state_lock.Unlock();
  }

  string getType()
  {
    return "SnapRemoveOp";
  }
};

class WatchOp : public TestOp {
  string oid;
public:
  WatchOp(int n,
	  RadosTestContext *context,
	  const string &_oid,
	  TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      oid(_oid)
  {}

  void _begin()
  {
    context->state_lock.Lock();
    ObjectDesc contents;
    context->find_object(oid, &contents);
    if (contents.deleted()) {
      context->kick();
      context->state_lock.Unlock();
      return;
    }
    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    TestWatchContext *ctx = context->get_watch_context(oid);
    context->state_lock.Unlock();
    int r;
    if (!ctx) {
      {
	Mutex::Locker l(context->state_lock);
	ctx = context->watch(oid);
      }

      r = context->io_ctx.watch2(context->prefix+oid,
				 &ctx->get_handle(),
				 ctx);
    } else {
      r = context->io_ctx.unwatch2(ctx->get_handle());
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
  bool done;
  librados::ObjectWriteOperation op;
  librados::AioCompletion *comp;
  ceph::shared_ptr<int> in_use;

  RollbackOp(int n,
	     RadosTestContext *context,
	     const string &_oid,
	     TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      oid(_oid), roll_back_to(-1), 
      done(false), comp(NULL)
  {}

  void _begin()
  {
    context->state_lock.Lock();
    if (context->get_watch_context(oid)) {
      context->kick();
      context->state_lock.Unlock();
      return;
    }

    if (context->snaps.empty()) {
      context->kick();
      context->state_lock.Unlock();
      done = true;
      return;
    }

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    roll_back_to = rand_choose(context->snaps)->first;
    in_use = context->snaps_in_use.lookup_or_create(
      roll_back_to,
      roll_back_to);


    cout << "rollback oid " << oid << " to " << roll_back_to << std::endl;

    context->roll_back(oid, roll_back_to);
    uint64_t snap = context->snaps[roll_back_to];

    context->state_lock.Unlock();

    if (context->pool_snaps) {
      op.snap_rollback(snap);
    } else {
      op.selfmanaged_snap_rollback(snap);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, NULL,
						&write_callback);
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
    context->update_object_version(oid, comp->get_version64());
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    in_use = ceph::shared_ptr<int>();
    context->kick();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "RollBackOp";
  }
};

class CopyFromOp : public TestOp {
public:
  string oid, oid_src;
  ObjectDesc src_value;
  librados::ObjectWriteOperation op;
  librados::ObjectReadOperation rd_op;
  librados::AioCompletion *comp;
  librados::AioCompletion *comp_racing_read;
  ceph::shared_ptr<int> in_use;
  int snap;
  int done;
  uint64_t version;
  int r;
  CopyFromOp(int n,
	     RadosTestContext *context,
	     const string &oid,
	     const string &oid_src,
	     TestOpStat *stat)
    : TestOp(n, context, stat),
      oid(oid), oid_src(oid_src),
      comp(NULL), snap(-1), done(0), 
      version(0), r(0)
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
      context->oid_in_use.insert(oid_src);
      context->oid_not_in_use.erase(oid_src);

      // choose source snap
      if (0 && !(rand() % 4) && !context->snaps.empty()) {
	snap = rand_choose(context->snaps)->first;
	in_use = context->snaps_in_use.lookup_or_create(snap, snap);
      } else {
	snap = -1;
      }
      context->find_object(oid_src, &src_value, snap);
      if (!src_value.deleted())
	context->update_object_full(oid, src_value);
    }

    string src = context->prefix+oid_src;
    op.copy_from(src.c_str(), context->io_ctx, src_value.version);

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, NULL,
						&write_callback);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op);

    // queue up a racing read, too.
    pair<TestOp*, TestOp::CallbackInfo*> *read_cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(1));
    comp_racing_read = context->rados.aio_create_completion((void*) read_cb_arg, NULL, &write_callback);
    rd_op.stat(NULL, NULL, NULL);
    context->io_ctx.aio_operate(context->prefix+oid, comp_racing_read, &rd_op,
				librados::OPERATION_ORDER_READS_WRITES,  // order wrt previous write/update
				NULL);

  }

  void _finish(CallbackInfo *info)
  {
    Mutex::Locker l(context->state_lock);

    // note that the read can (and atm will) come back before the
    // write reply, but will reflect the update and the versions will
    // match.

    if (info->id == 0) {
      // copy_from
      assert(comp->is_complete());
      cout << num << ":  finishing copy_from to " << context->prefix + oid << std::endl;
      if ((r = comp->get_return_value())) {
	if (r == -ENOENT && src_value.deleted()) {
	  cout << num << ":  got expected ENOENT (src dne)" << std::endl;
	} else {
	  cerr << "Error: oid " << oid << " copy_from " << oid_src << " returned error code "
	       << r << std::endl;
	  assert(0);
	}
      } else {
	assert(!version || comp->get_version64() == version);
	version = comp->get_version64();
	context->update_object_version(oid, comp->get_version64());
      }
    } else if (info->id == 1) {
      // racing read
      assert(comp_racing_read->is_complete());
      cout << num << ":  finishing copy_from racing read to " << context->prefix + oid << std::endl;
      if ((r = comp_racing_read->get_return_value())) {
	if (!(r == -ENOENT && src_value.deleted())) {
	  cerr << "Error: oid " << oid << " copy_from " << oid_src << " returned error code "
	       << r << std::endl;
	}
      } else {
	assert(comp_racing_read->get_return_value() == 0);
	assert(!version || comp_racing_read->get_version64() == version);
	version = comp_racing_read->get_version64();
      }
    }
    if (++done == 2) {
      context->oid_in_use.erase(oid);
      context->oid_not_in_use.insert(oid);
      context->oid_in_use.erase(oid_src);
      context->oid_not_in_use.insert(oid_src);
      context->kick();
    }
  }

  bool finished()
  {
    return done == 2;
  }

  string getType()
  {
    return "CopyFromOp";
  }
};

class HitSetListOp : public TestOp {
  bool done;
  librados::AioCompletion *comp1, *comp2;
  uint32_t hash;
  std::list< std::pair<time_t, time_t> > ls;
  bufferlist bl;

public:
  HitSetListOp(int n,
	       RadosTestContext *context,
	       uint32_t hash,
	       TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      done(false), comp1(NULL), comp2(NULL),
      hash(hash)
  {}

  void _begin()
  {
    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp1 = context->rados.aio_create_completion((void*) cb_arg, NULL,
						 &write_callback);
    int r = context->io_ctx.hit_set_list(hash, comp1, &ls);
    assert(r == 0);
  }

  void _finish(CallbackInfo *info) {
    Mutex::Locker l(context->state_lock);
    if (!comp2) {
      if (ls.empty()) {
	cerr << num << ": no hitsets" << std::endl;
	done = true;
      } else {
	cerr << num << ": hitsets are " << ls << std::endl;
	int r = rand() % ls.size();
	std::list<pair<time_t,time_t> >::iterator p = ls.begin();
	while (r--)
	  ++p;
	pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
	  new pair<TestOp*, TestOp::CallbackInfo*>(this,
						   new TestOp::CallbackInfo(0));
	comp2 = context->rados.aio_create_completion((void*) cb_arg, NULL,
						     &write_callback);
	r = context->io_ctx.hit_set_get(hash, comp2, p->second, &bl);
	assert(r == 0);
      }
    } else {
      int r = comp2->get_return_value();
      if (r == 0) {
	HitSet hitset;
	bufferlist::iterator p = bl.begin();
	::decode(hitset, p);
	cout << num << ": got hitset of type " << hitset.get_type_name()
	     << " size " << bl.length()
	     << std::endl;
      } else {
	// FIXME: we could verify that we did in fact race with a trim...
	assert(r == -ENOENT);
      }
      done = true;
    }

    context->kick();
  }

  bool finished() {
    return done;
  }

  string getType() {
    return "HitSetListOp";
  }
};

class UndirtyOp : public TestOp {
public:
  librados::AioCompletion *completion;
  librados::ObjectWriteOperation op;
  string oid;

  UndirtyOp(int n,
	    RadosTestContext *context,
	    const string &oid,
	    TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      completion(NULL),
      oid(oid)
  {}

  void _begin()
  {
    context->state_lock.Lock();
    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    completion = context->rados.aio_create_completion((void *) cb_arg, NULL,
						      &write_callback);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    context->update_object_undirty(oid);
    context->state_lock.Unlock();

    op.undirty();
    int r = context->io_ctx.aio_operate(context->prefix+oid, completion,
					&op, 0);
    assert(!r);
  }

  void _finish(CallbackInfo *info)
  {
    context->state_lock.Lock();
    assert(!done);
    assert(completion->is_complete());
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->update_object_version(oid, completion->get_version64());
    context->kick();
    done = true;
    context->state_lock.Unlock();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "UndirtyOp";
  }
};

class IsDirtyOp : public TestOp {
public:
  librados::AioCompletion *completion;
  librados::ObjectReadOperation op;
  string oid;
  bool dirty;
  ObjectDesc old_value;
  int snap;
  ceph::shared_ptr<int> in_use;

  IsDirtyOp(int n,
	    RadosTestContext *context,
	    const string &oid,
	    TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      completion(NULL),
      oid(oid),
      dirty(false)
  {}

  void _begin()
  {
    context->state_lock.Lock();

    if (!(rand() % 4) && !context->snaps.empty()) {
      snap = rand_choose(context->snaps)->first;
      in_use = context->snaps_in_use.lookup_or_create(snap, snap);
    } else {
      snap = -1;
    }
    std::cout << num << ": is_dirty oid " << oid << " snap " << snap
	      << std::endl;

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    completion = context->rados.aio_create_completion((void *) cb_arg, NULL,
						      &write_callback);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    context->state_lock.Unlock();

    if (snap >= 0) {
      context->io_ctx.snap_set_read(context->snaps[snap]);
    }

    op.is_dirty(&dirty, NULL);
    int r = context->io_ctx.aio_operate(context->prefix+oid, completion,
					&op, 0);
    assert(!r);

    if (snap >= 0) {
      context->io_ctx.snap_set_read(0);
    }
  }

  void _finish(CallbackInfo *info)
  {
    context->state_lock.Lock();
    assert(!done);
    assert(completion->is_complete());
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);

    assert(context->find_object(oid, &old_value, snap));

    int r = completion->get_return_value();
    if (r == 0) {
      cout << num << ":  " << (dirty ? "dirty" : "clean") << std::endl;
      assert(!old_value.deleted());
      assert(dirty == old_value.dirty);
    } else {
      cout << num << ":  got " << r << std::endl;
      assert(r == -ENOENT);
      assert(old_value.deleted());
    }
    context->kick();
    done = true;
    context->state_lock.Unlock();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "IsDirtyOp";
  }
};



class CacheFlushOp : public TestOp {
public:
  librados::AioCompletion *completion;
  librados::ObjectReadOperation op;
  string oid;
  bool blocking;
  int snap;
  bool can_fail;
  ceph::shared_ptr<int> in_use;

  CacheFlushOp(int n,
	       RadosTestContext *context,
	       const string &oid,
	       TestOpStat *stat,
	       bool b)
    : TestOp(n, context, stat),
      completion(NULL),
      oid(oid),
      blocking(b),
      snap(0),
      can_fail(false)
  {}

  void _begin()
  {
    context->state_lock.Lock();

    if (!(rand() % 4) && !context->snaps.empty()) {
      snap = rand_choose(context->snaps)->first;
      in_use = context->snaps_in_use.lookup_or_create(snap, snap);
    } else {
      snap = -1;
    }
    // not being particularly specific here about knowing which
    // flushes are on the oldest clean snap and which ones are not.
    can_fail = !blocking || !context->snaps.empty();
    // FIXME: we can could fail if we've ever removed a snap due to
    // the async snap trimming.
    can_fail = true;
    cout << num << ": " << (blocking ? "cache_flush" : "cache_try_flush")
	 << " oid " << oid << " snap " << snap << std::endl;

    if (snap >= 0) {
      context->io_ctx.snap_set_read(context->snaps[snap]);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    completion = context->rados.aio_create_completion((void *) cb_arg, NULL,
						      &write_callback);
    // leave object in unused list so that we race with other operations
    //context->oid_in_use.insert(oid);
    //context->oid_not_in_use.erase(oid);
    context->oid_flushing.insert(oid);
    context->oid_not_flushing.erase(oid);
    context->state_lock.Unlock();

    unsigned flags = librados::OPERATION_IGNORE_CACHE;
    if (blocking) {
      op.cache_flush();
    } else {
      op.cache_try_flush();
      flags = librados::OPERATION_SKIPRWLOCKS;
    }
    int r = context->io_ctx.aio_operate(context->prefix+oid, completion,
					&op, flags, NULL);
    assert(!r);

    if (snap >= 0) {
      context->io_ctx.snap_set_read(0);
    }
  }

  void _finish(CallbackInfo *info)
  {
    context->state_lock.Lock();
    assert(!done);
    assert(completion->is_complete());
    //context->oid_in_use.erase(oid);
    //context->oid_not_in_use.insert(oid);
    context->oid_flushing.erase(oid);
    context->oid_not_flushing.insert(oid);
    int r = completion->get_return_value();
    cout << num << ":  got " << cpp_strerror(r) << std::endl;
    if (r == 0) {
      context->update_object_version(oid, 0, snap);
    } else if (r == -EBUSY) {
      assert(can_fail);
    } else if (r == -EINVAL) {
      // caching not enabled?
    } else if (r == -ENOENT) {
      // may have raced with a remove?
    } else {
      assert(0 == "shouldn't happen");
    }
    context->kick();
    done = true;
    context->state_lock.Unlock();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "CacheFlushOp";
  }
};

class CacheEvictOp : public TestOp {
public:
  librados::AioCompletion *completion;
  librados::ObjectReadOperation op;
  string oid;
  ceph::shared_ptr<int> in_use;

  CacheEvictOp(int n,
	       RadosTestContext *context,
	       const string &oid,
	       TestOpStat *stat)
    : TestOp(n, context, stat),
      completion(NULL),
      oid(oid)
  {}

  void _begin()
  {
    context->state_lock.Lock();

    int snap;
    if (!(rand() % 4) && !context->snaps.empty()) {
      snap = rand_choose(context->snaps)->first;
      in_use = context->snaps_in_use.lookup_or_create(snap, snap);
    } else {
      snap = -1;
    }
    cout << num << ": cache_evict oid " << oid << " snap " << snap << std::endl;

    if (snap >= 0) {
      context->io_ctx.snap_set_read(context->snaps[snap]);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    completion = context->rados.aio_create_completion((void *) cb_arg, NULL,
						      &write_callback);
    // leave object in unused list so that we race with other operations
    //context->oid_in_use.insert(oid);
    //context->oid_not_in_use.erase(oid);
    context->state_lock.Unlock();

    op.cache_evict();
    int r = context->io_ctx.aio_operate(context->prefix+oid, completion,
					&op, librados::OPERATION_IGNORE_CACHE,
					NULL);
    assert(!r);

    if (snap >= 0) {
      context->io_ctx.snap_set_read(0);
    }
  }

  void _finish(CallbackInfo *info)
  {
    context->state_lock.Lock();
    assert(!done);
    assert(completion->is_complete());
    //context->oid_in_use.erase(oid);
    //context->oid_not_in_use.insert(oid);
    int r = completion->get_return_value();
    cout << num << ":  got " << cpp_strerror(r) << std::endl;
    if (r == 0) {
      // yay!
    } else if (r == -EBUSY) {
      // raced with something that dirtied the object
    } else if (r == -EINVAL) {
      // caching not enabled?
    } else if (r == -ENOENT) {
      // may have raced with a remove?
    } else {
      assert(0 == "shouldn't happen");
    }
    context->kick();
    done = true;
    context->state_lock.Unlock();
  }

  bool finished()
  {
    return done;
  }

  string getType()
  {
    return "CacheEvictOp";
  }
};


#endif
