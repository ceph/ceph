// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#include "include/int_types.h"

#include "common/ceph_mutex.h"
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
  TEST_OP_WRITESAME,
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
  TEST_OP_APPEND_EXCL,
  TEST_OP_SET_REDIRECT,
  TEST_OP_UNSET_REDIRECT,
  TEST_OP_CHUNK_READ,
  TEST_OP_TIER_PROMOTE,
  TEST_OP_TIER_FLUSH
};

class TestWatchContext : public librados::WatchCtx2 {
  TestWatchContext(const TestWatchContext&);
public:
  ceph::condition_variable cond;
  uint64_t handle = 0;
  bool waiting = false;
  ceph::mutex lock = ceph::make_mutex("watch lock");
  TestWatchContext() = default;
  void handle_notify(uint64_t notify_id, uint64_t cookie,
		     uint64_t notifier_id,
		     bufferlist &bl) override {
    std::lock_guard l{lock};
    waiting = false;
    cond.notify_all();
  }
  void handle_error(uint64_t cookie, int err) override {
    std::lock_guard l{lock};
    cout << "watch handle_error " << err << std::endl;
  }
  void start() {
    std::lock_guard l{lock};
    waiting = true;
  }
  void wait() {
    std::unique_lock l{lock};
    cond.wait(l, [this] { return !waiting; });
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
      done(false)
  {}

  virtual ~TestOp() {};

  /**
   * This struct holds data to be passed by a callback
   * to a TestOp::finish method.
   */
  struct CallbackInfo {
    uint64_t id;
    explicit CallbackInfo(uint64_t id) : id(id) {}
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
  ceph::mutex state_lock = ceph::make_mutex("Context Lock");
  ceph::condition_variable wait_cond;
  map<int, map<string,ObjectDesc> > pool_obj_cont;
  set<string> oid_in_use;
  set<string> oid_not_in_use;
  set<string> oid_flushing;
  set<string> oid_not_flushing;
  set<string> oid_redirect_not_in_use;
  set<string> oid_redirect_in_use;
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
  const bool no_sparse;
  bool pool_snaps;
  bool write_fadvise_dontneed;
  string low_tier_pool_name;
  librados::IoCtx low_tier_io_ctx;
  int snapname_num;
  map<string,string > redirect_objs;
  bool enable_dedup;

  RadosTestContext(const string &pool_name, 
		   int max_in_flight,
		   uint64_t max_size,
		   uint64_t min_stride_size,
		   uint64_t max_stride_size,
		   bool no_omap,
		   bool no_sparse,
		   bool pool_snaps,
		   bool write_fadvise_dontneed,
		   const string &low_tier_pool_name,
		   bool enable_dedup,
		   const char *id = 0) :
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
    no_sparse(no_sparse),
    pool_snaps(pool_snaps),
    write_fadvise_dontneed(write_fadvise_dontneed),
    low_tier_pool_name(low_tier_pool_name),
    snapname_num(0),
    enable_dedup(enable_dedup)
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
    if (!low_tier_pool_name.empty()) {
      r = rados.ioctx_create(low_tier_pool_name.c_str(), low_tier_io_ctx);
      if (r < 0) {
	rados.shutdown();
	return r;
      }
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
    if (enable_dedup) {
      r = rados.mon_command(
	"{\"prefix\": \"osd pool set\", \"pool\": \"" + pool_name +
	"\", \"var\": \"fingerprint_algorithm\", \"val\": \"" + "sha256" + "\"}",
	inbl, NULL, NULL);
      if (r < 0) {
	rados.shutdown();
	return r;
      }
    }

    char hostname_cstr[100];
    gethostname(hostname_cstr, 100);
    stringstream hostpid;
    hostpid << hostname_cstr << getpid() << "-";
    prefix = hostpid.str();
    ceph_assert(!initialized);
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
    ceph_assert(initialized);
    list<TestOp*> inflight;
    std::unique_lock state_locker{state_lock};

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
      state_lock.unlock();
      if (next) {
	(*inflight.rbegin())->begin();
      }
      state_lock.lock();
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
	  wait_cond.wait(state_locker);
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
  }

  void kick()
  {
    wait_cond.notify_all();
  }

  TestWatchContext *get_watch_context(const string &oid) {
    return watches.count(oid) ? watches[oid] : 0;
  }

  TestWatchContext *watch(const string &oid) {
    ceph_assert(!watches.count(oid));
    return (watches[oid] = new TestWatchContext);
  }

  void unwatch(const string &oid) {
    ceph_assert(watches.count(oid));
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
    ceph_assert(!get_watch_context(oid));
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

  void update_object_redirect_target(const string &oid, const string &target)
  {
    redirect_objs[oid] = target;
  }

  void update_object_chunk_target(const string &oid, uint64_t offset, const ChunkDesc &info)
  {
    for (map<int, map<string,ObjectDesc> >::const_reverse_iterator i = 
	   pool_obj_cont.rbegin();
	 i != pool_obj_cont.rend();
	 ++i) {
      if (i->second.count(oid) != 0) {
	ObjectDesc obj_desc = i->second.find(oid)->second;
	obj_desc.chunk_info[offset] = info;
	update_object_full(oid, obj_desc);
	return ;
      }
    }
    return;
  }

  bool object_existed_at(const string &oid, int snap = -1) const
  {
    ObjectDesc contents;
    bool found = find_object(oid, &contents, snap);
    return found && contents.exists;
  }

  void remove_snap(int snap)
  {
    map<int, map<string,ObjectDesc> >::iterator next_iter = pool_obj_cont.find(snap);
    ceph_assert(next_iter != pool_obj_cont.end());
    map<int, map<string,ObjectDesc> >::iterator current_iter = next_iter++;
    ceph_assert(current_iter != pool_obj_cont.end());
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
    ceph_assert(!get_watch_context(oid));
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
  RemoveAttrsOp(int n, RadosTestContext *context,
	       const string &oid,
	       TestOpStat *stat)
    : TestOp(n, context, stat), oid(oid), comp(NULL)
  {}

  void _begin() override
  {
    ContDesc cont;
    set<string> to_remove;
    {
      std::lock_guard l{context->state_lock};
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
    comp = context->rados.aio_create_completion((void*) cb_arg,
						&write_callback);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op);
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard l{context->state_lock};
    done = true;
    context->update_object_version(oid, comp->get_version64());
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
  {
    return "RemoveAttrsOp";
  }
};

class SetAttrsOp : public TestOp {
public:
  string oid;
  librados::ObjectWriteOperation op;
  librados::AioCompletion *comp;
  SetAttrsOp(int n,
	     RadosTestContext *context,
	     const string &oid,
	     TestOpStat *stat)
    : TestOp(n, context, stat),
      oid(oid), comp(NULL)
  {}

  void _begin() override
  {
    ContDesc cont;
    {
      std::lock_guard l{context->state_lock};
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
      std::lock_guard l{context->state_lock};
      context->update_object_header(oid, header);
      context->update_object_attrs(oid, omap);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, &write_callback);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op);
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard l{context->state_lock};
    int r;
    if ((r = comp->get_return_value())) {
      cerr << "err " << r << std::endl;
      ceph_abort();
    }
    done = true;
    context->update_object_version(oid, comp->get_version64());
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
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
      oid(oid), rcompletion(NULL), waiting_on(0), 
      last_acked_tid(0), do_append(do_append),
      do_excl(do_excl)
  {}
		
  void _begin() override
  {
    std::lock_guard state_locker{context->state_lock};
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
      bool requires;
      int r = context->io_ctx.pool_requires_alignment2(&requires);
      ceph_assert(r == 0);
      uint64_t alignment = 0;
      if (requires) {
        r = context->io_ctx.pool_required_alignment2(&alignment);
        ceph_assert(r == 0);
        ceph_assert(alignment != 0);
      }
      cont_gen = new AppendGenerator(
	prev_length,
	alignment,
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
    ContentsGenerator::iterator gen_pos = cont_gen->get_iterator(cont);
    uint64_t tid = 1;
    for (map<uint64_t, uint64_t>::iterator i = ranges.begin(); 
	 i != ranges.end();
	 ++i, ++tid) {
      gen_pos.seek(i->first);
      bufferlist to_write = gen_pos.gen_bl_advance(i->second);
      ceph_assert(to_write.length() == i->second);
      ceph_assert(to_write.length() > 0);
      std::cout << num << ":  writing " << context->prefix+oid
		<< " from " << i->first
		<< " to " << i->first + i->second << " tid " << tid << std::endl;
      pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
	new pair<TestOp*, TestOp::CallbackInfo*>(this,
						 new TestOp::CallbackInfo(tid));
      librados::AioCompletion *completion =
	context->rados.aio_create_completion((void*) cb_arg, &write_callback);
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
    encode(cont, contbl);
    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(
	this,
	new TestOp::CallbackInfo(++tid));
    librados::AioCompletion *completion = context->rados.aio_create_completion(
      (void*) cb_arg, &write_callback);
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
         (void*) cb_arg, &write_callback);
    waiting_on++;
    read_op.read(0, 1, &rbuffer, 0);
    context->io_ctx.aio_operate(
      context->prefix+oid, rcompletion,
      &read_op,
      librados::OPERATION_ORDER_READS_WRITES,  // order wrt previous write/update
      0);
  }

  void _finish(CallbackInfo *info) override
  {
    ceph_assert(info);
    std::lock_guard state_locker{context->state_lock};
    uint64_t tid = info->id;

    cout << num << ":  finishing write tid " << tid << " to " << context->prefix + oid << std::endl;

    if (tid <= last_acked_tid) {
      cerr << "Error: finished tid " << tid
	   << " when last_acked_tid was " << last_acked_tid << std::endl;
      ceph_abort();
    }
    last_acked_tid = tid;

    ceph_assert(!done);
    waiting_on--;
    if (waiting_on == 0) {
      uint64_t version = 0;
      for (set<librados::AioCompletion *>::iterator i = waiting.begin();
	   i != waiting.end();
	   ) {
	ceph_assert((*i)->is_complete());
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
	ceph_abort_msg("racing read got wrong version");
      }

      {
	ObjectDesc old_value;
	ceph_assert(context->find_object(oid, &old_value, -1));
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
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
  {
    return "WriteOp";
  }
};

class WriteSameOp : public TestOp {
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

  WriteSameOp(int n,
	  RadosTestContext *context,
	  const string &oid,
	  TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      oid(oid), rcompletion(NULL), waiting_on(0),
      last_acked_tid(0)
  {}

  void _begin() override
  {
    std::lock_guard state_locker{context->state_lock};
    done = 0;
    stringstream acc;
    acc << context->prefix << "OID: " << oid << " snap " << context->current_snap << std::endl;
    string prefix = acc.str();

    cont = ContDesc(context->seq_num, context->current_snap, context->seq_num, prefix);

    ContentsGenerator *cont_gen;
    cont_gen = new VarLenGenerator(
	context->max_size, context->min_stride_size, context->max_stride_size);
    context->update_object(cont_gen, oid, cont);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    map<uint64_t, uint64_t> ranges;

    cont_gen->get_ranges_map(cont, ranges);
    std::cout << num << ":  seq_num " << context->seq_num << " ranges " << ranges << std::endl;
    context->seq_num++;

    waiting_on = ranges.size();
    ContentsGenerator::iterator gen_pos = cont_gen->get_iterator(cont);
    uint64_t tid = 1;
    for (map<uint64_t, uint64_t>::iterator i = ranges.begin();
	 i != ranges.end();
	 ++i, ++tid) {
      gen_pos.seek(i->first);
      bufferlist to_write = gen_pos.gen_bl_advance(i->second);
      ceph_assert(to_write.length() == i->second);
      ceph_assert(to_write.length() > 0);
      std::cout << num << ":  writing " << context->prefix+oid
		<< " from " << i->first
		<< " to " << i->first + i->second << " tid " << tid << std::endl;
      pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
	new pair<TestOp*, TestOp::CallbackInfo*>(this,
						 new TestOp::CallbackInfo(tid));
      librados::AioCompletion *completion =
	context->rados.aio_create_completion((void*) cb_arg,
					     &write_callback);
      waiting.insert(completion);
      librados::ObjectWriteOperation op;
      /* no writesame multiplication factor for now */
      op.writesame(i->first, to_write.length(), to_write);

      context->io_ctx.aio_operate(
	context->prefix+oid, completion,
	&op);
    }

    bufferlist contbl;
    encode(cont, contbl);
    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(
	this,
	new TestOp::CallbackInfo(++tid));
    librados::AioCompletion *completion = context->rados.aio_create_completion(
      (void*) cb_arg, &write_callback);
    waiting.insert(completion);
    waiting_on++;
    write_op.setxattr("_header", contbl);
    write_op.truncate(cont_gen->get_length(cont));
    context->io_ctx.aio_operate(
      context->prefix+oid, completion, &write_op);

    cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(
	this,
	new TestOp::CallbackInfo(++tid));
    rcompletion = context->rados.aio_create_completion(
         (void*) cb_arg, &write_callback);
    waiting_on++;
    read_op.read(0, 1, &rbuffer, 0);
    context->io_ctx.aio_operate(
      context->prefix+oid, rcompletion,
      &read_op,
      librados::OPERATION_ORDER_READS_WRITES,  // order wrt previous write/update
      0);
  }

  void _finish(CallbackInfo *info) override
  {
    ceph_assert(info);
    std::lock_guard state_locker{context->state_lock};
    uint64_t tid = info->id;

    cout << num << ":  finishing writesame tid " << tid << " to " << context->prefix + oid << std::endl;

    if (tid <= last_acked_tid) {
      cerr << "Error: finished tid " << tid
	   << " when last_acked_tid was " << last_acked_tid << std::endl;
      ceph_abort();
    }
    last_acked_tid = tid;

    ceph_assert(!done);
    waiting_on--;
    if (waiting_on == 0) {
      uint64_t version = 0;
      for (set<librados::AioCompletion *>::iterator i = waiting.begin();
	   i != waiting.end();
	   ) {
	ceph_assert((*i)->is_complete());
	if (int err = (*i)->get_return_value()) {
	  cerr << "Error: oid " << oid << " writesame returned error code "
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
	ceph_abort_msg("racing read got wrong version");
      }

      {
	ObjectDesc old_value;
	ceph_assert(context->find_object(oid, &old_value, -1));
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
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
  {
    return "WriteSameOp";
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

  void _begin() override
  {
    std::unique_lock state_locker{context->state_lock};
    if (context->get_watch_context(oid)) {
      context->kick();
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
    state_locker.unlock();

    int r = 0;
    if (rand() % 2) {
      librados::ObjectWriteOperation op;
      op.assert_exists();
      op.remove();
      r = context->io_ctx.operate(context->prefix+oid, &op);
    } else {
      r = context->io_ctx.remove(context->prefix+oid);
    }
    if (r && !(r == -ENOENT && !present)) {
      cerr << "r is " << r << " while deleting " << oid << " and present is " << present << std::endl;
      ceph_abort();
    }

    state_locker.lock();
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
  }

  string getType() override
  {
    return "DeleteOp";
  }
};

class ReadOp : public TestOp {
public:
  vector<librados::AioCompletion *> completions;
  librados::ObjectReadOperation op;
  string oid;
  ObjectDesc old_value;
  int snap;
  bool balance_reads;
  bool localize_reads;

  std::shared_ptr<int> in_use;

  vector<bufferlist> results;
  vector<int> retvals;
  vector<std::map<uint64_t, uint64_t>> extent_results;
  vector<bool> is_sparse_read;
  uint64_t waiting_on;

  vector<bufferlist> checksums;
  vector<int> checksum_retvals;

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
	 bool balance_reads,
	 bool localize_reads,
	 TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      completions(3),
      oid(oid),
      snap(0),
      balance_reads(balance_reads),
      localize_reads(localize_reads),
      results(3),
      retvals(3),
      extent_results(3),
      is_sparse_read(3, false),
      waiting_on(0),
      checksums(3),
      checksum_retvals(3),
      attrretval(0)
  {}

  void _do_read(librados::ObjectReadOperation& read_op, int index) {
    uint64_t len = 0;
    if (old_value.has_contents())
      len = old_value.most_recent_gen()->get_length(old_value.most_recent());
    if (context->no_sparse || rand() % 2) {
      is_sparse_read[index] = false;
      read_op.read(0,
		   len,
		   &results[index],
		   &retvals[index]);
      bufferlist init_value_bl;
      encode(static_cast<uint32_t>(-1), init_value_bl);
      read_op.checksum(LIBRADOS_CHECKSUM_TYPE_CRC32C, init_value_bl, 0, len,
		       0, &checksums[index], &checksum_retvals[index]);
    } else {
      is_sparse_read[index] = true;
      read_op.sparse_read(0,
			  len,
			  &extent_results[index],
			  &results[index],
			  &retvals[index]);
    }
  }

  void _begin() override
  {
    std::unique_lock state_locker{context->state_lock};
    if (!(rand() % 4) && !context->snaps.empty()) {
      snap = rand_choose(context->snaps)->first;
      in_use = context->snaps_in_use.lookup_or_create(snap, snap);
    } else {
      snap = -1;
    }
    std::cout << num << ": read oid " << oid << " snap " << snap << std::endl;
    done = 0;
    for (uint32_t i = 0; i < 3; i++) {
      completions[i] = context->rados.aio_create_completion((void *) this, &read_callback);
    }

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    ceph_assert(context->find_object(oid, &old_value, snap));
    if (old_value.deleted())
      std::cout << num << ":  expect deleted" << std::endl;
    else
      std::cout << num << ":  expect " << old_value.most_recent() << std::endl;

    TestWatchContext *ctx = context->get_watch_context(oid);
    state_locker.unlock();
    if (ctx) {
      ceph_assert(old_value.exists);
      TestAlarm alarm;
      std::cerr << num << ":  about to start" << std::endl;
      ctx->start();
      std::cerr << num << ":  started" << std::endl;
      bufferlist bl;
      context->io_ctx.set_notify_timeout(600);
      int r = context->io_ctx.notify2(context->prefix+oid, bl, 0, NULL);
      if (r < 0) {
	std::cerr << "r is " << r << std::endl;
	ceph_abort();
      }
      std::cerr << num << ":  notified, waiting" << std::endl;
      ctx->wait();
    }
    state_locker.lock();
    if (snap >= 0) {
      context->io_ctx.snap_set_read(context->snaps[snap]);
    }
    _do_read(op, 0);
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
      // NOTE: we're ignore pmore here, which assumes the OSD limit is high
      // enough for us.
      op.omap_get_keys2("", -1, &omap_keys, nullptr, nullptr);
      op.omap_get_vals2("", -1, &omap, nullptr, nullptr);
      op.omap_get_header(&header, 0);
    }
    op.getxattrs(&xattrs, 0);

    unsigned flags = 0;
    if (balance_reads)
      flags |= librados::OPERATION_BALANCE_READS;
    if (localize_reads)
      flags |= librados::OPERATION_LOCALIZE_READS;

    ceph_assert(!context->io_ctx.aio_operate(context->prefix+oid, completions[0], &op,
					flags, NULL));
    waiting_on++;
 
    // send 2 pipelined reads on the same object/snap. This can help testing
    // OSD's read behavior in some scenarios
    for (uint32_t i = 1; i < 3; ++i) {
      librados::ObjectReadOperation pipeline_op;
      _do_read(pipeline_op, i);
      ceph_assert(!context->io_ctx.aio_operate(context->prefix+oid, completions[i], &pipeline_op, 0));
      waiting_on++;
    }

    if (snap >= 0) {
      context->io_ctx.snap_set_read(0);
    }
  }

  void _finish(CallbackInfo *info) override
  {
    std::unique_lock state_locker{context->state_lock};
    ceph_assert(!done);
    ceph_assert(waiting_on > 0);
    if (--waiting_on) {
      return;
    }

    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    int retval = completions[0]->get_return_value();
    for (vector<librados::AioCompletion *>::iterator it = completions.begin();
         it != completions.end(); ++it) {
      ceph_assert((*it)->is_complete());
      uint64_t version = (*it)->get_version64();
      int err = (*it)->get_return_value();
      if (err != retval) {
        cerr << num << ": Error: oid " << oid << " read returned different error codes: "
             << retval << " and " << err << std::endl;
	ceph_abort();
      }
      if (err) {
        if (!(err == -ENOENT && old_value.deleted())) {
          cerr << num << ": Error: oid " << oid << " read returned error code "
               << err << std::endl;
          ceph_abort();
        }
      } else if (version != old_value.version) {
	cerr << num << ": oid " << oid << " version is " << version
	     << " and expected " << old_value.version << std::endl;
	ceph_assert(version == old_value.version);
      }
    }
    if (!retval) {
      map<string, bufferlist>::iterator iter = xattrs.find("_header");
      bufferlist headerbl;
      if (iter == xattrs.end()) {
	if (old_value.has_contents()) {
	  cerr << num << ": Error: did not find header attr, has_contents: "
	       << old_value.has_contents()
	       << std::endl;
	  ceph_assert(!old_value.has_contents());
	}
      } else {
	headerbl = iter->second;
	xattrs.erase(iter);
      }
      if (old_value.deleted()) {
	std::cout << num << ":  expect deleted" << std::endl;
	ceph_abort_msg("expected deleted");
      } else {
	std::cout << num << ":  expect " << old_value.most_recent() << std::endl;
      }
      if (old_value.has_contents()) {
	ContDesc to_check;
	auto p = headerbl.cbegin();
	decode(to_check, p);
	if (to_check != old_value.most_recent()) {
	  cerr << num << ": oid " << oid << " found incorrect object contents " << to_check
	       << ", expected " << old_value.most_recent() << std::endl;
	  context->errors++;
	}
        for (unsigned i = 0; i < results.size(); i++) {
	  if (is_sparse_read[i]) {
	    if (!old_value.check_sparse(extent_results[i], results[i])) {
	      cerr << num << ": oid " << oid << " contents " << to_check << " corrupt" << std::endl;
	      context->errors++;
	    }
	  } else {
	    if (!old_value.check(results[i])) {
	      cerr << num << ": oid " << oid << " contents " << to_check << " corrupt" << std::endl;
	      context->errors++;
	    }

	    uint32_t checksum = 0;
	    if (checksum_retvals[i] == 0) {
	      try {
	        auto bl_it = checksums[i].cbegin();
	        uint32_t csum_count;
	        decode(csum_count, bl_it);
	        decode(checksum, bl_it);
	      } catch (const buffer::error &err) {
	        checksum_retvals[i] = -EBADMSG;
	      }
	    }
	    if (checksum_retvals[i] != 0 || checksum != results[i].crc32c(-1)) {
	      cerr << num << ": oid " << oid << " checksum " << checksums[i]
	           << " incorrect, expecting " << results[i].crc32c(-1)
                   << std::endl;
	      context->errors++;
	    }
	  }
	}
	if (context->errors) ceph_abort();
      }

      // Attributes
      if (!context->no_omap) {
	if (!(old_value.header == header)) {
	  cerr << num << ": oid " << oid << " header does not match, old size: "
	       << old_value.header.length() << " new size " << header.length()
	       << std::endl;
	  ceph_assert(old_value.header == header);
	}
	if (omap.size() != old_value.attrs.size()) {
	  cerr << num << ": oid " << oid << " omap.size() is " << omap.size()
	       << " and old is " << old_value.attrs.size() << std::endl;
	  ceph_assert(omap.size() == old_value.attrs.size());
	}
	if (omap_keys.size() != old_value.attrs.size()) {
	  cerr << num << ": oid " << oid << " omap.size() is " << omap_keys.size()
	       << " and old is " << old_value.attrs.size() << std::endl;
	  ceph_assert(omap_keys.size() == old_value.attrs.size());
	}
      }
      if (xattrs.size() != old_value.attrs.size()) {
	cerr << num << ": oid " << oid << " xattrs.size() is " << xattrs.size()
	     << " and old is " << old_value.attrs.size() << std::endl;
	ceph_assert(xattrs.size() == old_value.attrs.size());
      }
      for (map<string, ContDesc>::iterator iter = old_value.attrs.begin();
	   iter != old_value.attrs.end();
	   ++iter) {
	bufferlist bl = context->attr_gen.gen_bl(
	  iter->second);
	if (!context->no_omap) {
	  map<string, bufferlist>::iterator omap_iter = omap.find(iter->first);
	  ceph_assert(omap_iter != omap.end());
	  ceph_assert(bl.length() == omap_iter->second.length());
	  bufferlist::iterator k = bl.begin();
	  for(bufferlist::iterator l = omap_iter->second.begin();
	      !k.end() && !l.end();
	      ++k, ++l) {
	    ceph_assert(*l == *k);
	  }
	}
	map<string, bufferlist>::iterator xattr_iter = xattrs.find(iter->first);
	ceph_assert(xattr_iter != xattrs.end());
	ceph_assert(bl.length() == xattr_iter->second.length());
	bufferlist::iterator k = bl.begin();
	for (bufferlist::iterator j = xattr_iter->second.begin();
	     !k.end() && !j.end();
	     ++j, ++k) {
	  ceph_assert(*j == *k);
	}
      }
      if (!context->no_omap) {
	for (set<string>::iterator i = omap_requested_keys.begin();
	     i != omap_requested_keys.end();
	     ++i) {
	  if (!omap_returned_values.count(*i))
	    ceph_assert(!old_value.attrs.count(*i));
	  if (!old_value.attrs.count(*i))
	    ceph_assert(!omap_returned_values.count(*i));
	}
	for (map<string, bufferlist>::iterator i = omap_returned_values.begin();
	     i != omap_returned_values.end();
	     ++i) {
	  ceph_assert(omap_requested_keys.count(i->first));
	  ceph_assert(omap.count(i->first));
	  ceph_assert(old_value.attrs.count(i->first));
	  ceph_assert(i->second == omap[i->first]);
	}
      }
    }
    for (vector<librados::AioCompletion *>::iterator it = completions.begin();
         it != completions.end(); ++it) {
      (*it)->release();
    }
    context->kick();
    done = true;
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
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

  void _begin() override
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
	ceph_abort();
      }
      ceph_assert(!context->io_ctx.snap_lookup(snapname.c_str(), &snap));

    } else {
      ceph_assert(!context->io_ctx.selfmanaged_snap_create(&snap));
    }

    std::unique_lock state_locker{context->state_lock};
    context->add_snap(snap);

    if (!context->pool_snaps) {
      vector<uint64_t> snapset(context->snaps.size());

      int j = 0;
      for (map<int,uint64_t>::reverse_iterator i = context->snaps.rbegin();
	   i != context->snaps.rend();
	   ++i, ++j) {
	snapset[j] = i->second;
      }

      state_locker.unlock();

      int r = context->io_ctx.selfmanaged_snap_set_write_ctx(context->seq, snapset);
      if (r) {
	cerr << "r is " << r << " snapset is " << snapset << " seq is " << context->seq << std::endl;
	ceph_abort();
      }
    }
  }

  string getType() override
  {
    return "SnapCreateOp";
  }
  bool must_quiesce_other_ops() override { return context->pool_snaps; }
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

  void _begin() override
  {
    std::unique_lock state_locker{context->state_lock};
    uint64_t snap = context->snaps[to_remove];
    context->remove_snap(to_remove);

    if (context->pool_snaps) {
      string snapname;

      ceph_assert(!context->io_ctx.snap_get_name(snap, &snapname));
      ceph_assert(!context->io_ctx.snap_remove(snapname.c_str()));
     } else {
      ceph_assert(!context->io_ctx.selfmanaged_snap_remove(snap));

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
	ceph_abort();
      }
    }
  }

  string getType() override
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

  void _begin() override
  {
    std::unique_lock state_locker{context->state_lock};
    ObjectDesc contents;
    context->find_object(oid, &contents);
    if (contents.deleted()) {
      context->kick();
      return;
    }
    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    TestWatchContext *ctx = context->get_watch_context(oid);
    state_locker.unlock();
    int r;
    if (!ctx) {
      {
	std::lock_guard l{context->state_lock};
	ctx = context->watch(oid);
      }

      r = context->io_ctx.watch2(context->prefix+oid,
				 &ctx->get_handle(),
				 ctx);
    } else {
      r = context->io_ctx.unwatch2(ctx->get_handle());
      {
	std::lock_guard l{context->state_lock};
	context->unwatch(oid);
      }
    }

    if (r) {
      cerr << "r is " << r << std::endl;
      ceph_abort();
    }

    {
      std::lock_guard l{context->state_lock};
      context->oid_in_use.erase(oid);
      context->oid_not_in_use.insert(oid);
    }
  }

  string getType() override
  {
    return "WatchOp";
  }
};

class RollbackOp : public TestOp {
public:
  string oid;
  int roll_back_to;
  librados::ObjectWriteOperation zero_write_op1;
  librados::ObjectWriteOperation zero_write_op2;
  librados::ObjectWriteOperation op;
  vector<librados::AioCompletion *> comps;
  std::shared_ptr<int> in_use;
  int last_finished;
  int outstanding;

  RollbackOp(int n,
	     RadosTestContext *context,
	     const string &_oid,
	     TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      oid(_oid), roll_back_to(-1), 
      comps(3, NULL),
      last_finished(-1), outstanding(3)
  {}

  void _begin() override
  {
    context->state_lock.lock();
    if (context->get_watch_context(oid)) {
      context->kick();
      context->state_lock.unlock();
      return;
    }

    if (context->snaps.empty()) {
      context->kick();
      context->state_lock.unlock();
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

    bool existed_before = context->object_existed_at(oid);
    bool existed_after = context->object_existed_at(oid, roll_back_to);

    context->roll_back(oid, roll_back_to);
    uint64_t snap = context->snaps[roll_back_to];

    outstanding -= (!existed_before) + (!existed_after);

    context->state_lock.unlock();

    bufferlist bl, bl2;
    zero_write_op1.append(bl);
    zero_write_op2.append(bl2);

    if (context->pool_snaps) {
      op.snap_rollback(snap);
    } else {
      op.selfmanaged_snap_rollback(snap);
    }

    if (existed_before) {
      pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
	new pair<TestOp*, TestOp::CallbackInfo*>(this,
						 new TestOp::CallbackInfo(0));
      comps[0] = 
	context->rados.aio_create_completion((void*) cb_arg,
					     &write_callback);
      context->io_ctx.aio_operate(
	context->prefix+oid, comps[0], &zero_write_op1);
    }
    {
      pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
	new pair<TestOp*, TestOp::CallbackInfo*>(this,
						 new TestOp::CallbackInfo(1));
      comps[1] =
	context->rados.aio_create_completion((void*) cb_arg,
					     &write_callback);
      context->io_ctx.aio_operate(
	context->prefix+oid, comps[1], &op);
    }
    if (existed_after) {
      pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
	new pair<TestOp*, TestOp::CallbackInfo*>(this,
						 new TestOp::CallbackInfo(2));
      comps[2] =
	context->rados.aio_create_completion((void*) cb_arg,
					     &write_callback);
      context->io_ctx.aio_operate(
	context->prefix+oid, comps[2], &zero_write_op2);
    }
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard l{context->state_lock};
    uint64_t tid = info->id;
    cout << num << ":  finishing rollback tid " << tid
	 << " to " << context->prefix + oid << std::endl;
    ceph_assert((int)(info->id) > last_finished);
    last_finished = info->id;

    int r;
    if ((r = comps[last_finished]->get_return_value()) != 0) {
      cerr << "err " << r << std::endl;
      ceph_abort();
    }
    if (--outstanding == 0) {
      done = true;
      context->update_object_version(oid, comps[tid]->get_version64());
      context->oid_in_use.erase(oid);
      context->oid_not_in_use.insert(oid);
      in_use = std::shared_ptr<int>();
      context->kick();
    }
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
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
  librados::AioCompletion *comp_racing_read = nullptr;
  std::shared_ptr<int> in_use;
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

  void _begin() override
  {
    ContDesc cont;
    {
      std::lock_guard l{context->state_lock};
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
    op.copy_from(src.c_str(), context->io_ctx, src_value.version, 0);

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg,
						&write_callback);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op);

    // queue up a racing read, too.
    pair<TestOp*, TestOp::CallbackInfo*> *read_cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(1));
    comp_racing_read = context->rados.aio_create_completion((void*) read_cb_arg, &write_callback);
    rd_op.stat(NULL, NULL, NULL);
    context->io_ctx.aio_operate(context->prefix+oid, comp_racing_read, &rd_op,
				librados::OPERATION_ORDER_READS_WRITES,  // order wrt previous write/update
				NULL);

  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard l{context->state_lock};

    // note that the read can (and atm will) come back before the
    // write reply, but will reflect the update and the versions will
    // match.

    if (info->id == 0) {
      // copy_from
      ceph_assert(comp->is_complete());
      cout << num << ":  finishing copy_from to " << context->prefix + oid << std::endl;
      if ((r = comp->get_return_value())) {
	if (r == -ENOENT && src_value.deleted()) {
	  cout << num << ":  got expected ENOENT (src dne)" << std::endl;
	} else {
	  cerr << "Error: oid " << oid << " copy_from " << oid_src << " returned error code "
	       << r << std::endl;
	  ceph_abort();
	}
      } else {
	ceph_assert(!version || comp->get_version64() == version);
	version = comp->get_version64();
	context->update_object_version(oid, comp->get_version64());
      }
    } else if (info->id == 1) {
      // racing read
      ceph_assert(comp_racing_read->is_complete());
      cout << num << ":  finishing copy_from racing read to " << context->prefix + oid << std::endl;
      if ((r = comp_racing_read->get_return_value())) {
	if (!(r == -ENOENT && src_value.deleted())) {
	  cerr << "Error: oid " << oid << " copy_from " << oid_src << " returned error code "
	       << r << std::endl;
	}
      } else {
	ceph_assert(comp_racing_read->get_return_value() == 0);
	ceph_assert(!version || comp_racing_read->get_version64() == version);
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

  bool finished() override
  {
    return done == 2;
  }

  string getType() override
  {
    return "CopyFromOp";
  }
};

class ChunkReadOp : public TestOp {
public:
  vector<librados::AioCompletion *> completions;
  librados::ObjectReadOperation op;
  string oid;
  ObjectDesc old_value;
  ObjectDesc tgt_value;
  int snap;
  bool balance_reads;
  bool localize_reads;

  std::shared_ptr<int> in_use;

  vector<bufferlist> results;
  vector<int> retvals;
  vector<bool> is_sparse_read;
  uint64_t waiting_on;

  vector<bufferlist> checksums;
  vector<int> checksum_retvals;
  uint32_t offset = 0;
  uint32_t length = 0;
  string tgt_oid;
  string tgt_pool_name;
  uint32_t tgt_offset = 0;

  ChunkReadOp(int n,
	 RadosTestContext *context,
	 const string &oid,
	 const string &tgt_pool_name,
	 bool balance_reads,
	 bool localize_reads,
	 TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      completions(2),
      oid(oid),
      snap(0),
      balance_reads(balance_reads),
      localize_reads(localize_reads),
      results(2),
      retvals(2),
      waiting_on(0),
      checksums(2),
      checksum_retvals(2),
      tgt_pool_name(tgt_pool_name)
  {}

  void _do_read(librados::ObjectReadOperation& read_op, uint32_t offset, uint32_t length, int index) {
    read_op.read(offset,
		 length,
		 &results[index],
		 &retvals[index]);
    if (index != 0) {
      bufferlist init_value_bl;
      encode(static_cast<uint32_t>(-1), init_value_bl);
      read_op.checksum(LIBRADOS_CHECKSUM_TYPE_CRC32C, init_value_bl, offset, length,
		       0, &checksums[index], &checksum_retvals[index]);
    }

  }

  void _begin() override
  {
    context->state_lock.lock();
    std::cout << num << ": chunk read oid " << oid << " snap " << snap << std::endl;
    done = 0;
    for (uint32_t i = 0; i < 2; i++) {
      completions[i] = context->rados.aio_create_completion((void *) this, &read_callback);
    }

    context->find_object(oid, &old_value);

    if (old_value.chunk_info.size() == 0) {
      std::cout << ":  no chunks" << std::endl;
      context->kick();
      context->state_lock.unlock();
      done = true;
      return;
    }

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    if (old_value.deleted()) {
      std::cout << num << ":  expect deleted" << std::endl;
    } else {
      std::cout << num << ":  expect " << old_value.most_recent() << std::endl;
    }

    int rand_index = rand() % old_value.chunk_info.size();
    auto iter = old_value.chunk_info.begin();
    for (int i = 0; i < rand_index; i++) {
      iter++;
    }
    offset = iter->first;
    offset += (rand() % iter->second.length)/2;
    uint32_t t_length = rand() % iter->second.length;
    while (t_length + offset > iter->first + iter->second.length) {
      t_length = rand() % iter->second.length;
    }
    length = t_length;
    tgt_offset = iter->second.offset + offset - iter->first;
    tgt_oid = iter->second.oid;

    std::cout << num << ": ori offset " << iter->first << " req offset " << offset 
	      << " ori length " << iter->second.length << " req length " << length
	      << " ori tgt_offset " << iter->second.offset << " req tgt_offset " << tgt_offset 
	      << " tgt_oid " << tgt_oid << std::endl;

    TestWatchContext *ctx = context->get_watch_context(oid);
    context->state_lock.unlock();
    if (ctx) {
      ceph_assert(old_value.exists);
      TestAlarm alarm;
      std::cerr << num << ":  about to start" << std::endl;
      ctx->start();
      std::cerr << num << ":  started" << std::endl;
      bufferlist bl;
      context->io_ctx.set_notify_timeout(600);
      int r = context->io_ctx.notify2(context->prefix+oid, bl, 0, NULL);
      if (r < 0) {
	std::cerr << "r is " << r << std::endl;
	ceph_abort();
      }
      std::cerr << num << ":  notified, waiting" << std::endl;
      ctx->wait();
    }
    std::lock_guard state_locker{context->state_lock};

    _do_read(op, offset, length, 0);

    unsigned flags = 0;
    if (balance_reads)
      flags |= librados::OPERATION_BALANCE_READS;
    if (localize_reads)
      flags |= librados::OPERATION_LOCALIZE_READS;

    ceph_assert(!context->io_ctx.aio_operate(context->prefix+oid, completions[0], &op,
					flags, NULL));
    waiting_on++;

    _do_read(op, tgt_offset, length, 1);
    ceph_assert(!context->io_ctx.aio_operate(context->prefix+tgt_oid, completions[1], &op,
					flags, NULL));

    waiting_on++;
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard l{context->state_lock};
    ceph_assert(!done);
    ceph_assert(waiting_on > 0);
    if (--waiting_on) {
      return;
    }

    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    int retval = completions[0]->get_return_value();
    std::cout << ":  finish!! ret: " << retval << std::endl;
    context->find_object(tgt_oid, &tgt_value);

    for (int i = 0; i < 2; i++) {
      ceph_assert(completions[i]->is_complete()); 
      int err = completions[i]->get_return_value();
      if (err != retval) {
        cerr << num << ": Error: oid " << oid << " read returned different error codes: "
             << retval << " and " << err << std::endl;
	ceph_abort();
      }
      if (err) {
        if (!(err == -ENOENT && old_value.deleted())) {
          cerr << num << ": Error: oid " << oid << " read returned error code "
               << err << std::endl;
          ceph_abort();
        }
      }
    }

    if (!retval) {
      if (old_value.deleted()) {
	std::cout << num << ":  expect deleted" << std::endl;
	ceph_abort_msg("expected deleted");
      } else {
	std::cout << num << ":  expect " << old_value.most_recent() << std::endl;
      }
      if (tgt_value.has_contents()) {
	uint32_t checksum[2] = {0};
	if (checksum_retvals[1] == 0) {
	  try {
	    auto bl_it = checksums[1].cbegin();
	    uint32_t csum_count;
	    decode(csum_count, bl_it);
	    decode(checksum[1], bl_it);
	  } catch (const buffer::error &err) {
	    checksum_retvals[1] = -EBADMSG;
	  }
	}
    
	if (checksum_retvals[1] != 0) {
	  cerr << num << ": oid " << oid << " checksum retvals " << checksums[0]
	       << " error " << std::endl;
	  context->errors++;
	}

	checksum[0] = results[0].crc32c(-1);
      
	if (checksum[0] != checksum[1]) {
	  cerr << num << ": oid " << oid << " checksum src " << checksum[0]
	       << " chunksum tgt " << checksum[1] << " incorrect, expecting " 
	       << results[0].crc32c(-1)
	       << std::endl;
	  context->errors++;
	}
	if (context->errors) ceph_abort();
      }
    }
    for (vector<librados::AioCompletion *>::iterator it = completions.begin();
         it != completions.end(); ++it) {
      (*it)->release();
    }
    context->kick();
    done = true;
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
  {
    return "ChunkReadOp";
  }
};

class CopyOp : public TestOp {
public:
  string oid, oid_src, tgt_pool_name;
  librados::ObjectWriteOperation op;
  librados::ObjectReadOperation rd_op;
  librados::AioCompletion *comp;
  ObjectDesc src_value, tgt_value;
  int done;
  int r;
  CopyOp(int n,
	   RadosTestContext *context,
	   const string &oid_src,
	   const string &oid,
	   const string &tgt_pool_name,
	   TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      oid(oid), oid_src(oid_src), tgt_pool_name(tgt_pool_name),
      comp(NULL), done(0), r(0) 
  {}

  void _begin() override
  {
    std::lock_guard l{context->state_lock};
    context->oid_in_use.insert(oid_src);
    context->oid_not_in_use.erase(oid_src);

    string src = context->prefix+oid_src;
    context->find_object(oid_src, &src_value); 
    op.copy_from(src.c_str(), context->io_ctx, src_value.version, 0);

    cout << "copy op oid " << oid_src << " to " << oid << " tgt_pool_name " << tgt_pool_name <<  std::endl;

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, &write_callback);
    if (tgt_pool_name == context->low_tier_pool_name) {
      context->low_tier_io_ctx.aio_operate(context->prefix+oid, comp, &op);
    } else {
      context->io_ctx.aio_operate(context->prefix+oid, comp, &op);
    }
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard l{context->state_lock};

    if (info->id == 0) {
      ceph_assert(comp->is_complete());
      cout << num << ":  finishing copy op to oid " << oid << std::endl;
      if ((r = comp->get_return_value())) {
	cerr << "Error: oid " << oid << " write returned error code "
	     << r << std::endl;
	ceph_abort();
      }
    }

    if (++done == 1) {
      context->oid_in_use.erase(oid_src);
      context->oid_not_in_use.insert(oid_src);
      context->kick();
    }
  }

  bool finished() override
  {
    return done == 1;
  }

  string getType() override
  {
    return "CopyOp";
  }
};

class SetChunkOp : public TestOp {
public:
  string oid, oid_tgt, tgt_pool_name;
  ObjectDesc src_value, tgt_value;
  librados::ObjectReadOperation op;
  librados::ObjectReadOperation rd_op;
  librados::AioCompletion *comp;
  std::shared_ptr<int> in_use;
  int done;
  int r;
  uint64_t offset;
  uint32_t length;
  uint64_t tgt_offset;
  bool enable_with_reference;
  SetChunkOp(int n,
	     RadosTestContext *context,
	     const string &oid,
	     uint64_t offset,
	     uint32_t length,
	     const string &oid_tgt,
	     const string &tgt_pool_name,
	     uint64_t tgt_offset,
	     TestOpStat *stat = 0,
	     bool enable_with_reference = false)
    : TestOp(n, context, stat),
      oid(oid), oid_tgt(oid_tgt), tgt_pool_name(tgt_pool_name),
      comp(NULL), done(0), 
      r(0), offset(offset), length(length), 
      tgt_offset(tgt_offset), enable_with_reference(enable_with_reference)
  {}

  void _begin() override
  {
    std::lock_guard l{context->state_lock};
    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    if (tgt_pool_name.empty()) ceph_abort();

    context->find_object(oid, &src_value); 
    context->find_object(oid_tgt, &tgt_value);

    if (src_value.version != 0 && !src_value.deleted())
      op.assert_version(src_value.version);
    if (enable_with_reference) {
      op.set_chunk(offset, length, context->low_tier_io_ctx, 
		   context->prefix+oid_tgt, tgt_offset, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    } else {
      op.set_chunk(offset, length, context->low_tier_io_ctx, 
		   context->prefix+oid_tgt, tgt_offset);
    }

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg,
						&write_callback);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op,
				librados::OPERATION_ORDER_READS_WRITES, NULL);
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard l{context->state_lock};

    if (info->id == 0) {
      ceph_assert(comp->is_complete());
      cout << num << ":  finishing set_chunk to oid " << oid << std::endl;
      if ((r = comp->get_return_value())) {
	if (r == -ENOENT && src_value.deleted()) {
	  cout << num << ":  got expected ENOENT (src dne)" << std::endl;
	} else if (r == -EOPNOTSUPP) {
	  bool is_overlapped = false;
	  for (auto &p : src_value.chunk_info) {
	    if ((p.first <= offset && p.first + p.second.length > offset) ||
                (p.first > offset && p.first <= offset + length)) {
	      cout << " range is overlapped  offset: " << offset << " length: " << length
		    << " chunk_info offset: " << p.second.offset << " length " 
		    << p.second.length << std::endl;
	      is_overlapped = true;
	      context->update_object_version(oid, comp->get_version64());
	    } 
	  }
	  if (!is_overlapped) {
	    cerr << "Error: oid " << oid << " set_chunk " << oid_tgt << " returned error code "
		  << r << " offset: " << offset << " length: " << length <<  std::endl;
	    ceph_abort();
	  }
	} else {
	  cerr << "Error: oid " << oid << " set_chunk " << oid_tgt << " returned error code "
	       << r << std::endl;
	  ceph_abort();
	}
      } else {
	ChunkDesc info;
	info.offset = tgt_offset;
	info.length = length;
	info.oid = oid_tgt;
	context->update_object_chunk_target(oid, offset, info);
	context->update_object_version(oid, comp->get_version64());
      }
    }

    if (++done == 1) {
      context->oid_in_use.erase(oid);
      context->oid_not_in_use.insert(oid);
      context->kick();
    }
  }

  bool finished() override
  {
    return done == 1;
  }

  string getType() override
  {
    return "SetChunkOp";
  }
};

class SetRedirectOp : public TestOp {
public:
  string oid, oid_tgt, tgt_pool_name;
  ObjectDesc src_value, tgt_value;
  librados::ObjectWriteOperation op;
  librados::ObjectReadOperation rd_op;
  librados::AioCompletion *comp;
  std::shared_ptr<int> in_use;
  int done;
  int r;
  SetRedirectOp(int n,
	     RadosTestContext *context,
	     const string &oid,
	     const string &oid_tgt,
	     const string &tgt_pool_name,
	     TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      oid(oid), oid_tgt(oid_tgt), tgt_pool_name(tgt_pool_name),
      comp(NULL), done(0), 
      r(0)
  {}

  void _begin() override
  {
    std::lock_guard l{context->state_lock};
    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    context->oid_redirect_in_use.insert(oid_tgt);
    context->oid_redirect_not_in_use.erase(oid_tgt);

    if (tgt_pool_name.empty()) ceph_abort();

    context->find_object(oid, &src_value); 
    if(!context->redirect_objs[oid].empty()) {
      /* copy_from oid --> oid_tgt */
      comp = context->rados.aio_create_completion();
      string src = context->prefix+oid;
      op.copy_from(src.c_str(), context->io_ctx, src_value.version, 0);
      context->low_tier_io_ctx.aio_operate(context->prefix+oid_tgt, comp, &op,
					   librados::OPERATION_ORDER_READS_WRITES);
      comp->wait_for_complete();
      if ((r = comp->get_return_value())) {
	cerr << "Error: oid " << oid << " copy_from " << oid_tgt << " returned error code "
	     << r << std::endl;
	ceph_abort();
      }
      comp->release();

      /* unset redirect target */
      comp = context->rados.aio_create_completion();
      bool present = !src_value.deleted();
      context->remove_object(oid);
      op.remove();
      context->io_ctx.aio_operate(context->prefix+oid, comp, &op,
				  librados::OPERATION_ORDER_READS_WRITES |
				  librados::OPERATION_IGNORE_REDIRECT);
      comp->wait_for_complete();
      if ((r = comp->get_return_value())) {
	if (!(r == -ENOENT && !present)) {
	  cerr << "r is " << r << " while deleting " << oid << " and present is " << present << std::endl;
	  ceph_abort();
	}
      }
      comp->release();

      context->oid_redirect_not_in_use.insert(context->redirect_objs[oid]);
      context->oid_redirect_in_use.erase(context->redirect_objs[oid]);
    }

    comp = context->rados.aio_create_completion();
    rd_op.stat(NULL, NULL, NULL);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &rd_op,
     			  librados::OPERATION_ORDER_READS_WRITES |
     			  librados::OPERATION_IGNORE_REDIRECT,
     			  NULL);
    comp->wait_for_complete();
    if ((r = comp->get_return_value()) && !src_value.deleted()) {
      cerr << "Error: oid " << oid << " stat returned error code "
	   << r << std::endl;
      ceph_abort();
    }
    context->update_object_version(oid, comp->get_version64());
    comp->release();

    comp = context->rados.aio_create_completion();
    rd_op.stat(NULL, NULL, NULL);
    context->low_tier_io_ctx.aio_operate(context->prefix+oid_tgt, comp, &rd_op,
     			  librados::OPERATION_ORDER_READS_WRITES |
     			  librados::OPERATION_IGNORE_REDIRECT,
     			  NULL);
    comp->wait_for_complete();
    if ((r = comp->get_return_value())) {
      cerr << "Error: oid " << oid_tgt << " stat returned error code "
	   << r << std::endl;
      ceph_abort();
    }
    uint64_t tgt_version = comp->get_version64();
    comp->release();
    
    
    context->find_object(oid, &src_value); 

    if (src_value.version != 0 && !src_value.deleted())
      op.assert_version(src_value.version);
    op.set_redirect(context->prefix+oid_tgt, context->low_tier_io_ctx, tgt_version);

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp = context->rados.aio_create_completion((void*) cb_arg, &write_callback);
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op,
				librados::OPERATION_ORDER_READS_WRITES);
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard l{context->state_lock};

    if (info->id == 0) {
      ceph_assert(comp->is_complete());
      cout << num << ":  finishing set_redirect to oid " << oid << std::endl;
      if ((r = comp->get_return_value())) {
	if (r == -ENOENT && src_value.deleted()) {
	  cout << num << ":  got expected ENOENT (src dne)" << std::endl;
	} else {
	  cerr << "Error: oid " << oid << " set_redirect " << oid_tgt << " returned error code "
	       << r << std::endl;
	  ceph_abort();
	}
      } else {
	context->update_object_redirect_target(oid, oid_tgt);
	context->update_object_version(oid, comp->get_version64());
      }
    }

    if (++done == 1) {
      context->oid_in_use.erase(oid);
      context->oid_not_in_use.insert(oid);
      context->kick();
    }
  }

  bool finished() override
  {
    return done == 1;
  }

  string getType() override
  {
    return "SetRedirectOp";
  }
};

class UnsetRedirectOp : public TestOp {
public:
  string oid;
  librados::ObjectWriteOperation op;
  librados::AioCompletion *comp = nullptr;

  UnsetRedirectOp(int n,
	   RadosTestContext *context,
	   const string &oid,
	   TestOpStat *stat = 0)
    : TestOp(n, context, stat), oid(oid)
  {}

  void _begin() override
  {
    std::unique_lock state_locker{context->state_lock};
    if (context->get_watch_context(oid)) {
      context->kick();
      return;
    }

    ObjectDesc contents;
    context->find_object(oid, &contents);
    bool present = !contents.deleted();

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    context->seq_num++;

    context->remove_object(oid);

    state_locker.unlock();

    comp = context->rados.aio_create_completion();
    op.remove();
    context->io_ctx.aio_operate(context->prefix+oid, comp, &op,
				librados::OPERATION_ORDER_READS_WRITES |
				librados::OPERATION_IGNORE_REDIRECT);
    comp->wait_for_complete();
    int r = comp->get_return_value();
    if (r && !(r == -ENOENT && !present)) {
      cerr << "r is " << r << " while deleting " << oid << " and present is " << present << std::endl;
      ceph_abort();
    }
    state_locker.lock();
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    if(!context->redirect_objs[oid].empty()) {
      context->oid_redirect_not_in_use.insert(context->redirect_objs[oid]);
      context->oid_redirect_in_use.erase(context->redirect_objs[oid]);
      context->update_object_redirect_target(oid, string());
    }
    context->kick();
  }

  string getType() override
  {
    return "UnsetRedirectOp";
  }
};

class TierPromoteOp : public TestOp {
public:
  librados::AioCompletion *completion;
  librados::ObjectWriteOperation op;
  string oid;
  std::shared_ptr<int> in_use;

  TierPromoteOp(int n,
	       RadosTestContext *context,
	       const string &oid,
	       TestOpStat *stat)
    : TestOp(n, context, stat),
      completion(NULL),
      oid(oid)
  {}

  void _begin() override
  {
    context->state_lock.lock();

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    completion = context->rados.aio_create_completion((void *) cb_arg,
						      &write_callback);
    context->state_lock.unlock();

    op.tier_promote();
    int r = context->io_ctx.aio_operate(context->prefix+oid, completion,
					&op);
    ceph_assert(!r);
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard l{context->state_lock};
    ceph_assert(!done);
    ceph_assert(completion->is_complete());

    ObjectDesc oid_value;
    context->find_object(oid, &oid_value);
    int r = completion->get_return_value();
    cout << num << ":  got " << cpp_strerror(r) << std::endl;
    if (r == 0) {
      // sucess
    } else {
      ceph_abort_msg("shouldn't happen");
    }
    context->update_object_version(oid, completion->get_version64());
    context->find_object(oid, &oid_value);
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
    done = true;
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
  {
    return "TierPromoteOp";
  }
};

class TierFlushOp : public TestOp {
public:
  librados::AioCompletion *completion;
  librados::ObjectReadOperation op;
  string oid;
  std::shared_ptr<int> in_use;

  TierFlushOp(int n,
	       RadosTestContext *context,
	       const string &oid,
	       TestOpStat *stat)
    : TestOp(n, context, stat),
      completion(NULL),
      oid(oid)
  {}

  void _begin() override
  {
    context->state_lock.lock();

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);

    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    completion = context->rados.aio_create_completion((void *) cb_arg, 
						      &write_callback);
    context->state_lock.unlock();

    op.tier_flush();
    unsigned flags = librados::OPERATION_IGNORE_CACHE;
    int r = context->io_ctx.aio_operate(context->prefix+oid, completion,
					&op, flags, NULL);
    ceph_assert(!r);
  }

  void _finish(CallbackInfo *info) override
  {
    context->state_lock.lock();
    ceph_assert(!done);
    ceph_assert(completion->is_complete());

    int r = completion->get_return_value();
    cout << num << ":  got " << cpp_strerror(r) << std::endl;
    if (r == 0) {
      // sucess
    } else {
      ceph_abort_msg("shouldn't happen");
    }
    context->update_object_version(oid, completion->get_version64());
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->kick();
    done = true;
    context->state_lock.unlock();
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
  {
    return "TierFlushOp";
  }
};

class HitSetListOp : public TestOp {
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
      comp1(NULL), comp2(NULL),
      hash(hash)
  {}

  void _begin() override
  {
    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    comp1 = context->rados.aio_create_completion((void*) cb_arg,
						 &write_callback);
    int r = context->io_ctx.hit_set_list(hash, comp1, &ls);
    ceph_assert(r == 0);
  }

  void _finish(CallbackInfo *info) override {
    std::lock_guard l{context->state_lock};
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
	comp2 = context->rados.aio_create_completion((void*) cb_arg, &write_callback);
	r = context->io_ctx.hit_set_get(hash, comp2, p->second, &bl);
	ceph_assert(r == 0);
      }
    } else {
      int r = comp2->get_return_value();
      if (r == 0) {
	HitSet hitset;
	auto p = bl.cbegin();
	decode(hitset, p);
	cout << num << ": got hitset of type " << hitset.get_type_name()
	     << " size " << bl.length()
	     << std::endl;
      } else {
	// FIXME: we could verify that we did in fact race with a trim...
	ceph_assert(r == -ENOENT);
      }
      done = true;
    }

    context->kick();
  }

  bool finished() override {
    return done;
  }

  string getType() override {
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

  void _begin() override
  {
    context->state_lock.lock();
    pair<TestOp*, TestOp::CallbackInfo*> *cb_arg =
      new pair<TestOp*, TestOp::CallbackInfo*>(this,
					       new TestOp::CallbackInfo(0));
    completion = context->rados.aio_create_completion((void *) cb_arg,
						      &write_callback);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    context->update_object_undirty(oid);
    context->state_lock.unlock();

    op.undirty();
    int r = context->io_ctx.aio_operate(context->prefix+oid, completion,
					&op, 0);
    ceph_assert(!r);
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard state_locker{context->state_lock};
    ceph_assert(!done);
    ceph_assert(completion->is_complete());
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);
    context->update_object_version(oid, completion->get_version64());
    context->kick();
    done = true;
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
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
  int snap = 0;
  std::shared_ptr<int> in_use;

  IsDirtyOp(int n,
	    RadosTestContext *context,
	    const string &oid,
	    TestOpStat *stat = 0)
    : TestOp(n, context, stat),
      completion(NULL),
      oid(oid),
      dirty(false)
  {}

  void _begin() override
  {
    context->state_lock.lock();

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
    completion = context->rados.aio_create_completion((void *) cb_arg,
						      &write_callback);

    context->oid_in_use.insert(oid);
    context->oid_not_in_use.erase(oid);
    context->state_lock.unlock();

    if (snap >= 0) {
      context->io_ctx.snap_set_read(context->snaps[snap]);
    }

    op.is_dirty(&dirty, NULL);
    int r = context->io_ctx.aio_operate(context->prefix+oid, completion,
					&op, 0);
    ceph_assert(!r);

    if (snap >= 0) {
      context->io_ctx.snap_set_read(0);
    }
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard state_locker{context->state_lock};
    ceph_assert(!done);
    ceph_assert(completion->is_complete());
    context->oid_in_use.erase(oid);
    context->oid_not_in_use.insert(oid);

    ceph_assert(context->find_object(oid, &old_value, snap));

    int r = completion->get_return_value();
    if (r == 0) {
      cout << num << ":  " << (dirty ? "dirty" : "clean") << std::endl;
      ceph_assert(!old_value.deleted());
      ceph_assert(dirty == old_value.dirty);
    } else {
      cout << num << ":  got " << r << std::endl;
      ceph_assert(r == -ENOENT);
      ceph_assert(old_value.deleted());
    }
    context->kick();
    done = true;
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
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
  std::shared_ptr<int> in_use;

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

  void _begin() override
  {
    context->state_lock.lock();

    if (!(rand() % 4) && !context->snaps.empty()) {
      snap = rand_choose(context->snaps)->first;
      in_use = context->snaps_in_use.lookup_or_create(snap, snap);
    } else {
      snap = -1;
    }
    // not being particularly specific here about knowing which
    // flushes are on the oldest clean snap and which ones are not.
    can_fail = !blocking || !context->snaps.empty();
    // FIXME: we could fail if we've ever removed a snap due to
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
    completion = context->rados.aio_create_completion((void *) cb_arg,
						      &write_callback);
    context->oid_flushing.insert(oid);
    context->oid_not_flushing.erase(oid);
    context->state_lock.unlock();

    unsigned flags = librados::OPERATION_IGNORE_CACHE;
    if (blocking) {
      op.cache_flush();
    } else {
      op.cache_try_flush();
      flags = librados::OPERATION_SKIPRWLOCKS;
    }
    int r = context->io_ctx.aio_operate(context->prefix+oid, completion,
					&op, flags, NULL);
    ceph_assert(!r);

    if (snap >= 0) {
      context->io_ctx.snap_set_read(0);
    }
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard state_locker{context->state_lock};
    ceph_assert(!done);
    ceph_assert(completion->is_complete());
    context->oid_flushing.erase(oid);
    context->oid_not_flushing.insert(oid);
    int r = completion->get_return_value();
    cout << num << ":  got " << cpp_strerror(r) << std::endl;
    if (r == 0) {
      context->update_object_version(oid, 0, snap);
    } else if (r == -EBUSY) {
      ceph_assert(can_fail);
    } else if (r == -EINVAL) {
      // caching not enabled?
    } else if (r == -ENOENT) {
      // may have raced with a remove?
    } else {
      ceph_abort_msg("shouldn't happen");
    }
    context->kick();
    done = true;
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
  {
    return "CacheFlushOp";
  }
};

class CacheEvictOp : public TestOp {
public:
  librados::AioCompletion *completion;
  librados::ObjectReadOperation op;
  string oid;
  std::shared_ptr<int> in_use;

  CacheEvictOp(int n,
	       RadosTestContext *context,
	       const string &oid,
	       TestOpStat *stat)
    : TestOp(n, context, stat),
      completion(NULL),
      oid(oid)
  {}

  void _begin() override
  {
    context->state_lock.lock();

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
    completion = context->rados.aio_create_completion((void *) cb_arg,
						      &write_callback);
    context->state_lock.unlock();

    op.cache_evict();
    int r = context->io_ctx.aio_operate(context->prefix+oid, completion,
					&op, librados::OPERATION_IGNORE_CACHE,
					NULL);
    ceph_assert(!r);

    if (snap >= 0) {
      context->io_ctx.snap_set_read(0);
    }
  }

  void _finish(CallbackInfo *info) override
  {
    std::lock_guard state_locker{context->state_lock};
    ceph_assert(!done);
    ceph_assert(completion->is_complete());

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
      ceph_abort_msg("shouldn't happen");
    }
    context->kick();
    done = true;
  }

  bool finished() override
  {
    return done;
  }

  string getType() override
  {
    return "CacheEvictOp";
  }
};


#endif
