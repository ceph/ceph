// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <iterator>
#include <map>
#include <set>
#include <boost/scoped_ptr.hpp>
#include <sys/types.h>
#include <cstdlib>

#include "include/buffer.h"
#include "common/map_cacher.hpp"
#include "osd/osd_types_fmt.h"
#include "osd/SnapMapper.h"
#include "common/Cond.h"

#include "gtest/gtest.h"

using namespace std;

template <typename T>
typename T::iterator rand_choose(T &cont) {
  if (std::empty(cont)) {
    return std::end(cont);
  }
  return std::next(std::begin(cont), rand() % cont.size());
}

string random_string(size_t size)
{
  string name;
  for (size_t j = 0; j < size; ++j) {
    name.push_back('a' + (rand() % 26));
  }
  return name;
}

class PausyAsyncMap : public MapCacher::StoreDriver<string, bufferlist> {
  struct _Op {
    virtual void operate(map<string, bufferlist> *store) = 0;
    virtual ~_Op() {}
  };
  typedef std::shared_ptr<_Op> Op;
  struct Remove : public _Op {
    set<string> to_remove;
    explicit Remove(const set<string> &to_remove) : to_remove(to_remove) {}
    void operate(map<string, bufferlist> *store) override {
      for (set<string>::iterator i = to_remove.begin();
	   i != to_remove.end();
	   ++i) {
	store->erase(*i);
      }
    }
  };
  struct Insert : public _Op {
    map<string, bufferlist> to_insert;
    explicit Insert(const map<string, bufferlist> &to_insert) : to_insert(to_insert) {}
    void operate(map<string, bufferlist> *store) override {
      for (map<string, bufferlist>::iterator i = to_insert.begin();
	   i != to_insert.end();
	   ++i) {
	store->erase(i->first);
	store->insert(*i);
      }
    }
  };
  struct Callback : public _Op {
    Context *context;
    explicit Callback(Context *c) : context(c) {}
    void operate(map<string, bufferlist> *store) override {
      context->complete(0);
    }
  };
public:
  class Transaction : public MapCacher::Transaction<string, bufferlist> {
    friend class PausyAsyncMap;
    list<Op> ops;
    list<Op> callbacks;
  public:
    void set_keys(const map<string, bufferlist> &i) override {
      ops.push_back(Op(new Insert(i)));
    }
    void remove_keys(const set<string> &r) override {
      ops.push_back(Op(new Remove(r)));
    }
    void add_callback(Context *c) override {
      callbacks.push_back(Op(new Callback(c)));
    }
  };
private:

  ceph::mutex lock = ceph::make_mutex("PausyAsyncMap");
  map<string, bufferlist> store;

  class Doer : public Thread {
    static const size_t MAX_SIZE = 100;
    PausyAsyncMap *parent;
    ceph::mutex lock = ceph::make_mutex("Doer lock");
    ceph::condition_variable cond;
    int stopping;
    bool paused;
    list<Op> queue;
  public:
    explicit Doer(PausyAsyncMap *parent) :
      parent(parent), stopping(0), paused(false) {}
    void *entry() override {
      while (1) {
	list<Op> ops;
	{
	  std::unique_lock l{lock};
	  cond.wait(l, [this] {
            return stopping || (!queue.empty() && !paused);
	  });
	  if (stopping && queue.empty()) {
	    stopping = 2;
	    cond.notify_all();
	    return 0;
	  }
	  ceph_assert(!queue.empty());
	  ceph_assert(!paused);
	  ops.swap(queue);
	  cond.notify_all();
	}
	ceph_assert(!ops.empty());

	for (list<Op>::iterator i = ops.begin();
	     i != ops.end();
	     ops.erase(i++)) {
	  if (!(rand()%3))
	    usleep(1+(rand() % 5000));
	  std::lock_guard l{parent->lock};
	  (*i)->operate(&(parent->store));
	}
      }
    }

    void pause() {
      std::lock_guard l{lock};
      paused = true;
      cond.notify_all();
    }

    void resume() {
      std::lock_guard l{lock};
      paused = false;
      cond.notify_all();
    }

    void submit(list<Op> &in) {
      std::unique_lock l{lock};
      cond.wait(l, [this] { return queue.size() < MAX_SIZE;});
      queue.splice(queue.end(), in, in.begin(), in.end());
      cond.notify_all();
    }

    void stop() {
      std::unique_lock l{lock};
      stopping = 1;
      cond.notify_all();
      cond.wait(l, [this] { return stopping == 2; });
      cond.notify_all();
    }
  } doer;

public:
  PausyAsyncMap() : doer(this) {
    doer.create("doer");
  }
  ~PausyAsyncMap() override {
    doer.join();
  }
  int get_keys(
    const set<string> &keys,
    map<string, bufferlist> *out) override {
    std::lock_guard l{lock};
    for (set<string>::const_iterator i = keys.begin();
	 i != keys.end();
	 ++i) {
      map<string, bufferlist>::iterator j = store.find(*i);
      if (j != store.end())
	out->insert(*j);
    }
    return 0;
  }
  int get_next(
    const string &key,
    pair<string, bufferlist> *next) override {
    std::lock_guard l{lock};
    map<string, bufferlist>::iterator j = store.upper_bound(key);
    if (j != store.end()) {
      if (next)
	*next = *j;
      return 0;
    } else {
      return -ENOENT;
    }
  }
  int get_next_or_current(
    const string &key,
    pair<string, bufferlist> *next_or_current) override {
    std::lock_guard l{lock};
    map<string, bufferlist>::iterator j = store.lower_bound(key);
    if (j != store.end()) {
      if (next_or_current)
	*next_or_current = *j;
      return 0;
    } else {
      return -ENOENT;
    }
  }
  void submit(Transaction *t) {
    doer.submit(t->ops);
    doer.submit(t->callbacks);
  }

  void flush() {
    ceph::mutex lock = ceph::make_mutex("flush lock");
    ceph::condition_variable cond;
    bool done = false;

    class OnFinish : public Context {
      ceph::mutex *lock;
      ceph::condition_variable *cond;
      bool *done;
    public:
      OnFinish(ceph::mutex *lock, ceph::condition_variable *cond, bool *done)
	: lock(lock), cond(cond), done(done) {}
      void finish(int) override {
	std::lock_guard l{*lock};
	*done = true;
	cond->notify_all();
      }
    };
    Transaction t;
    t.add_callback(new OnFinish(&lock, &cond, &done));
    submit(&t);
    {
      std::unique_lock l{lock};
      cond.wait(l, [&] { return done; });
    }
  }

  void pause() {
    doer.pause();
  }
  void resume() {
    doer.resume();
  }
  void stop() {
    doer.stop();
  }

};

class MapCacherTest : public ::testing::Test {
protected:
  boost::scoped_ptr< PausyAsyncMap > driver;
  boost::scoped_ptr<MapCacher::MapCacher<string, bufferlist> > cache;
  map<string, bufferlist> truth;
  set<string> names;
public:
  void assert_bl_eq(bufferlist &bl1, bufferlist &bl2) {
    ASSERT_EQ(bl1.length(), bl2.length());
    bufferlist::iterator j = bl2.begin();
    for (bufferlist::iterator i = bl1.begin();
	 !i.end();
	 ++i, ++j) {
      ASSERT_TRUE(!j.end());
      ASSERT_EQ(*i, *j);
    }
  }
  void assert_bl_map_eq(map<string, bufferlist> &m1,
			map<string, bufferlist> &m2) {
    ASSERT_EQ(m1.size(), m2.size());
    map<string, bufferlist>::iterator j = m2.begin();
    for (map<string, bufferlist>::iterator i = m1.begin();
	 i != m1.end();
	 ++i, ++j) {
      ASSERT_TRUE(j != m2.end());
      ASSERT_EQ(i->first, j->first);
      assert_bl_eq(i->second, j->second);
    }

  }
  size_t random_num() {
    return random() % 10;
  }
  size_t random_size() {
    return random() % 1000;
  }
  void random_bl(size_t size, bufferlist *bl) {
    for (size_t i = 0; i < size; ++i) {
      bl->append(rand());
    }
  }
  void do_set() {
    size_t set_size = random_num();
    map<string, bufferlist> to_set;
    for (size_t i = 0; i < set_size; ++i) {
      bufferlist bl;
      random_bl(random_size(), &bl);
      string key = *rand_choose(names);
      to_set.insert(
	make_pair(key, bl));
    }
    for (map<string, bufferlist>::iterator i = to_set.begin();
	 i != to_set.end();
	 ++i) {
      truth.erase(i->first);
      truth.insert(*i);
    }
    {
      PausyAsyncMap::Transaction t;
      cache->set_keys(to_set, &t);
      driver->submit(&t);
    }
  }
  void remove() {
    size_t remove_size = random_num();
    set<string> to_remove;
    for (size_t i = 0; i < remove_size ; ++i) {
      to_remove.insert(*rand_choose(names));
    }
    for (set<string>::iterator i = to_remove.begin();
	 i != to_remove.end();
	 ++i) {
      truth.erase(*i);
    }
    {
      PausyAsyncMap::Transaction t;
      cache->remove_keys(to_remove, &t);
      driver->submit(&t);
    }
  }
  void get() {
    set<string> to_get;
    size_t get_size = random_num();
    for (size_t i = 0; i < get_size; ++i) {
      to_get.insert(*rand_choose(names));
    }

    map<string, bufferlist> got_truth;
    for (set<string>::iterator i = to_get.begin();
	 i != to_get.end();
	 ++i) {
      map<string, bufferlist>::iterator j = truth.find(*i);
      if (j != truth.end())
	got_truth.insert(*j);
    }

    map<string, bufferlist> got;
    cache->get_keys(to_get, &got);

    assert_bl_map_eq(got, got_truth);
  }

  void get_next() {
    string cur;
    while (true) {
      pair<string, bufferlist> next;
      int r = cache->get_next(cur, &next);

      pair<string, bufferlist> next_truth;
      map<string, bufferlist>::iterator i = truth.upper_bound(cur);
      int r_truth = (i == truth.end()) ? -ENOENT : 0;
      if (i != truth.end())
	next_truth = *i;

      ASSERT_EQ(r, r_truth);
      if (r == -ENOENT)
	break;

      ASSERT_EQ(next.first, next_truth.first);
      assert_bl_eq(next.second, next_truth.second);
      cur = next.first;
    }
  }
  void SetUp() override {
    driver.reset(new PausyAsyncMap());
    cache.reset(new MapCacher::MapCacher<string, bufferlist>(driver.get()));
    names.clear();
    truth.clear();
    size_t names_size(random_num() + 10);
    for (size_t i = 0; i < names_size; ++i) {
      names.insert(random_string(1 + (random_size() % 10)));
    }
  }
  void TearDown() override {
    driver->stop();
    cache.reset();
    driver.reset();
  }

};

TEST_F(MapCacherTest, Simple)
{
  driver->pause();
  map<string, bufferlist> truth;
  set<string> truth_keys;
  string blah("asdf");
  bufferlist bl;
  encode(blah, bl);
  truth[string("asdf")] = bl;
  truth_keys.insert(truth.begin()->first);
  {
    PausyAsyncMap::Transaction t;
    cache->set_keys(truth, &t);
    driver->submit(&t);
    cache->set_keys(truth, &t);
    driver->submit(&t);
  }

  map<string, bufferlist> got;
  cache->get_keys(truth_keys, &got);
  assert_bl_map_eq(got, truth);

  driver->resume();
  sleep(1);

  got.clear();
  cache->get_keys(truth_keys, &got);
  assert_bl_map_eq(got, truth);
}

TEST_F(MapCacherTest, Random)
{
  for (size_t i = 0; i < 5000; ++i) {
    if (!(i % 50)) {
      std::cout << "On iteration " << i << std::endl;
    }
    switch (rand() % 4) {
    case 0:
      get();
      break;
    case 1:
      do_set();
      break;
    case 2:
      get_next();
      break;
    case 3:
      remove();
      break;
    }
  }
}

class MapperVerifier {
  PausyAsyncMap *driver;
  boost::scoped_ptr< SnapMapper > mapper;
  map<snapid_t, set<hobject_t> > snap_to_hobject;
  map<hobject_t, set<snapid_t>> hobject_to_snap;
  snapid_t next;
  uint32_t mask;
  uint32_t bits;
  ceph::mutex lock = ceph::make_mutex("lock");
public:

  MapperVerifier(
    PausyAsyncMap *driver,
    uint32_t mask,
    uint32_t bits)
    : driver(driver),
      mapper(new SnapMapper(g_ceph_context, driver, mask, bits, 0, shard_id_t(1))),
	     mask(mask), bits(bits) {}

  hobject_t create_hobject(
    unsigned           idx,
    snapid_t           snapid,
    int64_t            pool,
    const std::string& nspace) {
    const object_t    oid("OID" + std::to_string(idx));
    const std::string key("KEY" + std::to_string(idx));
    const uint32_t    hash = (idx & ((~0)<<bits)) | (mask & ~((~0)<<bits));

    return hobject_t(oid, key, snapid, hash, pool, nspace);
  }

  hobject_t random_hobject() {
    return hobject_t(
      random_string(1+(rand() % 16)),
      random_string(1+(rand() % 16)),
      snapid_t(rand() % 1000),
      (rand() & ((~0)<<bits)) | (mask & ~((~0)<<bits)),
      0, random_string(rand() % 16));
  }

  void choose_random_snaps(int num, set<snapid_t> *snaps) {
    ceph_assert(snaps);
    ceph_assert(!snap_to_hobject.empty());
    for (int i = 0; i < num || snaps->empty(); ++i) {
      snaps->insert(rand_choose(snap_to_hobject)->first);
    }
  }

  snapid_t create_snap() {
    snapid_t snapid = next;
    snap_to_hobject[snapid];
    ++next;

    return snapid;
  }

  // must be called with lock held to protect access to
  // hobject_to_snap and snap_to_hobject
  void add_object_to_snaps(const hobject_t & obj, const set<snapid_t> &snaps) {
    hobject_to_snap[obj] = snaps;
    for (auto snap : snaps) {
      map<snapid_t, set<hobject_t> >::iterator j = snap_to_hobject.find(snap);
      ceph_assert(j != snap_to_hobject.end());
      j->second.insert(obj);
    }
    {
      PausyAsyncMap::Transaction t;
      mapper->add_oid(obj, snaps, &t);
      driver->submit(&t);
    }
  }

  void create_object() {
    std::lock_guard l{lock};
    if (snap_to_hobject.empty())
      return;
    hobject_t obj;
    do {
      obj = random_hobject();
    } while (hobject_to_snap.count(obj));

    set<snapid_t> snaps;
    choose_random_snaps(1 + (rand() % 20), &snaps);
    add_object_to_snaps(obj, snaps);
  }

  std::pair<std::string, ceph::buffer::list> to_raw(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_raw(to_map);
  }

  std::string to_legacy_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_legacy_raw_key(to_map);
  }

  template <typename... Args>
  std::string to_object_key(Args&&... args) {
    return mapper->to_object_key(std::forward<Args>(args)...);
  }

  std::string to_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_raw_key(to_map);
  }

  template <typename... Args>
  std::string make_purged_snap_key(Args&&... args) {
    return mapper->make_purged_snap_key(std::forward<Args>(args)...);
  }

  // must be called with lock held to protect access to
  // snap_to_hobject and hobject_to_snap
  int trim_snap(snapid_t snapid, unsigned max_count, vector<hobject_t> & out) {
    set<hobject_t>&   hobjects = snap_to_hobject[snapid];
    vector<hobject_t> hoids;
    int ret = mapper->get_next_objects_to_trim(snapid, max_count, &hoids);
    if (ret == 0) {
      out.insert(out.end(), hoids.begin(), hoids.end());
      for (auto &&hoid: hoids) {
	ceph_assert(!hoid.is_max());
	ceph_assert(hobjects.count(hoid));
	hobjects.erase(hoid);

	map<hobject_t, set<snapid_t>>::iterator j = hobject_to_snap.find(hoid);
	ceph_assert(j->second.count(snapid));
	set<snapid_t> old_snaps(j->second);
	j->second.erase(snapid);

	{
	  PausyAsyncMap::Transaction t;
	  mapper->update_snaps(
	    hoid,
	    j->second,
	    &old_snaps,
	    &t);
	  driver->submit(&t);
	}
	if (j->second.empty()) {
	  hobject_to_snap.erase(j);
	}
	hoid = hobject_t::get_max();
      }
      hoids.clear();
    }
    return ret;
  }

  // must be called with lock held to protect access to
  // snap_to_hobject and hobject_to_snap in trim_snap
  // will keep trimming until reaching max_count or failing a call to trim_snap()
  void trim_snap_force(snapid_t           snapid,
		       unsigned           max_count,
		       vector<hobject_t>& out) {
    int               guard = 1000;
    vector<hobject_t> tmp;
    unsigned          prev_size = 0;
    while (tmp.size() < max_count) {
      unsigned req_size = max_count - tmp.size();
      // each call adds more objects into the tmp vector
      trim_snap(snapid, req_size, tmp);
      if (prev_size < tmp.size()) {
	prev_size = tmp.size();
      }
      else{
	// the tmp vector size was not increased in the last call
	// which means we were unable to find anything to trim
	break;
      }
      ceph_assert(--guard > 0);
    }
    out.insert(out.end(), tmp.begin(), tmp.end());
  }

  void trim_snap() {
    std::lock_guard l{lock};
    if (snap_to_hobject.empty()) {
      return;
    }
    int ret = 0;
    map<snapid_t, set<hobject_t> >::iterator snap = rand_choose(snap_to_hobject);
    do {
      int max_count = rand() % 5 + 1;
      vector<hobject_t> out;
      ret = trim_snap(snap->first, max_count, out);
    } while(ret == 0);
    set<hobject_t> hobjects = snap->second;
    ceph_assert(hobjects.empty());
    snap_to_hobject.erase(snap);
  }

  void remove_oid() {
    std::lock_guard l{lock};
    if (hobject_to_snap.empty())
      return;
    map<hobject_t, set<snapid_t>>::iterator obj =
      rand_choose(hobject_to_snap);
    for (set<snapid_t>::iterator i = obj->second.begin();
	 i != obj->second.end();
	 ++i) {
      map<snapid_t, set<hobject_t> >::iterator j =
	snap_to_hobject.find(*i);
      ceph_assert(j->second.count(obj->first));
      j->second.erase(obj->first);
    }
    {
      PausyAsyncMap::Transaction t;
      mapper->remove_oid(
	obj->first,
	&t);
      driver->submit(&t);
    }
    hobject_to_snap.erase(obj);
  }

  void check_oid() {
    std::lock_guard l{lock};
    if (hobject_to_snap.empty())
      return;
    map<hobject_t, set<snapid_t>>::iterator obj =
      rand_choose(hobject_to_snap);
    set<snapid_t> snaps;
    int r = mapper->get_snaps(obj->first, &snaps);
    ceph_assert(r == 0);
    ASSERT_EQ(snaps, obj->second);
  }

  void test_prefix_itr() {
    // protects access to snap_to_hobject and hobject_to_snap
    std::lock_guard   l{lock};
    snapid_t          snapid = create_snap();
    // we initialize 32 PGS
    ceph_assert(bits == 5);

    const int64_t     pool(0);
    const std::string nspace("GBH");
    set<snapid_t>     snaps = { snapid };
    set<hobject_t>&   hobjects = snap_to_hobject[snapid];
    vector<hobject_t> trimmed_objs;
    vector<hobject_t> stored_objs;

    // add objects 0, 32, 64, 96, 128, 160, 192, 224
    // which should hit all the prefixes
    for (unsigned idx = 0; idx < 8; idx++) {
      hobject_t hobj = create_hobject(idx * 32, snapid, pool, nspace);
      add_object_to_snaps(hobj, snaps);
      stored_objs.push_back(hobj);
    }
    ceph_assert(hobjects.size() == 8);

    // trim 0, 32, 64, 96
    trim_snap(snapid, 4, trimmed_objs);
    ceph_assert(hobjects.size() == 4);

    // add objects (3, 35, 67) before the prefix_itr position
    // to force an iteartor reset later
    for (unsigned idx = 0; idx < 3; idx++) {
      hobject_t hobj = create_hobject(idx * 32 + 3, snapid, pool, nspace);
      add_object_to_snaps(hobj, snaps);
      stored_objs.push_back(hobj);
    }
    ceph_assert(hobjects.size() == 7);

    // will now trim 128, 160, 192, 224
    trim_snap(snapid, 4, trimmed_objs);
    ceph_assert(hobjects.size() == 3);

    // finally, trim 3, 35, 67
    // This will force a reset to the prefix_itr (which is what we test here)
    trim_snap(snapid, 3, trimmed_objs);
    ceph_assert(hobjects.size() == 0);

    ceph_assert(trimmed_objs.size() == 11);
    // trimmed objs must be in the same order they were inserted
    // this will prove that the second call to add_object_to_snaps inserted
    // them before the current prefix_itr
    ceph_assert(trimmed_objs.size() == stored_objs.size());
    ceph_assert(std::equal(trimmed_objs.begin(), trimmed_objs.end(),
			   stored_objs.begin()));
    snap_to_hobject.erase(snapid);
  }

  // insert 256 objects which should populate multiple prefixes
  // trim until we change prefix and then insert an old object
  // which we know for certain belongs to a prefix before prefix_itr
  void test_prefix_itr2() {
    // protects access to snap_to_hobject and hobject_to_snap
    std::lock_guard   l{lock};
    snapid_t          snapid = create_snap();
    // we initialize 32 PGS
    ceph_assert(bits == 5);

    const int64_t     pool(0);
    const std::string nspace("GBH");
    set<snapid_t>     snaps = { snapid };
    vector<hobject_t> trimmed_objs;
    vector<hobject_t> stored_objs;

    constexpr unsigned MAX_IDX = 256;
    for (unsigned idx = 0; idx < MAX_IDX; idx++) {
      hobject_t hobj = create_hobject(idx, snapid, pool, nspace);
      add_object_to_snaps(hobj, snaps);
      stored_objs.push_back(hobj);
    }

    hobject_t dup_hobj;
    bool      found = false;
    trim_snap(snapid, 1, trimmed_objs);
    const std::set<std::string>::iterator itr = mapper->get_prefix_itr();
    for (unsigned idx = 1; idx < MAX_IDX + 1; idx++) {
      trim_snap(snapid, 1, trimmed_objs);
      if (!found && mapper->get_prefix_itr() != itr) {
	// we changed prefix -> insert an OBJ belonging to perv prefix
	dup_hobj = create_hobject(idx - 1, snapid, pool, nspace);
	add_object_to_snaps(dup_hobj, snaps);
	stored_objs.push_back(dup_hobj);
	found = true;
      }
    }
    ceph_assert(found);

    sort(trimmed_objs.begin(), trimmed_objs.end());
    sort(stored_objs.begin(),  stored_objs.end());
    ceph_assert(trimmed_objs.size() == MAX_IDX+1);
    ceph_assert(trimmed_objs.size() == stored_objs.size());
    ceph_assert(std::equal(trimmed_objs.begin(), trimmed_objs.end(),
			   stored_objs.begin()));
    snap_to_hobject.erase(snapid);
  }

  void add_rand_hobjects(unsigned           count,
			 snapid_t           snapid,
			 int64_t            pool,
			 const std::string& nspace,
			 vector<hobject_t>& stored_objs) {
    constexpr unsigned MAX_VAL = 1000;
    set<snapid_t> snaps = { snapid };
    for (unsigned i = 0; i < count; i++) {
      hobject_t hobj;
      do {
	unsigned val = rand() % MAX_VAL;
	hobj = create_hobject(val, snapid, pool, nspace);
      }while (hobject_to_snap.count(hobj));
      add_object_to_snaps(hobj, snaps);
      stored_objs.push_back(hobj);
    }
  }

  // Start with a set of random objects then run a partial trim
  // followed by another random insert
  // This should cause *some* objects to be added before the prefix_itr
  // and will verify that we still remove them
  void test_prefix_itr_rand() {
    // protects access to snap_to_hobject and hobject_to_snap
    std::lock_guard   l{lock};
    snapid_t          snapid = create_snap();
    // we initialize 32 PGS
    ceph_assert(bits == 5);

    const int64_t     pool(0);
    const std::string nspace("GBH");
    vector<hobject_t> trimmed_objs;
    vector<hobject_t> stored_objs;
    set<hobject_t>&   hobjects = snap_to_hobject[snapid];
    ceph_assert(hobjects.size() == 0);

    // add 100 random objects
    add_rand_hobjects(100, snapid, pool, nspace, stored_objs);
    ceph_assert(hobjects.size() == 100);

    // trim the first 75 objects leaving 25 objects
    trim_snap(snapid, 75, trimmed_objs);
    ceph_assert(hobjects.size() == 25);

    // add another 25 random objects (now we got 50 objects)
    add_rand_hobjects(25, snapid, pool, nspace, stored_objs);
    ceph_assert(hobjects.size() == 50);

    // trim 49 objects leaving a single object
    // we must use a wrapper function to keep trimming while until -ENOENT
    trim_snap_force(snapid, 49, trimmed_objs);
    ceph_assert(hobjects.size() == 1);

    // add another 9 random objects (now we got 10 objects)
    add_rand_hobjects(9, snapid, pool, nspace, stored_objs);
    ceph_assert(hobjects.size() == 10);

    // trim 10 objects leaving no object in store
    trim_snap_force(snapid, 10, trimmed_objs);
    ceph_assert(hobjects.size() == 0);

    // add 10 random objects (now we got 10 objects)
    add_rand_hobjects(10, snapid, pool, nspace, stored_objs);
    ceph_assert(hobjects.size() == 10);

    // trim 10 objects leaving no object in store
    trim_snap_force(snapid, 10, trimmed_objs);
    ceph_assert(hobjects.size() == 0);

    sort(trimmed_objs.begin(), trimmed_objs.end());
    sort(stored_objs.begin(),  stored_objs.end());
    ceph_assert(trimmed_objs.size() == 144);
    ceph_assert(trimmed_objs.size() == stored_objs.size());

    bool are_equal = std::equal(trimmed_objs.begin(), trimmed_objs.end(),
				stored_objs.begin());
    ceph_assert(are_equal);
    snap_to_hobject.erase(snapid);
  }
};

class SnapMapperTest : public ::testing::Test {
protected:
  boost::scoped_ptr< PausyAsyncMap > driver;
  map<pg_t, std::shared_ptr<MapperVerifier> > mappers;
  uint32_t pgnum;

  void SetUp() override {
    driver.reset(new PausyAsyncMap());
    pgnum = 0;
  }

  void TearDown() override {
    driver->stop();
    mappers.clear();
    driver.reset();
  }

  MapperVerifier &get_tester() {
    //return *(mappers.begin()->second);
    return *(rand_choose(mappers)->second);
  }

  void init(uint32_t to_set) {
    pgnum = to_set;
    for (uint32_t i = 0; i < pgnum; ++i) {
      pg_t pgid(i, 0);
      mappers[pgid].reset(
	new MapperVerifier(
	  driver.get(),
	  i,
	  pgid.get_split_bits(pgnum)
	  )
	);
    }
  }

  void run() {
    for (int i = 0; i < 5000; ++i) {
      if (!(i % 50))
	std::cout << i << std::endl;
      switch (rand() % 5) {
      case 0:
	get_tester().create_snap();
	break;
      case 1:
	get_tester().create_object();
	break;
      case 2:
	get_tester().trim_snap();
	break;
      case 3:
	get_tester().check_oid();
	break;
      case 4:
	get_tester().remove_oid();
	break;
      }
    }
  }
};

// This test creates scenarios which are impossible to get with normal code.
// The normal code deletes the snap before calling TRIM and so no new clones
// can be added to that snap.
// Our test calls get_next_objects_to_trim() *without* deleting the snap first.
// This allows us to add objects to the (non-deleted) snap after trimming began.
// We test that SnapTrim will find them even when added into positions before the prefix_itr.
// Since those tests are doing illegal inserts we must disable osd_debug_trim_objects
// during those tests as otherwise the code will assert.
TEST_F(SnapMapperTest, prefix_itr) {
  bool orig_val = g_ceph_context->_conf.get_val<bool>("osd_debug_trim_objects");
  std::cout << "osd_debug_trim_objects = " << orig_val << std::endl;
  g_ceph_context->_conf.set_val("osd_debug_trim_objects", std::to_string(false));
  init(32);
  get_tester().test_prefix_itr();
  get_tester().test_prefix_itr2();
  get_tester().test_prefix_itr_rand();
  g_ceph_context->_conf.set_val("osd_debug_trim_objects", std::to_string(orig_val));
  bool curr_val = g_ceph_context->_conf.get_val<bool>("osd_debug_trim_objects");
  ceph_assert(curr_val == orig_val);
}

TEST_F(SnapMapperTest, Simple) {
  init(1);
  get_tester().create_snap();
  get_tester().create_object();
  get_tester().trim_snap();
}

TEST_F(SnapMapperTest, More) {
  init(1);
  run();
}

TEST_F(SnapMapperTest, MultiPG) {
  init(50);
  run();
}

// Check to_object_key against current format to detect accidental changes in encoding
TEST_F(SnapMapperTest, CheckObjectKeyFormat) {
  init(1);
  // <object, test_raw_key>
  std::vector<std::tuple<hobject_t, std::string>> object_to_object_key({
      {hobject_t{"test_object", "", 20, 0x01234567, 20, ""},
	  "OBJ_.1_0000000000000014.76543210.14.test%uobject.."},
      {hobject_t{"test._ob.ject", "k.ey", 20, 0x01234567, 20, ""},
	  "OBJ_.1_0000000000000014.76543210.14.test%e%uob%eject.k%eey."},
      {hobject_t{"test_object", "", 20, 0x01234567, 20, "namespace"},
	  "OBJ_.1_0000000000000014.76543210.14.test%uobject..namespace"},
      {hobject_t{
	  "test_object", "", std::numeric_limits<snapid_t>::max() - 20, 0x01234567,
	    std::numeric_limits<int64_t>::max() - 20, "namespace"},
	  "OBJ_.1_7FFFFFFFFFFFFFEB.76543210.ffffffffffffffec.test%uobject..namespace"}
    });

  for (auto &[object, test_object_key]: object_to_object_key) {
    auto object_key = get_tester().to_object_key(object);
    if (object_key != test_object_key) {
      std::cout << object << " should be "
	        << test_object_key << " is "
	        << get_tester().to_object_key(object)
	        << std::endl;
    }
    ASSERT_EQ(object_key, test_object_key);
  }
}


// Check to_raw_key against current format to detect accidental changes in encoding
TEST_F(SnapMapperTest, CheckRawKeyFormat) {
  init(1);
  // <object, snapid, test_raw_key>
  std::vector<std::tuple<hobject_t, snapid_t, std::string>> object_to_raw_key({
      {hobject_t{"test_object", "", 20, 0x01234567, 20, ""}, 25,
	  "SNA_20_0000000000000019_.1_0000000000000014.76543210.14.test%uobject.."},
      {hobject_t{"test._ob.ject", "k.ey", 20, 0x01234567, 20, ""}, 25,
	  "SNA_20_0000000000000019_.1_0000000000000014.76543210.14.test%e%uob%eject.k%eey."},
      {hobject_t{"test_object", "", 20, 0x01234567, 20, "namespace"}, 25,
	  "SNA_20_0000000000000019_.1_0000000000000014.76543210.14.test%uobject..namespace"},
      {hobject_t{
	  "test_object", "", std::numeric_limits<snapid_t>::max() - 20, 0x01234567,
	    std::numeric_limits<int64_t>::max() - 20, "namespace"}, std::numeric_limits<snapid_t>::max() - 20,
	  "SNA_9223372036854775787_FFFFFFFFFFFFFFEC_.1_7FFFFFFFFFFFFFEB.76543210.ffffffffffffffec.test%uobject..namespace"}
    });

  for (auto &[object, snap, test_raw_key]: object_to_raw_key) {
    auto raw_key = get_tester().to_raw_key(std::make_pair(snap, object));
    if (raw_key != test_raw_key) {
      std::cout << object << " " << snap << " should be "
	        << test_raw_key << " is "
	        << get_tester().to_raw_key(std::make_pair(snap, object))
	        << std::endl;
    }
    ASSERT_EQ(raw_key, test_raw_key);
  }
}

// Check make_purged_snap_key against current format to detect accidental changes
// in encoding
TEST_F(SnapMapperTest, CheckMakePurgedSnapKeyFormat) {
  init(1);
  // <pool, snap, test_key>
  std::vector<std::tuple<int64_t, snapid_t, std::string>> purged_snap_to_key({
      {20, 30, "PSN__20_000000000000001e"},
      {std::numeric_limits<int64_t>::max() - 20,
       std::numeric_limits<snapid_t>::max() - 20,
       "PSN__9223372036854775787_ffffffffffffffec"}
  });

  for (auto &[pool, snap, test_key]: purged_snap_to_key) {
    auto raw_purged_snap_key = get_tester().make_purged_snap_key(pool, snap);
    if (raw_purged_snap_key != test_key) {
      std::cout << "<" << pool << ", " << snap << "> should be " << test_key
                << " is " << raw_purged_snap_key << std::endl;
    }
    // retesting (mostly for test numbers accounting)
    ASSERT_EQ(raw_purged_snap_key, test_key);
  }
}

TEST_F(SnapMapperTest, LegacyKeyConvertion) {
    init(1);
    auto obj = get_tester().random_hobject();
    snapid_t snapid = random() % 10;
    auto snap_obj = make_pair(snapid, obj);
    auto raw = get_tester().to_raw(snap_obj);
    std::string old_key = get_tester().to_legacy_raw_key(snap_obj);
    std::string converted_key =
      SnapMapper::convert_legacy_key(old_key, raw.second);
    std::string new_key = get_tester().to_raw_key(snap_obj);
    if (converted_key != new_key) {
      std::cout << "Converted: " << old_key << "\nTo:        " << converted_key
	        << "\nNew key:   " << new_key << std::endl;
    }
    ASSERT_EQ(converted_key, new_key);
}

/**
 * 'DirectMapper' provides simple, controlled, interface to the underlying
 * SnapMapper.
 */
class DirectMapper {
public:
  std::unique_ptr<PausyAsyncMap> driver{make_unique<PausyAsyncMap>()};
  std::unique_ptr<SnapMapper> mapper;
  uint32_t mask;
  uint32_t bits;
  ceph::mutex lock = ceph::make_mutex("lock");

  DirectMapper(
    uint32_t mask,
    uint32_t bits)
    : mapper(new SnapMapper(g_ceph_context, driver.get(), mask, bits, 0, shard_id_t(1))),
             mask(mask), bits(bits) {}

  hobject_t random_hobject() {
    return hobject_t(
      random_string(1+(rand() % 16)),
      random_string(1+(rand() % 16)),
      snapid_t(rand() % 1000),
      (rand() & ((~0)<<bits)) | (mask & ~((~0)<<bits)),
      0, random_string(rand() % 16));
  }

  void create_object(const hobject_t& obj, const set<snapid_t> &snaps) {
    std::lock_guard l{lock};
      PausyAsyncMap::Transaction t;
      mapper->add_oid(obj, snaps, &t);
      driver->submit(&t);
  }

  std::pair<std::string, ceph::buffer::list> to_raw(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_raw(to_map);
  }

  std::string to_legacy_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_legacy_raw_key(to_map);
  }

  std::string to_raw_key(
    const std::pair<snapid_t, hobject_t> &to_map) {
    return mapper->to_raw_key(to_map);
  }

  void shorten_mapping_key(snapid_t snap, const hobject_t &clone)
  {
    // calculate the relevant key
    std::string k = mapper->to_raw_key(snap, clone);

    // find the value for this key
    map<string, bufferlist> kvmap;
    auto r = mapper->backend.get_keys(set{k}, &kvmap);
    ASSERT_GE(r, 0);

    // replace the key with its shortened version
    PausyAsyncMap::Transaction t;
    mapper->backend.remove_keys(set{k}, &t);
    auto short_k = k.substr(0, 10);
    mapper->backend.set_keys(map<string, bufferlist>{{short_k, kvmap[k]}}, &t);
    driver->submit(&t);
    driver->flush();
  }
};

class DirectMapperTest : public ::testing::Test {
 public:
  // ctor & initialization
  DirectMapperTest() = default;
  ~DirectMapperTest() = default;
  void SetUp() override;
  void TearDown() override;

 protected:
  std::unique_ptr<DirectMapper> direct;
};

void DirectMapperTest::SetUp()
{
  direct = std::make_unique<DirectMapper>(0, 0);
}

void DirectMapperTest::TearDown()
{
  direct->driver->stop();
  direct->mapper.reset();
  direct->driver.reset();
}


TEST_F(DirectMapperTest, BasciObject)
{
  auto obj = direct->random_hobject();
  set<snapid_t> snaps{100, 200};
  direct->create_object(obj, snaps);

  // verify that the OBJ_ & SNA_ entries are there
  auto osn1 = direct->mapper->get_snaps(obj);
  ASSERT_EQ(snaps, osn1);
  auto vsn1 = direct->mapper->get_snaps_check_consistency(obj);
  ASSERT_EQ(snaps, vsn1);
}

TEST_F(DirectMapperTest, CorruptedSnaRecord)
{
  object_t base_name{"obj"};
  std::string key{"key"};

  hobject_t head{base_name, key, CEPH_NOSNAP, 0x17, 0, ""};
  hobject_t cln1{base_name, key, 10, 0x17, 0, ""};
  hobject_t cln2{base_name, key, 20, 0x17, 0, ""}; // the oldest version
  set<snapid_t> head_snaps{400, 500};
  set<snapid_t> cln1_snaps{300};
  set<snapid_t> cln2_snaps{100, 200};

  PausyAsyncMap::Transaction t;
  direct->mapper->add_oid(head, head_snaps, &t);
  direct->mapper->add_oid(cln1, cln1_snaps, &t);
  direct->mapper->add_oid(cln2, cln2_snaps, &t);
  direct->driver->submit(&t);
  direct->driver->flush();

  // verify that the OBJ_ & SNA_ entries are there
  {
    auto osn1 = direct->mapper->get_snaps(cln1);
    EXPECT_EQ(cln1_snaps, osn1);
    auto osn2 = direct->mapper->get_snaps(cln2);
    EXPECT_EQ(cln2_snaps, osn2);
    auto osnh = direct->mapper->get_snaps(head);
    EXPECT_EQ(head_snaps, osnh);
  }
  {
    auto vsn1 = direct->mapper->get_snaps_check_consistency(cln1);
    EXPECT_EQ(cln1_snaps, vsn1);
    auto vsn2 = direct->mapper->get_snaps_check_consistency(cln2);
    EXPECT_EQ(cln2_snaps, vsn2);
    auto vsnh = direct->mapper->get_snaps_check_consistency(head);
    EXPECT_EQ(head_snaps, vsnh);
  }

  // corrupt the SNA_ entry for cln1
  direct->shorten_mapping_key(300, cln1);
  {
    auto vsnh = direct->mapper->get_snaps_check_consistency(head);
    EXPECT_EQ(head_snaps, vsnh);
    auto vsn1 = direct->mapper->get_snaps(cln1);
    EXPECT_EQ(cln1_snaps, vsn1);
    auto osn1 = direct->mapper->get_snaps_check_consistency(cln1);
    EXPECT_NE(cln1_snaps, osn1);
    auto vsn2 = direct->mapper->get_snaps_check_consistency(cln2);
    EXPECT_EQ(cln2_snaps, vsn2);
  }
}

///\todo test the case of a corrupted OBJ_ entry
