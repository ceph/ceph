// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "include/memory.h"
#include <map>
#include <set>
#include <boost/scoped_ptr.hpp>
#include <sys/types.h>
#include <cstdlib>

#include "include/buffer.h"
#include "common/map_cacher.hpp"
#include "osd/SnapMapper.h"
#include "test/unit.h"

#include "gtest/gtest.h"

using namespace std;

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
  typedef ceph::shared_ptr<_Op> Op;
  struct Remove : public _Op {
    set<string> to_remove;
    explicit Remove(const set<string> &to_remove) : to_remove(to_remove) {}
    void operate(map<string, bufferlist> *store) {
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
    void operate(map<string, bufferlist> *store) {
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
    void operate(map<string, bufferlist> *store) {
      context->complete(0);
    }
  };
public:
  class Transaction : public MapCacher::Transaction<string, bufferlist> {
    friend class PausyAsyncMap;
    list<Op> ops;
    list<Op> callbacks;
  public:
    void set_keys(const map<string, bufferlist> &i) {
      ops.push_back(Op(new Insert(i)));
    }
    void remove_keys(const set<string> &r) {
      ops.push_back(Op(new Remove(r)));
    }
    void add_callback(Context *c) {
      callbacks.push_back(Op(new Callback(c)));
    }
  };
private:

  Mutex lock;
  map<string, bufferlist> store;

  class Doer : public Thread {
    static const size_t MAX_SIZE = 100;
    PausyAsyncMap *parent;
    Mutex lock;
    Cond cond;
    int stopping;
    bool paused;
    list<Op> queue;
  public:
    explicit Doer(PausyAsyncMap *parent) :
      parent(parent), lock("Doer lock"), stopping(0), paused(false) {}
    virtual void *entry() {
      while (1) {
	list<Op> ops;
	{
	  Mutex::Locker l(lock);
	  while (!stopping && (queue.empty() || paused))
	    cond.Wait(lock);
	  if (stopping && queue.empty()) {
	    stopping = 2;
	    cond.Signal();
	    return 0;
	  }
	  assert(!queue.empty());
	  assert(!paused);
	  ops.swap(queue);
	  cond.Signal();
	}
	assert(!ops.empty());

	for (list<Op>::iterator i = ops.begin();
	     i != ops.end();
	     ops.erase(i++)) {
	  if (!(rand()%3))
	    usleep(1+(rand() % 5000));
	  Mutex::Locker l(parent->lock);
	  (*i)->operate(&(parent->store));
	}
      }
    }

    void pause() {
      Mutex::Locker l(lock);
      paused = true;
      cond.Signal();
    }

    void resume() {
      Mutex::Locker l(lock);
      paused = false;
      cond.Signal();
    }

    void submit(list<Op> &in) {
      Mutex::Locker l(lock);
      while (queue.size() >= MAX_SIZE)
	cond.Wait(lock);
      queue.splice(queue.end(), in, in.begin(), in.end());
      cond.Signal();
    }

    void stop() {
      Mutex::Locker l(lock);
      stopping = 1;
      cond.Signal();
      while (stopping != 2)
	cond.Wait(lock);
      cond.Signal();
    }
  } doer;

public:
  PausyAsyncMap() : lock("PausyAsyncMap"), doer(this) {
    doer.create("doer");
  }
  ~PausyAsyncMap() {
    doer.join();
  }
  int get_keys(
    const set<string> &keys,
    map<string, bufferlist> *out) {
    Mutex::Locker l(lock);
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
    pair<string, bufferlist> *next) {
    Mutex::Locker l(lock);
    map<string, bufferlist>::iterator j = store.upper_bound(key);
    if (j != store.end()) {
      if (next)
	*next = *j;
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
    Mutex lock("flush lock");
    Cond cond;
    bool done = false;

    class OnFinish : public Context {
      Mutex *lock;
      Cond *cond;
      bool *done;
    public:
      OnFinish(Mutex *lock, Cond *cond, bool *done)
	: lock(lock), cond(cond), done(done) {}
      void finish(int) {
	Mutex::Locker l(*lock);
	*done = true;
	cond->Signal();
      }
    };
    Transaction t;
    t.add_callback(new OnFinish(&lock, &cond, &done));
    submit(&t);
    {
      Mutex::Locker l(lock);
      while (!done)
	cond.Wait(lock);
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
  virtual void SetUp() {
    driver.reset(new PausyAsyncMap());
    cache.reset(new MapCacher::MapCacher<string, bufferlist>(driver.get()));
    names.clear();
    truth.clear();
    size_t names_size(random_num() + 10);
    for (size_t i = 0; i < names_size; ++i) {
      names.insert(random_string(1 + (random_size() % 10)));
    }
  }
  virtual void TearDown() {
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
  ::encode(blah, bl);
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
  map<snapid_t, set<hobject_t, hobject_t::BitwiseComparator> > snap_to_hobject;
  map<hobject_t, set<snapid_t>, hobject_t::BitwiseComparator> hobject_to_snap;
  snapid_t next;
  uint32_t mask;
  uint32_t bits;
  Mutex lock;
public:

  MapperVerifier(
    PausyAsyncMap *driver,
    uint32_t mask,
    uint32_t bits)
    : driver(driver),
      mapper(new SnapMapper(driver, mask, bits, 0, shard_id_t(1))),
             mask(mask), bits(bits),
      lock("lock") {}

  hobject_t random_hobject() {
    return hobject_t(
      random_string(1+(rand() % 16)),
      random_string(1+(rand() % 16)),
      snapid_t(rand() % 1000),
      (rand() & ((~0)<<bits)) | (mask & ~((~0)<<bits)),
      0, random_string(rand() % 16));
  }

  void choose_random_snaps(int num, set<snapid_t> *snaps) {
    assert(snaps);
    assert(!snap_to_hobject.empty());
    for (int i = 0; i < num || snaps->empty(); ++i) {
      snaps->insert(rand_choose(snap_to_hobject)->first);
    }
  }

  void create_snap() {
    snap_to_hobject[next];
    ++next;
  }

  void create_object() {
    Mutex::Locker l(lock);
    if (snap_to_hobject.empty())
      return;
    hobject_t obj;
    do {
      obj = random_hobject();
    } while (hobject_to_snap.count(obj));

    set<snapid_t> &snaps = hobject_to_snap[obj];
    choose_random_snaps(1 + (rand() % 20), &snaps);
    for (set<snapid_t>::iterator i = snaps.begin();
	 i != snaps.end();
	 ++i) {
      map<snapid_t, set<hobject_t, hobject_t::BitwiseComparator> >::iterator j = snap_to_hobject.find(*i);
      assert(j != snap_to_hobject.end());
      j->second.insert(obj);
    }
    {
      PausyAsyncMap::Transaction t;
      mapper->add_oid(obj, snaps, &t);
      driver->submit(&t);
    }
  }

  void trim_snap() {
    Mutex::Locker l(lock);
    if (snap_to_hobject.empty())
      return;
    map<snapid_t, set<hobject_t, hobject_t::BitwiseComparator> >::iterator snap =
      rand_choose(snap_to_hobject);
    set<hobject_t, hobject_t::BitwiseComparator> hobjects = snap->second;

    hobject_t hoid;
    while (mapper->get_next_object_to_trim(snap->first, &hoid) == 0) {
      assert(!hoid.is_max());
      assert(hobjects.count(hoid));
      hobjects.erase(hoid);

      map<hobject_t, set<snapid_t>, hobject_t::BitwiseComparator>::iterator j =
	hobject_to_snap.find(hoid);
      assert(j->second.count(snap->first));
      set<snapid_t> old_snaps(j->second);
      j->second.erase(snap->first);

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
    assert(hobjects.empty());

    snap_to_hobject.erase(snap);
  }

  void remove_oid() {
    Mutex::Locker l(lock);
    if (hobject_to_snap.empty())
      return;
    map<hobject_t, set<snapid_t>, hobject_t::BitwiseComparator>::iterator obj =
      rand_choose(hobject_to_snap);
    for (set<snapid_t>::iterator i = obj->second.begin();
	 i != obj->second.end();
	 ++i) {
      map<snapid_t, set<hobject_t, hobject_t::BitwiseComparator> >::iterator j =
	snap_to_hobject.find(*i);
      assert(j->second.count(obj->first));
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
    Mutex::Locker l(lock);
    if (hobject_to_snap.empty())
      return;
    map<hobject_t, set<snapid_t>, hobject_t::BitwiseComparator>::iterator obj =
      rand_choose(hobject_to_snap);
    set<snapid_t> snaps;
    int r = mapper->get_snaps(obj->first, &snaps);
    assert(r == 0);
    ASSERT_EQ(snaps, obj->second);
  }
};

class SnapMapperTest : public ::testing::Test {
protected:
  boost::scoped_ptr< PausyAsyncMap > driver;
  map<pg_t, ceph::shared_ptr<MapperVerifier> > mappers;
  uint32_t pgnum;

  virtual void SetUp() {
    driver.reset(new PausyAsyncMap());
    pgnum = 0;
  }

  virtual void TearDown() {
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
      pg_t pgid(i, 0, -1);
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
